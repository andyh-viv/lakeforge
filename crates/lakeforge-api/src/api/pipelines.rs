//! Delta Live Tables–style pipelines (`/api/2.0/pipelines`).
//!
//! A pipeline is a set of libraries (notebooks / files). At update time each
//! library's SQL is parsed for `CREATE [OR REFRESH] [STREAMING] [LIVE|MATERIALIZED]
//! [TABLE|VIEW] name AS <query>` definitions (plus `@dlt.table` Python cells,
//! handled by the kernel's `dlt` shim which emits the same declarations).
//! Definitions form a DAG by `LIVE.<name>` / bare-name references; the update
//! executes them in topological order as `CREATE OR REPLACE TABLE <target>.<name>
//! AS <query>` statements on the pipeline's cluster, recording per-flow
//! progress events.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::{empty, Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};

pub const KIND_PIPELINE: &str = "pipeline";
pub const KIND_UPDATE: &str = "pipeline_update";
pub const KIND_EVENT: &str = "pipeline_event";

#[derive(Debug, Clone)]
pub struct Dataset {
    pub name: String,
    pub kind: String, // TABLE | VIEW | STREAMING_TABLE | MATERIALIZED_VIEW
    pub query: String,
    pub deps: Vec<String>,
    pub comment: Option<String>,
    pub source: String,
}

/// Parse DLT SQL declarations out of a script.
pub fn parse_datasets(sql: &str, source: &str) -> Vec<Dataset> {
    let re = regex::Regex::new(r"(?is)CREATE\s+(?:OR\s+REFRESH\s+|OR\s+REPLACE\s+)?(?:TEMPORARY\s+)?(STREAMING\s+(?:LIVE\s+)?TABLE|LIVE\s+TABLE|MATERIALIZED\s+VIEW|LIVE\s+VIEW|STREAMING\s+TABLE|TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][\w\.]*|`[^`]+`)\s*(\([^)]*\))?\s*(?:COMMENT\s+'([^']*)')?\s*(?:TBLPROPERTIES\s*\([^)]*\))?\s*(?:PARTITIONED\s+BY\s*\([^)]*\))?\s*AS\s+").unwrap();
    let mut out = vec![];
    for stmt in super::sql::split_statements(sql) {
        let Some(m) = re.captures(&stmt) else { continue };
        let kw = m[1].to_ascii_uppercase().split_whitespace().collect::<Vec<_>>().join(" ");
        let kind = match kw.as_str() {
            "STREAMING TABLE" | "STREAMING LIVE TABLE" => "STREAMING_TABLE",
            "MATERIALIZED VIEW" => "MATERIALIZED_VIEW",
            "LIVE VIEW" | "VIEW" => "VIEW",
            _ => "TABLE",
        };
        let name = m[2].trim_matches('`').rsplit('.').next().unwrap_or("").to_string();
        let query = stmt[m.get(0).unwrap().end()..].trim().trim_end_matches(';').to_string();
        let dep_re = regex::Regex::new(r"(?i)\b(?:LIVE|STREAM)\s*\(?\s*(?:LIVE\.)?([A-Za-z_][\w]*)\s*\)?|\bLIVE\.([A-Za-z_][\w]*)").unwrap();
        let mut deps: Vec<String> = dep_re.captures_iter(&query).filter_map(|c| c.get(1).or(c.get(2)).map(|x| x.as_str().to_string())).collect();
        deps.sort();
        deps.dedup();
        out.push(Dataset { name, kind: kind.into(), query, deps, comment: m.get(4).map(|c| c.as_str().to_string()), source: source.to_string() });
    }
    out
}

/// Python cells declare datasets via the `dlt` shim which prints
/// `__LAKEFORGE_DLT__ {json}` lines; we recover them from kernel stdout.
fn parse_python_declarations(stdout: &str, source: &str) -> Vec<Dataset> {
    stdout
        .lines()
        .filter_map(|l| l.strip_prefix("__LAKEFORGE_DLT__ "))
        .filter_map(|j| serde_json::from_str::<Value>(j).ok())
        .map(|v| Dataset { name: v["name"].as_str().unwrap_or("").into(), kind: v["kind"].as_str().unwrap_or("TABLE").into(), query: v["query"].as_str().unwrap_or("").into(), deps: v["deps"].as_array().map(|a| a.iter().filter_map(|d| d.as_str().map(|s| s.to_string())).collect()).unwrap_or_default(), comment: v["comment"].as_str().map(|s| s.to_string()), source: source.into() })
        .collect()
}

fn topo(datasets: &[Dataset]) -> ApiResult<Vec<usize>> {
    let idx: HashMap<&str, usize> = datasets.iter().enumerate().map(|(i, d)| (d.name.as_str(), i)).collect();
    let mut indeg = vec![0usize; datasets.len()];
    let mut adj: Vec<Vec<usize>> = vec![vec![]; datasets.len()];
    for (i, d) in datasets.iter().enumerate() {
        for dep in &d.deps {
            if let Some(&j) = idx.get(dep.as_str()) {
                adj[j].push(i);
                indeg[i] += 1;
            }
        }
    }
    let mut ready: Vec<usize> = (0..datasets.len()).filter(|i| indeg[*i] == 0).collect();
    ready.reverse();
    let mut order = vec![];
    while let Some(n) = ready.pop() {
        order.push(n);
        for &m in &adj[n] {
            indeg[m] -= 1;
            if indeg[m] == 0 {
                ready.push(m);
            }
        }
    }
    if order.len() != datasets.len() {
        return Err(ApiError::invalid("Pipeline dataset graph contains a cycle"));
    }
    Ok(order)
}

fn rewrite_refs(query: &str, target: &str, names: &HashSet<String>) -> String {
    let mut q = query.to_string();
    let re = regex::Regex::new(r"(?i)\bSTREAM\s*\(\s*(?:LIVE\.)?([A-Za-z_]\w*)\s*\)").unwrap();
    q = re.replace_all(&q, |c: &regex::Captures| format!("{target}.{}", &c[1])).to_string();
    let re2 = regex::Regex::new(r"(?i)\bLIVE\.([A-Za-z_]\w*)").unwrap();
    q = re2.replace_all(&q, |c: &regex::Captures| format!("{target}.{}", &c[1])).to_string();
    // Bare references to sibling datasets in FROM/JOIN.
    let re3 = regex::Regex::new(r"(?i)\b(FROM|JOIN)\s+([A-Za-z_]\w*)([\s,)]|$)").unwrap();
    q = re3.replace_all(&q, |c: &regex::Captures| if names.contains(&c[2].to_lowercase()) { format!("{} {target}.{}{}", &c[1], &c[2], &c[3]) } else { c[0].to_string() }).to_string();
    q
}

impl AppState {
    pub async fn get_pipeline(&self, id: &str) -> ApiResult<Doc<Value>> {
        self.store.require(KIND_PIPELINE, id, "Pipeline").await
    }

    async fn pipeline_event(&self, pipeline_id: &str, update_id: &str, level: &str, event_type: &str, message: impl Into<String>, details: Value) {
        let id = uuid::Uuid::new_v4().to_string();
        let ev = json!({ "id": id, "sequence": { "data_plane_id": { "seq_no": now_ms() } }, "origin": { "pipeline_id": pipeline_id, "update_id": update_id, "cloud": self.config.cloud }, "timestamp": chrono::Utc::now().to_rfc3339(), "message": message.into(), "level": level, "event_type": event_type, "details": details, "maturity_level": "STABLE" });
        let _ = self.store.insert(KIND_EVENT, self.ws(), &id, Some(pipeline_id), None, &ev).await;
    }

    async fn set_update_state(&self, update_id: &str, state: &str, cause: Option<&str>) -> ApiResult<Value> {
        let doc = self.store.update::<Value, _>(KIND_UPDATE, update_id, "Update", |u| {
            u["state"] = json!(state);
            if let Some(c) = cause {
                u["cause"] = json!(c);
            }
            if matches!(state, "COMPLETED" | "FAILED" | "CANCELED") {
                u["end_time"] = json!(now_ms());
            }
            Ok(())
        }).await?;
        if let Some(pid) = doc.data["pipeline_id"].as_str() {
            let pipeline_state = match state {
                "COMPLETED" | "FAILED" | "CANCELED" => "IDLE",
                _ => "RUNNING",
            };
            let _ = self.store.update::<Value, _>(KIND_PIPELINE, pid, "Pipeline", |p| {
                p["state"] = json!(pipeline_state);
                p["latest_updates"] = json!([{ "update_id": update_id, "state": state, "creation_time": doc.data["creation_time"] }]);
                if state == "COMPLETED" || state == "FAILED" {
                    p["last_modified"] = json!(now_ms());
                }
                Ok(())
            }).await;
        }
        Ok(doc.data)
    }

    pub async fn start_pipeline_update(self: &Arc<Self>, p: &Principal, pipeline_id: &str, full_refresh: bool, cluster_id: Option<&str>) -> ApiResult<String> {
        let pipe = self.get_pipeline(pipeline_id).await?;
        if pipe.data["state"] == "RUNNING" {
            let running: Vec<Doc<Value>> = self.store.list(KIND_UPDATE, self.ws(), Filter { parent_id: Some(pipeline_id), newest_first: true, limit: Some(1), ..Default::default() }).await?;
            if let Some(u) = running.first() {
                if matches!(u.data["state"].as_str(), Some("QUEUED" | "CREATED" | "WAITING_FOR_RESOURCES" | "INITIALIZING" | "SETTING_UP_TABLES" | "RUNNING")) {
                    return Err(ApiError::InvalidState(format!("Pipeline {pipeline_id} already has an active update {}", u.id)));
                }
            }
        }
        let update_id = uuid::Uuid::new_v4().to_string();
        let update = json!({ "update_id": update_id, "pipeline_id": pipeline_id, "state": "QUEUED", "cause": "API_CALL", "creation_time": now_ms(), "full_refresh": full_refresh, "config": pipe.data, "creator_user_name": p.user_name, "cluster_id": cluster_id });
        self.store.insert(KIND_UPDATE, self.ws(), &update_id, Some(pipeline_id), None, &update).await?;
        self.set_update_state(&update_id, "QUEUED", None).await?;
        self.pipeline_event(pipeline_id, &update_id, "INFO", "create_update", "Update created", json!({ "create_update": { "cause": "API_CALL", "full_refresh": full_refresh } })).await;
        let st = Arc::clone(self);
        let p = p.clone();
        let pid = pipeline_id.to_string();
        let uid = update_id.clone();
        let cid = cluster_id.map(|s| s.to_string());
        tokio::spawn(async move {
            if let Err(e) = st.run_pipeline_update(&p, &pid, &uid, full_refresh, cid.as_deref()).await {
                let _ = st.set_update_state(&uid, "FAILED", Some(&e.to_string())).await;
                st.pipeline_event(&pid, &uid, "ERROR", "update_progress", e.to_string(), json!({ "update_progress": { "state": "FAILED" }, "error": { "exceptions": [{ "message": e.to_string() }] } })).await;
            }
        });
        Ok(update_id)
    }

    pub async fn wait_pipeline_update(&self, pipeline_id: &str, update_id: &str, timeout: Option<Duration>) -> ApiResult<Value> {
        let deadline = timeout.map(|t| tokio::time::Instant::now() + t);
        loop {
            let u = self.store.require::<Value>(KIND_UPDATE, update_id, "Update").await?;
            if u.data["pipeline_id"] != pipeline_id {
                return Err(ApiError::NotFound(format!("Update {update_id} not found for pipeline {pipeline_id}")));
            }
            if matches!(u.data["state"].as_str(), Some("COMPLETED" | "FAILED" | "CANCELED")) {
                return Ok(u.data);
            }
            if deadline.map(|d| tokio::time::Instant::now() >= d).unwrap_or(false) {
                let _ = self.set_update_state(update_id, "CANCELED", Some("timed out")).await;
                return Err(ApiError::InvalidState(format!("Pipeline update {update_id} timed out")));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn run_pipeline_update(self: &Arc<Self>, p: &Principal, pipeline_id: &str, update_id: &str, full_refresh: bool, cluster_id: Option<&str>) -> ApiResult<()> {
        let pipe = self.get_pipeline(pipeline_id).await?.data;
        let target = {
            let cat = pipe["catalog"].as_str().unwrap_or("main");
            let schema = pipe["target"].as_str().or(pipe["schema"].as_str()).unwrap_or("default");
            format!("{cat}.{schema}")
        };
        self.set_update_state(update_id, "WAITING_FOR_RESOURCES", None).await?;
        self.pipeline_event(pipeline_id, update_id, "INFO", "update_progress", "Waiting for resources", json!({ "update_progress": { "state": "WAITING_FOR_RESOURCES" } })).await;

        // Compute: explicit cluster, pipeline's clusters[0] existing id, or a job cluster.
        let mut created: Option<String> = None;
        let cluster_id = match cluster_id {
            Some(c) => c.to_string(),
            None => match pipe["clusters"].as_array().and_then(|a| a.first()).and_then(|c| c["existing_cluster_id"].as_str()) {
                Some(c) => c.to_string(),
                None => {
                    let running: Vec<Doc<super::clusters::Cluster>> = self.store.list(super::clusters::KIND, self.ws(), Filter::default()).await?;
                    if let Some(c) = running.iter().find(|c| c.data.state == super::clusters::ClusterState::Running) {
                        c.id.clone()
                    } else {
                        let mut spec = Map::new();
                        spec.insert("cluster_name".into(), json!(format!("dlt-{}", pipe["name"].as_str().unwrap_or(pipeline_id))));
                        spec.insert("num_workers".into(), json!(pipe["clusters"][0]["num_workers"].as_u64().unwrap_or(1)));
                        spec.insert("node_type_id".into(), json!(pipe["clusters"][0]["node_type_id"].as_str().unwrap_or("lf.small")));
                        spec.insert("cluster_source".into(), json!("PIPELINE"));
                        spec.insert("autotermination_minutes".into(), json!(10));
                        let id = self.create_cluster_from_json(p, spec, false).await?;
                        created = Some(id.clone());
                        id
                    }
                }
            },
        };
        let result = self.run_pipeline_on(p, &pipe, pipeline_id, update_id, &target, &cluster_id, full_refresh).await;
        if let Some(c) = created {
            let _ = self.terminate_cluster(&c, "PIPELINE_FINISHED").await;
        }
        result
    }

    async fn run_pipeline_on(self: &Arc<Self>, p: &Principal, pipe: &Value, pipeline_id: &str, update_id: &str, target: &str, cluster_id: &str, full_refresh: bool) -> ApiResult<()> {
        self.cluster_driver(cluster_id, true).await?;
        self.set_update_state(update_id, "INITIALIZING", None).await?;
        self.pipeline_event(pipeline_id, update_id, "INFO", "update_progress", "Initializing", json!({ "update_progress": { "state": "INITIALIZING" } })).await;

        // Collect datasets from libraries.
        let mut datasets: Vec<Dataset> = vec![];
        for lib in pipe["libraries"].as_array().cloned().unwrap_or_default() {
            let path = lib["notebook"]["path"].as_str().or(lib["file"]["path"].as_str()).or(lib["glob"]["include"].as_str());
            let Some(path) = path else { continue };
            let objs = if path.contains('*') { self.ws_glob(path).await? } else { vec![path.to_string()] };
            for path in objs {
                let obj = self.ws_require(&path).await?;
                if let Some(nb) = &obj.notebook {
                    let mut sql_cells = String::new();
                    let mut py_cells = String::new();
                    for c in &nb.cells {
                        let lang = if c.language.is_empty() { nb.default_language.clone() } else { c.language.clone() };
                        if lang.eq_ignore_ascii_case("markdown") {
                            continue;
                        }
                        let src = c.source.trim_start().trim_start_matches("%sql").trim_start_matches("%python");
                        if lang.eq_ignore_ascii_case("sql") || c.source.trim_start().starts_with("%sql") {
                            sql_cells.push_str(src);
                            sql_cells.push_str(";\n");
                        } else {
                            py_cells.push_str(src);
                            py_cells.push('\n');
                        }
                    }
                    datasets.extend(parse_datasets(&sql_cells, &path));
                    if py_cells.contains("dlt") {
                        let code = format!("import lakeforge_dlt as dlt\n{py_cells}\ndlt._emit()\n");
                        let ctx = self.create_context(p, cluster_id, "python", Some(&path), HashMap::new()).await?;
                        let rec = self.run_command(&ctx.id, "python", &code, Some(Duration::from_secs(300))).await;
                        self.destroy_context(&ctx.id).await;
                        let rec = rec?;
                        if rec.status == super::commands::CommandStatus::Error {
                            return Err(ApiError::invalid(format!("Python DLT declarations failed in {path}: {}", rec.error_text())));
                        }
                        datasets.extend(parse_python_declarations(&rec.stdout_text(), &path));
                    }
                } else {
                    let bytes = self.ws_read_file(&obj).await?;
                    let text = String::from_utf8_lossy(&bytes);
                    if path.ends_with(".sql") {
                        datasets.extend(parse_datasets(&text, &path));
                    }
                }
            }
        }
        if datasets.is_empty() {
            return Err(ApiError::invalid("Pipeline defines no datasets (expected CREATE [STREAMING] [LIVE] TABLE/VIEW or @dlt.table)"));
        }
        let order = topo(&datasets)?;
        let names: HashSet<String> = datasets.iter().map(|d| d.name.to_lowercase()).collect();
        let graph: Vec<Value> = datasets.iter().map(|d| json!({ "name": d.name, "type": d.kind, "depends_on": d.deps, "source": d.source, "comment": d.comment })).collect();
        self.store.update::<Value, _>(KIND_UPDATE, update_id, "Update", |u| {
            u["graph"] = json!(graph);
            Ok(())
        }).await?;
        self.pipeline_event(pipeline_id, update_id, "INFO", "graph_created", format!("Resolved {} datasets", datasets.len()), json!({ "graph": graph })).await;

        self.set_update_state(update_id, "SETTING_UP_TABLES", None).await?;
        let (cat, schema) = target.split_once('.').unwrap_or(("main", target));
        self.execute_sql(cluster_id, None, p, &format!("CREATE SCHEMA IF NOT EXISTS {cat}.{schema}"), HashMap::new(), 1).await.ok();

        self.set_update_state(update_id, "RUNNING", None).await?;
        let mut flows: BTreeMap<String, Value> = BTreeMap::new();
        for i in order {
            let d = &datasets[i];
            let started = now_ms();
            self.pipeline_event(pipeline_id, update_id, "INFO", "flow_progress", format!("Flow '{}' is STARTING", d.name), json!({ "flow_progress": { "status": "STARTING" }, "flow_name": d.name })).await;
            let query = rewrite_refs(&d.query, target, &names);
            let full_name = format!("{target}.{}", d.name);
            let sql = match d.kind.as_str() {
                "VIEW" => format!("CREATE OR REPLACE VIEW {full_name} AS {query}"),
                _ => {
                    if full_refresh || d.kind != "STREAMING_TABLE" {
                        format!("CREATE OR REPLACE TABLE {full_name} AS {query}")
                    } else {
                        // Streaming tables append; create on first run.
                        let exists = self.execute_sql(cluster_id, None, p, &format!("SELECT 1 FROM {full_name} LIMIT 0"), HashMap::new(), 1).await.is_ok();
                        if exists {
                            format!("INSERT INTO {full_name} {query}")
                        } else {
                            format!("CREATE TABLE {full_name} AS {query}")
                        }
                    }
                }
            };
            self.pipeline_event(pipeline_id, update_id, "INFO", "flow_progress", format!("Flow '{}' is RUNNING", d.name), json!({ "flow_progress": { "status": "RUNNING" }, "flow_name": d.name, "sql": sql })).await;
            match self.execute_sql(cluster_id, None, p, &sql, HashMap::new(), 10).await {
                Ok(res) => {
                    let rows = if d.kind == "VIEW" { Value::Null } else { self.execute_sql(cluster_id, None, p, &format!("SELECT count(*) FROM {full_name}"), HashMap::new(), 1).await.ok().and_then(|r| r.rows.first().and_then(|r| r.first().cloned().flatten())).and_then(|s| s.parse::<i64>().ok()).map(|n| json!(n)).unwrap_or(Value::Null) };
                    let f = json!({ "status": "COMPLETED", "started_ms": started, "finished_ms": now_ms(), "duration_ms": res.elapsed_ms, "num_output_rows": rows, "dataset": d.name, "type": d.kind });
                    self.pipeline_event(pipeline_id, update_id, "INFO", "flow_progress", format!("Flow '{}' has COMPLETED", d.name), json!({ "flow_progress": { "status": "COMPLETED", "metrics": { "num_output_rows": rows } }, "flow_name": d.name })).await;
                    flows.insert(d.name.clone(), f);
                }
                Err(e) => {
                    flows.insert(d.name.clone(), json!({ "status": "FAILED", "error": e.to_string(), "dataset": d.name }));
                    self.pipeline_event(pipeline_id, update_id, "ERROR", "flow_progress", format!("Flow '{}' has FAILED: {e}", d.name), json!({ "flow_progress": { "status": "FAILED" }, "flow_name": d.name, "error": { "exceptions": [{ "message": e.to_string() }] } })).await;
                    self.store.update::<Value, _>(KIND_UPDATE, update_id, "Update", |u| {
                        u["flows"] = json!(flows);
                        Ok(())
                    }).await?;
                    return Err(ApiError::invalid(format!("Dataset {} failed: {e}", d.name)));
                }
            }
            self.store.update::<Value, _>(KIND_UPDATE, update_id, "Update", |u| {
                u["flows"] = json!(flows);
                Ok(())
            }).await?;
        }
        self.set_update_state(update_id, "COMPLETED", None).await?;
        self.pipeline_event(pipeline_id, update_id, "INFO", "update_progress", "Update completed", json!({ "update_progress": { "state": "COMPLETED" } })).await;
        Ok(())
    }
}

// ---------------------------------------------------------------- handlers

fn normalize_pipeline(mut b: Map<String, Value>, id: &str, p: &Principal, existing: Option<&Value>) -> Map<String, Value> {
    b.insert("pipeline_id".into(), json!(id));
    b.entry("name").or_insert(json!(format!("pipeline-{}", &id[..8])));
    b.entry("continuous").or_insert(json!(false));
    b.entry("development").or_insert(json!(true));
    b.entry("photon").or_insert(json!(true));
    b.entry("serverless").or_insert(json!(false));
    b.entry("edition").or_insert(json!("ADVANCED"));
    b.entry("channel").or_insert(json!("CURRENT"));
    b.entry("catalog").or_insert(json!("main"));
    b.entry("libraries").or_insert(json!([]));
    b.entry("configuration").or_insert(json!({}));
    b.entry("clusters").or_insert(json!([]));
    b.insert("creator_user_name".into(), existing.and_then(|e| e.get("creator_user_name").cloned()).unwrap_or(json!(p.user_name)));
    b.insert("run_as_user_name".into(), json!(p.user_name));
    b.insert("state".into(), existing.and_then(|e| e.get("state").cloned()).unwrap_or(json!("IDLE")));
    b.insert("created_at".into(), existing.and_then(|e| e.get("created_at").cloned()).unwrap_or(json!(now_ms())));
    b.insert("last_modified".into(), json!(now_ms()));
    b.insert("health".into(), json!("HEALTHY"));
    b
}

async fn create(State(st): State<S>, Who(p): Who, Body(b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let id = uuid::Uuid::new_v4().to_string();
    let dry = b.get("dry_run").and_then(|v| v.as_bool()).unwrap_or(false);
    let b = normalize_pipeline(b, &id, &p, None);
    if let Some(name) = b["name"].as_str() {
        if st.store.find_by_name::<Value>(KIND_PIPELINE, st.ws(), None, name).await?.is_some() && !b.get("allow_duplicate_names").and_then(|v| v.as_bool()).unwrap_or(false) {
            return Err(ApiError::AlreadyExists(format!("Pipeline with name {name} already exists")));
        }
    }
    if dry {
        return Ok(Json(json!({ "pipeline_id": Value::Null, "effective_settings": b })));
    }
    let name = b["name"].as_str().unwrap_or("").to_string();
    st.store.insert(KIND_PIPELINE, st.ws(), &id, None, Some(&name), &Value::Object(b.clone())).await?;
    let mut settings = b.clone();
    settings.remove("state");
    Ok(Json(json!({ "pipeline_id": id, "effective_settings": settings })))
}

async fn get_one(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let doc = st.get_pipeline(&id).await?;
    let mut v = doc.data.clone();
    let updates: Vec<Doc<Value>> = st.store.list(KIND_UPDATE, st.ws(), Filter { parent_id: Some(&id), newest_first: true, limit: Some(5), ..Default::default() }).await?;
    v["latest_updates"] = json!(updates.iter().map(|u| json!({ "update_id": u.id, "state": u.data["state"], "creation_time": u.data["creation_time"] })).collect::<Vec<_>>());
    v["spec"] = doc.data.clone();
    v["cluster_id"] = updates.first().and_then(|u| u.data.get("cluster_id").cloned()).unwrap_or(Value::Null);
    Ok(Json(v))
}

#[derive(Debug, Deserialize)]
struct ListQ {
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    max_results: Option<i64>,
}

async fn list(State(st): State<S>, Query(q): Query<ListQ>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_PIPELINE, st.ws(), Filter { newest_first: true, ..Default::default() }).await?;
    let mut out: Vec<Value> = docs.into_iter().map(|d| json!({ "pipeline_id": d.id, "name": d.data["name"], "state": d.data["state"], "creator_user_name": d.data["creator_user_name"], "run_as_user_name": d.data["run_as_user_name"], "cluster_id": Value::Null, "health": d.data["health"], "latest_updates": d.data["latest_updates"] })).collect();
    if let Some(f) = &q.filter {
        // name LIKE '%x%'
        if let Some(pat) = f.split('\'').nth(1) {
            let needle = pat.trim_matches('%').to_lowercase();
            out.retain(|v| v["name"].as_str().map(|n| n.to_lowercase().contains(&needle)).unwrap_or(false));
        }
    }
    if let Some(n) = q.max_results {
        out.truncate(n.max(0) as usize);
    }
    Ok(Json(json!({ "statuses": out })))
}

async fn edit(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let existing = st.get_pipeline(&id).await?;
    let b = normalize_pipeline(b, &id, &p, Some(&existing.data));
    let name = b["name"].as_str().unwrap_or("").to_string();
    st.store.put(KIND_PIPELINE, &id, None, Some(&name), &Value::Object(b)).await?;
    Ok(empty())
}

async fn delete(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    st.get_pipeline(&id).await?;
    st.store.delete(KIND_PIPELINE, &id).await?;
    st.store.delete_children(KIND_UPDATE, &id).await?;
    st.store.delete_children(KIND_EVENT, &id).await?;
    Ok(empty())
}

async fn start_update(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let full = b.get("full_refresh").and_then(|v| v.as_bool()).unwrap_or(false);
    let uid = st.start_pipeline_update(&p, &id, full, None).await?;
    Ok(Json(json!({ "update_id": uid })))
}

async fn get_update(State(st): State<S>, Path((id, uid)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let u = st.store.require::<Value>(KIND_UPDATE, &uid, "Update").await?;
    if u.data["pipeline_id"] != id {
        return Err(ApiError::NotFound(format!("Update {uid} not found")));
    }
    Ok(Json(json!({ "update": u.data })))
}

async fn list_updates(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_UPDATE, st.ws(), Filter { parent_id: Some(&id), newest_first: true, ..Default::default() }).await?;
    Ok(Json(json!({ "updates": docs.iter().map(|d| &d.data).collect::<Vec<_>>() })))
}

async fn list_events(State(st): State<S>, Path(id): Path<String>, Query(q): Query<ListQ>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_EVENT, st.ws(), Filter { parent_id: Some(&id), newest_first: true, limit: q.max_results.or(Some(100)), ..Default::default() }).await?;
    Ok(Json(json!({ "events": docs.iter().map(|d| &d.data).collect::<Vec<_>>() })))
}

async fn stop(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_UPDATE, st.ws(), Filter { parent_id: Some(&id), newest_first: true, limit: Some(1), ..Default::default() }).await?;
    if let Some(u) = docs.first() {
        if !matches!(u.data["state"].as_str(), Some("COMPLETED" | "FAILED" | "CANCELED")) {
            st.set_update_state(&u.id, "CANCELED", Some("USER_ACTION")).await?;
        }
    }
    st.store.update::<Value, _>(KIND_PIPELINE, &id, "Pipeline", |p| {
        p["state"] = json!("IDLE");
        Ok(())
    }).await?;
    Ok(empty())
}

async fn reset(State(st): State<S>, Who(p): Who, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let uid = st.start_pipeline_update(&p, &id, true, None).await?;
    Ok(Json(json!({ "update_id": uid })))
}

async fn pipeline_permissions(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let acl = st.get_permissions("pipelines", &id).await?;
    Ok(Json(json!({ "object_id": format!("/pipelines/{id}"), "object_type": "pipeline", "access_control_list": acl })))
}

impl AppState {
    /// Expand a workspace glob like `/Repos/x/dlt/*.sql` into object paths.
    pub async fn ws_glob(&self, pattern: &str) -> ApiResult<Vec<String>> {
        let (dir, pat) = pattern.rsplit_once('/').unwrap_or(("/", pattern));
        let dir = if dir.is_empty() { "/" } else { dir };
        let re = regex::Regex::new(&format!("^{}$", regex::escape(pat).replace(r"\*", ".*"))).map_err(|e| ApiError::invalid(e.to_string()))?;
        let mut out = vec![];
        for o in self.ws_list(dir).await? {
            let base = o.path.rsplit('/').next().unwrap_or("");
            if re.is_match(base) {
                out.push(o.path.clone());
            }
        }
        Ok(out)
    }
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/pipelines", get(list).post(create))
        .route("/api/2.0/pipelines/{id}", get(get_one).put(edit).delete(delete))
        .route("/api/2.0/pipelines/{id}/updates", get(list_updates).post(start_update))
        .route("/api/2.0/pipelines/{id}/updates/{update_id}", get(get_update))
        .route("/api/2.0/pipelines/{id}/events", get(list_events))
        .route("/api/2.0/pipelines/{id}/stop", post(stop))
        .route("/api/2.0/pipelines/{id}/reset", post(reset))
        .route("/api/2.0/pipelines/{id}/permissions", get(pipeline_permissions))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_live_tables() {
        let sql = "CREATE OR REFRESH LIVE TABLE bronze COMMENT 'raw' AS SELECT * FROM main.default.orders;\nCREATE STREAMING LIVE TABLE silver AS SELECT * FROM STREAM(LIVE.bronze) WHERE amount > 0;\nCREATE LIVE VIEW gold AS SELECT region, sum(amount) FROM LIVE.silver GROUP BY region";
        let ds = parse_datasets(sql, "nb");
        assert_eq!(ds.len(), 3);
        assert_eq!(ds[0].name, "bronze");
        assert_eq!(ds[0].comment.as_deref(), Some("raw"));
        assert_eq!(ds[1].kind, "STREAMING_TABLE");
        assert_eq!(ds[1].deps, vec!["bronze"]);
        assert_eq!(ds[2].kind, "VIEW");
        assert_eq!(ds[2].deps, vec!["silver"]);
        let order = topo(&ds).unwrap();
        assert_eq!(order, vec![0, 1, 2]);
        let names: HashSet<String> = ds.iter().map(|d| d.name.clone()).collect();
        let q = rewrite_refs(&ds[1].query, "main.dlt", &names);
        assert!(q.contains("FROM main.dlt.bronze"), "{q}");
    }
}
