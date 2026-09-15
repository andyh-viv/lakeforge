//! Databricks SQL: warehouses, Statement Execution API, saved queries, alerts,
//! Lakeview dashboards and query history.
//!
//! A SQL warehouse is backed by a Forge cluster (`cluster_source = "SQL"`);
//! statements run through [`crate::forge::run_sql`] against its driver.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::task::AbortHandle;

use super::clusters::{Cluster, ClusterState, KIND as CLUSTER_KIND};
use super::{empty, Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::forge::{run_sql, SqlResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};

pub const KIND_WAREHOUSE: &str = "warehouse";
pub const KIND_QUERY: &str = "sql_query";
pub const KIND_ALERT: &str = "sql_alert";
pub const KIND_DASHBOARD: &str = "dashboard";
pub const KIND_HISTORY: &str = "query_history";

// ------------------------------------------------------------- statements

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatementState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
    Closed,
}

#[derive(Debug, Clone, Serialize)]
pub struct Statement {
    pub statement_id: String,
    pub state: StatementState,
    pub statement: String,
    pub warehouse_id: Option<String>,
    pub cluster_id: String,
    pub user_name: String,
    pub started_ms: i64,
    pub finished_ms: Option<i64>,
    pub error: Option<String>,
    pub result: Option<SqlResult>,
}

pub struct StatementHandle {
    pub record: Arc<RwLock<Statement>>,
    pub abort: Option<AbortHandle>,
}

impl Statement {
    pub fn view(&self, include_result: bool) -> Value {
        let mut v = json!({
            "statement_id": self.statement_id,
            "status": { "state": self.state },
        });
        if let Some(e) = &self.error {
            v["status"]["error"] = json!({ "error_code": "BAD_REQUEST", "message": e });
        }
        if let Some(r) = &self.result {
            v["manifest"] = r.manifest();
            if include_result {
                v["result"] = r.result_chunk();
            }
        }
        v
    }
}

fn duration_arg(s: &Option<String>) -> Duration {
    s.as_deref().and_then(|s| s.trim_end_matches('s').parse::<u64>().ok()).map(Duration::from_secs).unwrap_or(Duration::from_secs(10)).min(Duration::from_secs(50))
}

/// Split a script on `;` outside quotes/comments; drops empty statements.
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cur = String::new();
    let mut chars = sql.chars().peekable();
    let mut quote: Option<char> = None;
    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                cur.push(c);
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' | '`' => {
                    quote = Some(c);
                    cur.push(c);
                }
                '-' if chars.peek() == Some(&'-') => {
                    for c2 in chars.by_ref() {
                        if c2 == '\n' {
                            cur.push('\n');
                            break;
                        }
                    }
                }
                ';' => {
                    if !cur.trim().is_empty() {
                        out.push(cur.trim().to_string());
                    }
                    cur.clear();
                }
                _ => cur.push(c),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// Replace Databricks-style `:name` parameter markers with SQL literals.
pub fn bind_parameters(sql: &str, params: &[Value]) -> String {
    let mut out = sql.to_string();
    for p in params {
        let Some(name) = p.get("name").and_then(|v| v.as_str()) else { continue };
        let ty = p.get("type").and_then(|v| v.as_str()).unwrap_or("STRING").to_ascii_uppercase();
        let val = p.get("value");
        let lit = match val {
            None | Some(Value::Null) => "NULL".to_string(),
            Some(Value::String(s)) => match ty.as_str() {
                "INT" | "LONG" | "SHORT" | "BYTE" | "DOUBLE" | "FLOAT" | "DECIMAL" | "BOOLEAN" => s.clone(),
                "DATE" => format!("DATE '{}'", s.replace('\'', "''")),
                "TIMESTAMP" => format!("TIMESTAMP '{}'", s.replace('\'', "''")),
                _ => format!("'{}'", s.replace('\'', "''")),
            },
            Some(Value::Bool(b)) => b.to_string(),
            Some(Value::Number(n)) => n.to_string(),
            Some(other) => format!("'{}'", other.to_string().replace('\'', "''")),
        };
        out = out.replace(&format!(":{name}"), &lit);
    }
    out
}

impl AppState {
    /// Resolve `warehouse_id`/`cluster_id` to a cluster id.
    pub async fn resolve_compute(&self, warehouse_id: Option<&str>, cluster_id: Option<&str>) -> ApiResult<(String, Option<String>)> {
        if let Some(c) = cluster_id {
            return Ok((c.to_string(), None));
        }
        let wid = match warehouse_id {
            Some(w) => w.to_string(),
            None => {
                let docs: Vec<Doc<Warehouse>> = self.store.list(KIND_WAREHOUSE, self.ws(), Filter::default()).await?;
                docs.into_iter().find(|d| d.data.state_hint != "DELETED").map(|d| d.id).ok_or_else(|| ApiError::invalid("warehouse_id is required (no warehouses exist)"))?
            }
        };
        let wh = self.store.require::<Warehouse>(KIND_WAREHOUSE, &wid, "Warehouse").await?;
        Ok((wh.data.cluster_id, Some(wid)))
    }

    /// Execute `sql` on `cluster_id`, recording query history. Used by the
    /// statements API, notebooks (`%sql`), jobs and pipelines.
    pub async fn execute_sql(
        self: &Arc<Self>,
        cluster_id: &str,
        warehouse_id: Option<&str>,
        user: &Principal,
        sql: &str,
        conf: HashMap<String, String>,
        max_rows: usize,
    ) -> ApiResult<SqlResult> {
        let started = now_ms();
        let addr = self.cluster_driver(cluster_id, true).await?;
        let session_id = format!("user:{}", user.user_id);
        let res = run_sql(&self.forge, &addr, &session_id, sql, conf, max_rows).await;
        let finished = now_ms();
        let hist_id = uuid::Uuid::new_v4().to_string();
        let (status, rows, err) = match &res {
            Ok(r) => ("FINISHED", r.row_count as i64, None),
            Err(e) => ("FAILED", 0, Some(e.to_string())),
        };
        let stmt_type = sql.split_whitespace().next().map(|w| w.to_ascii_uppercase()).unwrap_or_default();
        let hist = json!({
            "query_id": hist_id,
            "status": status,
            "query_text": sql,
            "query_start_time_ms": started,
            "execution_end_time_ms": finished,
            "query_end_time_ms": finished,
            "duration": finished - started,
            "user_id": user.user_id,
            "user_name": user.user_name,
            "executed_as_user_name": user.user_name,
            "warehouse_id": warehouse_id,
            "endpoint_id": warehouse_id,
            "cluster_id": cluster_id,
            "rows_produced": rows,
            "error_message": err,
            "statement_type": stmt_type,
            "is_final": true,
            "metrics": res.as_ref().ok().map(|r| json!({ "total_time_ms": r.elapsed_ms, "rows_produced_count": r.row_count, "execution_time_ms": r.elapsed_ms, "compilation_time_ms": 0 })),
        });
        let _ = self.store.insert(KIND_HISTORY, self.ws(), &hist_id, warehouse_id, None, &hist).await;
        if res.is_ok() && matches!(stmt_type.as_str(), "CREATE" | "DROP") {
            self.observe_ddl(sql, &user.user_name).await;
        }
        res
    }

    pub fn statement_view(&self, id: &str, include_result: bool) -> Option<Value> {
        self.statements.get(id).map(|h| h.record.read().view(include_result))
    }
}

#[derive(Debug, Deserialize)]
pub struct ExecuteStatement {
    pub statement: String,
    #[serde(default)]
    pub warehouse_id: Option<String>,
    #[serde(default)]
    pub cluster_id: Option<String>,
    #[serde(default)]
    pub catalog: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub parameters: Vec<Value>,
    #[serde(default)]
    pub wait_timeout: Option<String>,
    #[serde(default)]
    pub on_wait_timeout: Option<String>,
    #[serde(default)]
    pub row_limit: Option<usize>,
    #[serde(default)]
    pub disposition: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub session_conf: HashMap<String, String>,
}

async fn execute_statement(State(st): State<S>, Who(p): Who, Body(b): Body<ExecuteStatement>) -> ApiResult<Json<Value>> {
    let (cluster_id, warehouse_id) = st.resolve_compute(b.warehouse_id.as_deref(), b.cluster_id.as_deref()).await?;
    let sql = bind_parameters(&b.statement, &b.parameters);
    let mut conf = b.session_conf.clone();
    if let Some(c) = &b.catalog {
        conf.insert("forge.sql.defaultCatalog".into(), c.clone());
    }
    if let Some(s) = &b.schema {
        conf.insert("forge.sql.defaultSchema".into(), s.clone());
    }
    let id = uuid::Uuid::new_v4().to_string();
    let record = Arc::new(RwLock::new(Statement {
        statement_id: id.clone(),
        state: StatementState::Pending,
        statement: sql.clone(),
        warehouse_id: warehouse_id.clone(),
        cluster_id: cluster_id.clone(),
        user_name: p.user_name.clone(),
        started_ms: now_ms(),
        finished_ms: None,
        error: None,
        result: None,
    }));
    let max_rows = b.row_limit.unwrap_or(10_000).min(100_000);
    let rec = Arc::clone(&record);
    let st2 = Arc::clone(&st);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        rec.write().state = StatementState::Running;
        let res = st2.execute_sql(&cluster_id, warehouse_id.as_deref(), &p, &sql, conf, max_rows).await;
        let mut r = rec.write();
        r.finished_ms = Some(now_ms());
        match res {
            Ok(v) => {
                r.result = Some(v);
                r.state = StatementState::Succeeded;
            }
            Err(e) => {
                r.error = Some(e.to_string());
                r.state = StatementState::Failed;
            }
        }
        let _ = done_tx.send(());
    });
    st.statements.insert(id.clone(), StatementHandle { record: Arc::clone(&record), abort: Some(task.abort_handle()) });

    let wait = duration_arg(&b.wait_timeout);
    if !wait.is_zero() {
        let _ = tokio::time::timeout(wait, done_rx).await;
        let finished = matches!(record.read().state, StatementState::Succeeded | StatementState::Failed);
        if !finished && b.on_wait_timeout.as_deref() == Some("CANCEL") {
            cancel_statement_inner(&st, &id);
        }
    }
    let view = record.read().view(true);
    Ok(Json(view))
}

fn cancel_statement_inner(st: &AppState, id: &str) {
    if let Some(h) = st.statements.get(id) {
        if let Some(a) = &h.abort {
            a.abort();
        }
        let mut r = h.record.write();
        if !matches!(r.state, StatementState::Succeeded | StatementState::Failed) {
            r.state = StatementState::Canceled;
            r.finished_ms = Some(now_ms());
        }
    }
}

async fn get_statement(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    st.statement_view(&id, true).map(Json).ok_or_else(|| ApiError::NotFound(format!("Statement {id} not found")))
}

async fn get_chunk(State(st): State<S>, Path((id, n)): Path<(String, usize)>) -> ApiResult<Json<Value>> {
    let h = st.statements.get(&id).ok_or_else(|| ApiError::NotFound(format!("Statement {id} not found")))?;
    let r = h.record.read();
    match (&r.result, n) {
        (Some(res), 0) => Ok(Json(res.result_chunk())),
        (Some(_), _) => Err(ApiError::NotFound(format!("chunk {n} does not exist"))),
        (None, _) => Err(ApiError::InvalidState("statement has no result yet".into())),
    }
}

async fn cancel_statement(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    cancel_statement_inner(&st, &id);
    Ok(empty())
}

async fn close_statement(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    cancel_statement_inner(&st, &id);
    if let Some(h) = st.statements.get(&id) {
        h.record.write().state = StatementState::Closed;
    }
    Ok(empty())
}

// ------------------------------------------------------------- warehouses

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Warehouse {
    pub id: String,
    pub name: String,
    #[serde(default = "default_size")]
    pub cluster_size: String,
    #[serde(default = "one")]
    pub min_num_clusters: u32,
    #[serde(default = "one")]
    pub max_num_clusters: u32,
    #[serde(default = "default_autostop")]
    pub auto_stop_mins: u32,
    #[serde(default)]
    pub tags: Value,
    #[serde(default)]
    pub spot_instance_policy: Option<String>,
    #[serde(default = "default_type")]
    pub warehouse_type: String,
    #[serde(default)]
    pub enable_photon: bool,
    #[serde(default)]
    pub enable_serverless_compute: bool,
    #[serde(default)]
    pub channel: Option<Value>,
    pub creator_name: String,
    pub cluster_id: String,
    #[serde(default)]
    pub state_hint: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

fn default_size() -> String {
    "2X-Small".into()
}
fn one() -> u32 {
    1
}
fn default_autostop() -> u32 {
    45
}
fn default_type() -> String {
    "PRO".into()
}

/// Map a T-shirt size onto a Forge cluster shape (workers, node type).
fn size_shape(size: &str) -> (u32, &'static str) {
    match size {
        "2X-Small" => (1, "lf.small"),
        "X-Small" => (1, "lf.medium"),
        "Small" => (2, "lf.medium"),
        "Medium" => (4, "lf.medium"),
        "Large" => (8, "lf.medium"),
        "X-Large" => (8, "lf.large"),
        "2X-Large" => (16, "lf.large"),
        "3X-Large" => (32, "lf.large"),
        "4X-Large" => (64, "lf.large"),
        _ => (1, "lf.small"),
    }
}

impl AppState {
    async fn warehouse_view(&self, w: &Warehouse) -> Value {
        let cluster = self.store.get::<Cluster>(CLUSTER_KIND, &w.cluster_id).await.ok().flatten().map(|d| d.data);
        let (state, health, num_clusters) = match cluster.as_ref().map(|c| c.state) {
            Some(ClusterState::Running) => ("RUNNING", "HEALTHY", 1),
            Some(ClusterState::Pending | ClusterState::Restarting | ClusterState::Resizing) => ("STARTING", "HEALTHY", 0),
            Some(ClusterState::Terminating) => ("STOPPING", "HEALTHY", 0),
            Some(ClusterState::Error) => ("STOPPED", "FAILED", 0),
            _ => ("STOPPED", "HEALTHY", 0),
        };
        let mut v = serde_json::to_value(w).unwrap_or_default();
        if let Some(o) = v.as_object_mut() {
            o.remove("state_hint");
            o.insert("state".into(), json!(state));
            o.insert("health".into(), json!({ "status": health, "message": cluster.as_ref().map(|c| c.state_message.clone()).unwrap_or_default() }));
            o.insert("num_clusters".into(), json!(num_clusters));
            o.insert("num_active_sessions".into(), json!(0));
            o.insert("jdbc_url".into(), json!(format!("jdbc:lakeforge://{}/default;transportMode=http;ssl=0;httpPath=/sql/1.0/warehouses/{}", self.config.public_url.trim_start_matches("http://").trim_start_matches("https://"), w.id)));
            o.insert("odbc_params".into(), json!({ "hostname": self.config.public_url, "path": format!("/sql/1.0/warehouses/{}", w.id), "protocol": "http", "port": self.config.bind.port() }));
            o.insert("creator_name".into(), json!(w.creator_name));
        }
        v
    }
}

impl AppState {
    /// Create a SQL warehouse and its backing Forge cluster; returns the warehouse id.
    pub async fn create_warehouse(self: &Arc<Self>, p: &Principal, mut o: Map<String, Value>, autostart: bool) -> ApiResult<String> {
        let st = self;
        let name = o.get("name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("name is required"))?.to_string();
        let size = o.get("cluster_size").and_then(|v| v.as_str()).unwrap_or("2X-Small").to_string();
        let (workers, node) = size_shape(&size);
        let auto_stop = o.get("auto_stop_mins").and_then(|v| v.as_u64()).unwrap_or(45) as u32;
        let id = uuid::Uuid::new_v4().simple().to_string()[..16].to_string();

        let cluster_body = json!({
            "cluster_name": format!("sql-warehouse-{name}"),
            "num_workers": workers,
            "node_type_id": node,
            "autotermination_minutes": auto_stop,
            "cluster_source": "SQL",
            "custom_tags": { "LakeforgeWarehouseId": id, "ResourceClass": "SQLWarehouse" },
            "spark_conf": { "forge.sql.shuffle.partitions": (workers.max(1) * 4).to_string() },
        });
        let cluster_id = st.create_cluster_from_json(p, cluster_body.as_object().unwrap().clone(), autostart).await?;

        o.insert("id".into(), json!(id));
        o.insert("cluster_id".into(), json!(cluster_id));
        o.insert("creator_name".into(), json!(p.user_name));
        o.insert("cluster_size".into(), json!(size));
        let wh: Warehouse = serde_json::from_value(Value::Object(o)).map_err(|e| ApiError::invalid(format!("invalid warehouse spec: {e}")))?;
        st.store.insert(KIND_WAREHOUSE, st.ws(), &id, None, Some(&name), &wh).await?;
        Ok(id)
    }

    /// First-boot default: a small "Starter Warehouse" so SQL works out of the box.
    pub async fn ensure_starter_warehouse(self: &Arc<Self>, p: &Principal) -> ApiResult<()> {
        let docs: Vec<Doc<Warehouse>> = self.store.list(KIND_WAREHOUSE, self.ws(), Filter { limit: Some(1), ..Default::default() }).await?;
        if docs.is_empty() {
            let mut o = Map::new();
            o.insert("name".into(), json!("Starter Warehouse"));
            o.insert("cluster_size".into(), json!("2X-Small"));
            o.insert("auto_stop_mins".into(), json!(10));
            o.insert("warehouse_type".into(), json!("PRO"));
            let id = self.create_warehouse(p, o, false).await?;
            tracing::info!(warehouse = %id, "created starter SQL warehouse");
        }
        Ok(())
    }
}

async fn create_warehouse(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let id = st.create_warehouse(&p, o, true).await?;
    Ok(Json(json!({ "id": id })))
}

async fn list_warehouses(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Warehouse>> = st.store.list(KIND_WAREHOUSE, st.ws(), Filter::default()).await?;
    let mut out = vec![];
    for d in docs {
        out.push(st.warehouse_view(&d.data).await);
    }
    Ok(Json(json!({ "warehouses": out })))
}

async fn get_warehouse(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let d = st.store.require::<Warehouse>(KIND_WAREHOUSE, &id, "Warehouse").await?;
    Ok(Json(st.warehouse_view(&d.data).await))
}

async fn edit_warehouse(State(st): State<S>, Path(id): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let d = st
        .store
        .update::<Warehouse, _>(KIND_WAREHOUSE, &id, "Warehouse", |w| {
            let mut v = serde_json::to_value(&*w)?;
            for (k, val) in &o {
                if !matches!(k.as_str(), "id" | "cluster_id" | "creator_name") {
                    v[k] = val.clone();
                }
            }
            *w = serde_json::from_value(v).map_err(|e| ApiError::invalid(format!("invalid warehouse spec: {e}")))?;
            Ok(())
        })
        .await?;
    // propagate size / autostop to the backing cluster
    let (workers, node) = size_shape(&d.data.cluster_size);
    let auto_stop = d.data.auto_stop_mins;
    let _ = st
        .store
        .update::<Cluster, _>(CLUSTER_KIND, &d.data.cluster_id, "Cluster", |c| {
            c.num_workers = workers;
            c.node_type_id = node.into();
            c.autotermination_minutes = auto_stop;
            c.cluster_name = format!("sql-warehouse-{}", d.data.name);
            Ok(())
        })
        .await;
    Ok(empty())
}

async fn delete_warehouse(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let d = st.store.require::<Warehouse>(KIND_WAREHOUSE, &id, "Warehouse").await?;
    let _ = st.terminate_cluster(&d.data.cluster_id, "USER_REQUEST").await;
    let _ = st.store.delete(CLUSTER_KIND, &d.data.cluster_id).await;
    st.store.delete(KIND_WAREHOUSE, &id).await?;
    Ok(empty())
}

async fn start_warehouse(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let d = st.store.require::<Warehouse>(KIND_WAREHOUSE, &id, "Warehouse").await?;
    st.start_cluster(&d.data.cluster_id).await?;
    Ok(empty())
}

async fn stop_warehouse(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let d = st.store.require::<Warehouse>(KIND_WAREHOUSE, &id, "Warehouse").await?;
    st.terminate_cluster(&d.data.cluster_id, "USER_REQUEST").await?;
    Ok(empty())
}

async fn warehouse_config(State(st): State<S>) -> Json<Value> {
    Json(json!({
        "security_policy": "DATA_ACCESS_CONTROL",
        "data_access_config": [],
        "sql_configuration_parameters": { "configuration_pairs": [] },
        "enabled_warehouse_types": [{ "warehouse_type": "PRO", "enabled": true }, { "warehouse_type": "CLASSIC", "enabled": true }],
        "instance_profile_arn": null,
        "google_service_account": null,
        "channel": { "name": "CHANNEL_NAME_CURRENT" },
        "cloud": st.config.cloud,
    }))
}

async fn data_sources(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Warehouse>> = st.store.list(KIND_WAREHOUSE, st.ws(), Filter::default()).await?;
    Ok(Json(Value::Array(
        docs.iter().map(|d| json!({ "id": d.id, "name": d.data.name, "warehouse_id": d.id, "type": "lakeforge", "syntax": "sql", "paused": 0, "supports_auto_limit": true })).collect(),
    )))
}

// ---------------------------------------------------------- saved queries

fn legacy_to_query(mut o: Map<String, Value>) -> Map<String, Value> {
    if let Some(n) = o.remove("name") {
        o.entry("display_name").or_insert(n);
    }
    if let Some(q) = o.remove("query") {
        o.entry("query_text").or_insert(q);
    }
    if let Some(d) = o.remove("data_source_id") {
        o.entry("warehouse_id").or_insert(d);
    }
    if let Some(Value::Object(opts)) = o.remove("options") {
        if let Some(p) = opts.get("parameters") {
            o.entry("parameters").or_insert(p.clone());
        }
    }
    o
}

fn query_to_legacy(v: &Value) -> Value {
    let mut o = v.as_object().cloned().unwrap_or_default();
    o.insert("name".into(), v["display_name"].clone());
    o.insert("query".into(), v["query_text"].clone());
    o.insert("data_source_id".into(), v["warehouse_id"].clone());
    o.insert("options".into(), json!({ "parameters": v["parameters"].as_array().cloned().unwrap_or_default() }));
    o.insert("created_at".into(), v["create_time"].clone());
    o.insert("updated_at".into(), v["update_time"].clone());
    o.insert("user".into(), json!({ "name": v["owner_user_name"], "email": v["owner_user_name"] }));
    Value::Object(o)
}

async fn upsert_query(st: &AppState, p: &Principal, id: Option<String>, o: Map<String, Value>) -> ApiResult<Value> {
    let mut o = legacy_to_query(o);
    let now = chrono::Utc::now().to_rfc3339();
    let (id, mut base) = match id {
        Some(id) => {
            let d = st.store.require::<Value>(KIND_QUERY, &id, "Query").await?;
            (id, d.data.as_object().cloned().unwrap_or_default())
        }
        None => {
            let id = uuid::Uuid::new_v4().to_string();
            let mut b = Map::new();
            b.insert("id".into(), json!(id));
            b.insert("create_time".into(), json!(now));
            b.insert("owner_user_name".into(), json!(p.user_name));
            b.insert("lifecycle_state".into(), json!("ACTIVE"));
            b.insert("run_as_mode".into(), json!("OWNER"));
            b.insert("parent_path".into(), json!(format!("/Users/{}", p.user_name)));
            (id, b)
        }
    };
    o.remove("id");
    for (k, v) in o {
        base.insert(k, v);
    }
    base.entry("display_name").or_insert(json!("Untitled query"));
    base.entry("query_text").or_insert(json!(""));
    base.entry("parameters").or_insert(json!([]));
    base.entry("tags").or_insert(json!([]));
    base.insert("update_time".into(), json!(now));
    let name = base["display_name"].as_str().unwrap_or("").to_string();
    let v = Value::Object(base);
    st.store.upsert(KIND_QUERY, st.ws(), &id, None, Some(&name), &v).await?;
    Ok(v)
}

#[derive(Debug, Deserialize)]
struct QueryEnvelope {
    #[serde(default)]
    query: Option<Map<String, Value>>,
    #[serde(flatten)]
    rest: Map<String, Value>,
}

impl QueryEnvelope {
    fn into_map(self) -> Map<String, Value> {
        match self.query {
            Some(q) if q.contains_key("display_name") || q.contains_key("query_text") => q,
            Some(q) => {
                let mut m = self.rest;
                m.insert("query".into(), Value::Object(q));
                m
            }
            None => self.rest,
        }
    }
}

async fn create_query(State(st): State<S>, Who(p): Who, Body(b): Body<QueryEnvelope>) -> ApiResult<Json<Value>> {
    Ok(Json(upsert_query(&st, &p, None, b.into_map()).await?))
}
async fn create_query_legacy(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(query_to_legacy(&upsert_query(&st, &p, None, o).await?)))
}
async fn update_query(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<QueryEnvelope>) -> ApiResult<Json<Value>> {
    Ok(Json(upsert_query(&st, &p, Some(id), b.into_map()).await?))
}
async fn update_query_legacy(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(query_to_legacy(&upsert_query(&st, &p, Some(id), o).await?)))
}
async fn get_query(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_QUERY, &id, "Query").await?.data))
}
async fn get_query_legacy(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(query_to_legacy(&st.store.require::<Value>(KIND_QUERY, &id, "Query").await?.data)))
}

#[derive(Debug, Deserialize)]
struct PageQ {
    #[serde(default)]
    page_size: Option<i64>,
    #[serde(default)]
    q: Option<String>,
}

async fn list_queries(State(st): State<S>, Query(q): Query<PageQ>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_QUERY, st.ws(), Filter { newest_first: true, limit: q.page_size, ..Default::default() }).await?;
    let needle = q.q.map(|s| s.to_ascii_lowercase());
    let items: Vec<Value> = docs.into_iter().map(|d| d.data).filter(|v| needle.as_ref().map(|n| v["display_name"].as_str().unwrap_or("").to_ascii_lowercase().contains(n)).unwrap_or(true)).collect();
    Ok(Json(json!({ "results": items })))
}
async fn list_queries_legacy(State(st): State<S>, Query(q): Query<PageQ>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_QUERY, st.ws(), Filter { newest_first: true, ..Default::default() }).await?;
    let items: Vec<Value> = docs.iter().map(|d| query_to_legacy(&d.data)).collect();
    let _ = q;
    Ok(Json(json!({ "count": items.len(), "page": 1, "page_size": items.len(), "results": items })))
}
async fn delete_query(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    st.store.delete(KIND_QUERY, &id).await?;
    Ok(empty())
}

// ------------------------------------------------------------------ alerts

async fn upsert_alert(st: &AppState, p: &Principal, id: Option<String>, mut o: Map<String, Value>) -> ApiResult<Value> {
    if let Some(Value::Object(inner)) = o.remove("alert") {
        for (k, v) in inner {
            o.insert(k, v);
        }
    }
    // legacy shape: { name, query_id, options: { column, op, value } }
    if let Some(n) = o.remove("name") {
        o.entry("display_name").or_insert(n);
    }
    if let Some(Value::Object(opts)) = o.remove("options") {
        let op = match opts.get("op").and_then(|v| v.as_str()).unwrap_or(">") {
            ">" => "GREATER_THAN",
            ">=" => "GREATER_THAN_OR_EQUAL",
            "<" => "LESS_THAN",
            "<=" => "LESS_THAN_OR_EQUAL",
            "==" | "=" => "EQUAL",
            "!=" => "NOT_EQUAL",
            other => other,
        };
        let val = opts.get("value").cloned().unwrap_or(Value::Null);
        let threshold = match &val {
            Value::Number(n) => json!({ "value": { "double_value": n.as_f64() } }),
            Value::Bool(b) => json!({ "value": { "bool_value": b } }),
            other => json!({ "value": { "string_value": other.as_str().unwrap_or_default() } }),
        };
        o.insert("condition".into(), json!({ "op": op, "operand": { "column": { "name": opts.get("column").cloned().unwrap_or(Value::Null) } }, "threshold": threshold }));
        if let Some(s) = opts.get("custom_subject") {
            o.insert("custom_subject".into(), s.clone());
        }
        if let Some(b) = opts.get("custom_body") {
            o.insert("custom_body".into(), b.clone());
        }
    }
    let now = chrono::Utc::now().to_rfc3339();
    let (id, mut base) = match id {
        Some(id) => {
            let d = st.store.require::<Value>(KIND_ALERT, &id, "Alert").await?;
            (id, d.data.as_object().cloned().unwrap_or_default())
        }
        None => {
            let id = uuid::Uuid::new_v4().to_string();
            let mut b = Map::new();
            b.insert("id".into(), json!(id));
            b.insert("create_time".into(), json!(now));
            b.insert("owner_user_name".into(), json!(p.user_name));
            b.insert("lifecycle_state".into(), json!("ACTIVE"));
            b.insert("state".into(), json!("UNKNOWN"));
            b.insert("parent_path".into(), json!(format!("/Users/{}", p.user_name)));
            (id, b)
        }
    };
    o.remove("id");
    for (k, v) in o {
        base.insert(k, v);
    }
    base.entry("display_name").or_insert(json!("Untitled alert"));
    base.entry("seconds_to_retrigger").or_insert(json!(0));
    base.insert("update_time".into(), json!(now));
    let name = base["display_name"].as_str().unwrap_or("").to_string();
    let v = Value::Object(base);
    st.store.upsert(KIND_ALERT, st.ws(), &id, None, Some(&name), &v).await?;
    Ok(v)
}

async fn create_alert(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(upsert_alert(&st, &p, None, o).await?))
}
async fn update_alert(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(upsert_alert(&st, &p, Some(id), o).await?))
}
async fn get_alert(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_ALERT, &id, "Alert").await?.data))
}
async fn list_alerts(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_ALERT, st.ws(), Filter { newest_first: true, ..Default::default() }).await?;
    Ok(Json(json!({ "results": docs.into_iter().map(|d| d.data).collect::<Vec<_>>() })))
}
async fn list_alerts_legacy(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_ALERT, st.ws(), Filter { newest_first: true, ..Default::default() }).await?;
    Ok(Json(Value::Array(docs.into_iter().map(|d| d.data).collect())))
}
async fn delete_alert(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    st.store.delete(KIND_ALERT, &id).await?;
    Ok(empty())
}

impl AppState {
    /// Run an alert's query and update its state.
    pub async fn evaluate_alert(self: &Arc<Self>, alert_id: &str) -> ApiResult<Value> {
        let a = self.store.require::<Value>(KIND_ALERT, alert_id, "Alert").await?.data;
        let qid = a["query_id"].as_str().ok_or_else(|| ApiError::invalid("alert has no query_id"))?;
        let q = self.store.require::<Value>(KIND_QUERY, qid, "Query").await?.data;
        let sql = q["query_text"].as_str().unwrap_or("").to_string();
        let owner = a["owner_user_name"].as_str().unwrap_or(&self.config.admin_user).to_string();
        let principal = match self.user_by_name(&owner).await? {
            Some(u) => self.principal_for_user(&u).await?,
            None => return Err(ApiError::InvalidState("alert owner no longer exists".into())),
        };
        let (cluster_id, wid) = self.resolve_compute(q["warehouse_id"].as_str(), None).await?;
        let res = self.execute_sql(&cluster_id, wid.as_deref(), &principal, &sql, HashMap::new(), 1000).await?;
        let cond = &a["condition"];
        let col = cond["operand"]["column"]["name"].as_str().unwrap_or("");
        let idx = res.columns.iter().position(|c| c.name == col).unwrap_or(0);
        let cell = res.rows.first().and_then(|r| r.get(idx)).cloned().flatten();
        let threshold = &cond["threshold"]["value"];
        let op = cond["op"].as_str().unwrap_or("GREATER_THAN");
        let triggered = match (cell, threshold.get("double_value").and_then(|v| v.as_f64())) {
            (Some(c), Some(t)) => {
                let cv: f64 = c.parse().unwrap_or(f64::NAN);
                match op {
                    "GREATER_THAN" => cv > t,
                    "GREATER_THAN_OR_EQUAL" => cv >= t,
                    "LESS_THAN" => cv < t,
                    "LESS_THAN_OR_EQUAL" => cv <= t,
                    "EQUAL" => cv == t,
                    "NOT_EQUAL" => cv != t,
                    _ => false,
                }
            }
            (Some(c), None) => {
                let t = threshold.get("string_value").and_then(|v| v.as_str()).map(str::to_string).or_else(|| threshold.get("bool_value").and_then(|v| v.as_bool()).map(|b| b.to_string())).unwrap_or_default();
                match op {
                    "EQUAL" => c == t,
                    "NOT_EQUAL" => c != t,
                    _ => c > t,
                }
            }
            (None, _) => false,
        };
        let state = if triggered { "TRIGGERED" } else { "OK" };
        let d = self
            .store
            .update::<Value, _>(KIND_ALERT, alert_id, "Alert", |v| {
                v["state"] = json!(state);
                v["last_evaluated_time"] = json!(chrono::Utc::now().to_rfc3339());
                if triggered {
                    v["trigger_time"] = json!(chrono::Utc::now().to_rfc3339());
                }
                Ok(())
            })
            .await?;
        Ok(d.data)
    }
}

async fn evaluate_alert_h(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.evaluate_alert(&id).await?))
}

// -------------------------------------------------------------- dashboards

async fn create_dashboard(State(st): State<S>, Who(p): Who, Body(mut o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    if let Some(Value::Object(inner)) = o.remove("dashboard") {
        for (k, v) in inner {
            o.insert(k, v);
        }
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let name = o.get("display_name").or(o.get("name")).and_then(|v| v.as_str()).unwrap_or("Untitled dashboard").to_string();
    o.insert("dashboard_id".into(), json!(id));
    o.insert("display_name".into(), json!(name));
    o.insert("create_time".into(), json!(now));
    o.insert("update_time".into(), json!(now));
    o.insert("lifecycle_state".into(), json!("ACTIVE"));
    o.insert("etag".into(), json!("1"));
    o.entry("serialized_dashboard").or_insert(json!("{\"pages\":[{\"name\":\"page1\",\"displayName\":\"Page 1\",\"layout\":[]}],\"datasets\":[]}"));
    o.entry("parent_path").or_insert(json!(format!("/Users/{}", p.user_name)));
    o.insert("path".into(), json!(format!("{}/{}.lvdash.json", o["parent_path"].as_str().unwrap_or(""), name)));
    o.insert("owner_user_name".into(), json!(p.user_name));
    let v = Value::Object(o);
    st.store.insert(KIND_DASHBOARD, st.ws(), &id, None, Some(&name), &v).await?;
    Ok(Json(v))
}

async fn list_dashboards(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_DASHBOARD, st.ws(), Filter { newest_first: true, ..Default::default() }).await?;
    Ok(Json(json!({ "dashboards": docs.into_iter().map(|d| d.data).collect::<Vec<_>>() })))
}
async fn get_dashboard(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_DASHBOARD, &id, "Dashboard").await?.data))
}
async fn update_dashboard(State(st): State<S>, Path(id): Path<String>, Body(mut o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    if let Some(Value::Object(inner)) = o.remove("dashboard") {
        for (k, v) in inner {
            o.insert(k, v);
        }
    }
    let d = st
        .store
        .update::<Value, _>(KIND_DASHBOARD, &id, "Dashboard", |v| {
            for (k, val) in &o {
                if matches!(k.as_str(), "display_name" | "serialized_dashboard" | "warehouse_id" | "parent_path") {
                    v[k] = val.clone();
                }
            }
            v["update_time"] = json!(chrono::Utc::now().to_rfc3339());
            let etag: u64 = v["etag"].as_str().and_then(|e| e.parse().ok()).unwrap_or(1);
            v["etag"] = json!((etag + 1).to_string());
            Ok(())
        })
        .await?;
    Ok(Json(d.data))
}
async fn delete_dashboard(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    st.store.delete(KIND_DASHBOARD, &id).await?;
    Ok(empty())
}
async fn publish_dashboard(State(st): State<S>, Path(id): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let d = st
        .store
        .update::<Value, _>(KIND_DASHBOARD, &id, "Dashboard", |v| {
            v["published"] = json!({ "embed_credentials": o.get("embed_credentials").cloned().unwrap_or(json!(true)), "warehouse_id": o.get("warehouse_id").cloned().unwrap_or(v["warehouse_id"].clone()), "revision_create_time": chrono::Utc::now().to_rfc3339(), "display_name": v["display_name"].clone() });
            Ok(())
        })
        .await?;
    Ok(Json(d.data["published"].clone()))
}
async fn get_published(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let d = st.store.require::<Value>(KIND_DASHBOARD, &id, "Dashboard").await?;
    d.data.get("published").cloned().map(Json).ok_or_else(|| ApiError::NotFound("dashboard is not published".into()))
}

// ----------------------------------------------------------------- history

#[derive(Debug, Deserialize)]
struct HistoryQ {
    #[serde(default)]
    max_results: Option<i64>,
    #[serde(default)]
    filter_by: Option<String>,
}

async fn query_history(State(st): State<S>, Query(q): Query<HistoryQ>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_HISTORY, st.ws(), Filter { newest_first: true, limit: q.max_results.or(Some(100)), ..Default::default() }).await?;
    let mut items: Vec<Value> = docs.into_iter().map(|d| d.data).collect();
    if let Some(f) = q.filter_by.as_deref().and_then(|f| serde_json::from_str::<Value>(f).ok()) {
        if let Some(ids) = f.get("warehouse_ids").and_then(|v| v.as_array()) {
            items.retain(|it| ids.contains(&it["warehouse_id"]));
        }
        if let Some(statuses) = f.get("statuses").and_then(|v| v.as_array()) {
            items.retain(|it| statuses.contains(&it["status"]));
        }
    }
    Ok(Json(json!({ "res": items, "has_next_page": false })))
}

pub fn router() -> Router<S> {
    Router::new()
        // Statement Execution API
        .route("/api/2.0/sql/statements", post(execute_statement))
        .route("/api/2.0/sql/statements/", post(execute_statement))
        .route("/api/2.0/sql/statements/{id}", get(get_statement).delete(close_statement))
        .route("/api/2.0/sql/statements/{id}/cancel", post(cancel_statement))
        .route("/api/2.0/sql/statements/{id}/result/chunks/{n}", get(get_chunk))
        // Warehouses
        .route("/api/2.0/sql/warehouses", get(list_warehouses).post(create_warehouse))
        .route("/api/2.0/sql/warehouses/{id}", get(get_warehouse).delete(delete_warehouse))
        .route("/api/2.0/sql/warehouses/{id}/edit", post(edit_warehouse))
        .route("/api/2.0/sql/warehouses/{id}/start", post(start_warehouse))
        .route("/api/2.0/sql/warehouses/{id}/stop", post(stop_warehouse))
        .route("/api/2.0/sql/config/warehouses", get(warehouse_config))
        .route("/api/2.0/sql/config/endpoints", get(warehouse_config))
        .route("/api/2.0/sql/endpoints", get(list_warehouses).post(create_warehouse))
        .route("/api/2.0/sql/endpoints/{id}", get(get_warehouse).delete(delete_warehouse))
        .route("/api/2.0/sql/endpoints/{id}/start", post(start_warehouse))
        .route("/api/2.0/sql/endpoints/{id}/stop", post(stop_warehouse))
        .route("/api/2.0/preview/sql/data_sources", get(data_sources))
        // Saved queries (new + legacy)
        .route("/api/2.0/sql/queries", get(list_queries).post(create_query))
        .route("/api/2.0/sql/queries/{id}", get(get_query).patch(update_query).delete(delete_query))
        .route("/api/2.0/preview/sql/queries", get(list_queries_legacy).post(create_query_legacy))
        .route("/api/2.0/preview/sql/queries/{id}", get(get_query_legacy).post(update_query_legacy).delete(delete_query))
        // Alerts
        .route("/api/2.0/sql/alerts", get(list_alerts).post(create_alert))
        .route("/api/2.0/sql/alerts/{id}", get(get_alert).patch(update_alert).delete(delete_alert))
        .route("/api/2.0/preview/sql/alerts", get(list_alerts_legacy).post(create_alert))
        .route("/api/2.0/preview/sql/alerts/{id}", get(get_alert).put(update_alert).delete(delete_alert))
        .route("/api/2.0/lakeforge/sql/alerts/{id}/evaluate", post(evaluate_alert_h))
        // Lakeview dashboards
        .route("/api/2.0/lakeview/dashboards", get(list_dashboards).post(create_dashboard))
        .route("/api/2.0/lakeview/dashboards/{id}", get(get_dashboard).patch(update_dashboard).delete(delete_dashboard))
        .route("/api/2.0/lakeview/dashboards/{id}/published", get(get_published).post(publish_dashboard))
        // History
        .route("/api/2.0/sql/history/queries", get(query_history))
}
