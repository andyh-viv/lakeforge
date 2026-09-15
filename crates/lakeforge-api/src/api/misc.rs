//! Smaller API families: workspace info/conf, instance pools, cluster
//! policies + policy families, global init scripts, IP access lists,
//! notification destinations, libraries, git credentials, settings, and a
//! simple generic-CRUD helper they share.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::{empty, Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};

pub const KIND_POOL: &str = "instance_pool";
pub const KIND_POLICY: &str = "cluster_policy";
pub const KIND_INIT_SCRIPT: &str = "global_init_script";
pub const KIND_IP_LIST: &str = "ip_access_list";
pub const KIND_NOTIFICATION_DEST: &str = "notification_destination";
pub const KIND_GIT_CRED: &str = "git_credential";
pub const KIND_LIBRARY: &str = "cluster_library";

// ---------------------------------------------------------------- info

async fn info(State(st): State<S>) -> Json<Value> {
    Json(json!({
        "name": "Lakeforge",
        "version": env!("CARGO_PKG_VERSION"),
        "workspace_id": st.ws(),
        "cloud": st.config.cloud,
        "public_url": st.config.public_url,
        "engine": { "name": "Forge", "sql": "DataFusion", "table_format": "Delta Lake" },
        "uptime_secs": (chrono::Utc::now() - st.started_at).num_seconds(),
        "features": ["clusters", "jobs", "workspace", "notebooks", "sql", "unity-catalog", "secrets", "dbfs", "files", "mlflow", "repos", "pipelines", "serving", "scim", "permissions", "command-execution"],
    }))
}

async fn workspace_status(State(st): State<S>) -> Json<Value> {
    Json(json!({ "workspace_id": st.ws(), "workspace_name": "lakeforge", "deployment_name": "lakeforge", "workspace_status": "RUNNING", "cloud": st.config.cloud, "pricing_tier": "PREMIUM" }))
}

// ------------------------------------------------------- workspace-conf

const DEFAULT_CONF: &[(&str, &str)] = &[
    ("enableTokensConfig", "true"),
    ("enableIpAccessLists", "false"),
    ("enableWebTerminal", "true"),
    ("enableDbfsFileBrowser", "true"),
    ("enableResultsDownloading", "true"),
    ("enableNotebookTableClipboard", "true"),
    ("enableExportNotebook", "true"),
    ("enableDcs", "false"),
    ("enableProjectTypeInWorkspace", "true"),
    ("maxTokenLifetimeDays", "0"),
    ("enableDeprecatedClusterNamedInitScripts", "false"),
    ("enableDeprecatedGlobalInitScripts", "false"),
    ("storeInteractiveNotebookResultsInCustomerAccount", "true"),
    ("enableVerboseAuditLogs", "false"),
    ("enforceUserIsolation", "false"),
];

#[derive(Debug, Deserialize)]
struct ConfQ {
    keys: String,
}

async fn get_conf(State(st): State<S>, Query(q): Query<ConfQ>) -> ApiResult<Json<Value>> {
    let mut out = Map::new();
    for k in q.keys.split(',').map(str::trim).filter(|k| !k.is_empty()) {
        let v = match st.store.kv_get(&format!("wsconf:{k}")).await? {
            Some(v) => Some(v),
            None => DEFAULT_CONF.iter().find(|(dk, _)| *dk == k).map(|(_, v)| v.to_string()),
        };
        out.insert(k.to_string(), v.map(Value::String).unwrap_or(Value::Null));
    }
    Ok(Json(Value::Object(out)))
}

async fn set_conf(State(st): State<S>, Who(p): Who, Body(b): Body<Map<String, Value>>) -> ApiResult<Response> {
    p.require_admin()?;
    for (k, v) in b {
        let s = match v {
            Value::String(s) => s,
            other => other.to_string(),
        };
        st.store.kv_set(&format!("wsconf:{k}"), &s).await?;
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

// ---------------------------------------------------------- instance pools

async fn pool_create(State(st): State<S>, Who(p): Who, Body(mut b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let id = uuid::Uuid::new_v4().simple().to_string();
    b.get("instance_pool_name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("instance_pool_name is required"))?;
    b.insert("instance_pool_id".into(), json!(id));
    b.insert("state".into(), json!("ACTIVE"));
    b.insert("status".into(), json!({}));
    b.insert("stats".into(), json!({ "used_count": 0, "idle_count": b.get("min_idle_instances").cloned().unwrap_or(json!(0)), "pending_used_count": 0, "pending_idle_count": 0 }));
    b.insert("default_tags".into(), json!({ "Vendor": "Lakeforge", "DatabricksInstancePoolCreatorId": p.user_id, "DatabricksInstancePoolId": id }));
    let name = b["instance_pool_name"].as_str().unwrap_or("").to_string();
    st.store.insert(KIND_POOL, st.ws(), &id, None, Some(&name), &Value::Object(b)).await?;
    Ok(Json(json!({ "instance_pool_id": id })))
}

async fn pool_edit(State(st): State<S>, Body(b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let id = b.get("instance_pool_id").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("instance_pool_id is required"))?.to_string();
    st.store.update::<Value, _>(KIND_POOL, &id, "Instance pool", |v| {
        if let Some(o) = v.as_object_mut() {
            for (k, val) in b {
                o.insert(k, val);
            }
        }
        Ok(())
    }).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct PoolIdQ {
    instance_pool_id: String,
}

async fn pool_get(State(st): State<S>, Query(q): Query<PoolIdQ>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_POOL, &q.instance_pool_id, "Instance pool").await?.data))
}

async fn pool_delete(State(st): State<S>, Body(b): Body<PoolIdQ>) -> ApiResult<Json<Value>> {
    st.store.delete(KIND_POOL, &b.instance_pool_id).await?;
    Ok(empty())
}

async fn pool_list(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_POOL, st.ws(), Filter::default()).await?;
    Ok(Json(json!({ "instance_pools": docs.iter().map(|d| &d.data).collect::<Vec<_>>() })))
}

// -------------------------------------------------------- cluster policies

fn policy_families() -> Vec<Value> {
    vec![
        json!({ "policy_family_id": "personal-vm", "name": "Personal Compute", "description": "Single-node compute for individual users.", "definition": "{\"spark_conf.spark.databricks.cluster.profile\":{\"type\":\"fixed\",\"value\":\"singleNode\",\"hidden\":true},\"num_workers\":{\"type\":\"fixed\",\"value\":0,\"hidden\":true},\"node_type_id\":{\"type\":\"allowlist\",\"values\":[\"lf.small\",\"lf.medium\"]},\"autotermination_minutes\":{\"type\":\"range\",\"maxValue\":120,\"defaultValue\":60}}" }),
        json!({ "policy_family_id": "shared-data-science", "name": "Shared Compute", "description": "Autoscaling shared clusters for interactive work.", "definition": "{\"autoscale.min_workers\":{\"type\":\"range\",\"minValue\":1,\"maxValue\":4,\"defaultValue\":1},\"autoscale.max_workers\":{\"type\":\"range\",\"minValue\":1,\"maxValue\":16,\"defaultValue\":4},\"autotermination_minutes\":{\"type\":\"range\",\"maxValue\":240,\"defaultValue\":120}}" }),
        json!({ "policy_family_id": "job-cluster", "name": "Job Compute", "description": "Clusters for scheduled workflows.", "definition": "{\"cluster_type\":{\"type\":\"fixed\",\"value\":\"job\"},\"autotermination_minutes\":{\"type\":\"fixed\",\"value\":0,\"hidden\":true}}" }),
        json!({ "policy_family_id": "power-user", "name": "Power User Compute", "description": "Large clusters for power users.", "definition": "{\"num_workers\":{\"type\":\"range\",\"maxValue\":32}}" }),
    ]
}

async fn policy_families_list() -> Json<Value> {
    Json(json!({ "policy_families": policy_families() }))
}

async fn policy_family_get(Path(id): Path<String>) -> ApiResult<Json<Value>> {
    policy_families().into_iter().find(|f| f["policy_family_id"] == id).map(Json).ok_or_else(|| ApiError::NotFound(format!("Policy family {id} not found")))
}

async fn policy_create(State(st): State<S>, Who(p): Who, Body(mut b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let name = b.get("name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("name is required"))?.to_string();
    if let Some(fam) = b.get("policy_family_id").and_then(|v| v.as_str()) {
        let fam = policy_families().into_iter().find(|f| f["policy_family_id"] == fam).ok_or_else(|| ApiError::invalid(format!("Unknown policy_family_id {fam}")))?;
        let mut def: Map<String, Value> = serde_json::from_str(fam["definition"].as_str().unwrap_or("{}")).unwrap_or_default();
        if let Some(ov) = b.get("policy_family_definition_overrides").and_then(|v| v.as_str()) {
            let ovm: Map<String, Value> = serde_json::from_str(ov).map_err(|e| ApiError::invalid(format!("Invalid overrides: {e}")))?;
            def.extend(ovm);
        }
        b.insert("definition".into(), json!(serde_json::to_string(&def)?));
    }
    let def = b.get("definition").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("definition (JSON string) or policy_family_id is required"))?;
    serde_json::from_str::<Map<String, Value>>(def).map_err(|e| ApiError::invalid(format!("definition must be a JSON object string: {e}")))?;
    let id = uuid::Uuid::new_v4().simple().to_string();
    b.insert("policy_id".into(), json!(id));
    b.insert("created_at_timestamp".into(), json!(now_ms()));
    b.insert("creator_user_name".into(), json!(p.user_name));
    b.insert("is_default".into(), json!(false));
    st.store.insert(KIND_POLICY, st.ws(), &id, None, Some(&name), &Value::Object(b)).await?;
    Ok(Json(json!({ "policy_id": id })))
}

async fn policy_edit(State(st): State<S>, Who(p): Who, Body(b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let id = b.get("policy_id").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("policy_id is required"))?.to_string();
    if let Some(def) = b.get("definition").and_then(|v| v.as_str()) {
        serde_json::from_str::<Map<String, Value>>(def).map_err(|e| ApiError::invalid(format!("definition must be a JSON object string: {e}")))?;
    }
    st.store.update::<Value, _>(KIND_POLICY, &id, "Cluster policy", |v| {
        if let Some(o) = v.as_object_mut() {
            for (k, val) in b {
                o.insert(k, val);
            }
        }
        Ok(())
    }).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct PolicyIdQ {
    policy_id: String,
}

async fn policy_get(State(st): State<S>, Query(q): Query<PolicyIdQ>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_POLICY, &q.policy_id, "Cluster policy").await?.data))
}

async fn policy_delete(State(st): State<S>, Who(p): Who, Body(b): Body<PolicyIdQ>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    st.store.delete(KIND_POLICY, &b.policy_id).await?;
    Ok(empty())
}

async fn policy_list(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_POLICY, st.ws(), Filter::default()).await?;
    Ok(Json(json!({ "policies": docs.iter().map(|d| &d.data).collect::<Vec<_>>(), "total_count": docs.len() })))
}

impl AppState {
    /// Validate a cluster spec against a policy definition (fixed / allowlist /
    /// blocklist / range / regex / forbidden / unlimited).
    pub async fn apply_cluster_policy(&self, policy_id: &str, spec: &mut Map<String, Value>) -> ApiResult<()> {
        let pol = self.store.require::<Value>(KIND_POLICY, policy_id, "Cluster policy").await?;
        let def: Map<String, Value> = serde_json::from_str(pol.data["definition"].as_str().unwrap_or("{}")).unwrap_or_default();
        for (path, rule) in def {
            let ty = rule["type"].as_str().unwrap_or("unlimited");
            let current = get_path(spec, &path);
            match ty {
                "fixed" => set_path(spec, &path, rule["value"].clone()),
                "forbidden" => {
                    if current.is_some() {
                        return Err(ApiError::invalid(format!("Cluster policy violation: attribute {path} is forbidden")));
                    }
                }
                "allowlist" => {
                    let vals = rule["values"].as_array().cloned().unwrap_or_default();
                    match current {
                        Some(v) if !vals.iter().any(|x| x == &v) => return Err(ApiError::invalid(format!("Cluster policy violation: {path}={v} not in allowlist"))),
                        None => {
                            if let Some(d) = rule.get("defaultValue") {
                                set_path(spec, &path, d.clone());
                            }
                        }
                        _ => {}
                    }
                }
                "blocklist" => {
                    let vals = rule["values"].as_array().cloned().unwrap_or_default();
                    if let Some(v) = current {
                        if vals.iter().any(|x| x == &v) {
                            return Err(ApiError::invalid(format!("Cluster policy violation: {path}={v} is blocked")));
                        }
                    }
                }
                "range" => match current.as_ref().and_then(|v| v.as_f64()) {
                    Some(n) => {
                        if let Some(min) = rule["minValue"].as_f64() {
                            if n < min {
                                return Err(ApiError::invalid(format!("Cluster policy violation: {path}={n} below minimum {min}")));
                            }
                        }
                        if let Some(max) = rule["maxValue"].as_f64() {
                            if n > max {
                                return Err(ApiError::invalid(format!("Cluster policy violation: {path}={n} above maximum {max}")));
                            }
                        }
                    }
                    None => {
                        if let Some(d) = rule.get("defaultValue") {
                            set_path(spec, &path, d.clone());
                        }
                    }
                },
                "regex" => {
                    if let (Some(v), Some(pat)) = (current.as_ref().and_then(|v| v.as_str()), rule["pattern"].as_str()) {
                        let re = regex::Regex::new(pat).map_err(|e| ApiError::invalid(format!("bad policy regex: {e}")))?;
                        if !re.is_match(v) {
                            return Err(ApiError::invalid(format!("Cluster policy violation: {path}={v} does not match {pat}")));
                        }
                    }
                }
                _ => {
                    if current.is_none() {
                        if let Some(d) = rule.get("defaultValue") {
                            set_path(spec, &path, d.clone());
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn get_path(m: &Map<String, Value>, path: &str) -> Option<Value> {
    let mut cur = Value::Object(m.clone());
    for seg in path.split('.') {
        cur = cur.get(seg)?.clone();
    }
    Some(cur)
}

fn set_path(m: &mut Map<String, Value>, path: &str, v: Value) {
    let segs: Vec<&str> = path.split('.').collect();
    if segs.len() == 1 {
        m.insert(path.to_string(), v);
        return;
    }
    let entry = m.entry(segs[0].to_string()).or_insert(json!({}));
    if let Some(inner) = entry.as_object_mut() {
        set_path(inner, &segs[1..].join("."), v);
    }
}

// ------------------------------------------------------ global init scripts

async fn init_create(State(st): State<S>, Who(p): Who, Body(mut b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let name = b.get("name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("name is required"))?.to_string();
    b.get("script").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("script (base64) is required"))?;
    let id = uuid::Uuid::new_v4().simple().to_string();
    let n: i64 = st.store.count(KIND_INIT_SCRIPT, st.ws(), None).await?;
    b.insert("script_id".into(), json!(id));
    b.entry("enabled").or_insert(json!(true));
    b.entry("position").or_insert(json!(n));
    b.insert("created_at".into(), json!(now_ms()));
    b.insert("created_by".into(), json!(p.user_name));
    st.store.insert(KIND_INIT_SCRIPT, st.ws(), &id, None, Some(&name), &Value::Object(b)).await?;
    Ok(Json(json!({ "script_id": id })))
}

async fn init_list(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_INIT_SCRIPT, st.ws(), Filter::default()).await?;
    let mut items: Vec<Value> = docs.into_iter().map(|d| {
        let mut v = d.data;
        if let Some(o) = v.as_object_mut() {
            o.remove("script");
        }
        v
    }).collect();
    items.sort_by_key(|v| v["position"].as_i64().unwrap_or(0));
    Ok(Json(json!({ "scripts": items })))
}

async fn init_get(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_INIT_SCRIPT, &id, "Global init script").await?.data))
}

async fn init_update(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    st.store.update::<Value, _>(KIND_INIT_SCRIPT, &id, "Global init script", |v| {
        if let Some(o) = v.as_object_mut() {
            for (k, val) in b {
                o.insert(k, val);
            }
            o.insert("updated_at".into(), json!(now_ms()));
            o.insert("updated_by".into(), json!(p.user_name));
        }
        Ok(())
    }).await?;
    Ok(empty())
}

async fn init_delete(State(st): State<S>, Who(p): Who, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    st.store.delete(KIND_INIT_SCRIPT, &id).await?;
    Ok(empty())
}

// ----------------------------------------------------------- IP access lists

async fn ip_create(State(st): State<S>, Who(p): Who, Body(mut b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let label = b.get("label").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("label is required"))?.to_string();
    let ty = b.get("list_type").and_then(|v| v.as_str()).unwrap_or("ALLOW");
    if !matches!(ty, "ALLOW" | "BLOCK") {
        return Err(ApiError::invalid("list_type must be ALLOW or BLOCK"));
    }
    let id = uuid::Uuid::new_v4().to_string();
    b.insert("list_id".into(), json!(id));
    b.entry("enabled").or_insert(json!(true));
    b.insert("created_at".into(), json!(now_ms()));
    b.insert("created_by".into(), json!(p.user_id));
    b.insert("address_count".into(), json!(b.get("ip_addresses").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0)));
    st.store.insert(KIND_IP_LIST, st.ws(), &id, None, Some(&label), &Value::Object(b.clone())).await?;
    Ok(Json(json!({ "ip_access_list": b })))
}

async fn ip_list(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_IP_LIST, st.ws(), Filter::default()).await?;
    Ok(Json(json!({ "ip_access_lists": docs.iter().map(|d| &d.data).collect::<Vec<_>>() })))
}

async fn ip_get(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "ip_access_list": st.store.require::<Value>(KIND_IP_LIST, &id, "IP access list").await?.data })))
}

async fn ip_update(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    st.store.update::<Value, _>(KIND_IP_LIST, &id, "IP access list", |v| {
        if let Some(o) = v.as_object_mut() {
            for (k, val) in b {
                o.insert(k, val);
            }
            o.insert("updated_at".into(), json!(now_ms()));
            let n = o.get("ip_addresses").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
            o.insert("address_count".into(), json!(n));
        }
        Ok(())
    }).await?;
    Ok(empty())
}

async fn ip_delete(State(st): State<S>, Who(p): Who, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    st.store.delete(KIND_IP_LIST, &id).await?;
    Ok(empty())
}

// ------------------------------------------------- notification destinations

async fn dest_create(State(st): State<S>, Who(p): Who, Body(mut b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let id = uuid::Uuid::new_v4().to_string();
    let name = b.get("display_name").and_then(|v| v.as_str()).unwrap_or("destination").to_string();
    let ty = b.get("config").and_then(|c| c.as_object()).and_then(|c| c.keys().next().cloned()).map(|k| k.to_ascii_uppercase()).unwrap_or_else(|| "EMAIL".into());
    b.insert("id".into(), json!(id));
    b.insert("destination_type".into(), json!(ty));
    st.store.insert(KIND_NOTIFICATION_DEST, st.ws(), &id, None, Some(&name), &Value::Object(b.clone())).await?;
    Ok(Json(Value::Object(b)))
}

async fn dest_list(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_NOTIFICATION_DEST, st.ws(), Filter::default()).await?;
    Ok(Json(json!({ "results": docs.iter().map(|d| json!({ "id": d.data["id"], "display_name": d.data["display_name"], "destination_type": d.data["destination_type"] })).collect::<Vec<_>>() })))
}

async fn dest_get(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_NOTIFICATION_DEST, &id, "Notification destination").await?.data))
}

async fn dest_update(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let doc = st.store.update::<Value, _>(KIND_NOTIFICATION_DEST, &id, "Notification destination", |v| {
        if let Some(o) = v.as_object_mut() {
            for (k, val) in b {
                o.insert(k, val);
            }
        }
        Ok(())
    }).await?;
    Ok(Json(doc.data))
}

async fn dest_delete(State(st): State<S>, Who(p): Who, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    st.store.delete(KIND_NOTIFICATION_DEST, &id).await?;
    Ok(empty())
}

// ---------------------------------------------------------------- libraries

#[derive(Debug, Deserialize)]
struct LibBody {
    cluster_id: String,
    #[serde(default)]
    libraries: Vec<Value>,
}

fn lib_key(l: &Value) -> String {
    l.to_string()
}

async fn lib_install(State(st): State<S>, Body(b): Body<LibBody>) -> ApiResult<Json<Value>> {
    st.get_cluster(&b.cluster_id).await?;
    let mut cur: Vec<Value> = st.store.get::<Value>(KIND_LIBRARY, &b.cluster_id).await?.and_then(|d| d.data["libraries"].as_array().cloned()).unwrap_or_default();
    for l in b.libraries {
        if !cur.iter().any(|c| lib_key(&c["library"]) == lib_key(&l)) {
            let status = if l.get("pypi").is_some() || l.get("requirements").is_some() { "PENDING" } else { "INSTALLED" };
            cur.push(json!({ "library": l, "status": status, "is_library_for_all_clusters": false, "messages": [] }));
        }
    }
    st.store.upsert(KIND_LIBRARY, st.ws(), &b.cluster_id, Some(&b.cluster_id), None, &json!({ "libraries": cur })).await?;
    st.install_cluster_libraries(&b.cluster_id).await;
    Ok(empty())
}

async fn lib_uninstall(State(st): State<S>, Body(b): Body<LibBody>) -> ApiResult<Json<Value>> {
    let mut cur: Vec<Value> = st.store.get::<Value>(KIND_LIBRARY, &b.cluster_id).await?.and_then(|d| d.data["libraries"].as_array().cloned()).unwrap_or_default();
    for l in &b.libraries {
        for c in cur.iter_mut() {
            if lib_key(&c["library"]) == lib_key(l) {
                c["status"] = json!("UNINSTALL_ON_RESTART");
            }
        }
    }
    st.store.upsert(KIND_LIBRARY, st.ws(), &b.cluster_id, Some(&b.cluster_id), None, &json!({ "libraries": cur })).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct ClusterIdQ {
    cluster_id: String,
}

async fn lib_status(State(st): State<S>, Query(q): Query<ClusterIdQ>) -> ApiResult<Json<Value>> {
    let cur: Vec<Value> = st.store.get::<Value>(KIND_LIBRARY, &q.cluster_id).await?.and_then(|d| d.data["libraries"].as_array().cloned()).unwrap_or_default();
    Ok(Json(json!({ "cluster_id": q.cluster_id, "library_statuses": cur })))
}

async fn lib_all_status(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_LIBRARY, st.ws(), Filter::default()).await?;
    Ok(Json(json!({ "statuses": docs.iter().map(|d| json!({ "cluster_id": d.id, "library_statuses": d.data["libraries"] })).collect::<Vec<_>>() })))
}

impl AppState {
    /// Install pending pip libraries into the cluster's kernel environment.
    pub async fn install_cluster_libraries(&self, cluster_id: &str) {
        let Ok(Some(doc)) = self.store.get::<Value>(KIND_LIBRARY, cluster_id).await else { return };
        let mut libs = doc.data["libraries"].as_array().cloned().unwrap_or_default();
        let mut changed = false;
        for l in libs.iter_mut() {
            if l["status"] != "PENDING" {
                continue;
            }
            let pkg = l["library"]["pypi"]["package"].as_str().map(|s| s.to_string());
            let req = l["library"]["requirements"].as_str().map(|s| s.to_string());
            let mut cmd = tokio::process::Command::new(&self.config.python);
            cmd.arg("-m").arg("pip").arg("install").arg("--quiet").arg("--disable-pip-version-check");
            if let Some(p) = &pkg {
                cmd.arg(p);
            } else if let Some(r) = &req {
                cmd.arg("-r").arg(r.trim_start_matches("dbfs:"));
            } else {
                continue;
            }
            if let Some(idx) = l["library"]["pypi"]["repo"].as_str() {
                cmd.arg("--index-url").arg(idx);
            }
            l["status"] = json!("INSTALLING");
            let out = tokio::time::timeout(std::time::Duration::from_secs(600), cmd.output()).await;
            match out {
                Ok(Ok(o)) if o.status.success() => l["status"] = json!("INSTALLED"),
                Ok(Ok(o)) => {
                    l["status"] = json!("FAILED");
                    l["messages"] = json!([String::from_utf8_lossy(&o.stderr).chars().take(2000).collect::<String>()]);
                }
                Ok(Err(e)) => {
                    l["status"] = json!("FAILED");
                    l["messages"] = json!([e.to_string()]);
                }
                Err(_) => {
                    l["status"] = json!("FAILED");
                    l["messages"] = json!(["pip install timed out"]);
                }
            }
            changed = true;
        }
        if changed {
            let _ = self.store.upsert(KIND_LIBRARY, self.ws(), cluster_id, Some(cluster_id), None, &json!({ "libraries": libs })).await;
        }
    }
}

// ----------------------------------------------------------- git credentials

async fn git_create(State(st): State<S>, Who(p): Who, Body(mut b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let provider = b.get("git_provider").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("git_provider is required"))?.to_string();
    let token = b.remove("personal_access_token").and_then(|v| v.as_str().map(|s| s.to_string()));
    let id = st.store.next_seq("git_credential_id").await?;
    b.insert("credential_id".into(), json!(id));
    b.insert("owner".into(), json!(p.user_name));
    let sealed = token.map(|t| st.auth.sealer().seal(t.as_bytes())).transpose()?;
    if let Some(s) = sealed {
        st.store.kv_set(&format!("gitcred:{id}"), &s).await?;
    }
    st.store.insert(KIND_GIT_CRED, st.ws(), &id.to_string(), Some(&p.user_name), Some(&provider), &Value::Object(b.clone())).await?;
    Ok(Json(Value::Object(b)))
}

async fn git_list(State(st): State<S>, Who(p): Who) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_GIT_CRED, st.ws(), Filter { parent_id: Some(&p.user_name), ..Default::default() }).await?;
    Ok(Json(json!({ "credentials": docs.iter().map(|d| &d.data).collect::<Vec<_>>() })))
}

async fn git_get(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_GIT_CRED, &id, "Git credential").await?.data))
}

async fn git_update(State(st): State<S>, Path(id): Path<String>, Body(mut b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    if let Some(t) = b.remove("personal_access_token").and_then(|v| v.as_str().map(|s| s.to_string())) {
        st.store.kv_set(&format!("gitcred:{id}"), &st.auth.sealer().seal(t.as_bytes())?).await?;
    }
    st.store.update::<Value, _>(KIND_GIT_CRED, &id, "Git credential", |v| {
        if let Some(o) = v.as_object_mut() {
            for (k, val) in b {
                o.insert(k, val);
            }
        }
        Ok(())
    }).await?;
    Ok(empty())
}

async fn git_delete(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    st.store.delete(KIND_GIT_CRED, &id).await?;
    st.store.kv_delete(&format!("gitcred:{id}")).await?;
    Ok(empty())
}

impl AppState {
    /// Resolve a git token for a user + provider (first matching credential).
    pub async fn git_token_for(&self, p: &Principal, provider: Option<&str>) -> ApiResult<Option<(String, String)>> {
        let docs: Vec<Doc<Value>> = self.store.list(KIND_GIT_CRED, self.ws(), Filter { parent_id: Some(&p.user_name), ..Default::default() }).await?;
        for d in docs {
            if provider.map(|pr| d.data["git_provider"].as_str().map(|g| g.eq_ignore_ascii_case(pr)).unwrap_or(false)).unwrap_or(true) {
                if let Some(sealed) = self.store.kv_get(&format!("gitcred:{}", d.id)).await? {
                    let tok = String::from_utf8_lossy(&self.auth.sealer().open(&sealed)?).to_string();
                    let user = d.data["git_username"].as_str().unwrap_or("token").to_string();
                    return Ok(Some((user, tok)));
                }
            }
        }
        Ok(None)
    }
}

// ------------------------------------------------------------------ settings

async fn setting_get(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    let v = st.store.kv_get(&format!("setting:{name}")).await?;
    let default = match name.as_str() {
        "default_namespace" => json!({ "namespace": { "value": "main" }, "etag": "0", "setting_name": "default" }),
        "automatic_cluster_update" => json!({ "automatic_cluster_update_workspace": { "enabled": false }, "etag": "0" }),
        "compliance_security_profile" => json!({ "compliance_security_profile_workspace": { "is_enabled": false, "compliance_standards": [] }, "etag": "0" }),
        "enhanced_security_monitoring" => json!({ "enhanced_security_monitoring_workspace": { "is_enabled": false }, "etag": "0" }),
        "restrict_workspace_admins" => json!({ "restrict_workspace_admins": { "status": "ALLOW_ALL" }, "etag": "0" }),
        _ => json!({ "etag": "0" }),
    };
    Ok(Json(v.and_then(|s| serde_json::from_str(&s).ok()).unwrap_or(default)))
}

async fn setting_patch(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let mut setting = b.get("setting").cloned().unwrap_or(b);
    setting["etag"] = json!(now_ms().to_string());
    st.store.kv_set(&format!("setting:{name}"), &setting.to_string()).await?;
    Ok(Json(setting))
}

async fn setting_delete(State(st): State<S>, Who(p): Who, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    st.store.kv_delete(&format!("setting:{name}")).await?;
    Ok(Json(json!({ "etag": now_ms().to_string() })))
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/lakeforge/info", get(info))
        .route("/api/2.0/lakeforge/workspace-status", get(workspace_status))
        .route("/api/2.0/workspace-conf", get(get_conf).patch(set_conf))
        .route("/api/2.0/instance-pools/create", post(pool_create))
        .route("/api/2.0/instance-pools/edit", post(pool_edit))
        .route("/api/2.0/instance-pools/get", get(pool_get))
        .route("/api/2.0/instance-pools/delete", post(pool_delete))
        .route("/api/2.0/instance-pools/list", get(pool_list))
        .route("/api/2.0/policy-families", get(policy_families_list))
        .route("/api/2.0/policy-families/{id}", get(policy_family_get))
        .route("/api/2.0/policies/clusters/create", post(policy_create))
        .route("/api/2.0/policies/clusters/edit", post(policy_edit))
        .route("/api/2.0/policies/clusters/get", get(policy_get))
        .route("/api/2.0/policies/clusters/delete", post(policy_delete))
        .route("/api/2.0/policies/clusters/list", get(policy_list))
        .route("/api/2.0/global-init-scripts", get(init_list).post(init_create))
        .route("/api/2.0/global-init-scripts/{id}", get(init_get).patch(init_update).delete(init_delete))
        .route("/api/2.0/ip-access-lists", get(ip_list).post(ip_create))
        .route("/api/2.0/ip-access-lists/{id}", get(ip_get).patch(ip_update).put(ip_update).delete(ip_delete))
        .route("/api/2.0/notification-destinations", get(dest_list).post(dest_create))
        .route("/api/2.0/notification-destinations/{id}", get(dest_get).patch(dest_update).delete(dest_delete))
        .route("/api/2.0/libraries/install", post(lib_install))
        .route("/api/2.0/libraries/uninstall", post(lib_uninstall))
        .route("/api/2.0/libraries/cluster-status", get(lib_status))
        .route("/api/2.0/libraries/all-cluster-statuses", get(lib_all_status))
        .route("/api/2.0/git-credentials", get(git_list).post(git_create))
        .route("/api/2.0/git-credentials/{id}", get(git_get).patch(git_update).delete(git_delete))
        .route("/api/2.0/settings/types/{name}/names/default", get(setting_get).patch(setting_patch).delete(setting_delete))
        .route("/api/2.0/lakeforge/settings/{name}", get(setting_get).patch(setting_patch))
}
