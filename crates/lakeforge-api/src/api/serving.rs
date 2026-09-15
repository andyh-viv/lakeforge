//! Model Serving — `/api/2.0/serving-endpoints`.
//!
//! An endpoint serves one or more *served entities*: registered MLflow model
//! versions (loaded into a dedicated Python worker process from the model's
//! artifacts) or *external models* (OpenAI-compatible providers proxied with a
//! key taken from a secret scope). Traffic is split by `traffic_config`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::{empty, Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::kernel::KernelEvent;
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};

pub const KIND_ENDPOINT: &str = "serving_endpoint";

const LOADER: &str = r#"
import json, os, sys, glob
def __lf_load_model(path):
    try:
        import mlflow.pyfunc
        return ("pyfunc", mlflow.pyfunc.load_model(path))
    except Exception as e:  # mlflow missing or flavor unsupported -> pickle fallback
        for name in ("model.pkl", "model.joblib", "model.pickle", "python_model.pkl"):
            hits = glob.glob(os.path.join(path, "**", name), recursive=True)
            if hits:
                try:
                    import joblib
                    return ("raw", joblib.load(hits[0]))
                except Exception:
                    import pickle
                    with open(hits[0], "rb") as f:
                        return ("raw", pickle.load(f))
        raise RuntimeError(f"cannot load model at {path}: {e}")

def __lf_to_input(payload):
    if "dataframe_records" in payload:
        try:
            import pandas as pd
            return pd.DataFrame.from_records(payload["dataframe_records"])
        except ImportError:
            return payload["dataframe_records"]
    if "dataframe_split" in payload:
        s = payload["dataframe_split"]
        try:
            import pandas as pd
            return pd.DataFrame(s.get("data", []), columns=s.get("columns"), index=s.get("index"))
        except ImportError:
            return s.get("data", [])
    if "instances" in payload:
        return payload["instances"]
    if "inputs" in payload:
        return payload["inputs"]
    return payload

def __lf_to_json(pred):
    try:
        import numpy as np
        if isinstance(pred, np.ndarray):
            return pred.tolist()
    except ImportError:
        pass
    try:
        import pandas as pd
        if isinstance(pred, pd.DataFrame):
            return pred.to_dict(orient="records")
        if isinstance(pred, pd.Series):
            return pred.tolist()
    except ImportError:
        pass
    if hasattr(pred, "tolist"):
        return pred.tolist()
    return pred

def __lf_predict(model, payload):
    kind, m = model
    x = __lf_to_input(payload)
    params = payload.get("params")
    if kind == "pyfunc":
        try:
            out = m.predict(x, params=params) if params is not None else m.predict(x)
        except TypeError:
            out = m.predict(x)
    else:
        out = m.predict(x)
    return {"predictions": __lf_to_json(out)}
"#;

fn json_escape(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())
}

impl AppState {
    fn serving_ctx(name: &str, entity: &str) -> String {
        format!("serving:{name}:{entity}")
    }

    async fn download_model_artifacts(&self, uri: &str, dir: &std::path::Path) -> ApiResult<usize> {
        let root = self.artifact_storage_path(uri).await?;
        let entries = self.storage.list_all(&root).await?;
        if entries.is_empty() {
            return Err(ApiError::invalid(format!("model artifacts not found at {uri}")));
        }
        tokio::fs::create_dir_all(dir).await.map_err(ApiError::internal)?;
        for e in &entries {
            let rel = e.path.strip_prefix(&root).unwrap_or(&e.path).trim_start_matches('/');
            let target = dir.join(rel);
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(ApiError::internal)?;
            }
            let bytes = self.storage.get(&e.path).await?;
            tokio::fs::write(&target, bytes).await.map_err(ApiError::internal)?;
        }
        Ok(entries.len())
    }

    /// Start (or restart) the Python worker for a served model entity.
    async fn load_served_entity(self: &Arc<Self>, p: &Principal, endpoint: &str, entity: &Value) -> ApiResult<()> {
        let ename = entity["name"].as_str().unwrap_or("").to_string();
        let model = entity["entity_name"].as_str().or(entity["model_name"].as_str()).ok_or_else(|| ApiError::invalid("served entity requires entity_name"))?;
        let version = entity["entity_version"].as_str().or(entity["model_version"].as_str()).unwrap_or("latest");
        let mv = self.resolve_model_version(model, version).await?;
        let dir = std::path::PathBuf::from(&self.config.work_dir).join("serving").join(endpoint).join(&ename);
        let _ = tokio::fs::remove_dir_all(&dir).await;
        self.download_model_artifacts(&mv.source, &dir).await?;
        let ctx = Self::serving_ctx(endpoint, &ename);
        self.kernels.stop(&ctx).await;
        let token = self.auth.issue_jwt(&p.user_id, &p.user_name, 30 * 24 * 3600)?;
        let mut env = HashMap::new();
        env.insert("LAKEFORGE_TOKEN".into(), token);
        env.insert("LAKEFORGE_USER".into(), p.user_name.clone());
        env.insert("LAKEFORGE_SERVING_ENDPOINT".into(), endpoint.to_string());
        if let Some(vars) = entity["environment_vars"].as_object() {
            for (k, v) in vars {
                let val = v.as_str().unwrap_or("").to_string();
                env.insert(k.clone(), self.resolve_secret_ref(&val).await.unwrap_or(val));
            }
        }
        self.kernels.start(&ctx, env).await?;
        let code = format!("{LOADER}\n__lf_model = __lf_load_model({})\nprint('loaded')", json_escape(&dir.to_string_lossy()));
        let (ok, err) = self.kernel_exec(&ctx, &code, Duration::from_secs(600)).await?;
        if !ok {
            self.kernels.stop(&ctx).await;
            return Err(ApiError::invalid(format!("failed to load model {model}/{}: {err}", mv.version)));
        }
        Ok(())
    }

    async fn kernel_exec(&self, ctx: &str, code: &str, timeout: Duration) -> ApiResult<(bool, String)> {
        let cmd_id = uuid::Uuid::new_v4().simple().to_string();
        let mut rx = self.kernels.execute(ctx, &cmd_id, "python", code).await?;
        let mut out = String::new();
        let mut err = String::new();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let ev = match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(ev)) => ev,
                Ok(None) => break,
                Err(_) => {
                    let _ = self.kernels.interrupt(ctx, &cmd_id).await;
                    return Err(ApiError::Unavailable("model worker timed out".into()));
                }
            };
            match ev {
                KernelEvent::Stdout { text } => out.push_str(&text),
                KernelEvent::Stderr { text } => err.push_str(&text),
                KernelEvent::Result { text } => out.push_str(&text),
                KernelEvent::Error { ename, evalue, .. } => {
                    err.push_str(&format!("{ename}: {evalue}"));
                }
                KernelEvent::Done { status } => {
                    return Ok((status == "ok" && err.is_empty(), if err.is_empty() { out } else { err }));
                }
                _ => {}
            }
        }
        Err(ApiError::Unavailable("model worker exited".into()))
    }

    /// Resolve `{{secrets/scope/key}}` references.
    pub async fn resolve_secret_ref(&self, s: &str) -> Option<String> {
        let inner = s.trim().strip_prefix("{{")?.strip_suffix("}}")?.trim();
        let rest = inner.strip_prefix("secrets/")?;
        let (scope, key) = rest.split_once('/')?;
        self.get_secret(scope, key).await.ok().flatten().and_then(|b| String::from_utf8(b).ok())
    }

    async fn endpoint_doc(&self, name: &str) -> ApiResult<Doc<Value>> {
        self.store.get::<Value>(KIND_ENDPOINT, name).await?.ok_or_else(|| ApiError::NotFound(format!("Endpoint with name '{name}' does not exist.")))
    }

    async fn set_endpoint_state(&self, name: &str, ready: &str, config_update: &str, msg: Option<&str>) {
        let _ = self.store.update::<Value, _>(KIND_ENDPOINT, name, "Endpoint", |e| {
            e["state"] = json!({ "ready": ready, "config_update": config_update });
            if let Some(m) = msg {
                e["state"]["message"] = json!(m);
            }
            e["last_updated_timestamp"] = json!(now_ms());
            Ok(())
        }).await;
    }

    /// Apply `config` to the endpoint: load every served model entity, then flip to READY.
    fn spawn_endpoint_update(self: &Arc<Self>, p: Principal, name: String, config: Value) {
        let st = Arc::clone(self);
        tokio::spawn(async move {
            st.set_endpoint_state(&name, "NOT_READY", "IN_PROGRESS", None).await;
            let mut entities: Vec<Value> = config["served_entities"].as_array().cloned().unwrap_or_default();
            entities.extend(config["served_models"].as_array().cloned().unwrap_or_default());
            let mut failure: Option<String> = None;
            for e in &entities {
                if e.get("external_model").is_some() || e.get("foundation_model").is_some() {
                    continue;
                }
                if let Err(err) = st.load_served_entity(&p, &name, e).await {
                    failure = Some(err.to_string());
                    break;
                }
            }
            match failure {
                None => {
                    let _ = st.store.update::<Value, _>(KIND_ENDPOINT, &name, "Endpoint", |ep| {
                        ep["config"] = normalize_config(&config, &name);
                        ep["pending_config"] = Value::Null;
                        Ok(())
                    }).await;
                    st.set_endpoint_state(&name, "READY", "NOT_UPDATING", None).await;
                }
                Some(msg) => {
                    let _ = st.store.update::<Value, _>(KIND_ENDPOINT, &name, "Endpoint", |ep| {
                        ep["pending_config"] = Value::Null;
                        Ok(())
                    }).await;
                    st.set_endpoint_state(&name, "NOT_READY", "UPDATE_FAILED", Some(&msg)).await;
                }
            }
        });
    }

    async fn stop_endpoint_workers(&self, ep: &Value) {
        for e in served(ep) {
            let ename = e["name"].as_str().unwrap_or("");
            self.kernels.stop(&Self::serving_ctx(ep["name"].as_str().unwrap_or(""), ename)).await;
        }
    }
}

fn served(ep: &Value) -> Vec<Value> {
    let mut v: Vec<Value> = ep["config"]["served_entities"].as_array().cloned().unwrap_or_default();
    v.extend(ep["config"]["served_models"].as_array().cloned().unwrap_or_default());
    v
}

fn entity_default_name(e: &Value) -> String {
    let model = e["entity_name"].as_str().or(e["model_name"].as_str()).or(e["external_model"]["name"].as_str()).unwrap_or("entity").rsplit('.').next().unwrap_or("entity").to_string();
    let ver = e["entity_version"].as_str().or(e["model_version"].as_str()).unwrap_or("");
    if ver.is_empty() { model } else { format!("{model}-{ver}") }
}

fn normalize_config(config: &Value, endpoint: &str) -> Value {
    let mut entities: Vec<Value> = config["served_entities"].as_array().cloned().unwrap_or_default();
    for m in config["served_models"].as_array().cloned().unwrap_or_default() {
        let mut e = m.clone();
        if let Some(n) = m["model_name"].as_str() {
            e["entity_name"] = json!(n);
        }
        if let Some(v) = m["model_version"].as_str() {
            e["entity_version"] = json!(v);
        }
        entities.push(e);
    }
    for e in entities.iter_mut() {
        if e["name"].as_str().map(|s| s.is_empty()).unwrap_or(true) {
            e["name"] = json!(entity_default_name(e));
        }
        if e.get("workload_size").is_none() {
            e["workload_size"] = json!("Small");
        }
        if e.get("scale_to_zero_enabled").is_none() {
            e["scale_to_zero_enabled"] = json!(false);
        }
        e["state"] = json!({ "deployment": "DEPLOYMENT_READY", "deployment_state_message": "" });
        e["creation_timestamp"] = e.get("creation_timestamp").cloned().unwrap_or(json!(now_ms()));
    }
    let routes: Vec<Value> = match config["traffic_config"]["routes"].as_array() {
        Some(r) if !r.is_empty() => r.clone(),
        _ => {
            let n = entities.len().max(1) as i64;
            let mut left = 100i64;
            entities.iter().enumerate().map(|(i, e)| {
                let pct = if i + 1 == entities.len() { left } else { 100 / n };
                left -= pct;
                json!({ "served_model_name": e["name"], "served_entity_name": e["name"], "traffic_percentage": pct })
            }).collect()
        }
    };
    let mut out = json!({
        "served_entities": entities,
        "served_models": entities,
        "traffic_config": { "routes": routes },
        "config_version": config["config_version"].as_i64().unwrap_or(1),
    });
    if let Some(ai) = config.get("auto_capture_config") {
        out["auto_capture_config"] = ai.clone();
    }
    let _ = endpoint;
    out
}

fn public_view(ep: &Value) -> Value {
    let mut v = ep.clone();
    if let Value::Object(o) = &mut v {
        o.remove("secrets");
    }
    v
}

fn summary_view(ep: &Value) -> Value {
    json!({
        "name": ep["name"], "creator": ep["creator"], "creation_timestamp": ep["creation_timestamp"], "last_updated_timestamp": ep["last_updated_timestamp"],
        "state": ep["state"], "config": { "served_entities": ep["config"]["served_entities"], "served_models": ep["config"]["served_models"] }, "tags": ep["tags"], "id": ep["id"], "task": ep["task"], "endpoint_type": ep["endpoint_type"],
    })
}

// ------------------------------------------------------------------ routes

#[derive(Debug, Deserialize)]
struct CreateEndpoint {
    name: String,
    #[serde(default)]
    config: Value,
    #[serde(default)]
    tags: Vec<Value>,
    #[serde(default)]
    rate_limits: Vec<Value>,
    #[serde(default)]
    ai_gateway: Option<Value>,
    #[serde(default)]
    route_optimized: bool,
    #[serde(default)]
    task: Option<String>,
}

async fn create(State(st): State<S>, Who(p): Who, Body(b): Body<CreateEndpoint>) -> ApiResult<Json<Value>> {
    if b.name.is_empty() || !b.name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(ApiError::invalid("Endpoint name must be alphanumeric with dashes/underscores"));
    }
    if st.store.get::<Value>(KIND_ENDPOINT, &b.name).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("Endpoint with name '{}' already exists.", b.name)));
    }
    let ep = json!({
        "name": b.name, "id": uuid::Uuid::new_v4().simple().to_string(), "creator": p.user_name, "creation_timestamp": now_ms(), "last_updated_timestamp": now_ms(),
        "state": { "ready": "NOT_READY", "config_update": "IN_PROGRESS" },
        "config": Value::Null, "pending_config": normalize_config(&b.config, &b.name), "tags": b.tags, "rate_limits": b.rate_limits, "ai_gateway": b.ai_gateway,
        "route_optimized": b.route_optimized, "task": b.task, "endpoint_type": if b.config["served_entities"].as_array().map(|a| a.iter().any(|e| e.get("external_model").is_some())).unwrap_or(false) { "EXTERNAL_MODEL" } else { "CUSTOM_MODEL_SERVING" },
        "permission_level": "CAN_MANAGE",
    });
    st.store.insert(KIND_ENDPOINT, st.ws(), &b.name, Some(&p.user_name), Some(&b.name), &ep).await?;
    st.spawn_endpoint_update(p, b.name.clone(), b.config);
    let doc = st.endpoint_doc(&b.name).await?;
    Ok(Json(public_view(&doc.data)))
}

async fn list(State(st): State<S>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_ENDPOINT, st.ws(), Filter::default()).await?;
    Ok(Json(json!({ "endpoints": docs.iter().map(|d| summary_view(&d.data)).collect::<Vec<_>>() })))
}

async fn get_one(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(public_view(&st.endpoint_doc(&name).await?.data)))
}

async fn update_config(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    let doc = st.endpoint_doc(&name).await?;
    if doc.data["state"]["config_update"] == "IN_PROGRESS" {
        return Err(ApiError::InvalidState(format!("Endpoint '{name}' has a config update in progress.")));
    }
    let mut cfg = b;
    let ver = doc.data["config"]["config_version"].as_i64().unwrap_or(0) + 1;
    cfg["config_version"] = json!(ver);
    st.store.update::<Value, _>(KIND_ENDPOINT, &name, "Endpoint", |ep| {
        ep["pending_config"] = normalize_config(&cfg, &name);
        Ok(())
    }).await?;
    st.stop_endpoint_workers(&doc.data).await;
    st.spawn_endpoint_update(p, name.clone(), cfg);
    Ok(Json(public_view(&st.endpoint_doc(&name).await?.data)))
}

#[derive(Debug, Deserialize)]
struct PatchEndpoint {
    #[serde(default)]
    add_tags: Vec<Value>,
    #[serde(default)]
    delete_tags: Vec<String>,
}

async fn patch_tags(State(st): State<S>, Path(name): Path<String>, Body(b): Body<PatchEndpoint>) -> ApiResult<Json<Value>> {
    let d = st.store.update::<Value, _>(KIND_ENDPOINT, &name, "Endpoint", |ep| {
        let mut tags = ep["tags"].as_array().cloned().unwrap_or_default();
        tags.retain(|t| !b.delete_tags.iter().any(|k| t["key"] == json!(k)));
        for t in &b.add_tags {
            tags.retain(|x| x["key"] != t["key"]);
            tags.push(t.clone());
        }
        ep["tags"] = json!(tags);
        Ok(())
    }).await?;
    Ok(Json(d.data["tags"].clone()))
}

async fn put_rate_limits(State(st): State<S>, Path(name): Path<String>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    let d = st.store.update::<Value, _>(KIND_ENDPOINT, &name, "Endpoint", |ep| {
        ep["rate_limits"] = b["rate_limits"].clone();
        Ok(())
    }).await?;
    Ok(Json(json!({ "rate_limits": d.data["rate_limits"] })))
}

async fn put_ai_gateway(State(st): State<S>, Path(name): Path<String>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    let d = st.store.update::<Value, _>(KIND_ENDPOINT, &name, "Endpoint", |ep| {
        ep["ai_gateway"] = b.clone();
        Ok(())
    }).await?;
    Ok(Json(d.data["ai_gateway"].clone()))
}

async fn delete(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    let doc = st.endpoint_doc(&name).await?;
    st.stop_endpoint_workers(&doc.data).await;
    let _ = tokio::fs::remove_dir_all(std::path::PathBuf::from(&st.config.work_dir).join("serving").join(&name)).await;
    st.store.delete(KIND_ENDPOINT, &name).await?;
    Ok(empty())
}

async fn build_logs(State(st): State<S>, Path((name, entity)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let doc = st.endpoint_doc(&name).await?;
    let ready = doc.data["state"]["ready"] == "READY";
    Ok(Json(json!({ "logs": if ready { format!("served entity {entity}: model loaded, worker ready") } else { doc.data["state"]["message"].as_str().unwrap_or("pending").to_string() } })))
}

async fn logs(State(st): State<S>, Path((name, entity)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let running = st.kernels.has(&AppState::serving_ctx(&name, &entity));
    Ok(Json(json!({ "logs": if running { "worker running" } else { "worker not running" } })))
}

async fn metrics(State(st): State<S>, Path(name): Path<String>) -> ApiResult<String> {
    let doc = st.endpoint_doc(&name).await?;
    let ready = if doc.data["state"]["ready"] == "READY" { 1 } else { 0 };
    let n = doc.data["stats"]["requests"].as_i64().unwrap_or(0);
    let lat = doc.data["stats"]["latency_ms_total"].as_i64().unwrap_or(0);
    Ok(format!("# TYPE lakeforge_endpoint_ready gauge\nlakeforge_endpoint_ready{{endpoint=\"{name}\"}} {ready}\n# TYPE lakeforge_endpoint_requests_total counter\nlakeforge_endpoint_requests_total{{endpoint=\"{name}\"}} {n}\n# TYPE lakeforge_endpoint_latency_ms_total counter\nlakeforge_endpoint_latency_ms_total{{endpoint=\"{name}\"}} {lat}\n"))
}

async fn export_metrics(st: State<S>, name: Path<String>) -> ApiResult<String> {
    metrics(st, name).await
}

// --------------------------------------------------------------- inference

fn pick_route(ep: &Value) -> Option<Value> {
    let entities = served(ep);
    if entities.is_empty() {
        return None;
    }
    let routes = ep["config"]["traffic_config"]["routes"].as_array().cloned().unwrap_or_default();
    if routes.is_empty() {
        return entities.first().cloned();
    }
    let total: i64 = routes.iter().map(|r| r["traffic_percentage"].as_i64().unwrap_or(0)).sum::<i64>().max(1);
    let mut roll = (rand::random::<u64>() % total as u64) as i64;
    for r in &routes {
        let pct = r["traffic_percentage"].as_i64().unwrap_or(0);
        if roll < pct {
            let name = r["served_entity_name"].as_str().or(r["served_model_name"].as_str()).unwrap_or("");
            return entities.iter().find(|e| e["name"] == json!(name)).cloned().or_else(|| entities.first().cloned());
        }
        roll -= pct;
    }
    entities.first().cloned()
}

async fn invoke_external(st: &AppState, entity: &Value, body: &Value, task: Option<&str>) -> ApiResult<Value> {
    let ext = &entity["external_model"];
    let provider = ext["provider"].as_str().unwrap_or("openai");
    let model = ext["name"].as_str().unwrap_or("");
    let task = ext["task"].as_str().or(task).unwrap_or("llm/v1/chat");
    let cfg = &ext[format!("{provider}_config")];
    let key_ref = cfg["openai_api_key"].as_str().or(cfg["anthropic_api_key"].as_str()).or(cfg["api_key"].as_str()).unwrap_or("");
    let key = if key_ref.starts_with("{{") { st.resolve_secret_ref(key_ref).await.unwrap_or_default() } else { cfg["openai_api_key_plaintext"].as_str().or(cfg["api_key_plaintext"].as_str()).unwrap_or(key_ref).to_string() };
    let base = cfg["openai_api_base"].as_str().or(cfg["base_url"].as_str()).unwrap_or(match provider {
        "anthropic" => "https://api.anthropic.com/v1",
        _ => "https://api.openai.com/v1",
    }).trim_end_matches('/').to_string();
    let path = match task {
        "llm/v1/chat" => if provider == "anthropic" { "/messages" } else { "/chat/completions" },
        "llm/v1/completions" => "/completions",
        "llm/v1/embeddings" => "/embeddings",
        _ => "/chat/completions",
    };
    let mut payload = body.clone();
    if payload.get("model").is_none() {
        payload["model"] = json!(model);
    }
    if task == "llm/v1/embeddings" && payload.get("input").is_none() {
        payload["input"] = body["inputs"].clone();
    }
    let client = reqwest::Client::builder().timeout(Duration::from_secs(120)).build().map_err(ApiError::internal)?;
    let mut req = client.post(format!("{base}{path}")).json(&payload);
    req = if provider == "anthropic" { req.header("x-api-key", key).header("anthropic-version", "2023-06-01") } else { req.bearer_auth(key) };
    let resp = req.send().await.map_err(|e| ApiError::Unavailable(format!("external model request failed: {e}")))?;
    let status = resp.status();
    let v: Value = resp.json().await.map_err(|e| ApiError::Unavailable(format!("external model returned non-JSON: {e}")))?;
    if !status.is_success() {
        return Err(ApiError::Unavailable(format!("external model error {status}: {}", v["error"]["message"].as_str().unwrap_or("request failed"))));
    }
    Ok(v)
}

async fn invoke(State(st): State<S>, Path(name): Path<String>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    let started = std::time::Instant::now();
    let doc = st.endpoint_doc(&name).await?;
    if doc.data["state"]["ready"] != "READY" {
        return Err(ApiError::InvalidState(format!("Endpoint '{name}' is not ready ({}).", doc.data["state"]["config_update"].as_str().unwrap_or("NOT_READY"))));
    }
    let entity = pick_route(&doc.data).ok_or_else(|| ApiError::InvalidState("endpoint has no served entities".into()))?;
    let out = if entity.get("external_model").is_some() {
        invoke_external(&st, &entity, &b, doc.data["task"].as_str()).await?
    } else {
        let ctx = AppState::serving_ctx(&name, entity["name"].as_str().unwrap_or(""));
        if !st.kernels.has(&ctx) {
            return Err(ApiError::Unavailable("model worker is not running; update the endpoint config to reload".into()));
        }
        let code = format!("print(json.dumps(__lf_predict(__lf_model, json.loads({}))))", json_escape(&serde_json::to_string(&b)?));
        let (ok, text) = st.kernel_exec(&ctx, &code, Duration::from_secs(120)).await?;
        if !ok {
            return Err(ApiError::invalid(format!("model prediction failed: {text}")));
        }
        let line = text.lines().rev().find(|l| l.trim_start().starts_with('{')).unwrap_or("{}");
        serde_json::from_str(line).map_err(|e| ApiError::internal(format!("bad prediction output: {e}")))?
    };
    let elapsed = started.elapsed().as_millis() as i64;
    let _ = st.store.update::<Value, _>(KIND_ENDPOINT, &name, "Endpoint", |ep| {
        let n = ep["stats"]["requests"].as_i64().unwrap_or(0) + 1;
        let lat = ep["stats"]["latency_ms_total"].as_i64().unwrap_or(0) + elapsed;
        ep["stats"] = json!({ "requests": n, "latency_ms_total": lat, "last_request_ms": now_ms() });
        Ok(())
    }).await;
    Ok(Json(out))
}

async fn served_entity_state(State(st): State<S>, Path(name): Path<String>, Query(q): Query<HashMap<String, String>>) -> ApiResult<Json<Value>> {
    let doc = st.endpoint_doc(&name).await?;
    let ename = q.get("served_entity_name").cloned().unwrap_or_default();
    let running = st.kernels.has(&AppState::serving_ctx(&name, &ename));
    Ok(Json(json!({ "name": ename, "running": running, "endpoint_state": doc.data["state"] })))
}

async fn endpoint_permissions(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    let acl = st.get_permissions("serving-endpoints", &name).await?;
    Ok(Json(json!({ "object_id": format!("/serving-endpoints/{name}"), "object_type": "serving-endpoint", "access_control_list": acl })))
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/serving-endpoints", get(list).post(create))
        .route("/api/2.0/serving-endpoints/{name}", get(get_one).delete(delete).patch(patch_tags))
        .route("/api/2.0/serving-endpoints/{name}/config", put(update_config))
        .route("/api/2.0/serving-endpoints/{name}/tags", axum::routing::patch(patch_tags))
        .route("/api/2.0/serving-endpoints/{name}/rate-limits", put(put_rate_limits))
        .route("/api/2.0/serving-endpoints/{name}/ai-gateway", put(put_ai_gateway))
        .route("/api/2.0/serving-endpoints/{name}/metrics", get(metrics))
        .route("/api/2.0/serving-endpoints/{name}/openapi", get(openapi))
        .route("/api/2.0/serving-endpoints/{name}/served-models/{entity}/build-logs", get(build_logs))
        .route("/api/2.0/serving-endpoints/{name}/served-models/{entity}/logs", get(logs))
        .route("/api/2.0/serving-endpoints/{name}/served-entities/{entity}/build-logs", get(build_logs))
        .route("/api/2.0/serving-endpoints/{name}/served-entities/{entity}/logs", get(logs))
        .route("/api/2.0/serving-endpoints/{name}/served-entities/state", get(served_entity_state))
        .route("/api/2.0/serving-endpoints/{name}/permissions", get(endpoint_permissions))
        .route("/api/2.0/lakeforge/serving-endpoints/{name}/metrics", get(export_metrics))
        .route("/serving-endpoints/{name}/invocations", post(invoke))
        .route("/api/2.0/serving-endpoints/{name}/invocations", post(invoke))
}

async fn openapi(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    st.endpoint_doc(&name).await?;
    let mut paths = Map::new();
    paths.insert(format!("/serving-endpoints/{name}/invocations"), json!({ "post": { "summary": "Query the endpoint", "requestBody": { "content": { "application/json": { "schema": { "type": "object", "properties": { "dataframe_records": { "type": "array" }, "dataframe_split": { "type": "object" }, "instances": { "type": "array" }, "inputs": {}, "params": { "type": "object" } } } } } }, "responses": { "200": { "description": "predictions" } } } }));
    Ok(Json(json!({ "openapi": "3.1.0", "info": { "title": name, "version": "1.0" }, "servers": [{ "url": st.config.public_url }], "paths": paths })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_normalisation_splits_traffic() {
        let cfg = json!({ "served_models": [ { "model_name": "m", "model_version": "1" }, { "model_name": "m", "model_version": "2" } ] });
        let n = normalize_config(&cfg, "ep");
        let ents = n["served_entities"].as_array().unwrap();
        assert_eq!(ents[0]["name"], "m-1");
        assert_eq!(ents[0]["entity_name"], "m");
        let routes = n["traffic_config"]["routes"].as_array().unwrap();
        let total: i64 = routes.iter().map(|r| r["traffic_percentage"].as_i64().unwrap()).sum();
        assert_eq!(total, 100);
    }
}
