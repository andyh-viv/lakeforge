//! Command Execution API 1.2 (`/api/1.2/contexts/*`, `/api/1.2/commands/*`)
//! plus a Lakeforge SSE stream for live cell output.
//!
//! An execution context is a Python kernel process bound to a cluster; the
//! kernel talks back to this control plane (SQL, dbutils, secrets) with a
//! short-lived JWT.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use dashmap::DashMap;
use futures::Stream;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::broadcast;

use super::{Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::kernel::KernelEvent;
use crate::state::AppState;
use crate::store::now_ms;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum CommandStatus {
    Queued,
    Running,
    Cancelling,
    Finished,
    Cancelled,
    Error,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandRecord {
    pub id: String,
    pub context_id: String,
    pub status: CommandStatus,
    pub language: String,
    pub command: String,
    pub started_ms: i64,
    pub finished_ms: Option<i64>,
    pub outputs: Vec<KernelEvent>,
}

impl CommandRecord {
    pub fn stdout_text(&self) -> String {
        self.outputs.iter().filter_map(|e| match e {
            KernelEvent::Stdout { text } => Some(text.as_str()),
            _ => None,
        }).collect()
    }

    pub fn error_text(&self) -> String {
        self.outputs.iter().rev().find_map(|e| match e {
            KernelEvent::Error { ename, evalue, .. } => Some(format!("{ename}: {evalue}")),
            _ => None,
        }).unwrap_or_default()
    }
}

pub struct Command {
    pub record: RwLock<CommandRecord>,
    pub events: broadcast::Sender<KernelEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Context {
    pub id: String,
    pub cluster_id: String,
    pub language: String,
    pub user_name: String,
    pub created_ms: i64,
    pub notebook_path: Option<String>,
}

#[derive(Default)]
pub struct Contexts {
    pub contexts: DashMap<String, Context>,
    pub commands: DashMap<String, Arc<Command>>,
    /// Task values shared between tasks of a job run (context_id -> key -> value).
    pub task_values: DashMap<String, HashMap<String, Value>>,
}

impl AppState {
    /// Create (or reuse) an execution context bound to `cluster_id`. Starts the
    /// cluster if it is not running.
    pub async fn create_context(self: &Arc<Self>, p: &Principal, cluster_id: &str, language: &str, notebook_path: Option<&str>, extra_env: HashMap<String, String>) -> ApiResult<Context> {
        let driver = self.cluster_driver(cluster_id, true).await?;
        let id = uuid::Uuid::new_v4().simple().to_string();
        let token = self.auth.issue_jwt(&p.user_id, &p.user_name, 12 * 3600)?;
        let mut env = HashMap::new();
        env.insert("LAKEFORGE_TOKEN".to_string(), token.clone());
        env.insert("DATABRICKS_TOKEN".to_string(), token);
        env.insert("LAKEFORGE_CLUSTER_ID".to_string(), cluster_id.to_string());
        env.insert("LAKEFORGE_DRIVER_ADDR".to_string(), driver);
        env.insert("LAKEFORGE_USER".to_string(), p.user_name.clone());
        if let Some(nb) = notebook_path {
            env.insert("LAKEFORGE_NOTEBOOK_PATH".to_string(), nb.to_string());
        }
        env.extend(extra_env);
        self.kernels.start(&id, env).await?;
        let ctx = Context { id: id.clone(), cluster_id: cluster_id.to_string(), language: language.to_string(), user_name: p.user_name.clone(), created_ms: now_ms(), notebook_path: notebook_path.map(|s| s.to_string()) };
        self.contexts.contexts.insert(id, ctx.clone());
        self.touch_cluster(cluster_id).await;
        Ok(ctx)
    }

    pub async fn destroy_context(&self, context_id: &str) {
        self.kernels.stop(context_id).await;
        self.contexts.contexts.remove(context_id);
        self.contexts.task_values.remove(context_id);
        let ids: Vec<String> = self.contexts.commands.iter().filter(|c| c.record.read().context_id == context_id).map(|c| c.key().clone()).collect();
        for id in ids {
            self.contexts.commands.remove(&id);
        }
    }

    /// Submit code to a context; returns immediately with the command handle.
    pub async fn submit_command(self: &Arc<Self>, context_id: &str, language: &str, code: &str) -> ApiResult<Arc<Command>> {
        let ctx = self.contexts.contexts.get(context_id).map(|c| c.clone()).ok_or_else(|| ApiError::NotFound(format!("Context {context_id} not found; create one with /api/1.2/contexts/create.")))?;
        if !self.kernels.has(context_id) {
            return Err(ApiError::InvalidState(format!("Context {context_id} kernel has exited.")));
        }
        let command_id = uuid::Uuid::new_v4().simple().to_string();
        let rx = self.kernels.execute(context_id, &command_id, language, code).await?;
        let (tx, _) = broadcast::channel(1024);
        let cmd = Arc::new(Command {
            record: RwLock::new(CommandRecord { id: command_id.clone(), context_id: context_id.to_string(), status: CommandStatus::Running, language: language.to_string(), command: code.to_string(), started_ms: now_ms(), finished_ms: None, outputs: vec![] }),
            events: tx,
        });
        self.contexts.commands.insert(command_id.clone(), Arc::clone(&cmd));
        self.touch_cluster(&ctx.cluster_id).await;

        let c2 = Arc::clone(&cmd);
        tokio::spawn(async move {
            let mut rx = rx;
            while let Some(ev) = rx.recv().await {
                let done = matches!(ev, KernelEvent::Done { .. });
                {
                    let mut r = c2.record.write();
                    if let KernelEvent::Done { status } = &ev {
                        r.status = match status.as_str() {
                            "ok" => CommandStatus::Finished,
                            "cancelled" | "interrupted" => CommandStatus::Cancelled,
                            _ => CommandStatus::Error,
                        };
                        r.finished_ms = Some(now_ms());
                    } else {
                        r.outputs.push(ev.clone());
                    }
                }
                let _ = c2.events.send(ev);
                if done {
                    break;
                }
            }
            let mut r = c2.record.write();
            if r.finished_ms.is_none() {
                r.status = CommandStatus::Error;
                r.finished_ms = Some(now_ms());
                r.outputs.push(KernelEvent::Error { ename: "KernelExit".into(), evalue: "kernel exited before the command finished".into(), traceback: vec![] });
            }
        });
        Ok(cmd)
    }

    /// Run `code` to completion and collect its outputs.
    pub async fn run_command(self: &Arc<Self>, context_id: &str, language: &str, code: &str, timeout: Option<std::time::Duration>) -> ApiResult<CommandRecord> {
        let cmd = self.submit_command(context_id, language, code).await?;
        let mut rx = cmd.events.subscribe();
        let wait = async {
            loop {
                if cmd.record.read().finished_ms.is_some() {
                    break;
                }
                match rx.recv().await {
                    Ok(KernelEvent::Done { .. }) | Err(broadcast::error::RecvError::Closed) => break,
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        };
        match timeout {
            Some(t) => {
                if tokio::time::timeout(t, wait).await.is_err() {
                    let cmd_id = cmd.record.read().id.clone();
                    let _ = self.kernels.interrupt(context_id, &cmd_id).await;
                    let mut r = cmd.record.write();
                    r.status = CommandStatus::Cancelled;
                    r.finished_ms = Some(now_ms());
                    r.outputs.push(KernelEvent::Error { ename: "Timeout".into(), evalue: format!("command exceeded {}s", t.as_secs()), traceback: vec![] });
                }
            }
            None => wait.await,
        }
        let rec = cmd.record.read().clone();
        Ok(rec)
    }
}

/// Databricks 1.2 `results` envelope.
pub fn command_results(r: &CommandRecord) -> Value {
    let mut text = String::new();
    let mut table: Option<Value> = None;
    let mut error: Option<Value> = None;
    for o in &r.outputs {
        match o {
            KernelEvent::Stdout { text: t } | KernelEvent::Stderr { text: t } | KernelEvent::Result { text: t } => text.push_str(t),
            KernelEvent::Display { mime, data } => {
                if mime == "text/plain" {
                    text.push_str(data.as_str().unwrap_or_default());
                }
            }
            KernelEvent::Table { columns, rows, truncated } => {
                table = Some(json!({ "resultType": "table", "schema": columns.iter().map(|c| json!({"name": c, "type": "\"string\"", "metadata": "{}"})).collect::<Vec<_>>(), "data": rows, "truncated": truncated, "isJsonSchema": true }));
            }
            KernelEvent::Error { ename, evalue, traceback } => {
                error = Some(json!({ "resultType": "error", "summary": format!("{ename}: {evalue}"), "cause": traceback.join("\n") }));
            }
            KernelEvent::Exit { value } => {
                text.push_str(value.as_deref().unwrap_or(""));
            }
            KernelEvent::Done { .. } => {}
        }
    }
    let results = if let Some(e) = error { e } else if let Some(t) = table { t } else { json!({ "resultType": "text", "data": text }) };
    json!({ "id": r.id, "status": r.status, "results": results })
}

// ---------------------------------------------------------------- handlers

#[derive(Debug, Deserialize)]
struct CreateContext {
    #[serde(alias = "clusterId")]
    cluster_id: String,
    #[serde(default = "default_lang")]
    language: String,
    #[serde(default)]
    notebook_path: Option<String>,
}

fn default_lang() -> String {
    "python".into()
}

async fn create_context(State(st): State<S>, Who(p): Who, Body(b): Body<CreateContext>) -> ApiResult<Json<Value>> {
    let ctx = st.create_context(&p, &b.cluster_id, &b.language, b.notebook_path.as_deref(), HashMap::new()).await?;
    Ok(Json(json!({ "id": ctx.id })))
}

#[derive(Debug, Deserialize)]
struct ContextQ {
    #[serde(alias = "clusterId")]
    #[allow(dead_code)]
    cluster_id: Option<String>,
    #[serde(alias = "contextId")]
    context_id: String,
}

async fn context_status(State(st): State<S>, Query(q): Query<ContextQ>) -> ApiResult<Json<Value>> {
    let ctx = st.contexts.contexts.get(&q.context_id).map(|c| c.clone()).ok_or_else(|| ApiError::NotFound(format!("Context {} not found", q.context_id)))?;
    let status = if st.kernels.has(&ctx.id) { "Running" } else { "Error" };
    Ok(Json(json!({ "id": ctx.id, "status": status, "cluster_id": ctx.cluster_id, "language": ctx.language, "user_name": ctx.user_name, "created_ms": ctx.created_ms, "notebook_path": ctx.notebook_path })))
}

async fn destroy_context(State(st): State<S>, Body(b): Body<ContextQ>) -> ApiResult<Json<Value>> {
    st.destroy_context(&b.context_id).await;
    Ok(Json(json!({ "id": b.context_id })))
}

async fn list_contexts(State(st): State<S>) -> ApiResult<Json<Value>> {
    let items: Vec<Value> = st.contexts.contexts.iter().map(|c| json!({ "id": c.id, "cluster_id": c.cluster_id, "language": c.language, "user_name": c.user_name, "created_ms": c.created_ms, "notebook_path": c.notebook_path, "alive": st.kernels.has(&c.id) })).collect();
    Ok(Json(json!({ "contexts": items })))
}

#[derive(Debug, Deserialize)]
struct ExecuteBody {
    #[serde(alias = "clusterId")]
    #[allow(dead_code)]
    cluster_id: Option<String>,
    #[serde(alias = "contextId")]
    context_id: String,
    #[serde(default)]
    language: Option<String>,
    command: String,
}

async fn execute(State(st): State<S>, Body(b): Body<ExecuteBody>) -> ApiResult<Json<Value>> {
    let lang = b.language.clone().or_else(|| st.contexts.contexts.get(&b.context_id).map(|c| c.language.clone())).unwrap_or_else(|| "python".into());
    let cmd = st.submit_command(&b.context_id, &lang, &b.command).await?;
    let id = cmd.record.read().id.clone();
    Ok(Json(json!({ "id": id })))
}

#[derive(Debug, Deserialize)]
struct CommandQ {
    #[serde(alias = "clusterId")]
    #[allow(dead_code)]
    cluster_id: Option<String>,
    #[serde(alias = "contextId")]
    #[allow(dead_code)]
    context_id: Option<String>,
    #[serde(alias = "commandId")]
    command_id: String,
}

fn get_command(st: &AppState, id: &str) -> ApiResult<Arc<Command>> {
    st.contexts.commands.get(id).map(|c| Arc::clone(&c)).ok_or_else(|| ApiError::NotFound(format!("Command {id} not found")))
}

async fn command_status(State(st): State<S>, Query(q): Query<CommandQ>) -> ApiResult<Json<Value>> {
    let cmd = get_command(&st, &q.command_id)?;
    let r = cmd.record.read().clone();
    Ok(Json(command_results(&r)))
}

async fn cancel(State(st): State<S>, Body(b): Body<CommandQ>) -> ApiResult<Json<Value>> {
    let cmd = get_command(&st, &b.command_id)?;
    let ctx_id = cmd.record.read().context_id.clone();
    cmd.record.write().status = CommandStatus::Cancelling;
    st.kernels.interrupt(&ctx_id, &b.command_id).await?;
    Ok(Json(json!({ "id": b.command_id })))
}

/// Full structured outputs (used by the notebook UI).
async fn command_outputs(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<CommandRecord>> {
    Ok(Json(get_command(&st, &id)?.record.read().clone()))
}

/// Live SSE stream: replays buffered outputs then follows new events until `done`.
async fn command_events(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>> {
    let cmd = get_command(&st, &id)?;
    let mut rx = cmd.events.subscribe();
    let (buffered, finished) = {
        let r = cmd.record.read();
        (r.outputs.clone(), r.finished_ms.is_some())
    };
    let status = cmd.record.read().status;
    let stream = async_stream::stream! {
        for ev in buffered {
            yield Ok(Event::default().json_data(&ev).unwrap_or_else(|_| Event::default()));
        }
        if finished {
            yield Ok(Event::default().json_data(json!({"type": "done", "status": status})).unwrap_or_else(|_| Event::default()));
            return;
        }
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    let done = matches!(ev, KernelEvent::Done { .. });
                    yield Ok(Event::default().json_data(&ev).unwrap_or_else(|_| Event::default()));
                    if done { break; }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

#[derive(Debug, Deserialize)]
struct TaskValueBody {
    context_id: String,
    key: String,
    value: Value,
}

async fn set_task_value(State(st): State<S>, Body(b): Body<TaskValueBody>) -> ApiResult<Json<Value>> {
    st.contexts.task_values.entry(b.context_id).or_default().insert(b.key, b.value);
    Ok(super::empty())
}

#[derive(Debug, Deserialize)]
struct TaskValueQ {
    context_id: String,
    #[serde(default)]
    task_key: Option<String>,
    key: String,
}

async fn get_task_value(State(st): State<S>, Query(q): Query<TaskValueQ>) -> ApiResult<Json<Value>> {
    // Values set by sibling tasks of the same run are namespaced `<run_id>/<task_key>`.
    let run_scope = st.contexts.contexts.get(&q.context_id).and_then(|c| c.notebook_path.clone());
    let _ = run_scope;
    let direct = st.contexts.task_values.get(&q.context_id).and_then(|m| m.get(&q.key).cloned());
    let sibling = q.task_key.as_ref().and_then(|tk| {
        let scope = st.jobs_task_context.get(&q.context_id).map(|s| s.clone())?;
        let sib_ctx = st.jobs_task_context.iter().find(|e| e.value().run_id == scope.run_id && e.value().task_key == *tk).map(|e| e.key().clone())?;
        st.contexts.task_values.get(&sib_ctx).and_then(|m| m.get(&q.key).cloned())
    });
    Ok(Json(json!({ "value": sibling.or(direct) })))
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/1.2/contexts/create", post(create_context))
        .route("/api/1.2/contexts/status", get(context_status))
        .route("/api/1.2/contexts/destroy", post(destroy_context))
        .route("/api/1.2/commands/execute", post(execute))
        .route("/api/1.2/commands/status", get(command_status))
        .route("/api/1.2/commands/cancel", post(cancel))
        .route("/api/2.0/lakeforge/contexts", get(list_contexts))
        .route("/api/2.0/lakeforge/commands/{id}", get(command_outputs))
        .route("/api/2.0/lakeforge/commands/{id}/events", get(command_events))
        .route("/api/2.0/lakeforge/task-values", post(set_task_value).get(get_task_value))
}
