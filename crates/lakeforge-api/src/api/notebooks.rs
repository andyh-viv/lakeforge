//! Lakeforge-native notebook endpoints: structured read/write of cells and a
//! non-interactive "run notebook" engine used by `dbutils.notebook.run`,
//! `%run` and job tasks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::commands::CommandStatus;
use super::workspace::{Cell, Language, Notebook, ObjectType};
use super::{Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::kernel::KernelEvent;
use crate::state::{AppState, TaskScope};
use crate::store::now_ms;

#[derive(Debug, Clone, Serialize)]
pub struct CellRun {
    pub cell_id: String,
    pub language: String,
    pub status: CommandStatus,
    pub outputs: Vec<KernelEvent>,
    pub duration_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NotebookRun {
    pub path: String,
    pub status: String,
    pub result: Option<String>,
    pub error: Option<String>,
    pub cells: Vec<CellRun>,
    pub context_id: String,
    pub started_ms: i64,
    pub finished_ms: i64,
}

#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    pub arguments: HashMap<String, String>,
    pub timeout: Option<Duration>,
    /// Execute in an existing kernel (shares globals — `%run` semantics).
    pub inline_context: Option<String>,
    pub scope: Option<TaskScope>,
    pub extra_env: HashMap<String, String>,
}

impl AppState {
    pub async fn run_notebook(self: &Arc<Self>, p: &Principal, path: &str, cluster_id: &str, opts: RunOptions) -> ApiResult<NotebookRun> {
        let obj = self.ws_require(path).await?;
        if obj.object_type != ObjectType::Notebook {
            return Err(ApiError::invalid(format!("{path} is not a notebook")));
        }
        let nb = obj.notebook.clone().unwrap_or_default();
        let started = now_ms();

        let (context_id, owned) = match &opts.inline_context {
            Some(c) if self.kernels.has(c) => (c.clone(), false),
            _ => {
                let mut env = opts.extra_env.clone();
                env.insert("LAKEFORGE_WIDGETS".into(), serde_json::to_string(&opts.arguments)?);
                let ctx = self.create_context(p, cluster_id, &nb.default_language, Some(&obj.path), env).await?;
                if let Some(scope) = &opts.scope {
                    self.jobs_task_context.insert(ctx.id.clone(), scope.clone());
                }
                (ctx.id, true)
            }
        };

        let deadline = opts.timeout.map(|t| tokio::time::Instant::now() + t);
        let mut cells = vec![];
        let mut status = "SUCCESS".to_string();
        let mut result: Option<String> = None;
        let mut error: Option<String> = None;

        'cells: for cell in &nb.cells {
            if cell.language == "markdown" || cell.source.trim().is_empty() {
                continue;
            }
            let remaining = deadline.map(|d| d.saturating_duration_since(tokio::time::Instant::now()));
            if matches!(remaining, Some(r) if r.is_zero()) {
                status = "TIMEDOUT".into();
                error = Some("notebook run timed out".into());
                break;
            }
            let t0 = now_ms();
            let rec = self.run_command(&context_id, &cell.language, &cell.source, remaining).await?;
            let run = CellRun { cell_id: cell.id.clone(), language: cell.language.clone(), status: rec.status, outputs: rec.outputs.clone(), duration_ms: now_ms() - t0 };
            for o in &rec.outputs {
                match o {
                    KernelEvent::Exit { value } => {
                        result = value.clone();
                        cells.push(run.clone());
                        break 'cells;
                    }
                    KernelEvent::Error { ename, evalue, .. } => {
                        error = Some(format!("{ename}: {evalue}"));
                    }
                    _ => {}
                }
            }
            let failed = rec.status != CommandStatus::Finished;
            cells.push(run);
            if failed {
                status = if rec.status == CommandStatus::Cancelled { "CANCELED".into() } else { "FAILED".into() };
                if error.is_none() {
                    error = Some(format!("cell {} {:?}", cell.id, rec.status));
                }
                break;
            }
        }

        if owned {
            self.destroy_context(&context_id).await;
        }
        if status == "SUCCESS" {
            error = None;
        }
        Ok(NotebookRun { path: obj.path, status, result, error, cells, context_id, started_ms: started, finished_ms: now_ms() })
    }
}

// ---------------------------------------------------------------- handlers

#[derive(Debug, Deserialize)]
struct PathQ {
    path: String,
}

async fn get_notebook(State(st): State<S>, Query(q): Query<PathQ>) -> ApiResult<Json<Value>> {
    let obj = st.ws_require(&q.path).await?;
    if obj.object_type != ObjectType::Notebook {
        return Err(ApiError::invalid(format!("{} is not a notebook", q.path)));
    }
    let mut v = obj.status();
    v["notebook"] = serde_json::to_value(obj.notebook.unwrap_or_default())?;
    Ok(Json(v))
}

#[derive(Debug, Deserialize)]
struct SaveBody {
    path: String,
    #[serde(default)]
    language: Option<Language>,
    #[serde(default)]
    default_language: Option<String>,
    #[serde(default)]
    cells: Vec<Cell>,
    #[serde(default)]
    widgets: Value,
    /// If true, fail when the path already exists.
    #[serde(default)]
    create_only: bool,
}

async fn save_notebook(State(st): State<S>, Who(p): Who, Body(b): Body<SaveBody>) -> ApiResult<Json<Value>> {
    let lang = b.language.or_else(|| b.default_language.as_deref().and_then(Language::from_cell)).unwrap_or(Language::Python);
    let mut cells = b.cells;
    for c in &mut cells {
        if c.id.is_empty() {
            c.id = uuid::Uuid::new_v4().simple().to_string();
        }
    }
    if cells.is_empty() {
        cells.push(Cell { id: uuid::Uuid::new_v4().simple().to_string(), language: lang.cell_name().into(), ..Default::default() });
    }
    let nb = Notebook { default_language: lang.cell_name().into(), cells, widgets: if b.widgets.is_null() { json!({}) } else { b.widgets } };
    let obj = st.ws_put_notebook(&b.path, lang, nb, &p.user_name, !b.create_only).await?;
    Ok(Json(obj.status()))
}

#[derive(Debug, Deserialize)]
struct RunBody {
    path: String,
    #[serde(default)]
    cluster_id: Option<String>,
    #[serde(default)]
    arguments: HashMap<String, String>,
    #[serde(default)]
    timeout_seconds: u64,
    #[serde(default)]
    inline_context: Option<String>,
}

async fn run(State(st): State<S>, Who(p): Who, Body(b): Body<RunBody>) -> ApiResult<Json<NotebookRun>> {
    let cluster_id = match b.cluster_id.clone().filter(|c| !c.is_empty()) {
        Some(c) => c,
        None => b.inline_context.as_ref().and_then(|c| st.contexts.contexts.get(c).map(|x| x.cluster_id.clone())).ok_or_else(|| ApiError::invalid("cluster_id is required"))?,
    };
    let opts = RunOptions { arguments: b.arguments, timeout: (b.timeout_seconds > 0).then(|| Duration::from_secs(b.timeout_seconds)), inline_context: b.inline_context, scope: None, extra_env: HashMap::new() };
    let run = st.run_notebook(&p, &b.path, &cluster_id, opts).await?;
    Ok(Json(run))
}

/// Persist cell outputs after interactive execution (the UI calls this).
#[derive(Debug, Deserialize)]
struct OutputsBody {
    path: String,
    cell_id: String,
    outputs: Vec<Value>,
}

async fn save_outputs(State(st): State<S>, Who(p): Who, Body(b): Body<OutputsBody>) -> ApiResult<Json<Value>> {
    let obj = st.ws_require(&b.path).await?;
    let mut nb = obj.notebook.clone().unwrap_or_default();
    if let Some(c) = nb.cells.iter_mut().find(|c| c.id == b.cell_id) {
        c.outputs = b.outputs;
    }
    st.ws_put_notebook(&obj.path, obj.language.unwrap_or(Language::Python), nb, &p.user_name, true).await?;
    Ok(super::empty())
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/lakeforge/notebooks", get(get_notebook).put(save_notebook).post(save_notebook))
        .route("/api/2.0/lakeforge/notebooks/run", post(run))
        .route("/api/2.0/lakeforge/notebooks/outputs", post(save_outputs))
}
