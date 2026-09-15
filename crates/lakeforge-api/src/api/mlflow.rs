//! MLflow tracking + model registry — `/api/2.0/mlflow/*` and the artifact
//! proxy `/api/2.0/mlflow-artifacts/artifacts/*`.
//!
//! Wire-compatible with the MLflow REST API so `mlflow.set_tracking_uri("databricks")`
//! (or a plain `http://…` URI) from the Python client works unchanged.

use std::collections::BTreeMap;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete as del, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{empty, Body, S};
use crate::auth::Who;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};

pub const KIND_EXPERIMENT: &str = "mlflow_experiment";
pub const KIND_RUN: &str = "mlflow_run";
pub const KIND_METRIC: &str = "mlflow_metric";
pub const KIND_MODEL: &str = "mlflow_model";
pub const KIND_VERSION: &str = "mlflow_model_version";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Experiment {
    pub experiment_id: String,
    pub name: String,
    pub artifact_location: String,
    pub lifecycle_stage: String,
    pub last_update_time: i64,
    pub creation_time: i64,
    #[serde(default)]
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub key: String,
    pub value: f64,
    pub timestamp: i64,
    #[serde(default)]
    pub step: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunInfo {
    pub run_id: String,
    pub run_uuid: String,
    pub run_name: String,
    pub experiment_id: String,
    pub user_id: String,
    pub status: String,
    pub start_time: i64,
    #[serde(default)]
    pub end_time: Option<i64>,
    pub artifact_uri: String,
    pub lifecycle_stage: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RunData {
    #[serde(default)]
    pub metrics: Vec<Metric>,
    #[serde(default)]
    pub params: Vec<Tag>,
    #[serde(default)]
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub info: RunInfo,
    pub data: RunData,
    #[serde(default)]
    pub inputs: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricHistory {
    pub run_id: String,
    pub key: String,
    pub points: Vec<Metric>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredModel {
    pub name: String,
    pub creation_timestamp: i64,
    pub last_updated_timestamp: i64,
    pub user_id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<Tag>,
    #[serde(default)]
    pub aliases: Vec<Alias>,
    #[serde(default)]
    pub latest_versions: Vec<ModelVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Alias {
    pub alias: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelVersion {
    pub name: String,
    pub version: String,
    pub creation_timestamp: i64,
    pub last_updated_timestamp: i64,
    pub user_id: String,
    pub current_stage: String,
    #[serde(default)]
    pub description: Option<String>,
    pub source: String,
    #[serde(default)]
    pub run_id: Option<String>,
    pub status: String,
    #[serde(default)]
    pub status_message: Option<String>,
    #[serde(default)]
    pub tags: Vec<Tag>,
    #[serde(default)]
    pub run_link: Option<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
}

fn version_id(name: &str, version: &str) -> String {
    format!("{name}@{version}")
}

fn set_tag(tags: &mut Vec<Tag>, key: &str, value: &str) {
    if let Some(t) = tags.iter_mut().find(|t| t.key == key) {
        t.value = value.to_string();
    } else {
        tags.push(Tag { key: key.into(), value: value.into() });
    }
}

fn mlflow_err(code: &str, msg: impl Into<String>) -> ApiError {
    let msg = msg.into();
    match code {
        "RESOURCE_DOES_NOT_EXIST" => ApiError::NotFound(msg),
        "RESOURCE_ALREADY_EXISTS" => ApiError::AlreadyExists(msg),
        _ => ApiError::InvalidParameter(msg),
    }
}

// ------------------------------------------------------------------ helpers

impl AppState {
    pub async fn experiment_by_name(&self, name: &str) -> ApiResult<Option<Doc<Experiment>>> {
        let docs: Vec<Doc<Experiment>> = self.store.list(KIND_EXPERIMENT, self.ws(), Filter { name: Some(name), ..Filter::default() }).await?;
        Ok(docs.into_iter().find(|d| d.data.lifecycle_stage == "active"))
    }

    pub async fn ensure_default_experiment(&self) -> ApiResult<Experiment> {
        if let Some(d) = self.store.get::<Experiment>(KIND_EXPERIMENT, "0").await? {
            return Ok(d.data);
        }
        let e = Experiment {
            experiment_id: "0".into(),
            name: "Default".into(),
            artifact_location: self.storage.url_for("/mlflow/0"),
            lifecycle_stage: "active".into(),
            last_update_time: now_ms(),
            creation_time: now_ms(),
            tags: vec![],
        };
        self.store.insert(KIND_EXPERIMENT, self.ws(), "0", None, Some("Default"), &e).await?;
        Ok(e)
    }

    async fn run_doc(&self, run_id: &str) -> ApiResult<Doc<Run>> {
        self.store.get::<Run>(KIND_RUN, run_id).await?.ok_or_else(|| mlflow_err("RESOURCE_DOES_NOT_EXIST", format!("Run with id={run_id} not found")))
    }

    async fn metric_history(&self, run_id: &str, key: &str) -> ApiResult<Vec<Metric>> {
        let id = format!("{run_id}:{key}");
        Ok(self.store.get::<MetricHistory>(KIND_METRIC, &id).await?.map(|d| d.data.points).unwrap_or_default())
    }

    async fn append_metrics(&self, run_id: &str, metrics: &[Metric]) -> ApiResult<()> {
        let mut by_key: BTreeMap<&str, Vec<&Metric>> = BTreeMap::new();
        for m in metrics {
            by_key.entry(m.key.as_str()).or_default().push(m);
        }
        for (key, ms) in by_key {
            let id = format!("{run_id}:{key}");
            let mut hist = self.store.get::<MetricHistory>(KIND_METRIC, &id).await?.map(|d| d.data).unwrap_or(MetricHistory { run_id: run_id.into(), key: key.into(), points: vec![] });
            hist.points.extend(ms.into_iter().cloned());
            self.store.upsert(KIND_METRIC, self.ws(), &id, Some(run_id), Some(key), &hist).await?;
        }
        Ok(())
    }

    /// Resolve an artifact URI (`dbfs:/…`, `/mlflow/…`, `models:/name/1`, `runs:/id/path`) to a storage path.
    pub async fn artifact_storage_path(&self, uri: &str) -> ApiResult<String> {
        if let Some(rest) = uri.strip_prefix("models:/") {
            let mut it = rest.splitn(2, '/');
            let name = it.next().unwrap_or("");
            let sel = it.next().unwrap_or("");
            let mv = self.resolve_model_version(name, sel).await?;
            return Box::pin(self.artifact_storage_path(&mv.source)).await;
        }
        if let Some(rest) = uri.strip_prefix("runs:/") {
            let mut it = rest.splitn(2, '/');
            let run_id = it.next().unwrap_or("");
            let sub = it.next().unwrap_or("");
            let run = self.run_doc(run_id).await?;
            let base = Box::pin(self.artifact_storage_path(&run.data.info.artifact_uri)).await?;
            return Ok(format!("{}/{}", base.trim_end_matches('/'), sub.trim_start_matches('/')).trim_end_matches('/').to_string());
        }
        if let Some(p) = self.storage.path_of(uri) {
            return Ok(p);
        }
        if let Some(p) = uri.strip_prefix("dbfs:") {
            return Ok(super::dbfs::dbfs_path(p));
        }
        if uri.starts_with("mlflow-artifacts:/") || uri.starts_with("mlflow-artifacts://") {
            let rest = uri.trim_start_matches("mlflow-artifacts:").trim_start_matches('/');
            let rest = rest.strip_prefix("api/2.0/mlflow-artifacts/artifacts/").unwrap_or(rest);
            return Ok(format!("/mlflow/{}", rest.trim_start_matches("mlflow/")));
        }
        if uri.starts_with('/') {
            return Ok(uri.to_string());
        }
        Err(ApiError::invalid(format!("Unsupported artifact URI: {uri}")))
    }

    pub async fn resolve_model_version(&self, name: &str, selector: &str) -> ApiResult<ModelVersion> {
        let model: Doc<RegisteredModel> = self.store.get(KIND_MODEL, name).await?.ok_or_else(|| mlflow_err("RESOURCE_DOES_NOT_EXIST", format!("Registered Model with name={name} not found")))?;
        let versions: Vec<Doc<ModelVersion>> = self.store.list(KIND_VERSION, self.ws(), Filter { parent_id: Some(name), ..Filter::default() }).await?;
        let pick = |pred: &dyn Fn(&ModelVersion) -> bool| versions.iter().map(|d| &d.data).filter(|v| pred(v)).max_by_key(|v| v.version.parse::<i64>().unwrap_or(0)).cloned();
        let sel = selector.trim_matches('/');
        let mv = if sel.is_empty() || sel.eq_ignore_ascii_case("latest") {
            pick(&|_| true)
        } else if let Some(alias) = sel.strip_prefix('@') {
            let v = model.data.aliases.iter().find(|a| a.alias == alias).map(|a| a.version.clone());
            v.and_then(|v| pick(&|m| m.version == v))
        } else if sel.chars().all(|c| c.is_ascii_digit()) {
            pick(&|m| m.version == sel)
        } else {
            pick(&|m| m.current_stage.eq_ignore_ascii_case(sel))
        };
        mv.ok_or_else(|| mlflow_err("RESOURCE_DOES_NOT_EXIST", format!("Model version {name}/{selector} not found")))
    }

    async fn latest_versions(&self, name: &str, stages: &[String]) -> ApiResult<Vec<ModelVersion>> {
        let versions: Vec<Doc<ModelVersion>> = self.store.list(KIND_VERSION, self.ws(), Filter { parent_id: Some(name), ..Filter::default() }).await?;
        let mut by_stage: BTreeMap<String, ModelVersion> = BTreeMap::new();
        for d in versions {
            let v = d.data;
            if !stages.is_empty() && !stages.iter().any(|s| s.eq_ignore_ascii_case(&v.current_stage)) {
                continue;
            }
            let cur = by_stage.entry(v.current_stage.clone()).or_insert_with(|| v.clone());
            if v.version.parse::<i64>().unwrap_or(0) > cur.version.parse::<i64>().unwrap_or(0) {
                *cur = v;
            }
        }
        Ok(by_stage.into_values().collect())
    }

    async fn model_with_latest(&self, mut m: RegisteredModel) -> ApiResult<RegisteredModel> {
        m.latest_versions = self.latest_versions(&m.name, &[]).await?;
        Ok(m)
    }
}

// -------------------------------------------------------------- experiments

#[derive(Debug, Deserialize)]
struct CreateExperiment {
    name: String,
    #[serde(default)]
    artifact_location: Option<String>,
    #[serde(default)]
    tags: Vec<Tag>,
}

async fn create_experiment(State(st): State<S>, Body(b): Body<CreateExperiment>) -> ApiResult<Json<Value>> {
    st.ensure_default_experiment().await?;
    if st.experiment_by_name(&b.name).await?.is_some() {
        return Err(mlflow_err("RESOURCE_ALREADY_EXISTS", format!("Experiment '{}' already exists.", b.name)));
    }
    let id = st.store.next_seq("mlflow_experiment").await?.to_string();
    let e = Experiment {
        experiment_id: id.clone(),
        name: b.name.clone(),
        artifact_location: b.artifact_location.unwrap_or_else(|| st.storage.url_for(&format!("/mlflow/{id}"))),
        lifecycle_stage: "active".into(),
        last_update_time: now_ms(),
        creation_time: now_ms(),
        tags: b.tags,
    };
    st.store.insert(KIND_EXPERIMENT, st.ws(), &id, None, Some(&b.name), &e).await?;
    Ok(Json(json!({ "experiment_id": id })))
}

#[derive(Debug, Deserialize)]
struct ExperimentQ {
    #[serde(default)]
    experiment_id: Option<String>,
    #[serde(default)]
    experiment_name: Option<String>,
}

async fn get_experiment(State(st): State<S>, Query(q): Query<ExperimentQ>) -> ApiResult<Json<Value>> {
    st.ensure_default_experiment().await?;
    let id = q.experiment_id.ok_or_else(|| ApiError::invalid("experiment_id is required"))?;
    let d: Doc<Experiment> = st.store.get(KIND_EXPERIMENT, &id).await?.ok_or_else(|| mlflow_err("RESOURCE_DOES_NOT_EXIST", format!("Could not find experiment with ID {id}")))?;
    Ok(Json(json!({ "experiment": d.data })))
}

async fn get_experiment_by_name(State(st): State<S>, Query(q): Query<ExperimentQ>) -> ApiResult<Json<Value>> {
    st.ensure_default_experiment().await?;
    let name = q.experiment_name.ok_or_else(|| ApiError::invalid("experiment_name is required"))?;
    let d = st.experiment_by_name(&name).await?.ok_or_else(|| mlflow_err("RESOURCE_DOES_NOT_EXIST", format!("Could not find experiment with name '{name}'")))?;
    Ok(Json(json!({ "experiment": d.data })))
}

#[derive(Debug, Deserialize, Default)]
struct SearchExperiments {
    #[serde(default)]
    max_results: Option<usize>,
    #[serde(default)]
    page_token: Option<String>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    view_type: Option<String>,
    #[serde(default)]
    order_by: Vec<String>,
}

fn view_ok(view: Option<&str>, stage: &str) -> bool {
    match view.unwrap_or("ACTIVE_ONLY") {
        "ALL" => true,
        "DELETED_ONLY" => stage == "deleted",
        _ => stage == "active",
    }
}

async fn search_experiments_impl(st: &AppState, b: SearchExperiments) -> ApiResult<Json<Value>> {
    st.ensure_default_experiment().await?;
    let docs: Vec<Doc<Experiment>> = st.store.list(KIND_EXPERIMENT, st.ws(), Filter::default()).await?;
    let mut exps: Vec<Experiment> = docs.into_iter().map(|d| d.data).filter(|e| view_ok(b.view_type.as_deref(), &e.lifecycle_stage)).collect();
    if let Some(f) = &b.filter {
        let f = parse_filter(f);
        exps.retain(|e| f.iter().all(|c| c.matches(&|ident| match ident {
            "name" => Some(e.name.clone()),
            other => other.strip_prefix("tags.").and_then(|k| e.tags.iter().find(|t| t.key == k.trim_matches('`')).map(|t| t.value.clone())),
        })));
    }
    if b.order_by.iter().any(|o| o.to_ascii_lowercase().contains("last_update_time")) {
        exps.sort_by_key(|e| std::cmp::Reverse(e.last_update_time));
    } else {
        exps.sort_by(|a, b| a.name.cmp(&b.name));
    }
    let start: usize = b.page_token.as_deref().and_then(|t| t.parse().ok()).unwrap_or(0);
    let max = b.max_results.unwrap_or(1000).clamp(1, 50_000);
    let page: Vec<Experiment> = exps.iter().skip(start).take(max).cloned().collect();
    let next = if start + max < exps.len() { Some((start + max).to_string()) } else { None };
    Ok(Json(json!({ "experiments": page, "next_page_token": next })))
}

async fn search_experiments(State(st): State<S>, Body(b): Body<SearchExperiments>) -> ApiResult<Json<Value>> {
    search_experiments_impl(&st, b).await
}

async fn search_experiments_get(State(st): State<S>, Query(b): Query<SearchExperiments>) -> ApiResult<Json<Value>> {
    search_experiments_impl(&st, b).await
}

#[derive(Debug, Deserialize)]
struct UpdateExperiment {
    experiment_id: String,
    #[serde(default)]
    new_name: Option<String>,
}

async fn update_experiment(State(st): State<S>, Body(b): Body<UpdateExperiment>) -> ApiResult<Json<Value>> {
    if let Some(n) = &b.new_name {
        if st.experiment_by_name(n).await?.map(|d| d.id != b.experiment_id).unwrap_or(false) {
            return Err(mlflow_err("RESOURCE_ALREADY_EXISTS", format!("Experiment '{n}' already exists.")));
        }
    }
    let d = st.store.update::<Experiment, _>(KIND_EXPERIMENT, &b.experiment_id, "Experiment", |e| {
        if let Some(n) = &b.new_name {
            e.name = n.clone();
        }
        e.last_update_time = now_ms();
        Ok(())
    }).await?;
    if b.new_name.is_some() {
        st.store.put(KIND_EXPERIMENT, &d.id, None, Some(&d.data.name), &d.data).await?;
    }
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct ExperimentId {
    experiment_id: String,
}

async fn set_experiment_stage(st: &AppState, id: &str, stage: &str) -> ApiResult<Json<Value>> {
    st.store.update::<Experiment, _>(KIND_EXPERIMENT, id, "Experiment", |e| {
        e.lifecycle_stage = stage.into();
        e.last_update_time = now_ms();
        Ok(())
    }).await?;
    let runs: Vec<Doc<Run>> = st.store.list(KIND_RUN, st.ws(), Filter { parent_id: Some(id), ..Filter::default() }).await?;
    for r in runs {
        let _ = st.store.update::<Run, _>(KIND_RUN, &r.id, "Run", |run| {
            run.info.lifecycle_stage = stage.into();
            Ok(())
        }).await;
    }
    Ok(empty())
}

async fn delete_experiment(State(st): State<S>, Body(b): Body<ExperimentId>) -> ApiResult<Json<Value>> {
    set_experiment_stage(&st, &b.experiment_id, "deleted").await
}

async fn restore_experiment(State(st): State<S>, Body(b): Body<ExperimentId>) -> ApiResult<Json<Value>> {
    set_experiment_stage(&st, &b.experiment_id, "active").await
}

#[derive(Debug, Deserialize)]
struct SetExperimentTag {
    experiment_id: String,
    key: String,
    value: String,
}

async fn set_experiment_tag(State(st): State<S>, Body(b): Body<SetExperimentTag>) -> ApiResult<Json<Value>> {
    st.store.update::<Experiment, _>(KIND_EXPERIMENT, &b.experiment_id, "Experiment", |e| {
        set_tag(&mut e.tags, &b.key, &b.value);
        Ok(())
    }).await?;
    Ok(empty())
}

// --------------------------------------------------------------------- runs

#[derive(Debug, Deserialize)]
struct CreateRun {
    #[serde(default)]
    experiment_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    run_name: Option<String>,
    #[serde(default)]
    start_time: Option<i64>,
    #[serde(default)]
    tags: Vec<Tag>,
}

async fn create_run(State(st): State<S>, Who(p): Who, Body(b): Body<CreateRun>) -> ApiResult<Json<Value>> {
    st.ensure_default_experiment().await?;
    let exp_id = b.experiment_id.unwrap_or_else(|| "0".into());
    let exp: Doc<Experiment> = st.store.get(KIND_EXPERIMENT, &exp_id).await?.ok_or_else(|| mlflow_err("RESOURCE_DOES_NOT_EXIST", format!("Could not find experiment with ID {exp_id}")))?;
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let mut tags = b.tags;
    let name = b.run_name.clone().or_else(|| tags.iter().find(|t| t.key == "mlflow.runName").map(|t| t.value.clone())).unwrap_or_else(|| format!("run-{}", &run_id[..8]));
    set_tag(&mut tags, "mlflow.runName", &name);
    if !tags.iter().any(|t| t.key == "mlflow.user") {
        set_tag(&mut tags, "mlflow.user", &p.user_name);
    }
    let info = RunInfo {
        run_id: run_id.clone(),
        run_uuid: run_id.clone(),
        run_name: name,
        experiment_id: exp_id.clone(),
        user_id: b.user_id.unwrap_or_else(|| p.user_name.clone()),
        status: "RUNNING".into(),
        start_time: b.start_time.unwrap_or_else(now_ms),
        end_time: None,
        artifact_uri: format!("{}/{run_id}/artifacts", exp.data.artifact_location.trim_end_matches('/')),
        lifecycle_stage: "active".into(),
    };
    let run = Run { info, data: RunData { tags, ..RunData::default() }, inputs: json!({ "dataset_inputs": [] }) };
    st.store.insert(KIND_RUN, st.ws(), &run_id, Some(&exp_id), Some(&run.info.run_name), &run).await?;
    Ok(Json(json!({ "run": run })))
}

#[derive(Debug, Deserialize)]
struct RunQ {
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    run_uuid: Option<String>,
}

impl RunQ {
    fn id(&self) -> ApiResult<&str> {
        self.run_id.as_deref().or(self.run_uuid.as_deref()).ok_or_else(|| ApiError::invalid("run_id is required"))
    }
}

async fn get_run(State(st): State<S>, Query(q): Query<RunQ>) -> ApiResult<Json<Value>> {
    let d = st.run_doc(q.id()?).await?;
    Ok(Json(json!({ "run": d.data })))
}

#[derive(Debug, Deserialize)]
struct UpdateRun {
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    run_uuid: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    end_time: Option<i64>,
    #[serde(default)]
    run_name: Option<String>,
}

async fn update_run(State(st): State<S>, Body(b): Body<UpdateRun>) -> ApiResult<Json<Value>> {
    let id = b.run_id.clone().or(b.run_uuid.clone()).ok_or_else(|| ApiError::invalid("run_id is required"))?;
    let d = st.store.update::<Run, _>(KIND_RUN, &id, "Run", |r| {
        if let Some(s) = &b.status {
            r.info.status = s.clone();
        }
        if let Some(e) = b.end_time {
            r.info.end_time = Some(e);
        }
        if let Some(n) = &b.run_name {
            r.info.run_name = n.clone();
            set_tag(&mut r.data.tags, "mlflow.runName", n);
        }
        Ok(())
    }).await?;
    Ok(Json(json!({ "run_info": d.data.info })))
}

async fn delete_run(State(st): State<S>, Body(b): Body<RunQ>) -> ApiResult<Json<Value>> {
    st.store.update::<Run, _>(KIND_RUN, b.id()?, "Run", |r| {
        r.info.lifecycle_stage = "deleted".into();
        Ok(())
    }).await?;
    Ok(empty())
}

async fn restore_run(State(st): State<S>, Body(b): Body<RunQ>) -> ApiResult<Json<Value>> {
    st.store.update::<Run, _>(KIND_RUN, b.id()?, "Run", |r| {
        r.info.lifecycle_stage = "active".into();
        Ok(())
    }).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct LogMetric {
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    run_uuid: Option<String>,
    key: String,
    value: Value,
    timestamp: i64,
    #[serde(default)]
    step: i64,
}

fn metric_value(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap_or(f64::NAN),
        Value::String(s) => match s.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            other => other.parse().unwrap_or(f64::NAN),
        },
        _ => f64::NAN,
    }
}

async fn log_metric(State(st): State<S>, Body(b): Body<LogMetric>) -> ApiResult<Json<Value>> {
    let id = b.run_id.clone().or(b.run_uuid.clone()).ok_or_else(|| ApiError::invalid("run_id is required"))?;
    let m = Metric { key: b.key, value: metric_value(&b.value), timestamp: b.timestamp, step: b.step };
    log_batch_impl(&st, &id, vec![m], vec![], vec![]).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct LogParam {
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    run_uuid: Option<String>,
    key: String,
    value: String,
}

async fn log_param(State(st): State<S>, Body(b): Body<LogParam>) -> ApiResult<Json<Value>> {
    let id = b.run_id.clone().or(b.run_uuid.clone()).ok_or_else(|| ApiError::invalid("run_id is required"))?;
    log_batch_impl(&st, &id, vec![], vec![Tag { key: b.key, value: b.value }], vec![]).await?;
    Ok(empty())
}

async fn set_run_tag(State(st): State<S>, Body(b): Body<LogParam>) -> ApiResult<Json<Value>> {
    let id = b.run_id.clone().or(b.run_uuid.clone()).ok_or_else(|| ApiError::invalid("run_id is required"))?;
    log_batch_impl(&st, &id, vec![], vec![], vec![Tag { key: b.key, value: b.value }]).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct DeleteTag {
    run_id: String,
    key: String,
}

async fn delete_run_tag(State(st): State<S>, Body(b): Body<DeleteTag>) -> ApiResult<Json<Value>> {
    st.store.update::<Run, _>(KIND_RUN, &b.run_id, "Run", |r| {
        r.data.tags.retain(|t| t.key != b.key);
        Ok(())
    }).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct LogBatch {
    run_id: String,
    #[serde(default)]
    metrics: Vec<LogMetricItem>,
    #[serde(default)]
    params: Vec<Tag>,
    #[serde(default)]
    tags: Vec<Tag>,
}

#[derive(Debug, Deserialize)]
struct LogMetricItem {
    key: String,
    value: Value,
    timestamp: i64,
    #[serde(default)]
    step: i64,
}

async fn log_batch_impl(st: &AppState, run_id: &str, metrics: Vec<Metric>, params: Vec<Tag>, tags: Vec<Tag>) -> ApiResult<()> {
    if metrics.len() > 1000 || params.len() > 100 || tags.len() > 100 {
        return Err(ApiError::invalid("log_batch limits: 1000 metrics, 100 params, 100 tags per request"));
    }
    st.store.update::<Run, _>(KIND_RUN, run_id, "Run", |r| {
        for p in &params {
            if let Some(existing) = r.data.params.iter().find(|x| x.key == p.key) {
                if existing.value != p.value {
                    return Err(ApiError::invalid(format!("Changing param values is not allowed. Param with key='{}' was already logged with value='{}'", p.key, existing.value)));
                }
            } else {
                r.data.params.push(p.clone());
            }
        }
        for t in &tags {
            set_tag(&mut r.data.tags, &t.key, &t.value);
            if t.key == "mlflow.runName" {
                r.info.run_name = t.value.clone();
            }
        }
        // `data.metrics` holds the latest value per key (max step, then latest timestamp).
        for m in &metrics {
            match r.data.metrics.iter_mut().find(|x| x.key == m.key) {
                Some(cur) => {
                    if (m.step, m.timestamp) >= (cur.step, cur.timestamp) {
                        *cur = m.clone();
                    }
                }
                None => r.data.metrics.push(m.clone()),
            }
        }
        Ok(())
    }).await?;
    if !metrics.is_empty() {
        st.append_metrics(run_id, &metrics).await?;
    }
    Ok(())
}

async fn log_batch(State(st): State<S>, Body(b): Body<LogBatch>) -> ApiResult<Json<Value>> {
    let metrics = b.metrics.into_iter().map(|m| Metric { key: m.key, value: metric_value(&m.value), timestamp: m.timestamp, step: m.step }).collect();
    log_batch_impl(&st, &b.run_id, metrics, b.params, b.tags).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct LogInputs {
    run_id: String,
    #[serde(default)]
    datasets: Vec<Value>,
    #[serde(default)]
    models: Vec<Value>,
}

async fn log_inputs(State(st): State<S>, Body(b): Body<LogInputs>) -> ApiResult<Json<Value>> {
    st.store.update::<Run, _>(KIND_RUN, &b.run_id, "Run", |r| {
        if !r.inputs.is_object() {
            r.inputs = json!({ "dataset_inputs": [], "model_inputs": [] });
        }
        if let Some(arr) = r.inputs["dataset_inputs"].as_array_mut() {
            arr.extend(b.datasets.iter().cloned());
        } else {
            r.inputs["dataset_inputs"] = json!(b.datasets);
        }
        if !b.models.is_empty() {
            let mut cur = r.inputs["model_inputs"].as_array().cloned().unwrap_or_default();
            cur.extend(b.models.iter().cloned());
            r.inputs["model_inputs"] = json!(cur);
        }
        Ok(())
    }).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct LogModel {
    run_id: String,
    model_json: String,
}

async fn log_model(State(st): State<S>, Body(b): Body<LogModel>) -> ApiResult<Json<Value>> {
    let model: Value = serde_json::from_str(&b.model_json).map_err(|e| ApiError::invalid(format!("model_json: {e}")))?;
    st.store.update::<Run, _>(KIND_RUN, &b.run_id, "Run", |r| {
        let mut hist: Vec<Value> = r.data.tags.iter().find(|t| t.key == "mlflow.log-model.history").and_then(|t| serde_json::from_str(&t.value).ok()).unwrap_or_default();
        hist.push(model.clone());
        set_tag(&mut r.data.tags, "mlflow.log-model.history", &serde_json::to_string(&hist).unwrap_or_default());
        Ok(())
    }).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct MetricHistoryQ {
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    run_uuid: Option<String>,
    metric_key: String,
    #[serde(default)]
    max_results: Option<usize>,
    #[serde(default)]
    page_token: Option<String>,
}

async fn get_metric_history(State(st): State<S>, Query(q): Query<MetricHistoryQ>) -> ApiResult<Json<Value>> {
    let id = q.run_id.as_deref().or(q.run_uuid.as_deref()).ok_or_else(|| ApiError::invalid("run_id is required"))?;
    let pts = st.metric_history(id, &q.metric_key).await?;
    let start: usize = q.page_token.as_deref().and_then(|t| t.parse().ok()).unwrap_or(0);
    let max = q.max_results.unwrap_or(25_000).max(1);
    let page: Vec<&Metric> = pts.iter().skip(start).take(max).collect();
    let next = if start + max < pts.len() { Some((start + max).to_string()) } else { None };
    Ok(Json(json!({ "metrics": page, "next_page_token": next })))
}

// --------------------------------------------------------- run search filter

#[derive(Debug, Clone)]
struct Cond {
    ident: String,
    op: String,
    value: String,
}

impl Cond {
    fn matches(&self, lookup: &dyn Fn(&str) -> Option<String>) -> bool {
        let Some(actual) = lookup(&self.ident) else {
            return matches!(self.op.as_str(), "!=" | "NOT IN" | "NOT LIKE") ;
        };
        let cmp_num = |f: &dyn Fn(std::cmp::Ordering) -> bool| match (actual.parse::<f64>(), self.value.parse::<f64>()) {
            (Ok(a), Ok(b)) => a.partial_cmp(&b).map(f).unwrap_or(false),
            _ => f(actual.cmp(&self.value)),
        };
        match self.op.as_str() {
            "=" => actual == self.value,
            "!=" => actual != self.value,
            ">" => cmp_num(&|o| o == std::cmp::Ordering::Greater),
            ">=" => cmp_num(&|o| o != std::cmp::Ordering::Less),
            "<" => cmp_num(&|o| o == std::cmp::Ordering::Less),
            "<=" => cmp_num(&|o| o != std::cmp::Ordering::Greater),
            "LIKE" => like(&actual, &self.value, true),
            "ILIKE" => like(&actual, &self.value, false),
            "IN" => self.value.split(',').any(|v| v.trim().trim_matches(|c| c == '\'' || c == '"') == actual),
            "NOT IN" => !self.value.split(',').any(|v| v.trim().trim_matches(|c| c == '\'' || c == '"') == actual),
            _ => false,
        }
    }
}

fn like(actual: &str, pattern: &str, case_sensitive: bool) -> bool {
    let (a, p) = if case_sensitive { (actual.to_string(), pattern.to_string()) } else { (actual.to_ascii_lowercase(), pattern.to_ascii_lowercase()) };
    let parts: Vec<&str> = p.split('%').collect();
    if parts.len() == 1 {
        return a == p;
    }
    let mut pos = 0usize;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        match a[pos..].find(part) {
            Some(idx) => {
                if i == 0 && idx != 0 {
                    return false;
                }
                pos += idx + part.len();
            }
            None => return false,
        }
    }
    parts.last().map(|l| l.is_empty() || a.ends_with(l)).unwrap_or(true)
}

/// Parse an MLflow search filter: `metrics.rmse < 1 and params.model = 'cnn' and tags."k" = 'v'`.
fn parse_filter(s: &str) -> Vec<Cond> {
    let mut out = vec![];
    for clause in split_and(s) {
        let clause = clause.trim();
        if clause.is_empty() {
            continue;
        }
        let ops = [">=", "<=", "!=", "=", ">", "<", " NOT LIKE ", " NOT IN ", " ILIKE ", " LIKE ", " IN "];
        let upper = clause.to_ascii_uppercase();
        let mut best: Option<(usize, &str)> = None;
        for op in ops {
            if let Some(i) = upper.find(op) {
                if best.map(|(bi, bop)| i < bi || (i == bi && op.len() > bop.len())).unwrap_or(true) {
                    best = Some((i, op));
                }
            }
        }
        let Some((i, op)) = best else { continue };
        let ident = clause[..i].trim().trim_matches(|c| c == '`' || c == '"').to_string();
        let ident = ident.replace(".`", ".").replace('`', "").replace(".\"", ".").replace('"', "");
        let raw = clause[i + op.len()..].trim();
        let value = raw.trim_matches(|c| c == '\'' || c == '"' || c == '(' || c == ')').to_string();
        out.push(Cond { ident, op: op.trim().to_string(), value });
    }
    out
}

fn split_and(s: &str) -> Vec<String> {
    let mut parts = vec![];
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if let Some(q) = quote {
            cur.push(c);
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if c == '\'' || c == '"' || c == '`' {
            quote = Some(c);
            cur.push(c);
            i += 1;
            continue;
        }
        if (c == 'a' || c == 'A') && i + 3 <= chars.len() && chars[i..i + 3].iter().collect::<String>().eq_ignore_ascii_case("and") && (i == 0 || chars[i - 1].is_whitespace()) && (i + 3 == chars.len() || chars[i + 3].is_whitespace()) {
            parts.push(std::mem::take(&mut cur));
            i += 3;
            continue;
        }
        cur.push(c);
        i += 1;
    }
    parts.push(cur);
    parts
}

fn run_lookup(run: &Run, ident: &str) -> Option<String> {
    let (ns, key) = ident.split_once('.').unwrap_or(("attributes", ident));
    match ns {
        "metrics" | "metric" => run.data.metrics.iter().find(|m| m.key == key).map(|m| m.value.to_string()),
        "params" | "param" | "parameters" => run.data.params.iter().find(|p| p.key == key).map(|p| p.value.clone()),
        "tags" | "tag" => run.data.tags.iter().find(|t| t.key == key).map(|t| t.value.clone()),
        "attributes" | "attr" | "attribute" | "run" => match key {
            "run_id" | "run_uuid" => Some(run.info.run_id.clone()),
            "run_name" => Some(run.info.run_name.clone()),
            "status" => Some(run.info.status.clone()),
            "user_id" => Some(run.info.user_id.clone()),
            "start_time" => Some(run.info.start_time.to_string()),
            "end_time" => run.info.end_time.map(|e| e.to_string()),
            "artifact_uri" => Some(run.info.artifact_uri.clone()),
            "experiment_id" => Some(run.info.experiment_id.clone()),
            "lifecycle_stage" => Some(run.info.lifecycle_stage.clone()),
            _ => None,
        },
        "datasets" | "dataset" => None,
        _ => None,
    }
}

#[derive(Debug, Deserialize)]
struct SearchRuns {
    #[serde(default)]
    experiment_ids: Vec<String>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    run_view_type: Option<String>,
    #[serde(default)]
    max_results: Option<usize>,
    #[serde(default)]
    order_by: Vec<String>,
    #[serde(default)]
    page_token: Option<String>,
}

async fn search_runs(State(st): State<S>, Body(b): Body<SearchRuns>) -> ApiResult<Json<Value>> {
    let mut runs: Vec<Run> = vec![];
    let ids = if b.experiment_ids.is_empty() { vec!["0".to_string()] } else { b.experiment_ids.clone() };
    for eid in &ids {
        let docs: Vec<Doc<Run>> = st.store.list(KIND_RUN, st.ws(), Filter { parent_id: Some(eid), ..Filter::default() }).await?;
        runs.extend(docs.into_iter().map(|d| d.data));
    }
    runs.retain(|r| view_ok(b.run_view_type.as_deref(), &r.info.lifecycle_stage));
    if let Some(f) = &b.filter {
        let conds = parse_filter(f);
        runs.retain(|r| conds.iter().all(|c| c.matches(&|ident| run_lookup(r, ident))));
    }
    let mut order: Vec<(String, bool)> = b.order_by.iter().map(|o| {
        let mut it = o.split_whitespace();
        let ident = it.next().unwrap_or("").to_string();
        let desc = it.next().map(|d| d.eq_ignore_ascii_case("DESC")).unwrap_or(false);
        (ident.trim_matches('`').to_string(), desc)
    }).collect();
    if order.is_empty() {
        order.push(("attributes.start_time".into(), true));
    }
    runs.sort_by(|a, b| {
        for (ident, desc) in &order {
            let va = run_lookup(a, ident);
            let vb = run_lookup(b, ident);
            let ord = match (va.as_ref().and_then(|v| v.parse::<f64>().ok()), vb.as_ref().and_then(|v| v.parse::<f64>().ok())) {
                (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
                _ => va.cmp(&vb),
            };
            let ord = if *desc { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        b.info.start_time.cmp(&a.info.start_time)
    });
    let start: usize = b.page_token.as_deref().and_then(|t| t.parse().ok()).unwrap_or(0);
    let max = b.max_results.unwrap_or(1000).clamp(1, 50_000);
    let page: Vec<&Run> = runs.iter().skip(start).take(max).collect();
    let next = if start + max < runs.len() { Some((start + max).to_string()) } else { None };
    Ok(Json(json!({ "runs": page, "next_page_token": next })))
}

// ---------------------------------------------------------------- artifacts

#[derive(Debug, Deserialize)]
struct ListArtifactsQ {
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    run_uuid: Option<String>,
    #[serde(default)]
    path: Option<String>,
}

async fn list_artifacts(State(st): State<S>, Query(q): Query<ListArtifactsQ>) -> ApiResult<Json<Value>> {
    let id = q.run_id.as_deref().or(q.run_uuid.as_deref()).ok_or_else(|| ApiError::invalid("run_id is required"))?;
    let run = st.run_doc(id).await?;
    let root = st.artifact_storage_path(&run.data.info.artifact_uri).await?;
    let sub = q.path.clone().unwrap_or_default();
    let dir = format!("{}/{}", root.trim_end_matches('/'), sub.trim_matches('/')).trim_end_matches('/').to_string();
    let entries = st.storage.list_dir(&dir).await?;
    let files: Vec<Value> = entries.iter().map(|e| {
        let rel = e.path.strip_prefix(&root).unwrap_or(&e.path).trim_start_matches('/').to_string();
        let mut v = json!({ "path": rel, "is_dir": e.is_dir });
        if !e.is_dir {
            v["file_size"] = json!(e.size);
        }
        v
    }).collect();
    Ok(Json(json!({ "root_uri": run.data.info.artifact_uri, "files": files, "next_page_token": Value::Null })))
}

/// `mlflow-artifacts:/…` proxy: the client PUTs/GETs raw bytes under `/api/2.0/mlflow-artifacts/artifacts/<path>`.
async fn artifact_put(State(st): State<S>, Path(path): Path<String>, body: Bytes) -> ApiResult<Json<Value>> {
    let p = format!("/mlflow/{}", path.trim_start_matches('/'));
    st.storage.put(&p, body).await?;
    Ok(empty())
}

async fn artifact_get(State(st): State<S>, Path(path): Path<String>) -> ApiResult<Response> {
    let p = format!("/mlflow/{}", path.trim_start_matches('/'));
    let bytes = st.storage.get(&p).await.map_err(|_| ApiError::NotFound(format!("artifact {path} not found")))?;
    let mime = mime_guess::from_path(&path).first_or_octet_stream().to_string();
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, mime)], bytes).into_response())
}

async fn artifact_delete(State(st): State<S>, Path(path): Path<String>) -> ApiResult<Json<Value>> {
    let p = format!("/mlflow/{}", path.trim_start_matches('/'));
    if st.storage.head(&p).await?.is_some() {
        st.storage.delete(&p).await?;
    } else {
        st.storage.delete_prefix(&p).await?;
    }
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct ArtifactListQ {
    #[serde(default)]
    path: Option<String>,
}

async fn artifact_list(State(st): State<S>, Query(q): Query<ArtifactListQ>) -> ApiResult<Json<Value>> {
    let sub = q.path.unwrap_or_default();
    let dir = format!("/mlflow/{}", sub.trim_matches('/')).trim_end_matches('/').to_string();
    let entries = st.storage.list_dir(&dir).await?;
    let files: Vec<Value> = entries.iter().map(|e| {
        let rel = e.path.strip_prefix("/mlflow/").unwrap_or(&e.path).to_string();
        let mut v = json!({ "path": rel, "is_dir": e.is_dir });
        if !e.is_dir {
            v["file_size"] = json!(e.size);
        }
        v
    }).collect();
    Ok(Json(json!({ "files": files })))
}

/// Databricks-specific: resolve an artifact URI to a signed/direct download URI.
#[derive(Debug, Deserialize)]
struct DownloadUriQ {
    name: String,
    version: String,
}

async fn model_version_download_uri(State(st): State<S>, Query(q): Query<DownloadUriQ>) -> ApiResult<Json<Value>> {
    let mv = st.resolve_model_version(&q.name, &q.version).await?;
    Ok(Json(json!({ "artifact_uri": mv.source })))
}

// ------------------------------------------------------------ model registry

#[derive(Debug, Deserialize)]
struct CreateModel {
    name: String,
    #[serde(default)]
    tags: Vec<Tag>,
    #[serde(default)]
    description: Option<String>,
}

async fn create_registered_model(State(st): State<S>, Who(p): Who, Body(b): Body<CreateModel>) -> ApiResult<Json<Value>> {
    if st.store.get::<RegisteredModel>(KIND_MODEL, &b.name).await?.is_some() {
        return Err(mlflow_err("RESOURCE_ALREADY_EXISTS", format!("Registered Model (name={}) already exists.", b.name)));
    }
    let m = RegisteredModel { name: b.name.clone(), creation_timestamp: now_ms(), last_updated_timestamp: now_ms(), user_id: p.user_name.clone(), description: b.description, tags: b.tags, aliases: vec![], latest_versions: vec![] };
    st.store.insert(KIND_MODEL, st.ws(), &b.name, None, Some(&b.name), &m).await?;
    Ok(Json(json!({ "registered_model": m })))
}

#[derive(Debug, Deserialize)]
struct NameQ {
    name: String,
}

async fn get_registered_model(State(st): State<S>, Query(q): Query<NameQ>) -> ApiResult<Json<Value>> {
    let d: Doc<RegisteredModel> = st.store.get(KIND_MODEL, &q.name).await?.ok_or_else(|| mlflow_err("RESOURCE_DOES_NOT_EXIST", format!("Registered Model with name={} not found", q.name)))?;
    let m = st.model_with_latest(d.data).await?;
    Ok(Json(json!({ "registered_model": m })))
}

#[derive(Debug, Deserialize)]
struct RenameModel {
    name: String,
    new_name: String,
}

async fn rename_registered_model(State(st): State<S>, Body(b): Body<RenameModel>) -> ApiResult<Json<Value>> {
    let d: Doc<RegisteredModel> = st.store.require(KIND_MODEL, &b.name, "Registered Model").await?;
    if st.store.get::<RegisteredModel>(KIND_MODEL, &b.new_name).await?.is_some() {
        return Err(mlflow_err("RESOURCE_ALREADY_EXISTS", format!("Registered Model (name={}) already exists.", b.new_name)));
    }
    let mut m = d.data;
    m.name = b.new_name.clone();
    m.last_updated_timestamp = now_ms();
    st.store.insert(KIND_MODEL, st.ws(), &b.new_name, None, Some(&b.new_name), &m).await?;
    let versions: Vec<Doc<ModelVersion>> = st.store.list(KIND_VERSION, st.ws(), Filter { parent_id: Some(&b.name), ..Filter::default() }).await?;
    for v in versions {
        let mut mv = v.data;
        mv.name = b.new_name.clone();
        st.store.insert(KIND_VERSION, st.ws(), &version_id(&b.new_name, &mv.version), Some(&b.new_name), Some(&mv.version), &mv).await?;
        st.store.delete(KIND_VERSION, &v.id).await?;
    }
    st.store.delete(KIND_MODEL, &b.name).await?;
    let m = st.model_with_latest(m).await?;
    Ok(Json(json!({ "registered_model": m })))
}

#[derive(Debug, Deserialize)]
struct UpdateModel {
    name: String,
    #[serde(default)]
    description: Option<String>,
}

async fn update_registered_model(State(st): State<S>, Body(b): Body<UpdateModel>) -> ApiResult<Json<Value>> {
    let d = st.store.update::<RegisteredModel, _>(KIND_MODEL, &b.name, "Registered Model", |m| {
        if b.description.is_some() {
            m.description = b.description.clone();
        }
        m.last_updated_timestamp = now_ms();
        Ok(())
    }).await?;
    let m = st.model_with_latest(d.data).await?;
    Ok(Json(json!({ "registered_model": m })))
}

async fn delete_registered_model(State(st): State<S>, Body(b): Body<NameQ>) -> ApiResult<Json<Value>> {
    st.store.require::<RegisteredModel>(KIND_MODEL, &b.name, "Registered Model").await?;
    let versions: Vec<Doc<ModelVersion>> = st.store.list(KIND_VERSION, st.ws(), Filter { parent_id: Some(&b.name), ..Filter::default() }).await?;
    for v in versions {
        st.store.delete(KIND_VERSION, &v.id).await?;
    }
    st.store.delete(KIND_MODEL, &b.name).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize, Default)]
struct SearchModels {
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    max_results: Option<usize>,
    #[serde(default)]
    order_by: Vec<String>,
    #[serde(default)]
    page_token: Option<String>,
}

async fn search_registered_models(State(st): State<S>, Query(q): Query<SearchModels>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<RegisteredModel>> = st.store.list(KIND_MODEL, st.ws(), Filter::default()).await?;
    let mut models: Vec<RegisteredModel> = docs.into_iter().map(|d| d.data).collect();
    if let Some(f) = &q.filter {
        let conds = parse_filter(f);
        models.retain(|m| conds.iter().all(|c| c.matches(&|ident| match ident {
            "name" => Some(m.name.clone()),
            other => other.strip_prefix("tags.").or(other.strip_prefix("tag.")).and_then(|k| m.tags.iter().find(|t| t.key == k).map(|t| t.value.clone())),
        })));
    }
    if q.order_by.iter().any(|o| o.to_ascii_lowercase().starts_with("last_updated_timestamp")) {
        models.sort_by_key(|m| std::cmp::Reverse(m.last_updated_timestamp));
    } else {
        models.sort_by(|a, b| a.name.cmp(&b.name));
    }
    let start: usize = q.page_token.as_deref().and_then(|t| t.parse().ok()).unwrap_or(0);
    let max = q.max_results.unwrap_or(100).clamp(1, 1000);
    let mut page = vec![];
    for m in models.iter().skip(start).take(max) {
        page.push(st.model_with_latest(m.clone()).await?);
    }
    let next = if start + max < models.len() { Some((start + max).to_string()) } else { None };
    Ok(Json(json!({ "registered_models": page, "next_page_token": next })))
}

#[derive(Debug, Deserialize)]
struct LatestVersionsBody {
    name: String,
    #[serde(default)]
    stages: Vec<String>,
}

async fn get_latest_versions(State(st): State<S>, Body(b): Body<LatestVersionsBody>) -> ApiResult<Json<Value>> {
    st.store.require::<RegisteredModel>(KIND_MODEL, &b.name, "Registered Model").await?;
    Ok(Json(json!({ "model_versions": st.latest_versions(&b.name, &b.stages).await? })))
}

#[derive(Debug, Deserialize)]
struct ModelTag {
    name: String,
    key: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    version: Option<String>,
}

async fn set_registered_model_tag(State(st): State<S>, Body(b): Body<ModelTag>) -> ApiResult<Json<Value>> {
    st.store.update::<RegisteredModel, _>(KIND_MODEL, &b.name, "Registered Model", |m| {
        set_tag(&mut m.tags, &b.key, &b.value);
        Ok(())
    }).await?;
    Ok(empty())
}

async fn delete_registered_model_tag(State(st): State<S>, Body(b): Body<ModelTag>) -> ApiResult<Json<Value>> {
    st.store.update::<RegisteredModel, _>(KIND_MODEL, &b.name, "Registered Model", |m| {
        m.tags.retain(|t| t.key != b.key);
        Ok(())
    }).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct AliasBody {
    name: String,
    alias: String,
    #[serde(default)]
    version: Option<String>,
}

async fn set_alias(State(st): State<S>, Body(b): Body<AliasBody>) -> ApiResult<Json<Value>> {
    let version = b.version.clone().ok_or_else(|| ApiError::invalid("version is required"))?;
    st.store.require::<ModelVersion>(KIND_VERSION, &version_id(&b.name, &version), "Model Version").await?;
    st.store.update::<RegisteredModel, _>(KIND_MODEL, &b.name, "Registered Model", |m| {
        m.aliases.retain(|a| a.alias != b.alias);
        m.aliases.push(Alias { alias: b.alias.clone(), version: version.clone() });
        m.last_updated_timestamp = now_ms();
        Ok(())
    }).await?;
    Ok(empty())
}

async fn delete_alias(State(st): State<S>, Query(b): Query<AliasBody>) -> ApiResult<Json<Value>> {
    st.store.update::<RegisteredModel, _>(KIND_MODEL, &b.name, "Registered Model", |m| {
        m.aliases.retain(|a| a.alias != b.alias);
        Ok(())
    }).await?;
    Ok(empty())
}

async fn get_by_alias(State(st): State<S>, Query(b): Query<AliasBody>) -> ApiResult<Json<Value>> {
    let mv = st.resolve_model_version(&b.name, &format!("@{}", b.alias)).await?;
    Ok(Json(json!({ "model_version": mv })))
}

// -------------------------------------------------------------- model versions

#[derive(Debug, Deserialize)]
struct CreateVersion {
    name: String,
    source: String,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    tags: Vec<Tag>,
    #[serde(default)]
    run_link: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

async fn create_model_version(State(st): State<S>, Who(p): Who, Body(b): Body<CreateVersion>) -> ApiResult<Json<Value>> {
    if st.store.get::<RegisteredModel>(KIND_MODEL, &b.name).await?.is_none() {
        let m = RegisteredModel { name: b.name.clone(), creation_timestamp: now_ms(), last_updated_timestamp: now_ms(), user_id: p.user_name.clone(), description: None, tags: vec![], aliases: vec![], latest_versions: vec![] };
        st.store.insert(KIND_MODEL, st.ws(), &b.name, None, Some(&b.name), &m).await?;
    }
    let versions: Vec<Doc<ModelVersion>> = st.store.list(KIND_VERSION, st.ws(), Filter { parent_id: Some(&b.name), ..Filter::default() }).await?;
    let next = versions.iter().filter_map(|v| v.data.version.parse::<i64>().ok()).max().unwrap_or(0) + 1;
    let version = next.to_string();
    let mut tags = b.tags;
    // Resolve `runs:/…` sources to a concrete artifact location so the version survives run deletion.
    let mut source = b.source.clone();
    if let Some(rest) = b.source.strip_prefix("runs:/") {
        let mut it = rest.splitn(2, '/');
        let run_id = it.next().unwrap_or("");
        let sub = it.next().unwrap_or("");
        if let Ok(run) = st.run_doc(run_id).await {
            source = format!("{}/{}", run.data.info.artifact_uri.trim_end_matches('/'), sub.trim_start_matches('/'));
            set_tag(&mut tags, "mlflow.source.run_id", run_id);
        }
    }
    let mv = ModelVersion {
        name: b.name.clone(),
        version: version.clone(),
        creation_timestamp: now_ms(),
        last_updated_timestamp: now_ms(),
        user_id: p.user_name.clone(),
        current_stage: "None".into(),
        description: b.description,
        source,
        run_id: b.run_id,
        status: "READY".into(),
        status_message: None,
        tags,
        run_link: b.run_link,
        aliases: vec![],
    };
    st.store.insert(KIND_VERSION, st.ws(), &version_id(&b.name, &version), Some(&b.name), Some(&version), &mv).await?;
    let _ = st.store.update::<RegisteredModel, _>(KIND_MODEL, &b.name, "Registered Model", |m| {
        m.last_updated_timestamp = now_ms();
        Ok(())
    }).await;
    Ok(Json(json!({ "model_version": mv })))
}

#[derive(Debug, Deserialize)]
struct VersionQ {
    name: String,
    version: String,
}

fn with_aliases(mut mv: ModelVersion, model: &RegisteredModel) -> ModelVersion {
    mv.aliases = model.aliases.iter().filter(|a| a.version == mv.version).map(|a| a.alias.clone()).collect();
    mv
}

async fn get_model_version(State(st): State<S>, Query(q): Query<VersionQ>) -> ApiResult<Json<Value>> {
    let d: Doc<ModelVersion> = st.store.get(KIND_VERSION, &version_id(&q.name, &q.version)).await?.ok_or_else(|| mlflow_err("RESOURCE_DOES_NOT_EXIST", format!("Model Version (name={}, version={}) not found", q.name, q.version)))?;
    let model: Doc<RegisteredModel> = st.store.require(KIND_MODEL, &q.name, "Registered Model").await?;
    Ok(Json(json!({ "model_version": with_aliases(d.data, &model.data) })))
}

#[derive(Debug, Deserialize)]
struct UpdateVersion {
    name: String,
    version: String,
    #[serde(default)]
    description: Option<String>,
}

async fn update_model_version(State(st): State<S>, Body(b): Body<UpdateVersion>) -> ApiResult<Json<Value>> {
    let d = st.store.update::<ModelVersion, _>(KIND_VERSION, &version_id(&b.name, &b.version), "Model Version", |v| {
        if b.description.is_some() {
            v.description = b.description.clone();
        }
        v.last_updated_timestamp = now_ms();
        Ok(())
    }).await?;
    Ok(Json(json!({ "model_version": d.data })))
}

async fn delete_model_version(State(st): State<S>, Body(b): Body<VersionQ>) -> ApiResult<Json<Value>> {
    st.store.delete(KIND_VERSION, &version_id(&b.name, &b.version)).await?;
    let _ = st.store.update::<RegisteredModel, _>(KIND_MODEL, &b.name, "Registered Model", |m| {
        m.aliases.retain(|a| a.version != b.version);
        Ok(())
    }).await;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct Transition {
    name: String,
    version: String,
    stage: String,
    #[serde(default)]
    archive_existing_versions: bool,
}

const STAGES: &[&str] = &["None", "Staging", "Production", "Archived"];

async fn transition_stage(State(st): State<S>, Body(b): Body<Transition>) -> ApiResult<Json<Value>> {
    let stage = STAGES.iter().find(|s| s.eq_ignore_ascii_case(&b.stage)).ok_or_else(|| ApiError::invalid(format!("Invalid Model Version stage: {}. Value must be one of {:?}", b.stage, STAGES)))?;
    if b.archive_existing_versions && (*stage == "Staging" || *stage == "Production") {
        let versions: Vec<Doc<ModelVersion>> = st.store.list(KIND_VERSION, st.ws(), Filter { parent_id: Some(&b.name), ..Filter::default() }).await?;
        for v in versions {
            if v.data.current_stage == *stage && v.data.version != b.version {
                let _ = st.store.update::<ModelVersion, _>(KIND_VERSION, &v.id, "Model Version", |mv| {
                    mv.current_stage = "Archived".into();
                    mv.last_updated_timestamp = now_ms();
                    Ok(())
                }).await;
            }
        }
    }
    let d = st.store.update::<ModelVersion, _>(KIND_VERSION, &version_id(&b.name, &b.version), "Model Version", |v| {
        v.current_stage = stage.to_string();
        v.last_updated_timestamp = now_ms();
        Ok(())
    }).await?;
    Ok(Json(json!({ "model_version": d.data })))
}

#[derive(Debug, Deserialize, Default)]
struct SearchVersions {
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    max_results: Option<usize>,
    #[serde(default)]
    order_by: Vec<String>,
    #[serde(default)]
    page_token: Option<String>,
}

async fn search_model_versions(State(st): State<S>, Query(q): Query<SearchVersions>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<ModelVersion>> = st.store.list(KIND_VERSION, st.ws(), Filter::default()).await?;
    let mut versions: Vec<ModelVersion> = docs.into_iter().map(|d| d.data).collect();
    if let Some(f) = &q.filter {
        let conds = parse_filter(f);
        versions.retain(|v| conds.iter().all(|c| c.matches(&|ident| match ident {
            "name" => Some(v.name.clone()),
            "run_id" => v.run_id.clone(),
            "source_path" | "source" => Some(v.source.clone()),
            "version_number" | "version" => Some(v.version.clone()),
            "current_stage" | "stage" => Some(v.current_stage.clone()),
            other => other.strip_prefix("tags.").or(other.strip_prefix("tag.")).and_then(|k| v.tags.iter().find(|t| t.key == k).map(|t| t.value.clone())),
        })));
    }
    if q.order_by.iter().any(|o| o.to_ascii_lowercase().starts_with("version_number")) {
        versions.sort_by_key(|v| std::cmp::Reverse(v.version.parse::<i64>().unwrap_or(0)));
    } else {
        versions.sort_by_key(|v| std::cmp::Reverse(v.last_updated_timestamp));
    }
    let start: usize = q.page_token.as_deref().and_then(|t| t.parse().ok()).unwrap_or(0);
    let max = q.max_results.unwrap_or(200_000).max(1);
    let page: Vec<&ModelVersion> = versions.iter().skip(start).take(max).collect();
    let next = if start + max < versions.len() { Some((start + max).to_string()) } else { None };
    Ok(Json(json!({ "model_versions": page, "next_page_token": next })))
}

async fn set_model_version_tag(State(st): State<S>, Body(b): Body<ModelTag>) -> ApiResult<Json<Value>> {
    let version = b.version.clone().ok_or_else(|| ApiError::invalid("version is required"))?;
    st.store.update::<ModelVersion, _>(KIND_VERSION, &version_id(&b.name, &version), "Model Version", |v| {
        set_tag(&mut v.tags, &b.key, &b.value);
        Ok(())
    }).await?;
    Ok(empty())
}

async fn delete_model_version_tag(State(st): State<S>, Body(b): Body<ModelTag>) -> ApiResult<Json<Value>> {
    let version = b.version.clone().ok_or_else(|| ApiError::invalid("version is required"))?;
    st.store.update::<ModelVersion, _>(KIND_VERSION, &version_id(&b.name, &version), "Model Version", |v| {
        v.tags.retain(|t| t.key != b.key);
        Ok(())
    }).await?;
    Ok(empty())
}

async fn model_version_download_uri_get(State(st): State<S>, Query(q): Query<VersionQ>) -> ApiResult<Json<Value>> {
    let mv = st.resolve_model_version(&q.name, &q.version).await?;
    Ok(Json(json!({ "artifact_uri": mv.source })))
}

/// Databricks: `/api/2.0/mlflow/databricks/registered-models/get` etc. return the same shapes with ACL info.
async fn databricks_get_model(State(st): State<S>, Query(q): Query<NameQ>) -> ApiResult<Json<Value>> {
    let d: Doc<RegisteredModel> = st.store.require(KIND_MODEL, &q.name, "Registered Model").await?;
    let m = st.model_with_latest(d.data).await?;
    let mut v = serde_json::to_value(&m).unwrap_or_default();
    if let Value::Object(o) = &mut v {
        o.insert("id".into(), json!(m.name));
        o.insert("permission_level".into(), json!("CAN_MANAGE"));
    }
    Ok(Json(json!({ "registered_model_databricks": v })))
}

/// Databricks `mlflow/experiments/list` (deprecated but still used by older clients).
async fn list_experiments(State(st): State<S>, Query(q): Query<SearchExperiments>) -> ApiResult<Json<Value>> {
    let Json(v) = search_experiments_impl(&st, q).await?;
    Ok(Json(json!({ "experiments": v["experiments"], "next_page_token": v["next_page_token"] })))
}

pub fn router() -> Router<S> {
    let mut r = Router::new();
    for prefix in ["/api/2.0/mlflow", "/api/2.0/preview/mlflow", "/ajax-api/2.0/mlflow"] {
        r = r
            .route(&format!("{prefix}/experiments/create"), post(create_experiment))
            .route(&format!("{prefix}/experiments/get"), get(get_experiment))
            .route(&format!("{prefix}/experiments/get-by-name"), get(get_experiment_by_name))
            .route(&format!("{prefix}/experiments/search"), post(search_experiments).get(search_experiments_get))
            .route(&format!("{prefix}/experiments/list"), get(list_experiments))
            .route(&format!("{prefix}/experiments/update"), post(update_experiment))
            .route(&format!("{prefix}/experiments/delete"), post(delete_experiment))
            .route(&format!("{prefix}/experiments/restore"), post(restore_experiment))
            .route(&format!("{prefix}/experiments/set-experiment-tag"), post(set_experiment_tag))
            .route(&format!("{prefix}/runs/create"), post(create_run))
            .route(&format!("{prefix}/runs/get"), get(get_run))
            .route(&format!("{prefix}/runs/update"), post(update_run))
            .route(&format!("{prefix}/runs/delete"), post(delete_run))
            .route(&format!("{prefix}/runs/restore"), post(restore_run))
            .route(&format!("{prefix}/runs/search"), post(search_runs))
            .route(&format!("{prefix}/runs/log-metric"), post(log_metric))
            .route(&format!("{prefix}/runs/log-parameter"), post(log_param))
            .route(&format!("{prefix}/runs/set-tag"), post(set_run_tag))
            .route(&format!("{prefix}/runs/delete-tag"), post(delete_run_tag))
            .route(&format!("{prefix}/runs/log-batch"), post(log_batch))
            .route(&format!("{prefix}/runs/log-inputs"), post(log_inputs))
            .route(&format!("{prefix}/runs/log-model"), post(log_model))
            .route(&format!("{prefix}/metrics/get-history"), get(get_metric_history))
            .route(&format!("{prefix}/artifacts/list"), get(list_artifacts))
            .route(&format!("{prefix}/registered-models/create"), post(create_registered_model))
            .route(&format!("{prefix}/registered-models/get"), get(get_registered_model))
            .route(&format!("{prefix}/registered-models/rename"), post(rename_registered_model))
            .route(&format!("{prefix}/registered-models/update"), axum::routing::patch(update_registered_model).post(update_registered_model))
            .route(&format!("{prefix}/registered-models/delete"), del(delete_registered_model).post(delete_registered_model))
            .route(&format!("{prefix}/registered-models/search"), get(search_registered_models))
            .route(&format!("{prefix}/registered-models/list"), get(search_registered_models))
            .route(&format!("{prefix}/registered-models/get-latest-versions"), post(get_latest_versions).get(get_latest_versions_get))
            .route(&format!("{prefix}/registered-models/set-tag"), post(set_registered_model_tag))
            .route(&format!("{prefix}/registered-models/delete-tag"), del(delete_registered_model_tag).post(delete_registered_model_tag))
            .route(&format!("{prefix}/registered-models/alias"), post(set_alias).delete(delete_alias).get(get_by_alias))
            .route(&format!("{prefix}/model-versions/create"), post(create_model_version))
            .route(&format!("{prefix}/model-versions/get"), get(get_model_version))
            .route(&format!("{prefix}/model-versions/update"), axum::routing::patch(update_model_version).post(update_model_version))
            .route(&format!("{prefix}/model-versions/delete"), del(delete_model_version).post(delete_model_version))
            .route(&format!("{prefix}/model-versions/search"), get(search_model_versions))
            .route(&format!("{prefix}/model-versions/transition-stage"), post(transition_stage))
            .route(&format!("{prefix}/model-versions/set-tag"), post(set_model_version_tag))
            .route(&format!("{prefix}/model-versions/delete-tag"), del(delete_model_version_tag).post(delete_model_version_tag))
            .route(&format!("{prefix}/model-versions/get-download-uri"), get(model_version_download_uri_get))
            .route(&format!("{prefix}/databricks/registered-models/get"), get(databricks_get_model))
            .route(&format!("{prefix}/databricks/model-versions/get-download-uri"), get(model_version_download_uri));
    }
    r.route("/api/2.0/mlflow-artifacts/artifacts", get(artifact_list))
        .route("/api/2.0/mlflow-artifacts/artifacts/{*path}", get(artifact_get).put(artifact_put).delete(artifact_delete))
}

#[derive(Debug, Deserialize)]
struct LatestVersionsQ {
    name: String,
    #[serde(default)]
    stages: Vec<String>,
}

async fn get_latest_versions_get(State(st): State<S>, Query(q): Query<LatestVersionsQ>) -> ApiResult<Json<Value>> {
    st.store.require::<RegisteredModel>(KIND_MODEL, &q.name, "Registered Model").await?;
    Ok(Json(json!({ "model_versions": st.latest_versions(&q.name, &q.stages).await? })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_parsing() {
        let c = parse_filter("metrics.rmse < 1.5 and params.model = 'cnn' AND tags.`mlflow.runName` LIKE 'exp%'");
        assert_eq!(c.len(), 3);
        assert_eq!(c[0].ident, "metrics.rmse");
        assert_eq!(c[0].op, "<");
        assert_eq!(c[1].value, "cnn");
        assert_eq!(c[2].ident, "tags.mlflow.runName");
        assert!(c[2].matches(&|_| Some("exp-1".into())));
        assert!(!c[2].matches(&|_| Some("run-1".into())));
        assert!(c[0].matches(&|_| Some("1.2".into())));
        assert!(!c[0].matches(&|_| Some("2".into())));
    }

    #[test]
    fn like_patterns() {
        assert!(like("hello world", "hello%", true));
        assert!(like("hello world", "%world", true));
        assert!(like("hello world", "%lo wo%", true));
        assert!(!like("hello world", "world%", true));
        assert!(like("Hello", "hello", false));
    }
}
