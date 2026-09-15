//! Jobs API 2.1 (`/api/2.1/jobs/*`): multi-task workflows with a DAG runner.
//!
//! Supported task types: `notebook_task`, `spark_python_task`, `sql_task`
//! (query / file / alert / dashboard refresh), `pipeline_task`, `run_job_task`,
//! `condition_task`, `for_each_task` (sequential). Wheel/JAR/dbt tasks are
//! recorded as unsupported.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use super::notebooks::RunOptions;
use super::{empty, Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::state::{AppState, TaskScope};
use crate::store::{now_ms, Doc, Filter};

pub const KIND_JOB: &str = "job";
pub const KIND_RUN: &str = "job_run";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub job_id: i64,
    pub creator_user_name: String,
    #[serde(default)]
    pub run_as_user_name: String,
    pub created_time: i64,
    pub settings: Map<String, Value>,
    #[serde(default)]
    pub next_run_ms: Option<i64>,
    #[serde(default)]
    pub trigger_state: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RunState {
    pub life_cycle_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_state: Option<String>,
    #[serde(default)]
    pub state_message: String,
    #[serde(default)]
    pub user_cancelled_or_timedout: bool,
}

impl RunState {
    fn pending() -> Self {
        Self { life_cycle_state: "PENDING".into(), ..Default::default() }
    }
    fn running() -> Self {
        Self { life_cycle_state: "RUNNING".into(), ..Default::default() }
    }
    fn terminated(result: &str, msg: impl Into<String>) -> Self {
        Self { life_cycle_state: "TERMINATED".into(), result_state: Some(result.into()), state_message: msg.into(), user_cancelled_or_timedout: matches!(result, "CANCELED" | "TIMEDOUT") }
    }
    fn is_terminal(&self) -> bool {
        matches!(self.life_cycle_state.as_str(), "TERMINATED" | "SKIPPED" | "INTERNAL_ERROR")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRun {
    pub run_id: i64,
    pub task_key: String,
    #[serde(default)]
    pub depends_on: Vec<Value>,
    #[serde(default)]
    pub run_if: Option<String>,
    pub state: RunState,
    #[serde(default)]
    pub start_time: i64,
    #[serde(default)]
    pub end_time: i64,
    #[serde(default)]
    pub setup_duration: i64,
    #[serde(default)]
    pub execution_duration: i64,
    #[serde(default)]
    pub cleanup_duration: i64,
    #[serde(default)]
    pub attempt_number: u32,
    #[serde(default)]
    pub cluster_instance: Option<Value>,
    /// The task definition (notebook_task, sql_task, ... + cluster fields).
    #[serde(flatten)]
    pub def: Map<String, Value>,
    #[serde(default)]
    pub output: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub run_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<i64>,
    pub run_name: String,
    pub number_in_job: i64,
    pub creator_user_name: String,
    pub state: RunState,
    pub start_time: i64,
    #[serde(default)]
    pub end_time: i64,
    #[serde(default)]
    pub setup_duration: i64,
    #[serde(default)]
    pub execution_duration: i64,
    #[serde(default)]
    pub cleanup_duration: i64,
    pub trigger: String,
    pub run_type: String,
    pub tasks: Vec<TaskRun>,
    #[serde(default)]
    pub job_clusters: Vec<Value>,
    #[serde(default)]
    pub job_parameters: Vec<Value>,
    #[serde(default)]
    pub overriding_parameters: Value,
    #[serde(default)]
    pub repair_history: Vec<Value>,
    #[serde(default)]
    pub created_clusters: Vec<String>,
    #[serde(default)]
    pub parent_run_id: Option<i64>,
    #[serde(default)]
    pub format: String,
    #[serde(default)]
    pub git_source: Option<Value>,
}

impl Run {
    pub fn page_url(&self, public_url: &str) -> String {
        match self.job_id {
            Some(j) => format!("{public_url}/#job/{j}/run/{}", self.run_id),
            None => format!("{public_url}/#job/runs/{}", self.run_id),
        }
    }
    pub fn view(&self, public_url: &str) -> Value {
        let mut v = serde_json::to_value(self).unwrap_or(Value::Null);
        v["run_page_url"] = json!(self.page_url(public_url));
        if let Some(o) = v.as_object_mut() {
            o.remove("created_clusters");
        }
        v
    }
}

// ------------------------------------------------------------ parameters

fn params_from_settings(settings: &Map<String, Value>, overrides: &Map<String, Value>) -> Vec<Value> {
    let mut out = vec![];
    if let Some(ps) = settings.get("parameters").and_then(|v| v.as_array()) {
        for p in ps {
            let name = p["name"].as_str().unwrap_or("").to_string();
            let default = p["default"].clone();
            let value = overrides.get(&name).cloned().unwrap_or_else(|| default.clone());
            out.push(json!({ "name": name, "default": default, "value": value }));
        }
    }
    for (k, v) in overrides {
        if !out.iter().any(|p| p["name"] == *k) {
            out.push(json!({ "name": k, "default": Value::Null, "value": v }));
        }
    }
    out
}

/// Build the `{{...}}` substitution table for a task.
fn refs(run: &Run, task: &TaskRun, public_url: &str, ws_id: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    let start = Utc.timestamp_millis_opt(run.start_time).single().unwrap_or_else(Utc::now);
    let job_id = run.job_id.map(|j| j.to_string()).unwrap_or_default();
    m.insert("job.id".into(), job_id.clone());
    m.insert("job_id".into(), job_id);
    m.insert("job.name".into(), run.run_name.clone());
    m.insert("job.run_id".into(), run.run_id.to_string());
    m.insert("run_id".into(), run.run_id.to_string());
    m.insert("parent_run_id".into(), run.run_id.to_string());
    m.insert("job.repair_count".into(), run.repair_history.len().to_string());
    m.insert("job.trigger.type".into(), run.trigger.clone());
    m.insert("task.name".into(), task.task_key.clone());
    m.insert("task_key".into(), task.task_key.clone());
    m.insert("task.run_id".into(), task.run_id.to_string());
    m.insert("task_run_id".into(), task.run_id.to_string());
    m.insert("job.start_time.iso_date".into(), start.format("%Y-%m-%d").to_string());
    m.insert("start_date".into(), start.format("%Y-%m-%d").to_string());
    m.insert("job.start_time.iso_datetime".into(), start.format("%Y-%m-%dT%H:%M:%SZ").to_string());
    m.insert("start_time".into(), run.start_time.to_string());
    m.insert("job.start_time.year".into(), start.format("%Y").to_string());
    m.insert("job.start_time.month".into(), start.format("%m").to_string());
    m.insert("job.start_time.day".into(), start.format("%d").to_string());
    m.insert("job.start_time.hour".into(), start.format("%H").to_string());
    m.insert("job.start_time.minute".into(), start.format("%M").to_string());
    m.insert("job.start_time.second".into(), start.format("%S").to_string());
    m.insert("job.start_time.timestamp_ms".into(), run.start_time.to_string());
    m.insert("workspace.id".into(), ws_id.to_string());
    m.insert("workspace.url".into(), public_url.to_string());
    for p in &run.job_parameters {
        if let Some(n) = p["name"].as_str() {
            m.insert(format!("job.parameters.{n}"), value_str(&p["value"]));
        }
    }
    m
}

fn value_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub fn substitute(s: &str, refs: &HashMap<String, String>, task_values: &dyn Fn(&str, &str) -> Option<String>) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("{{") {
        out.push_str(&rest[..i]);
        let after = &rest[i + 2..];
        match after.find("}}") {
            Some(j) => {
                let key = after[..j].trim();
                let val = refs.get(key).cloned().or_else(|| {
                    // tasks.<task_key>.values.<key>
                    let parts: Vec<&str> = key.split('.').collect();
                    if parts.len() == 4 && parts[0] == "tasks" && parts[2] == "values" {
                        task_values(parts[1], parts[3])
                    } else {
                        None
                    }
                });
                match val {
                    Some(v) => out.push_str(&v),
                    None => out.push_str(&rest[i..i + 2 + j + 2]),
                }
                rest = &after[j + 2..];
            }
            None => {
                out.push_str(rest);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

fn substitute_value(v: &Value, refs: &HashMap<String, String>, tv: &dyn Fn(&str, &str) -> Option<String>) -> Value {
    match v {
        Value::String(s) => Value::String(substitute(s, refs, tv)),
        Value::Array(a) => Value::Array(a.iter().map(|x| substitute_value(x, refs, tv)).collect()),
        Value::Object(o) => Value::Object(o.iter().map(|(k, x)| (k.clone(), substitute_value(x, refs, tv))).collect()),
        other => other.clone(),
    }
}

// ------------------------------------------------------------ scheduling

pub fn next_fire(quartz: &str, tz: &str, after: chrono::DateTime<Utc>) -> Option<i64> {
    let schedule = cron::Schedule::from_str(quartz.trim()).ok()?;
    let tz: chrono_tz::Tz = tz.parse().unwrap_or(chrono_tz::UTC);
    let after_tz = after.with_timezone(&tz);
    schedule.after(&after_tz).next().map(|t| t.with_timezone(&Utc).timestamp_millis())
}

pub fn is_paused(settings: &Map<String, Value>) -> bool {
    settings.get("schedule").and_then(|s| s.get("pause_status")).and_then(|p| p.as_str()) == Some("PAUSED")
        || settings.get("continuous").and_then(|s| s.get("pause_status")).and_then(|p| p.as_str()) == Some("PAUSED")
        || settings.get("trigger").and_then(|s| s.get("pause_status")).and_then(|p| p.as_str()) == Some("PAUSED")
}

impl AppState {
    pub async fn get_job(&self, id: i64) -> ApiResult<Doc<Job>> {
        self.store.require::<Job>(KIND_JOB, &id.to_string(), "Job").await
    }

    pub async fn save_job(&self, j: &Job) -> ApiResult<()> {
        let name = j.settings.get("name").and_then(|v| v.as_str()).map(|s| s.to_string());
        self.store.upsert(KIND_JOB, self.ws(), &j.job_id.to_string(), None, name.as_deref(), j).await
    }

    pub async fn get_run(&self, id: i64) -> ApiResult<Doc<Run>> {
        self.store.require::<Run>(KIND_RUN, &id.to_string(), "Run").await
    }

    pub async fn save_run(&self, r: &Run) -> ApiResult<()> {
        self.store
            .upsert(KIND_RUN, self.ws(), &r.run_id.to_string(), r.job_id.map(|j| j.to_string()).as_deref(), Some(&r.run_name), r)
            .await
    }

    /// Task-run id -> parent run (for `runs/get-output`).
    pub async fn run_for_task(&self, task_run_id: i64) -> ApiResult<(Doc<Run>, usize)> {
        if let Some(rid) = self.store.kv_get(&format!("task_run:{task_run_id}")).await? {
            let run = self.get_run(rid.parse().map_err(|_| ApiError::internal("bad task_run index"))?).await?;
            if let Some(i) = run.data.tasks.iter().position(|t| t.run_id == task_run_id) {
                return Ok((run, i));
            }
        }
        // A run id for a single-task run maps to its only task.
        let run = self.get_run(task_run_id).await?;
        if run.data.tasks.len() == 1 {
            return Ok((run, 0));
        }
        Err(ApiError::NotFound(format!("Run {task_run_id} does not exist.")))
    }

    /// Materialise a run from job settings (or a one-time submit payload).
    #[allow(clippy::too_many_arguments)]
    pub async fn create_run(&self, p: &Principal, job: Option<&Job>, settings: &Map<String, Value>, trigger: &str, run_type: &str, overrides: Map<String, Value>, notebook_params: Map<String, Value>, parent_run_id: Option<i64>) -> ApiResult<Run> {
        let run_id = self.store.next_seq("run_id").await?;
        let tasks_def = settings.get("tasks").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        if tasks_def.is_empty() {
            return Err(ApiError::invalid("At least one task is required."));
        }
        let mut tasks = vec![];
        for t in &tasks_def {
            let mut def = t.as_object().cloned().unwrap_or_default();
            let task_key = def.remove("task_key").and_then(|v| v.as_str().map(|s| s.to_string())).ok_or_else(|| ApiError::invalid("task_key is required for every task"))?;
            let depends_on = def.remove("depends_on").and_then(|v| v.as_array().cloned()).unwrap_or_default();
            let run_if = def.remove("run_if").and_then(|v| v.as_str().map(|s| s.to_string()));
            if !notebook_params.is_empty() {
                if let Some(nb) = def.get_mut("notebook_task").and_then(|v| v.as_object_mut()) {
                    let bp = nb.entry("base_parameters").or_insert(json!({}));
                    if let Some(bp) = bp.as_object_mut() {
                        for (k, v) in &notebook_params {
                            bp.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
            let trid = self.store.next_seq("run_id").await?;
            self.store.kv_set(&format!("task_run:{trid}"), &run_id.to_string()).await?;
            tasks.push(TaskRun { run_id: trid, task_key, depends_on, run_if, state: RunState::pending(), start_time: 0, end_time: 0, setup_duration: 0, execution_duration: 0, cleanup_duration: 0, attempt_number: 0, cluster_instance: None, def, output: None });
        }
        let number_in_job = match job {
            Some(j) => self.store.next_seq(&format!("job_runs:{}", j.job_id)).await?,
            None => 1,
        };
        let run = Run {
            run_id,
            job_id: job.map(|j| j.job_id),
            run_name: settings.get("run_name").or(settings.get("name")).and_then(|v| v.as_str()).unwrap_or("Untitled").to_string(),
            number_in_job,
            creator_user_name: p.user_name.clone(),
            state: RunState::pending(),
            start_time: now_ms(),
            end_time: 0,
            setup_duration: 0,
            execution_duration: 0,
            cleanup_duration: 0,
            trigger: trigger.into(),
            run_type: run_type.into(),
            tasks,
            job_clusters: settings.get("job_clusters").and_then(|v| v.as_array()).cloned().unwrap_or_default(),
            job_parameters: params_from_settings(settings, &overrides),
            overriding_parameters: json!({ "job_parameters": overrides, "notebook_params": notebook_params }),
            repair_history: vec![],
            created_clusters: vec![],
            parent_run_id,
            format: settings.get("format").and_then(|v| v.as_str()).unwrap_or("MULTI_TASK").into(),
            git_source: settings.get("git_source").cloned(),
        };
        self.save_run(&run).await?;
        Ok(run)
    }

    /// Spawn the run in the background and return immediately.
    pub fn spawn_run(self: &Arc<Self>, p: Principal, run: Run, timeout_secs: u64) {
        let st = Arc::clone(self);
        let run_id = run.run_id;
        let handle = tokio::spawn(async move {
            let res = st.execute_run(&p, run, timeout_secs).await;
            if let Err(e) = res {
                tracing::error!(run = run_id, error = %e, "run failed internally");
                if let Ok(mut doc) = st.get_run(run_id).await {
                    doc.data.state = RunState { life_cycle_state: "INTERNAL_ERROR".into(), result_state: Some("FAILED".into()), state_message: e.to_string(), user_cancelled_or_timedout: false };
                    doc.data.end_time = now_ms();
                    let _ = st.save_run(&doc.data).await;
                }
            }
            st.runs.remove(&run_id);
        });
        self.runs.insert(run_id, handle.abort_handle());
    }

    fn execute_run<'a>(self: &'a Arc<Self>, p: &'a Principal, run: Run, timeout_secs: u64) -> futures::future::BoxFuture<'a, ApiResult<()>> {
        Box::pin(self.execute_run_inner(p, run, timeout_secs))
    }

    async fn execute_run_inner(self: &Arc<Self>, p: &Principal, mut run: Run, timeout_secs: u64) -> ApiResult<()> {
        run.state = RunState::running();
        run.start_time = now_ms();
        self.save_run(&run).await?;
        let deadline = (timeout_secs > 0).then(|| tokio::time::Instant::now() + Duration::from_secs(timeout_secs));
        let public_url = self.config.public_url.clone();
        let ws_id = self.ws().to_string();

        // job_cluster_key -> created cluster id (lazily created).
        let mut job_clusters: HashMap<String, String> = HashMap::new();
        let mut done: HashMap<String, RunState> = HashMap::new();
        let mut timed_out = false;

        loop {
            let remaining: Vec<usize> = run.tasks.iter().enumerate().filter(|(_, t)| !t.state.is_terminal()).map(|(i, _)| i).collect();
            if remaining.is_empty() {
                break;
            }
            if let Some(d) = deadline {
                if tokio::time::Instant::now() >= d {
                    timed_out = true;
                    for i in remaining {
                        run.tasks[i].state = RunState::terminated("TIMEDOUT", "Run exceeded timeout_seconds");
                        run.tasks[i].end_time = now_ms();
                    }
                    break;
                }
            }
            // Ready = all dependencies terminal.
            let ready: Vec<usize> = remaining
                .iter()
                .copied()
                .filter(|&i| run.tasks[i].depends_on.iter().all(|d| d["task_key"].as_str().map(|k| done.contains_key(k)).unwrap_or(true)))
                .collect();
            if ready.is_empty() {
                for i in remaining {
                    run.tasks[i].state = RunState::terminated("FAILED", "Dependency cycle or missing task_key in depends_on");
                }
                break;
            }
            // run_if gate
            let mut to_run = vec![];
            for i in ready {
                let t = &run.tasks[i];
                let dep_states: Vec<(&str, &RunState)> = t.depends_on.iter().filter_map(|d| d["task_key"].as_str()).filter_map(|k| done.get(k).map(|s| (k, s))).collect();
                let outcome_ok = |k: &str| t.depends_on.iter().find(|d| d["task_key"] == k).and_then(|d| d["outcome"].as_str()).map(|o| done.get(k).and_then(|s| s.result_state.as_deref()) == Some(o) || done.get(k).and_then(|s| s.state_message.strip_prefix("outcome=")) == Some(o)).unwrap_or(true);
                let all_success = dep_states.iter().all(|(k, s)| s.result_state.as_deref() == Some("SUCCESS") && outcome_ok(k));
                let all_done = dep_states.iter().all(|(_, s)| s.is_terminal());
                let any_failed = dep_states.iter().any(|(k, s)| s.result_state.as_deref() != Some("SUCCESS") || !outcome_ok(k));
                let none_failed = !any_failed;
                let all_failed = !dep_states.is_empty() && dep_states.iter().all(|(_, s)| s.result_state.as_deref() == Some("FAILED"));
                let run_it = match t.run_if.as_deref().unwrap_or("ALL_SUCCESS") {
                    "ALL_DONE" => all_done,
                    "NONE_FAILED" => none_failed,
                    "AT_LEAST_ONE_SUCCESS" => dep_states.iter().any(|(_, s)| s.result_state.as_deref() == Some("SUCCESS")),
                    "ALL_FAILED" => all_failed,
                    "AT_LEAST_ONE_FAILED" => any_failed,
                    _ => all_success,
                };
                if run_it {
                    to_run.push(i);
                } else {
                    run.tasks[i].state = RunState { life_cycle_state: "SKIPPED".into(), result_state: Some("EXCLUDED".into()), state_message: "Skipped by run_if / upstream outcome".into(), user_cancelled_or_timedout: false };
                    run.tasks[i].end_time = now_ms();
                    done.insert(run.tasks[i].task_key.clone(), run.tasks[i].state.clone());
                }
            }
            if to_run.is_empty() {
                self.save_run(&run).await?;
                continue;
            }

            // Resolve clusters for the batch (job clusters are shared across tasks).
            for &i in &to_run {
                let def = &run.tasks[i].def;
                let cluster_id = if let Some(c) = def.get("existing_cluster_id").and_then(|v| v.as_str()) {
                    Some(c.to_string())
                } else if let Some(key) = def.get("job_cluster_key").and_then(|v| v.as_str()) {
                    match job_clusters.get(key) {
                        Some(c) => Some(c.clone()),
                        None => {
                            let spec = run.job_clusters.iter().find(|jc| jc["job_cluster_key"] == key).and_then(|jc| jc["new_cluster"].as_object().cloned());
                            match spec {
                                Some(mut spec) => {
                                    spec.insert("cluster_name".into(), json!(format!("job-{}-run-{}-{key}", run.job_id.unwrap_or(0), run.run_id)));
                                    spec.insert("cluster_source".into(), json!("JOB"));
                                    let id = self.create_cluster_from_json(p, spec, false).await?;
                                    job_clusters.insert(key.to_string(), id.clone());
                                    run.created_clusters.push(id.clone());
                                    Some(id)
                                }
                                None => None,
                            }
                        }
                    }
                } else if let Some(spec) = def.get("new_cluster").and_then(|v| v.as_object()) {
                    let mut spec = spec.clone();
                    spec.insert("cluster_name".into(), json!(format!("job-{}-run-{}-{}", run.job_id.unwrap_or(0), run.run_id, run.tasks[i].task_key)));
                    spec.insert("cluster_source".into(), json!("JOB"));
                    let id = self.create_cluster_from_json(p, spec, false).await?;
                    run.created_clusters.push(id.clone());
                    Some(id)
                } else {
                    None
                };
                let cluster_id = match cluster_id {
                    Some(c) => Some(c),
                    None if needs_cluster(&run.tasks[i].def) => Some(self.default_job_cluster(p, &run).await?),
                    None => None,
                };
                if let Some(c) = &cluster_id {
                    if !run.created_clusters.contains(c) && !def.contains_key("existing_cluster_id") {
                        run.created_clusters.push(c.clone());
                    }
                }
                run.tasks[i].cluster_instance = cluster_id.map(|c| json!({ "cluster_id": c }));
                run.tasks[i].state = RunState::running();
                run.tasks[i].start_time = now_ms();
                run.tasks[i].attempt_number += 1;
            }
            self.save_run(&run).await?;

            // Execute the batch concurrently.
            let mut set = tokio::task::JoinSet::new();
            for &i in &to_run {
                let st = Arc::clone(self);
                let p = p.clone();
                let run_snapshot = run.clone();
                let task = run.tasks[i].clone();
                let public_url = public_url.clone();
                let ws_id = ws_id.clone();
                let task_deadline = deadline;
                set.spawn(async move {
                    let mut attempt = 0u32;
                    let max_retries = task.def.get("max_retries").and_then(|v| v.as_i64()).unwrap_or(0);
                    let retry_wait = task.def.get("min_retry_interval_millis").and_then(|v| v.as_u64()).unwrap_or(0);
                    let retry_on_timeout = task.def.get("retry_on_timeout").and_then(|v| v.as_bool()).unwrap_or(false);
                    loop {
                        let res = st.execute_task(&p, &run_snapshot, &task, &public_url, &ws_id, task_deadline).await;
                        let retry = match &res {
                            (s, _) if s.result_state.as_deref() == Some("FAILED") => max_retries < 0 || (attempt as i64) < max_retries,
                            (s, _) if s.result_state.as_deref() == Some("TIMEDOUT") => retry_on_timeout && (max_retries < 0 || (attempt as i64) < max_retries),
                            _ => false,
                        };
                        if !retry {
                            return (i, attempt + 1, res);
                        }
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(retry_wait)).await;
                    }
                });
            }
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok((i, attempts, (state, output))) => {
                        let t = &mut run.tasks[i];
                        t.state = state.clone();
                        t.output = Some(output);
                        t.end_time = now_ms();
                        t.execution_duration = t.end_time - t.start_time;
                        t.attempt_number = attempts;
                        done.insert(t.task_key.clone(), state);
                    }
                    Err(e) => tracing::error!(error = %e, "task join error"),
                }
                self.save_run(&run).await?;
            }
        }

        // Finalise
        let failed = run.tasks.iter().any(|t| matches!(t.state.result_state.as_deref(), Some("FAILED") | Some("TIMEDOUT") | Some("CANCELED")));
        let canceled = run.tasks.iter().any(|t| t.state.result_state.as_deref() == Some("CANCELED"));
        run.state = if timed_out {
            RunState::terminated("TIMEDOUT", "Run timed out")
        } else if canceled {
            RunState::terminated("CANCELED", "Run cancelled")
        } else if failed {
            let msg = run.tasks.iter().filter(|t| t.state.result_state.as_deref() != Some("SUCCESS") && t.state.life_cycle_state != "SKIPPED").map(|t| format!("{}: {}", t.task_key, t.state.state_message)).collect::<Vec<_>>().join("; ");
            RunState::terminated("FAILED", msg)
        } else {
            RunState::terminated("SUCCESS", "")
        };
        run.end_time = now_ms();
        run.execution_duration = run.end_time - run.start_time;
        self.save_run(&run).await?;
        self.cleanup_run(&run).await;
        self.notify_run(&run).await;
        Ok(())
    }

    async fn default_job_cluster(self: &Arc<Self>, p: &Principal, run: &Run) -> ApiResult<String> {
        // Prefer a running interactive cluster, else spin up a small job cluster.
        let clusters: Vec<Doc<super::clusters::Cluster>> = self.store.list(super::clusters::KIND, self.ws(), Filter::default()).await?;
        if let Some(c) = clusters.iter().find(|c| c.data.state == super::clusters::ClusterState::Running && c.data.cluster_source != "JOB") {
            return Ok(c.data.cluster_id.clone());
        }
        let mut spec = Map::new();
        spec.insert("cluster_name".into(), json!(format!("job-{}-run-{}", run.job_id.unwrap_or(0), run.run_id)));
        spec.insert("num_workers".into(), json!(1));
        spec.insert("node_type_id".into(), json!("lf.small"));
        spec.insert("cluster_source".into(), json!("JOB"));
        spec.insert("autotermination_minutes".into(), json!(10));
        self.create_cluster_from_json(p, spec, false).await
    }

    async fn cleanup_run(&self, run: &Run) {
        for c in &run.created_clusters {
            if let Ok(doc) = self.get_cluster(c).await {
                if doc.data.cluster_source == "JOB" {
                    let _ = self.terminate_cluster(c, "JOB_FINISHED").await;
                }
            }
        }
        let ctxs: Vec<String> = self.jobs_task_context.iter().filter(|e| e.value().run_id == run.run_id).map(|e| e.key().clone()).collect();
        for c in ctxs {
            self.jobs_task_context.remove(&c);
            self.destroy_context(&c).await;
        }
    }

    async fn notify_run(&self, run: &Run) {
        let Some(job_id) = run.job_id else { return };
        let Ok(job) = self.get_job(job_id).await else { return };
        let ok = run.state.result_state.as_deref() == Some("SUCCESS");
        let key = if ok { "on_success" } else { "on_failure" };
        if let Some(emails) = job.data.settings.get("email_notifications").and_then(|e| e.get(key)).and_then(|v| v.as_array()) {
            for e in emails {
                tracing::info!(run = run.run_id, to = %e, result = ?run.state.result_state, "email notification (log only)");
            }
        }
        if let Some(hooks) = job.data.settings.get("webhook_notifications").and_then(|e| e.get(key)).and_then(|v| v.as_array()) {
            for h in hooks {
                let id = h["id"].as_str().unwrap_or("");
                if let Ok(Some(dest)) = self.store.get::<Value>(super::misc::KIND_NOTIFICATION_DEST, id).await {
                    if let Some(url) = dest.data.pointer("/config/generic_webhook/url").and_then(|u| u.as_str()) {
                        let body = json!({ "event_type": if ok { "jobs.on_success" } else { "jobs.on_failure" }, "job": { "job_id": job_id, "name": run.run_name }, "run": { "run_id": run.run_id, "run_name": run.run_name, "state": run.state, "start_time": run.start_time, "end_time": run.end_time, "run_page_url": run.page_url(&self.config.public_url) } });
                        let _ = reqwest::Client::new().post(url).json(&body).timeout(Duration::from_secs(10)).send().await;
                    }
                }
            }
        }
    }

    /// Execute one task attempt. Returns (state, output).
    async fn execute_task(self: &Arc<Self>, p: &Principal, run: &Run, task: &TaskRun, public_url: &str, ws_id: &str, deadline: Option<tokio::time::Instant>) -> (RunState, Value) {
        let r = refs(run, task, public_url, ws_id);
        let scope = TaskScope { run_id: run.run_id, task_key: task.task_key.clone() };
        let tv = |task_key: &str, key: &str| -> Option<String> {
            let ctx = self.jobs_task_context.iter().find(|e| e.value().run_id == run.run_id && e.value().task_key == task_key).map(|e| e.key().clone())?;
            self.contexts.task_values.get(&ctx).and_then(|m| m.get(key).map(value_str))
        };
        let def = substitute_value(&Value::Object(task.def.clone()), &r, &tv);
        let def = def.as_object().cloned().unwrap_or_default();
        let task_timeout = def.get("timeout_seconds").and_then(|v| v.as_u64()).filter(|t| *t > 0).map(Duration::from_secs);
        let timeout = match (task_timeout, deadline) {
            (Some(t), Some(d)) => Some(t.min(d.saturating_duration_since(tokio::time::Instant::now()))),
            (Some(t), None) => Some(t),
            (None, Some(d)) => Some(d.saturating_duration_since(tokio::time::Instant::now())),
            (None, None) => None,
        };
        let cluster_id = task.cluster_instance.as_ref().and_then(|c| c["cluster_id"].as_str()).unwrap_or("").to_string();

        let out = async {
            if let Some(nb) = def.get("notebook_task") {
                let path = nb["notebook_path"].as_str().ok_or_else(|| ApiError::invalid("notebook_task.notebook_path is required"))?;
                let mut args: HashMap<String, String> = nb["base_parameters"].as_object().map(|o| o.iter().map(|(k, v)| (k.clone(), value_str(v))).collect()).unwrap_or_default();
                for jp in &run.job_parameters {
                    if let Some(n) = jp["name"].as_str() {
                        args.entry(n.to_string()).or_insert_with(|| value_str(&jp["value"]));
                    }
                }
                let res = self.run_notebook(p, path, &cluster_id, RunOptions { arguments: args, timeout, inline_context: None, scope: Some(scope.clone()), extra_env: HashMap::new() }).await?;
                let logs = res.cells.iter().flat_map(|c| c.outputs.iter()).filter_map(|o| match o {
                    crate::kernel::KernelEvent::Stdout { text } | crate::kernel::KernelEvent::Stderr { text } | crate::kernel::KernelEvent::Result { text } => Some(text.clone()),
                    _ => None,
                }).collect::<Vec<_>>().join("");
                let output = json!({ "notebook_output": { "result": res.result, "truncated": false }, "logs": logs, "logs_truncated": false, "error": res.error, "error_trace": res.cells.last().and_then(|c| c.outputs.iter().find_map(|o| match o { crate::kernel::KernelEvent::Error { traceback, .. } => Some(traceback.join("")), _ => None })), "metadata": { "cells": res.cells.len() } });
                let state = match res.status.as_str() {
                    "SUCCESS" => RunState::terminated("SUCCESS", ""),
                    "TIMEDOUT" => RunState::terminated("TIMEDOUT", res.error.unwrap_or_default()),
                    "CANCELED" => RunState::terminated("CANCELED", res.error.unwrap_or_default()),
                    _ => RunState::terminated("FAILED", res.error.unwrap_or_default()),
                };
                return Ok::<(RunState, Value), ApiError>((state, output));
            }
            if let Some(pt) = def.get("spark_python_task") {
                let file = pt["python_file"].as_str().ok_or_else(|| ApiError::invalid("spark_python_task.python_file is required"))?;
                let params: Vec<String> = pt["parameters"].as_array().map(|a| a.iter().map(value_str).collect()).unwrap_or_default();
                let code = self.read_text_any(file).await?;
                let ctx = self.create_context(p, &cluster_id, "python", None, HashMap::new()).await?;
                self.jobs_task_context.insert(ctx.id.clone(), scope.clone());
                let argv = serde_json::to_string(&std::iter::once(file.to_string()).chain(params).collect::<Vec<_>>())?;
                let prelude = format!("import sys as _sys, json as _json\n_sys.argv = _json.loads({argv:?})\n");
                let rec = self.run_command(&ctx.id, "python", &format!("{prelude}{code}"), timeout).await?;
                self.destroy_context(&ctx.id).await;
                let (logs, err, trace) = summarize_outputs(&rec.outputs);
                let state = match rec.status {
                    super::commands::CommandStatus::Finished => RunState::terminated("SUCCESS", ""),
                    super::commands::CommandStatus::Cancelled => RunState::terminated("TIMEDOUT", err.clone().unwrap_or_default()),
                    _ => RunState::terminated("FAILED", err.clone().unwrap_or_default()),
                };
                return Ok((state, json!({ "logs": logs, "error": err, "error_trace": trace })));
            }
            if let Some(sq) = def.get("sql_task") {
                let warehouse_id = sq["warehouse_id"].as_str();
                let params: HashMap<String, String> = sq["parameters"].as_object().map(|o| o.iter().map(|(k, v)| (k.clone(), value_str(v))).collect()).unwrap_or_default();
                let sql_text = if let Some(q) = sq.get("query") {
                    if let Some(text) = q["query_text"].as_str() {
                        text.to_string()
                    } else {
                        let qid = q["query_id"].as_str().ok_or_else(|| ApiError::invalid("sql_task.query.query_id (or query_text) is required"))?;
                        let doc = self.store.require::<Value>(super::sql::KIND_QUERY, qid, "Query").await?;
                        doc.data["query_text"].as_str().unwrap_or("").to_string()
                    }
                } else if let Some(f) = sq.get("file") {
                    let path = f["path"].as_str().ok_or_else(|| ApiError::invalid("sql_task.file.path is required"))?;
                    self.read_text_any(path).await?
                } else if let Some(a) = sq.get("alert") {
                    let aid = a["alert_id"].as_str().ok_or_else(|| ApiError::invalid("sql_task.alert.alert_id is required"))?;
                    let v = self.evaluate_alert(aid).await?;
                    return Ok((RunState::terminated("SUCCESS", ""), json!({ "sql_output": { "alert_output": v } })));
                } else if let Some(d) = sq.get("dashboard") {
                    let did = d["dashboard_id"].as_str().unwrap_or("");
                    return Ok((RunState::terminated("SUCCESS", ""), json!({ "sql_output": { "dashboard_output": { "dashboard_id": did, "refreshed": true } } })));
                } else {
                    return Err(ApiError::invalid("sql_task requires query, file, alert or dashboard"));
                };
                let mut sql = sql_text;
                for (k, v) in &params {
                    sql = sql.replace(&format!("{{{{{k}}}}}"), v).replace(&format!(":{k}"), &format!("'{}'", v.replace('\'', "''")));
                }
                let (cid, wid) = self.resolve_compute(warehouse_id, if cluster_id.is_empty() { None } else { Some(&cluster_id) }).await?;
                let mut last = json!(null);
                for stmt in super::sql::split_statements(&sql) {
                    let res = self.execute_sql(&cid, wid.as_deref(), p, &stmt, HashMap::new(), 1000).await?;
                    last = json!({ "columns": res.columns, "rows": res.rows, "row_count": res.row_count });
                }
                return Ok((RunState::terminated("SUCCESS", ""), json!({ "sql_output": { "query_output": { "output_link": format!("{public_url}/#sql/history"), "result": last } } })));
            }
            if let Some(pl) = def.get("pipeline_task") {
                let pid = pl["pipeline_id"].as_str().ok_or_else(|| ApiError::invalid("pipeline_task.pipeline_id is required"))?;
                let full = pl["full_refresh"].as_bool().unwrap_or(false);
                let update_id = self.start_pipeline_update(p, pid, full, Some(cluster_id.as_str()).filter(|c| !c.is_empty())).await?;
                let state = self.wait_pipeline_update(pid, &update_id, timeout).await?;
                let ok = state["state"] == "COMPLETED";
                return Ok((if ok { RunState::terminated("SUCCESS", "") } else { RunState::terminated("FAILED", state["cause"].as_str().unwrap_or("pipeline update failed").to_string()) }, json!({ "pipeline_output": { "update_id": update_id, "state": state } })));
            }
            if let Some(rj) = def.get("run_job_task") {
                let jid = rj["job_id"].as_i64().ok_or_else(|| ApiError::invalid("run_job_task.job_id is required"))?;
                let overrides = rj["job_parameters"].as_object().cloned().unwrap_or_default();
                let job = self.get_job(jid).await?.data;
                let child = self.create_run(p, Some(&job), &job.settings, "RUN_JOB_TASK", "WORKFLOW_RUN", overrides, Map::new(), Some(run.run_id)).await?;
                let child_id = child.run_id;
                let job_timeout = job.settings.get("timeout_seconds").and_then(|v| v.as_u64()).unwrap_or(0);
                self.execute_run(p, child, job_timeout).await?;
                let child = self.get_run(child_id).await?.data;
                let ok = child.state.result_state.as_deref() == Some("SUCCESS");
                return Ok((if ok { RunState::terminated("SUCCESS", "") } else { RunState::terminated("FAILED", child.state.state_message.clone()) }, json!({ "run_job_output": { "run_id": child_id, "state": child.state } })));
            }
            if let Some(ct) = def.get("condition_task") {
                let op = ct["op"].as_str().unwrap_or("EQUAL_TO");
                let left = value_str(&ct["left"]);
                let right = value_str(&ct["right"]);
                let num = |s: &str| s.trim().parse::<f64>().ok();
                let result = match (op, num(&left), num(&right)) {
                    ("EQUAL_TO", Some(a), Some(b)) => a == b,
                    ("NOT_EQUAL", Some(a), Some(b)) => a != b,
                    ("GREATER_THAN", Some(a), Some(b)) => a > b,
                    ("GREATER_THAN_OR_EQUAL", Some(a), Some(b)) => a >= b,
                    ("LESS_THAN", Some(a), Some(b)) => a < b,
                    ("LESS_THAN_OR_EQUAL", Some(a), Some(b)) => a <= b,
                    ("EQUAL_TO", _, _) => left.trim() == right.trim(),
                    ("NOT_EQUAL", _, _) => left.trim() != right.trim(),
                    ("GREATER_THAN", _, _) => left > right,
                    ("GREATER_THAN_OR_EQUAL", _, _) => left >= right,
                    ("LESS_THAN", _, _) => left < right,
                    ("LESS_THAN_OR_EQUAL", _, _) => left <= right,
                    _ => false,
                };
                let outcome = if result { "true" } else { "false" };
                return Ok((RunState { life_cycle_state: "TERMINATED".into(), result_state: Some("SUCCESS".into()), state_message: format!("outcome={outcome}"), user_cancelled_or_timedout: false }, json!({ "condition_output": { "result": result, "outcome": outcome, "left": left, "right": right, "op": op } })));
            }
            if let Some(fe) = def.get("for_each_task") {
                let inputs: Vec<Value> = match &fe["inputs"] {
                    Value::String(s) => serde_json::from_str(s).unwrap_or_else(|_| s.split(',').map(|x| json!(x.trim())).collect()),
                    Value::Array(a) => a.clone(),
                    _ => vec![],
                };
                let inner = fe["task"].as_object().cloned().ok_or_else(|| ApiError::invalid("for_each_task.task is required"))?;
                let mut iterations = vec![];
                let mut failed = 0;
                for (idx, input) in inputs.iter().enumerate() {
                    let mut it_refs = r.clone();
                    it_refs.insert("input".into(), value_str(input));
                    let it_def = substitute_value(&Value::Object(inner.clone()), &it_refs, &tv).as_object().cloned().unwrap_or_default();
                    let it_task = TaskRun { run_id: task.run_id, task_key: format!("{}[{idx}]", task.task_key), depends_on: vec![], run_if: None, state: RunState::pending(), start_time: now_ms(), end_time: 0, setup_duration: 0, execution_duration: 0, cleanup_duration: 0, attempt_number: 1, cluster_instance: task.cluster_instance.clone(), def: it_def, output: None };
                    let (s, o) = Box::pin(self.execute_task(p, run, &it_task, public_url, ws_id, deadline)).await;
                    if s.result_state.as_deref() != Some("SUCCESS") {
                        failed += 1;
                    }
                    iterations.push(json!({ "input": input, "state": s, "output": o }));
                }
                let state = if failed == 0 { RunState::terminated("SUCCESS", "") } else { RunState::terminated("FAILED", format!("{failed}/{} iterations failed", inputs.len())) };
                return Ok((state, json!({ "for_each_output": { "iterations": iterations } })));
            }
            let kind = ["python_wheel_task", "spark_jar_task", "spark_submit_task", "dbt_task"].iter().find(|k| def.contains_key(**k)).copied().unwrap_or("unknown");
            Err(ApiError::invalid(format!("Task type {kind} is not supported by Lakeforge (supported: notebook_task, spark_python_task, sql_task, pipeline_task, run_job_task, condition_task, for_each_task)")))
        }
        .await;

        match out {
            Ok(v) => v,
            Err(e) => (RunState::terminated("FAILED", e.to_string()), json!({ "error": e.to_string() })),
        }
    }

    /// Read a text file from the workspace (`/Workspace/...` or `/...`) or DBFS (`dbfs:/...`).
    pub async fn read_text_any(&self, path: &str) -> ApiResult<String> {
        if let Some(p) = path.strip_prefix("dbfs:") {
            return Ok(String::from_utf8_lossy(&self.storage.get(&super::dbfs::dbfs_path(p)).await?).to_string());
        }
        let p = path.strip_prefix("/Workspace").unwrap_or(path);
        let obj = self.ws_require(p).await?;
        match obj.object_type {
            super::workspace::ObjectType::File => Ok(String::from_utf8_lossy(&self.ws_read_file(&obj).await?).to_string()),
            super::workspace::ObjectType::Notebook => {
                let nb = obj.notebook.clone().unwrap_or_default();
                Ok(nb.cells.iter().filter(|c| c.language != "markdown").map(|c| c.source.clone()).collect::<Vec<_>>().join("\n\n"))
            }
            _ => Err(ApiError::invalid(format!("{path} is not a file"))),
        }
    }

    /// Scheduler tick: fire cron schedules, continuous jobs and file-arrival triggers.
    pub async fn tick_jobs(self: &Arc<Self>) -> ApiResult<()> {
        let jobs: Vec<Doc<Job>> = self.store.list(KIND_JOB, self.ws(), Filter::default()).await?;
        let now = now_ms();
        for doc in jobs {
            let mut job = doc.data;
            if is_paused(&job.settings) {
                continue;
            }
            let admin = self.system_principal(&job.run_as_user_name).await;
            let active = self.active_runs(job.job_id).await?;
            let max_conc = job.settings.get("max_concurrent_runs").and_then(|v| v.as_i64()).unwrap_or(1).max(1);

            if let Some(sched) = job.settings.get("schedule").cloned() {
                let expr = sched["quartz_cron_expression"].as_str().unwrap_or("");
                let tz = sched["timezone_id"].as_str().unwrap_or("UTC");
                match job.next_run_ms {
                    None => {
                        job.next_run_ms = next_fire(expr, tz, Utc::now());
                        self.save_job(&job).await?;
                    }
                    Some(t) if t <= now => {
                        job.next_run_ms = next_fire(expr, tz, Utc::now());
                        self.save_job(&job).await?;
                        if active < max_conc {
                            let run = self.create_run(&admin, Some(&job), &job.settings, "PERIODIC", "JOB_RUN", Map::new(), Map::new(), None).await?;
                            let t = job.settings.get("timeout_seconds").and_then(|v| v.as_u64()).unwrap_or(0);
                            self.spawn_run(admin.clone(), run, t);
                        }
                    }
                    _ => {}
                }
            }
            if job.settings.get("continuous").is_some() && active == 0 {
                let run = self.create_run(&admin, Some(&job), &job.settings, "CONTINUOUS", "JOB_RUN", Map::new(), Map::new(), None).await?;
                self.spawn_run(admin.clone(), run, 0);
            }
            if let Some(fa) = job.settings.get("trigger").and_then(|t| t.get("file_arrival")).cloned() {
                let url = fa["url"].as_str().unwrap_or("");
                let path = self.storage.path_of(url).or_else(|| url.strip_prefix("dbfs:").map(super::dbfs::dbfs_path)).unwrap_or_else(|| url.to_string());
                let min_gap = fa["min_time_between_triggers_seconds"].as_i64().unwrap_or(0) * 1000;
                let last_fired = job.trigger_state.get("last_fired_ms").and_then(|v| v.as_i64()).unwrap_or(0);
                let seen = job.trigger_state.get("last_seen_ms").and_then(|v| v.as_i64());
                if let Ok(entries) = self.storage.list_all(&path).await {
                    let newest = entries.iter().map(|e| e.modified_ms).max().unwrap_or(0);
                    match seen {
                        None => {
                            job.trigger_state.insert("last_seen_ms".into(), json!(newest));
                            self.save_job(&job).await?;
                        }
                        Some(s) if newest > s && now - last_fired >= min_gap => {
                            job.trigger_state.insert("last_seen_ms".into(), json!(newest));
                            job.trigger_state.insert("last_fired_ms".into(), json!(now));
                            self.save_job(&job).await?;
                            if active < max_conc {
                                let run = self.create_run(&admin, Some(&job), &job.settings, "FILE_ARRIVAL", "JOB_RUN", Map::new(), Map::new(), None).await?;
                                self.spawn_run(admin.clone(), run, 0);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn active_runs(&self, job_id: i64) -> ApiResult<i64> {
        let runs: Vec<Doc<Run>> = self.store.list(KIND_RUN, self.ws(), Filter { parent_id: Some(&job_id.to_string()), ..Default::default() }).await?;
        Ok(runs.iter().filter(|r| !r.data.state.is_terminal()).count() as i64)
    }

    pub async fn cancel_run(&self, run_id: i64) -> ApiResult<()> {
        let mut doc = self.get_run(run_id).await?;
        if doc.data.state.is_terminal() {
            return Ok(());
        }
        if let Some((_, h)) = self.runs.remove(&run_id) {
            h.abort();
        }
        for t in &mut doc.data.tasks {
            if !t.state.is_terminal() {
                t.state = RunState::terminated("CANCELED", "Cancelled by user");
                t.end_time = now_ms();
            }
        }
        doc.data.state = RunState::terminated("CANCELED", "Cancelled by user");
        doc.data.end_time = now_ms();
        self.save_run(&doc.data).await?;
        self.cleanup_run(&doc.data).await;
        Ok(())
    }
}

fn needs_cluster(def: &Map<String, Value>) -> bool {
    def.contains_key("notebook_task") || def.contains_key("spark_python_task") || def.contains_key("for_each_task")
}

fn summarize_outputs(outputs: &[crate::kernel::KernelEvent]) -> (String, Option<String>, Option<String>) {
    let mut logs = String::new();
    let mut err = None;
    let mut trace = None;
    for o in outputs {
        match o {
            crate::kernel::KernelEvent::Stdout { text } | crate::kernel::KernelEvent::Stderr { text } | crate::kernel::KernelEvent::Result { text } => logs.push_str(text),
            crate::kernel::KernelEvent::Error { ename, evalue, traceback } => {
                err = Some(format!("{ename}: {evalue}"));
                trace = Some(traceback.join(""));
            }
            _ => {}
        }
    }
    (logs, err, trace)
}

fn validate_settings(s: &Map<String, Value>) -> ApiResult<()> {
    let tasks = s.get("tasks").and_then(|v| v.as_array()).ok_or_else(|| ApiError::invalid("tasks is required"))?;
    let mut keys = HashSet::new();
    for t in tasks {
        let k = t["task_key"].as_str().ok_or_else(|| ApiError::invalid("task_key is required"))?;
        if !keys.insert(k.to_string()) {
            return Err(ApiError::invalid(format!("Duplicate task_key {k}")));
        }
    }
    for t in tasks {
        for d in t["depends_on"].as_array().cloned().unwrap_or_default() {
            if let Some(k) = d["task_key"].as_str() {
                if !keys.contains(k) {
                    return Err(ApiError::invalid(format!("depends_on references unknown task_key {k}")));
                }
            }
        }
    }
    if let Some(sch) = s.get("schedule") {
        let expr = sch["quartz_cron_expression"].as_str().ok_or_else(|| ApiError::invalid("schedule.quartz_cron_expression is required"))?;
        cron::Schedule::from_str(expr.trim()).map_err(|e| ApiError::invalid(format!("Invalid quartz_cron_expression: {e}")))?;
    }
    Ok(())
}

// ---------------------------------------------------------------- handlers

async fn create(State(st): State<S>, Who(p): Who, Body(mut settings): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let access = settings.remove("access_control_list");
    validate_settings(&settings)?;
    settings.entry("name").or_insert(json!("Untitled"));
    settings.entry("format").or_insert(json!("MULTI_TASK"));
    settings.entry("max_concurrent_runs").or_insert(json!(1));
    let job_id = st.store.next_seq("job_id").await?;
    let run_as = settings.get("run_as").and_then(|r| r.get("user_name")).and_then(|v| v.as_str()).unwrap_or(&p.user_name).to_string();
    let job = Job { job_id, creator_user_name: p.user_name.clone(), run_as_user_name: run_as, created_time: now_ms(), settings, next_run_ms: None, trigger_state: Map::new() };
    st.store.insert(KIND_JOB, st.ws(), &job_id.to_string(), None, job.settings.get("name").and_then(|v| v.as_str()), &job).await?;
    if let Some(acl) = access {
        let _ = st.set_permissions("jobs", &job_id.to_string(), &acl, &p.user_name).await;
    }
    Ok(Json(json!({ "job_id": job_id })))
}

fn job_view(j: &Job, expand: bool) -> Value {
    let mut v = json!({ "job_id": j.job_id, "creator_user_name": j.creator_user_name, "run_as_user_name": j.run_as_user_name, "created_time": j.created_time, "settings": j.settings });
    if !expand {
        if let Some(s) = v["settings"].as_object_mut() {
            s.remove("tasks");
            s.remove("job_clusters");
        }
    }
    if let Some(n) = j.next_run_ms {
        v["next_run_ms"] = json!(n);
    }
    v
}

#[derive(Debug, Deserialize)]
struct ListJobsQ {
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    offset: Option<i64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    expand_tasks: Option<bool>,
    #[serde(default)]
    page_token: Option<String>,
}

async fn list(State(st): State<S>, Query(q): Query<ListJobsQ>) -> ApiResult<Json<Value>> {
    let limit = q.limit.unwrap_or(20).clamp(1, 100);
    let offset = q.page_token.as_deref().and_then(|t| t.parse().ok()).or(q.offset).unwrap_or(0);
    let mut docs: Vec<Doc<Job>> = st.store.list(KIND_JOB, st.ws(), Filter { name: q.name.as_deref(), ..Default::default() }).await?;
    docs.sort_by_key(|d| d.data.job_id);
    let total = docs.len() as i64;
    let page: Vec<Value> = docs.iter().skip(offset as usize).take(limit as usize).map(|d| job_view(&d.data, q.expand_tasks.unwrap_or(false))).collect();
    let has_more = offset + limit < total;
    let mut v = json!({ "jobs": page, "has_more": has_more });
    if has_more {
        v["next_page_token"] = json!((offset + limit).to_string());
    }
    if offset > 0 {
        v["prev_page_token"] = json!((offset - limit).max(0).to_string());
    }
    Ok(Json(v))
}

#[derive(Debug, Deserialize)]
struct JobIdQ {
    job_id: i64,
}

async fn get_job_h(State(st): State<S>, Query(q): Query<JobIdQ>) -> ApiResult<Json<Value>> {
    Ok(Json(job_view(&st.get_job(q.job_id).await?.data, true)))
}

#[derive(Debug, Deserialize)]
struct ResetBody {
    job_id: i64,
    new_settings: Map<String, Value>,
}

async fn reset(State(st): State<S>, Body(b): Body<ResetBody>) -> ApiResult<Json<Value>> {
    validate_settings(&b.new_settings)?;
    let mut job = st.get_job(b.job_id).await?.data;
    job.settings = b.new_settings;
    job.next_run_ms = None;
    st.save_job(&job).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct UpdateBody {
    job_id: i64,
    #[serde(default)]
    new_settings: Map<String, Value>,
    #[serde(default)]
    fields_to_remove: Vec<String>,
}

async fn update(State(st): State<S>, Body(b): Body<UpdateBody>) -> ApiResult<Json<Value>> {
    let mut job = st.get_job(b.job_id).await?.data;
    for (k, v) in b.new_settings {
        if k == "tasks" {
            // Merge tasks by task_key.
            let mut tasks = job.settings.get("tasks").and_then(|t| t.as_array()).cloned().unwrap_or_default();
            for nt in v.as_array().cloned().unwrap_or_default() {
                match tasks.iter_mut().find(|t| t["task_key"] == nt["task_key"]) {
                    Some(t) => *t = nt,
                    None => tasks.push(nt),
                }
            }
            job.settings.insert(k, Value::Array(tasks));
        } else {
            job.settings.insert(k, v);
        }
    }
    for f in b.fields_to_remove {
        if let Some(tk) = f.strip_prefix("tasks/") {
            if let Some(tasks) = job.settings.get_mut("tasks").and_then(|t| t.as_array_mut()) {
                tasks.retain(|t| t["task_key"] != tk);
            }
        } else if let Some(jc) = f.strip_prefix("job_clusters/") {
            if let Some(cs) = job.settings.get_mut("job_clusters").and_then(|t| t.as_array_mut()) {
                cs.retain(|c| c["job_cluster_key"] != jc);
            }
        } else {
            job.settings.remove(&f);
        }
    }
    validate_settings(&job.settings)?;
    job.next_run_ms = None;
    st.save_job(&job).await?;
    Ok(empty())
}

async fn delete(State(st): State<S>, Body(b): Body<JobIdQ>) -> ApiResult<Json<Value>> {
    st.get_job(b.job_id).await?;
    let runs: Vec<Doc<Run>> = st.store.list(KIND_RUN, st.ws(), Filter { parent_id: Some(&b.job_id.to_string()), ..Default::default() }).await?;
    for r in runs {
        let _ = st.cancel_run(r.data.run_id).await;
    }
    st.store.delete(KIND_JOB, &b.job_id.to_string()).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct RunNowBody {
    job_id: i64,
    #[serde(default)]
    job_parameters: Map<String, Value>,
    #[serde(default)]
    notebook_params: Map<String, Value>,
    #[serde(default)]
    python_params: Vec<Value>,
    #[serde(default)]
    idempotency_token: Option<String>,
}

async fn run_now(State(st): State<S>, Who(p): Who, Body(b): Body<RunNowBody>) -> ApiResult<Json<Value>> {
    if let Some(tok) = &b.idempotency_token {
        if let Some(existing) = st.store.kv_get(&format!("idem:{}:{tok}", b.job_id)).await? {
            let run = st.get_run(existing.parse().unwrap_or(0)).await?.data;
            return Ok(Json(json!({ "run_id": run.run_id, "number_in_job": run.number_in_job })));
        }
    }
    let job = st.get_job(b.job_id).await?.data;
    let active = st.active_runs(job.job_id).await?;
    let max_conc = job.settings.get("max_concurrent_runs").and_then(|v| v.as_i64()).unwrap_or(1).max(1);
    if active >= max_conc {
        return Err(ApiError::InvalidState(format!("Job {} already has {active} active run(s); max_concurrent_runs={max_conc}", job.job_id)));
    }
    let mut settings = job.settings.clone();
    if !b.python_params.is_empty() {
        if let Some(tasks) = settings.get_mut("tasks").and_then(|t| t.as_array_mut()) {
            for t in tasks {
                if let Some(pt) = t.get_mut("spark_python_task") {
                    pt["parameters"] = Value::Array(b.python_params.clone());
                }
            }
        }
    }
    let run = st.create_run(&p, Some(&job), &settings, "ONE_TIME", "JOB_RUN", b.job_parameters, b.notebook_params, None).await?;
    if let Some(tok) = &b.idempotency_token {
        st.store.kv_set(&format!("idem:{}:{tok}", b.job_id), &run.run_id.to_string()).await?;
    }
    let timeout = job.settings.get("timeout_seconds").and_then(|v| v.as_u64()).unwrap_or(0);
    let out = json!({ "run_id": run.run_id, "number_in_job": run.number_in_job });
    st.spawn_run(p, run, timeout);
    Ok(Json(out))
}

async fn submit(State(st): State<S>, Who(p): Who, Body(mut b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    if let Some(idem) = b.get("idempotency_token").and_then(|v| v.as_str()) {
        if let Some(existing) = st.store.kv_get(&format!("idem:submit:{idem}")).await? {
            return Ok(Json(json!({ "run_id": existing.parse::<i64>().unwrap_or(0) })));
        }
    }
    // Legacy single-task submit shape (notebook_task at top level).
    if !b.contains_key("tasks") {
        let mut task = Map::new();
        task.insert("task_key".into(), json!("main"));
        for k in ["notebook_task", "spark_python_task", "sql_task", "pipeline_task", "spark_jar_task", "python_wheel_task", "existing_cluster_id", "new_cluster", "timeout_seconds", "libraries"] {
            if let Some(v) = b.remove(k) {
                task.insert(k.into(), v);
            }
        }
        b.insert("tasks".into(), json!([task]));
    }
    validate_settings(&b)?;
    b.entry("run_name").or_insert(json!(format!("Untitled run {}", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"))));
    let timeout = b.get("timeout_seconds").and_then(|v| v.as_u64()).unwrap_or(0);
    let run = st.create_run(&p, None, &b, "ONE_TIME", "SUBMIT_RUN", Map::new(), Map::new(), None).await?;
    if let Some(idem) = b.get("idempotency_token").and_then(|v| v.as_str()) {
        st.store.kv_set(&format!("idem:submit:{idem}"), &run.run_id.to_string()).await?;
    }
    let out = json!({ "run_id": run.run_id });
    st.spawn_run(p, run, timeout);
    Ok(Json(out))
}

#[derive(Debug, Deserialize)]
struct ListRunsQ {
    #[serde(default)]
    job_id: Option<i64>,
    #[serde(default)]
    active_only: Option<bool>,
    #[serde(default)]
    completed_only: Option<bool>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    offset: Option<i64>,
    #[serde(default)]
    page_token: Option<String>,
    #[serde(default)]
    run_type: Option<String>,
    #[serde(default)]
    expand_tasks: Option<bool>,
    #[serde(default)]
    start_time_from: Option<i64>,
    #[serde(default)]
    start_time_to: Option<i64>,
}

async fn list_runs(State(st): State<S>, Query(q): Query<ListRunsQ>) -> ApiResult<Json<Value>> {
    let limit = q.limit.unwrap_or(20).clamp(1, 100);
    let offset = q.page_token.as_deref().and_then(|t| t.parse().ok()).or(q.offset).unwrap_or(0);
    let jid = q.job_id.map(|j| j.to_string());
    let mut docs: Vec<Doc<Run>> = st.store.list(KIND_RUN, st.ws(), Filter { parent_id: jid.as_deref(), ..Default::default() }).await?;
    docs.retain(|d| {
        let r = &d.data;
        (!q.active_only.unwrap_or(false) || !r.state.is_terminal())
            && (!q.completed_only.unwrap_or(false) || r.state.is_terminal())
            && q.run_type.as_deref().map(|t| t == r.run_type).unwrap_or(true)
            && q.start_time_from.map(|t| r.start_time >= t).unwrap_or(true)
            && q.start_time_to.map(|t| r.start_time <= t).unwrap_or(true)
    });
    docs.sort_by_key(|d| std::cmp::Reverse(d.data.start_time));
    let total = docs.len() as i64;
    let expand = q.expand_tasks.unwrap_or(false);
    let page: Vec<Value> = docs
        .iter()
        .skip(offset as usize)
        .take(limit as usize)
        .map(|d| {
            let mut v = d.data.view(&st.config.public_url);
            if !expand {
                if let Some(o) = v.as_object_mut() {
                    o.remove("tasks");
                    o.remove("job_clusters");
                }
            }
            v
        })
        .collect();
    let has_more = offset + limit < total;
    let mut v = json!({ "runs": page, "has_more": has_more });
    if has_more {
        v["next_page_token"] = json!((offset + limit).to_string());
    }
    Ok(Json(v))
}

#[derive(Debug, Deserialize)]
struct RunIdQ {
    run_id: i64,
    #[serde(default)]
    #[allow(dead_code)]
    include_history: Option<bool>,
}

async fn get_run(State(st): State<S>, Query(q): Query<RunIdQ>) -> ApiResult<Json<Value>> {
    match st.get_run(q.run_id).await {
        Ok(doc) => Ok(Json(doc.data.view(&st.config.public_url))),
        Err(ApiError::NotFound(_)) => {
            // task run id
            let (run, i) = st.run_for_task(q.run_id).await?;
            let mut v = serde_json::to_value(&run.data.tasks[i])?;
            v["job_id"] = json!(run.data.job_id);
            v["parent_run_id"] = json!(run.data.run_id);
            v["run_page_url"] = json!(run.data.page_url(&st.config.public_url));
            Ok(Json(v))
        }
        Err(e) => Err(e),
    }
}

async fn get_output(State(st): State<S>, Query(q): Query<RunIdQ>) -> ApiResult<Json<Value>> {
    let (run, i) = st.run_for_task(q.run_id).await?;
    let t = &run.data.tasks[i];
    let mut meta = run.data.view(&st.config.public_url);
    if let Some(o) = meta.as_object_mut() {
        o.remove("tasks");
        o.insert("task".into(), json!({ "task_key": t.task_key, "state": t.state, "run_id": t.run_id, "attempt_number": t.attempt_number, "cluster_instance": t.cluster_instance }));
    }
    let mut v = t.output.clone().unwrap_or_else(|| json!({}));
    if !t.state.is_terminal() {
        return Err(ApiError::InvalidState(format!("Run {} is still {}", q.run_id, t.state.life_cycle_state)));
    }
    if v.get("error").is_none() && t.state.result_state.as_deref() != Some("SUCCESS") {
        v["error"] = json!(t.state.state_message);
    }
    v["metadata"] = meta;
    Ok(Json(v))
}

async fn cancel(State(st): State<S>, Body(b): Body<RunIdQ>) -> ApiResult<Json<Value>> {
    match st.cancel_run(b.run_id).await {
        Ok(()) => Ok(empty()),
        Err(ApiError::NotFound(_)) => {
            let (run, _) = st.run_for_task(b.run_id).await?;
            st.cancel_run(run.data.run_id).await?;
            Ok(empty())
        }
        Err(e) => Err(e),
    }
}

async fn cancel_all(State(st): State<S>, Body(b): Body<JobIdQ>) -> ApiResult<Json<Value>> {
    let runs: Vec<Doc<Run>> = st.store.list(KIND_RUN, st.ws(), Filter { parent_id: Some(&b.job_id.to_string()), ..Default::default() }).await?;
    for r in runs {
        if !r.data.state.is_terminal() {
            st.cancel_run(r.data.run_id).await?;
        }
    }
    Ok(empty())
}

async fn delete_run(State(st): State<S>, Body(b): Body<RunIdQ>) -> ApiResult<Json<Value>> {
    let run = st.get_run(b.run_id).await?.data;
    if !run.state.is_terminal() {
        return Err(ApiError::InvalidState("Cannot delete an active run; cancel it first.".into()));
    }
    for t in &run.tasks {
        let _ = st.store.kv_delete(&format!("task_run:{}", t.run_id)).await;
    }
    st.store.delete(KIND_RUN, &b.run_id.to_string()).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct RepairBody {
    run_id: i64,
    #[serde(default)]
    rerun_tasks: Vec<String>,
    #[serde(default)]
    rerun_all_failed_tasks: bool,
    #[serde(default)]
    rerun_dependent_tasks: bool,
    #[serde(default)]
    job_parameters: Map<String, Value>,
    #[serde(default)]
    notebook_params: Map<String, Value>,
}

async fn repair(State(st): State<S>, Who(p): Who, Body(b): Body<RepairBody>) -> ApiResult<Json<Value>> {
    let mut run = st.get_run(b.run_id).await?.data;
    if !run.state.is_terminal() {
        return Err(ApiError::InvalidState("Run is still active".into()));
    }
    let mut rerun: HashSet<String> = b.rerun_tasks.iter().cloned().collect();
    if b.rerun_all_failed_tasks {
        rerun.extend(run.tasks.iter().filter(|t| t.state.result_state.as_deref() != Some("SUCCESS")).map(|t| t.task_key.clone()));
    }
    if b.rerun_dependent_tasks {
        let mut changed = true;
        while changed {
            changed = false;
            for t in &run.tasks {
                if !rerun.contains(&t.task_key) && t.depends_on.iter().any(|d| d["task_key"].as_str().map(|k| rerun.contains(k)).unwrap_or(false)) {
                    rerun.insert(t.task_key.clone());
                    changed = true;
                }
            }
        }
    }
    if rerun.is_empty() {
        return Err(ApiError::invalid("Nothing to repair: specify rerun_tasks or rerun_all_failed_tasks"));
    }
    let repair_id = st.store.next_seq("repair_id").await?;
    let mut settings = run.overriding_parameters.clone();
    if let Some(jp) = settings.get_mut("job_parameters").and_then(|v| v.as_object_mut()) {
        for (k, v) in &b.job_parameters {
            jp.insert(k.clone(), v.clone());
        }
    }
    for t in &mut run.tasks {
        if rerun.contains(&t.task_key) {
            t.state = RunState::pending();
            t.output = None;
            t.end_time = 0;
            if !b.notebook_params.is_empty() {
                if let Some(nb) = t.def.get_mut("notebook_task").and_then(|v| v.as_object_mut()) {
                    let bp = nb.entry("base_parameters").or_insert(json!({}));
                    if let Some(bp) = bp.as_object_mut() {
                        for (k, v) in &b.notebook_params {
                            bp.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }
    }
    run.job_parameters = run.job_parameters.iter().map(|p| {
        let mut p = p.clone();
        if let Some(v) = p["name"].as_str().and_then(|n| b.job_parameters.get(n)) {
            p["value"] = v.clone();
        }
        p
    }).collect();
    run.repair_history.push(json!({ "id": repair_id, "type": "REPAIR", "start_time": now_ms(), "state": { "life_cycle_state": "RUNNING" }, "task_run_ids": run.tasks.iter().filter(|t| rerun.contains(&t.task_key)).map(|t| t.run_id).collect::<Vec<_>>() }));
    run.state = RunState::pending();
    run.end_time = 0;
    st.save_run(&run).await?;
    let timeout = match run.job_id {
        Some(j) => st.get_job(j).await.ok().and_then(|d| d.data.settings.get("timeout_seconds").and_then(|v| v.as_u64())).unwrap_or(0),
        None => 0,
    };
    st.spawn_run(p, run, timeout);
    Ok(Json(json!({ "repair_id": repair_id })))
}

async fn export_run(State(st): State<S>, Query(q): Query<RunIdQ>) -> ApiResult<Json<Value>> {
    let (run, i) = st.run_for_task(q.run_id).await?;
    let t = &run.data.tasks[i];
    let mut views = vec![];
    if let Some(path) = t.def.get("notebook_task").and_then(|n| n["notebook_path"].as_str()) {
        if let Ok((bytes, _)) = super::workspace::export_bytes(&st, path, "HTML").await {
            views.push(json!({ "content": String::from_utf8_lossy(&bytes), "name": path, "type": "NOTEBOOK" }));
        }
    }
    Ok(Json(json!({ "views": views })))
}

pub fn router() -> Router<S> {
    let r = Router::new();
    let mut r = r;
    for v in ["2.0", "2.1"] {
        r = r
            .route(&format!("/api/{v}/jobs/create"), post(create))
            .route(&format!("/api/{v}/jobs/list"), get(list))
            .route(&format!("/api/{v}/jobs/get"), get(get_job_h))
            .route(&format!("/api/{v}/jobs/reset"), post(reset))
            .route(&format!("/api/{v}/jobs/update"), post(update))
            .route(&format!("/api/{v}/jobs/delete"), post(delete))
            .route(&format!("/api/{v}/jobs/run-now"), post(run_now))
            .route(&format!("/api/{v}/jobs/runs/submit"), post(submit))
            .route(&format!("/api/{v}/jobs/runs/list"), get(list_runs))
            .route(&format!("/api/{v}/jobs/runs/get"), get(get_run))
            .route(&format!("/api/{v}/jobs/runs/get-output"), get(get_output))
            .route(&format!("/api/{v}/jobs/runs/cancel"), post(cancel))
            .route(&format!("/api/{v}/jobs/runs/cancel-all"), post(cancel_all))
            .route(&format!("/api/{v}/jobs/runs/delete"), post(delete_run))
            .route(&format!("/api/{v}/jobs/runs/repair"), post(repair))
            .route(&format!("/api/{v}/jobs/runs/export"), get(export_run));
    }
    r
}
