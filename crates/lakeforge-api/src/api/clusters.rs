//! Clusters API (`/api/2.0/clusters/*`) and the cluster lifecycle service.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use lakeforge_cluster_manager::{BackendState, ClusterHandle, LaunchSpec};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use super::{empty, Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};

pub const KIND: &str = "cluster";
pub const KIND_EVENT: &str = "cluster_event";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ClusterState {
    Pending,
    Running,
    Restarting,
    Resizing,
    Terminating,
    Terminated,
    Error,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    pub cluster_id: String,
    pub cluster_name: String,
    #[serde(default = "default_spark_version")]
    pub spark_version: String,
    #[serde(default = "default_node_type")]
    pub node_type_id: String,
    #[serde(default)]
    pub driver_node_type_id: Option<String>,
    #[serde(default)]
    pub num_workers: u32,
    #[serde(default)]
    pub autoscale: Option<Autoscale>,
    #[serde(default = "default_autotermination")]
    pub autotermination_minutes: u32,
    #[serde(default)]
    pub spark_conf: BTreeMap<String, String>,
    #[serde(default)]
    pub spark_env_vars: BTreeMap<String, String>,
    #[serde(default)]
    pub custom_tags: BTreeMap<String, String>,
    pub state: ClusterState,
    #[serde(default)]
    pub state_message: String,
    pub creator_user_name: String,
    #[serde(default = "default_source")]
    pub cluster_source: String,
    pub start_time: i64,
    #[serde(default)]
    pub terminated_time: i64,
    #[serde(default)]
    pub last_state_loss_time: i64,
    #[serde(default)]
    pub last_activity_time: i64,
    #[serde(default)]
    pub last_restarted_time: i64,
    #[serde(default = "default_security_mode")]
    pub data_security_mode: String,
    #[serde(default = "default_runtime_engine")]
    pub runtime_engine: String,
    #[serde(default)]
    pub single_user_name: Option<String>,
    #[serde(default)]
    pub policy_id: Option<String>,
    #[serde(default)]
    pub instance_pool_id: Option<String>,
    #[serde(default)]
    pub enable_elastic_disk: bool,
    #[serde(default)]
    pub is_single_node: bool,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub init_scripts: Vec<Value>,
    #[serde(default)]
    pub cluster_log_conf: Option<Value>,
    #[serde(default)]
    pub docker_image: Option<Value>,
    #[serde(default)]
    pub aws_attributes: Option<Value>,
    #[serde(default)]
    pub azure_attributes: Option<Value>,
    #[serde(default)]
    pub gcp_attributes: Option<Value>,
    #[serde(default)]
    pub termination_reason: Option<Value>,
    #[serde(default)]
    pub handle: Option<ClusterHandle>,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub libraries: Vec<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Autoscale {
    pub min_workers: u32,
    pub max_workers: u32,
}

fn default_spark_version() -> String {
    "forge-0.1.x-lts".into()
}
fn default_node_type() -> String {
    "lf.medium".into()
}
fn default_autotermination() -> u32 {
    120
}
fn default_source() -> String {
    "API".into()
}
fn default_security_mode() -> String {
    "SINGLE_USER".into()
}
fn default_runtime_engine() -> String {
    "FORGE".into()
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeType {
    pub node_type_id: String,
    pub memory_mb: u64,
    pub num_cores: f32,
    pub description: String,
    pub category: String,
    pub is_deprecated: bool,
    pub node_instance_type: Value,
}

pub fn node_types() -> Vec<NodeType> {
    let mk = |id: &str, mem: u64, cores: f32, desc: &str, cat: &str| NodeType {
        node_type_id: id.into(),
        memory_mb: mem,
        num_cores: cores,
        description: desc.into(),
        category: cat.into(),
        is_deprecated: false,
        node_instance_type: json!({ "instance_type_id": id, "local_disks": 1, "local_disk_size_gb": 100 }),
    };
    vec![
        mk("lf.small", 4096, 2.0, "2 slots, 4 GB", "General Purpose"),
        mk("lf.medium", 8192, 4.0, "4 slots, 8 GB", "General Purpose"),
        mk("lf.large", 16384, 8.0, "8 slots, 16 GB", "General Purpose"),
        mk("lf.xlarge", 32768, 16.0, "16 slots, 32 GB", "General Purpose"),
        mk("lf.memory-large", 65536, 8.0, "8 slots, 64 GB", "Memory Optimized"),
        mk("lf.compute-xlarge", 16384, 32.0, "32 slots, 16 GB", "Compute Optimized"),
    ]
}

pub fn spark_versions() -> Vec<Value> {
    vec![
        json!({ "key": "forge-0.1.x-lts", "name": "Forge 0.1 LTS (DataFusion 53, Delta 4, Arrow 58)" }),
        json!({ "key": "forge-0.1.x-photon-lts", "name": "Forge 0.1 LTS Vectorized (native Rust engine)" }),
        json!({ "key": "forge-0.1.x-ml-lts", "name": "Forge 0.1 LTS ML (Python kernel with pandas/sklearn/mlflow)" }),
    ]
}

/// (slots, memory_mb) for a node type. Unknown cloud instance names fall back to medium.
pub fn node_shape(node_type_id: &str) -> (u32, u64) {
    node_types()
        .into_iter()
        .find(|n| n.node_type_id == node_type_id)
        .map(|n| (n.num_cores as u32, n.memory_mb))
        .unwrap_or((4, 8192))
}

impl AppState {
    pub async fn get_cluster(&self, id: &str) -> ApiResult<Doc<Cluster>> {
        self.store.require(KIND, id, "Cluster").await
    }

    pub async fn save_cluster(&self, c: &Cluster) -> ApiResult<()> {
        self.store.put(KIND, &c.cluster_id, None, Some(&c.cluster_name), c).await
    }

    pub async fn cluster_event(&self, cluster_id: &str, kind: &str, details: Value) -> ApiResult<()> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let ev = json!({ "cluster_id": cluster_id, "timestamp": now_ms(), "type": kind, "details": details });
        self.store.insert(KIND_EVENT, self.ws(), &id, Some(cluster_id), None, &ev).await?;
        Ok(())
    }

    pub fn launch_spec(&self, c: &Cluster) -> LaunchSpec {
        let (slots, mem) = node_shape(&c.node_type_id);
        let (_, dmem) = node_shape(c.driver_node_type_id.as_deref().unwrap_or(&c.node_type_id));
        let workers = c.autoscale.as_ref().map(|a| a.min_workers.max(1)).unwrap_or(c.num_workers);
        let mut conf: BTreeMap<String, String> = c.spark_conf.clone();
        conf.entry("forge.sql.shuffle.partitions".into()).or_insert_with(|| ((workers.max(1) * slots) * 2).to_string());
        conf.entry("forge.sql.warehouse.dir".into()).or_insert_with(|| self.warehouse_dir());
        LaunchSpec {
            cluster_id: c.cluster_id.clone(),
            cluster_name: c.cluster_name.clone(),
            num_workers: if c.is_single_node { 0 } else { workers },
            slots_per_worker: slots,
            worker_memory_mb: mem,
            driver_memory_mb: dmem,
            conf,
            env: c.spark_env_vars.clone(),
            image: c.docker_image.as_ref().and_then(|d| d.get("url")).and_then(|u| u.as_str()).map(str::to_string),
        }
    }

    /// Start a cluster: launch on the backend, wait for the driver, sync the metastore.
    pub async fn start_cluster(self: &Arc<Self>, id: &str) -> ApiResult<()> {
        let mut c = self.get_cluster(id).await?.data;
        if matches!(c.state, ClusterState::Running | ClusterState::Pending | ClusterState::Restarting) {
            return Ok(());
        }
        c.state = ClusterState::Pending;
        c.state_message = "Launching Forge driver and executors".into();
        c.start_time = now_ms();
        c.terminated_time = 0;
        c.termination_reason = None;
        self.save_cluster(&c).await?;
        self.cluster_event(id, "STARTING", json!({ "user": c.creator_user_name })).await?;

        let spec = self.launch_spec(&c);
        let handle = match self.backend.launch(&spec).await {
            Ok(h) => h,
            Err(e) => {
                c.state = ClusterState::Error;
                c.state_message = e.to_string();
                c.termination_reason = Some(json!({ "code": "LAUNCH_FAILURE", "type": "SERVICE_FAULT", "parameters": { "message": e.to_string() } }));
                self.save_cluster(&c).await?;
                return Err(ApiError::Internal(format!("cluster launch failed: {e}")));
            }
        };
        c.handle = Some(handle.clone());
        self.save_cluster(&c).await?;

        let st = Arc::clone(self);
        let id = id.to_string();
        tokio::spawn(async move {
            let ok = wait_for_driver(&st, &handle.driver_addr, 120).await;
            let Ok(mut c) = st.get_cluster(&id).await.map(|d| d.data) else { return };
            if c.state != ClusterState::Pending {
                return;
            }
            if ok {
                c.state = ClusterState::Running;
                c.state_message = String::new();
                c.last_activity_time = now_ms();
                let _ = st.save_cluster(&c).await;
                let _ = st.cluster_event(&id, "RUNNING", json!({ "current_num_workers": c.num_workers })).await;
                if let Err(e) = st.sync_metastore_to_cluster(&c).await {
                    tracing::warn!(cluster = %id, error = %e, "metastore sync failed");
                }
                st.refresh_all_system_tables().await;
            } else {
                c.state = ClusterState::Error;
                c.state_message = "Driver did not become reachable".into();
                let _ = st.save_cluster(&c).await;
                let _ = st.backend.terminate(&handle).await;
            }
        });
        Ok(())
    }

    pub async fn terminate_cluster(&self, id: &str, reason: &str) -> ApiResult<()> {
        let mut c = self.get_cluster(id).await?.data;
        if matches!(c.state, ClusterState::Terminated) {
            return Ok(());
        }
        // A cluster left in `Terminating` by a previous attempt whose cleanup did
        // not finish is retried here instead of being reported as done.
        c.state = ClusterState::Terminating;
        self.save_cluster(&c).await?;
        if let Some(h) = &c.handle {
            self.forge.forget(&h.driver_addr);
            if let Err(e) = self.backend.terminate(h).await {
                // The backend could not reap every process. Keep the handle and the
                // state so a later reconcile retries the cleanup, and surface the
                // failure: clearing the handle here would strand a live process with
                // nothing left pointing at it.
                tracing::warn!(cluster = %id, error = %e, "terminate incomplete");
                c.state_message = format!("cleanup incomplete: {e}");
                self.save_cluster(&c).await?;
                return Err(ApiError::internal(format!("cluster {id}: {e}")));
            }
        }
        c.state = ClusterState::Terminated;
        c.terminated_time = now_ms();
        c.state_message = String::new();
        c.termination_reason = Some(json!({ "code": reason, "type": if reason == "USER_REQUEST" { "SUCCESS" } else { "CLIENT_ERROR" } }));
        c.handle = None;
        self.save_cluster(&c).await?;
        self.cluster_event(id, "TERMINATING", json!({ "reason": { "code": reason } })).await?;
        Ok(())
    }

    /// Resolve a cluster to a reachable driver address, auto-starting if needed.
    pub async fn cluster_driver(self: &Arc<Self>, id: &str, autostart: bool) -> ApiResult<String> {
        let c = self.get_cluster(id).await?.data;
        match (&c.state, &c.handle) {
            (ClusterState::Running, Some(h)) => {
                let mut c = c.clone();
                c.last_activity_time = now_ms();
                let _ = self.save_cluster(&c).await;
                Ok(h.driver_addr.clone())
            }
            (ClusterState::Pending, Some(h)) => {
                if wait_for_driver(self, &h.driver_addr, 120).await {
                    Ok(h.driver_addr.clone())
                } else {
                    Err(ApiError::InvalidState(format!("Cluster {id} is not reachable")))
                }
            }
            _ if autostart => {
                self.start_cluster(id).await?;
                for _ in 0..240 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    let c = self.get_cluster(id).await?.data;
                    match (c.state, c.handle) {
                        (ClusterState::Running, Some(h)) => return Ok(h.driver_addr),
                        (ClusterState::Error | ClusterState::Terminated, _) => {
                            return Err(ApiError::InvalidState(format!("Cluster {id} failed to start: {}", c.state_message)))
                        }
                        _ => {}
                    }
                }
                Err(ApiError::InvalidState(format!("Cluster {id} did not start in time")))
            }
            _ => Err(ApiError::InvalidState(format!("Cluster {id} is in state {:?}; start it first.", c.state))),
        }
    }

    /// Periodic health check: reconcile state with the backend, autoterminate idle clusters.
    pub async fn monitor_clusters(&self) -> ApiResult<()> {
        let clusters: Vec<Doc<Cluster>> = self.store.list(KIND, self.ws(), Filter::default()).await?;
        for doc in clusters {
            let mut c = doc.data;
            let Some(h) = c.handle.clone() else { continue };
            if !matches!(c.state, ClusterState::Running | ClusterState::Pending) {
                continue;
            }
            match self.backend.status(&h).await {
                Ok(s) if s.state == BackendState::Terminated || s.state == BackendState::Error => {
                    c.state = ClusterState::Terminated;
                    c.terminated_time = now_ms();
                    c.last_state_loss_time = now_ms();
                    c.state_message = s.message.unwrap_or_else(|| "Backend reported cluster gone".into());
                    c.termination_reason = Some(json!({ "code": "DRIVER_UNREACHABLE", "type": "SERVICE_FAULT" }));
                    c.handle = None;
                    self.forge.forget(&h.driver_addr);
                    self.save_cluster(&c).await?;
                    self.cluster_event(&c.cluster_id, "DRIVER_NOT_RESPONDING", json!({})).await?;
                    continue;
                }
                Ok(_) => {}
                Err(e) => tracing::debug!(cluster = %c.cluster_id, error = %e, "backend status error"),
            }
            if c.state == ClusterState::Running && c.autotermination_minutes > 0 {
                let idle_ms = now_ms() - c.last_activity_time.max(c.start_time);
                if idle_ms > (c.autotermination_minutes as i64) * 60_000 {
                    tracing::info!(cluster = %c.cluster_id, "autoterminating idle cluster");
                    self.terminate_cluster(&c.cluster_id, "INACTIVITY").await?;
                }
            }
        }
        Ok(())
    }
}

pub async fn wait_for_driver(state: &AppState, addr: &str, secs: u64) -> bool {
    for _ in 0..(secs * 2) {
        if let Ok(c) = state.forge.client(addr).await {
            if c.status().await.is_ok() {
                return true;
            }
            state.forge.forget(addr);
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    false
}

/// Public JSON view (adds derived fields, hides the backend handle).
pub fn view(c: &Cluster) -> Value {
    let mut v = serde_json::to_value(c).unwrap_or_default();
    if let Some(o) = v.as_object_mut() {
        let handle = o.remove("handle");
        let (slots, mem) = node_shape(&c.node_type_id);
        let workers = c.num_workers.max(if c.is_single_node { 0 } else { 1 });
        o.insert("cluster_cores".into(), json!(slots * workers.max(1)));
        o.insert("cluster_memory_mb".into(), json!(mem * workers.max(1) as u64));
        o.insert("default_tags".into(), json!({ "ClusterName": c.cluster_name, "ClusterId": c.cluster_id, "Creator": c.creator_user_name, "Vendor": "Lakeforge" }));
        if let Some(Value::Object(h)) = handle {
            if let Some(addr) = h.get("driver_addr").and_then(|a| a.as_str()) {
                let host = addr.trim_start_matches("http://").trim_start_matches("https://").split(':').next().unwrap_or("");
                o.insert("driver".into(), json!({ "host_private_ip": host, "private_ip": host, "node_id": format!("{}-driver", c.cluster_id), "start_timestamp": c.start_time, "driver_addr": addr }));
            }
        }
        o.insert("jdbc_port".into(), json!(10000));
        o.insert("spark_context_id".into(), json!(c.start_time));
    }
    v
}

#[derive(Debug, Deserialize)]
pub struct CreateCluster {
    pub cluster_name: Option<String>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

fn merge_into_cluster(c: &mut Cluster, body: &Map<String, Value>) -> ApiResult<()> {
    let mut v = serde_json::to_value(&*c)?;
    let obj = v.as_object_mut().unwrap();
    for (k, val) in body {
        if matches!(k.as_str(), "cluster_id" | "state" | "handle" | "creator_user_name" | "start_time" | "state_message") {
            continue;
        }
        obj.insert(k.clone(), val.clone());
    }
    *c = serde_json::from_value(v).map_err(|e| ApiError::invalid(format!("invalid cluster spec: {e}")))?;
    if c.autoscale.is_none() && c.num_workers == 0 && !c.is_single_node {
        c.is_single_node = true;
    }
    Ok(())
}

impl AppState {
    pub async fn touch_cluster(&self, id: &str) {
        if let Ok(doc) = self.get_cluster(id).await {
            let mut c = doc.data;
            c.last_activity_time = now_ms();
            let _ = self.save_cluster(&c).await;
        }
    }

    /// Create a cluster from a Databricks `clusters/create`-shaped body.
    /// `autostart=false` leaves it TERMINATED (jobs start their own clusters).
    pub async fn create_cluster_from_json(self: &Arc<Self>, p: &Principal, body: Map<String, Value>, autostart: bool) -> ApiResult<String> {
    let name = body.get("cluster_name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("cluster_name is required"))?;
    let id = format!("{}-{}", chrono::Utc::now().format("%m%d-%H%M%S"), &uuid::Uuid::new_v4().simple().to_string()[..8]);
    let st = self;
    let mut c = Cluster {
        cluster_id: id.clone(),
        cluster_name: name.to_string(),
        spark_version: default_spark_version(),
        node_type_id: default_node_type(),
        driver_node_type_id: None,
        num_workers: 0,
        autoscale: None,
        autotermination_minutes: default_autotermination(),
        spark_conf: Default::default(),
        spark_env_vars: Default::default(),
        custom_tags: Default::default(),
        state: ClusterState::Terminated,
        state_message: String::new(),
        creator_user_name: p.user_name.clone(),
        cluster_source: default_source(),
        start_time: 0,
        terminated_time: 0,
        last_state_loss_time: 0,
        last_activity_time: 0,
        last_restarted_time: 0,
        data_security_mode: default_security_mode(),
        runtime_engine: default_runtime_engine(),
        single_user_name: Some(p.user_name.clone()),
        policy_id: None,
        instance_pool_id: None,
        enable_elastic_disk: false,
        is_single_node: false,
        kind: None,
        init_scripts: vec![],
        cluster_log_conf: None,
        docker_image: None,
        aws_attributes: None,
        azure_attributes: None,
        gcp_attributes: None,
        termination_reason: None,
        handle: None,
        pinned: false,
        libraries: vec![],
        extra: Default::default(),
    };
    merge_into_cluster(&mut c, &body)?;
    st.store.insert(KIND, st.ws(), &id, None, Some(name), &c).await?;
    st.cluster_event(&id, "CREATING", json!({ "user": p.user_name })).await?;
    if autostart {
        st.start_cluster(&id).await?;
    }
    Ok(id)
    }
}

async fn create(State(st): State<S>, Who(p): Who, Body(body): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let autostart = body.get("no_autostart").and_then(|v| v.as_bool()) != Some(true);
    let id = st.create_cluster_from_json(&p, body, autostart).await?;
    Ok(Json(json!({ "cluster_id": id })))
}

#[derive(Debug, Deserialize)]
struct IdBody {
    cluster_id: String,
}

#[derive(Debug, Deserialize)]
struct IdQuery {
    cluster_id: Option<String>,
}

async fn get_cluster(State(st): State<S>, Query(q): Query<IdQuery>) -> ApiResult<Json<Value>> {
    let id = q.cluster_id.ok_or_else(|| ApiError::invalid("cluster_id is required"))?;
    Ok(Json(view(&st.get_cluster(&id).await?.data)))
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default)]
    filter_by: Option<Value>,
    #[serde(default)]
    page_size: Option<usize>,
}

async fn list(State(st): State<S>, Query(q): Query<ListQuery>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Cluster>> = st.store.list(KIND, st.ws(), Filter { newest_first: true, ..Default::default() }).await?;
    let mut items: Vec<Value> = docs.iter().map(|d| view(&d.data)).collect();
    if let Some(n) = q.page_size {
        items.truncate(n);
    }
    let _ = q.filter_by;
    Ok(Json(json!({ "clusters": items })))
}

async fn edit(State(st): State<S>, Body(body): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let id = body.get("cluster_id").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("cluster_id is required"))?.to_string();
    let mut c = st.get_cluster(&id).await?.data;
    let was_running = c.state == ClusterState::Running;
    merge_into_cluster(&mut c, &body)?;
    st.save_cluster(&c).await?;
    st.cluster_event(&id, "EDITED", json!({})).await?;
    if was_running {
        st.terminate_cluster(&id, "USER_REQUEST").await?;
        st.start_cluster(&id).await?;
    }
    Ok(empty())
}

async fn start(State(st): State<S>, Body(b): Body<IdBody>) -> ApiResult<Json<Value>> {
    st.start_cluster(&b.cluster_id).await?;
    Ok(empty())
}

async fn restart(State(st): State<S>, Body(b): Body<IdBody>) -> ApiResult<Json<Value>> {
    st.terminate_cluster(&b.cluster_id, "USER_REQUEST").await?;
    let mut c = st.get_cluster(&b.cluster_id).await?.data;
    c.last_restarted_time = now_ms();
    st.save_cluster(&c).await?;
    st.start_cluster(&b.cluster_id).await?;
    Ok(empty())
}

async fn delete(State(st): State<S>, Body(b): Body<IdBody>) -> ApiResult<Json<Value>> {
    st.terminate_cluster(&b.cluster_id, "USER_REQUEST").await?;
    Ok(empty())
}

async fn permanent_delete(State(st): State<S>, Body(b): Body<IdBody>) -> ApiResult<Json<Value>> {
    let _ = st.terminate_cluster(&b.cluster_id, "USER_REQUEST").await;
    st.store.delete(KIND, &b.cluster_id).await?;
    st.store.delete_children(KIND_EVENT, &b.cluster_id).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct ResizeBody {
    cluster_id: String,
    num_workers: Option<u32>,
    autoscale: Option<Autoscale>,
}

async fn resize(State(st): State<S>, Body(b): Body<ResizeBody>) -> ApiResult<Json<Value>> {
    let mut c = st.get_cluster(&b.cluster_id).await?.data;
    if let Some(n) = b.num_workers {
        c.num_workers = n;
    }
    if let Some(a) = b.autoscale {
        c.autoscale = Some(a);
    }
    if let Some(h) = &c.handle {
        c.state = ClusterState::Resizing;
        st.save_cluster(&c).await?;
        let spec = st.launch_spec(&c);
        let h2 = st.backend.resize(&spec, h).await?;
        c.handle = Some(h2);
        c.state = ClusterState::Running;
    }
    st.save_cluster(&c).await?;
    st.cluster_event(&b.cluster_id, "RESIZING", json!({ "target_num_workers": c.num_workers })).await?;
    Ok(empty())
}

async fn pin(State(st): State<S>, Body(b): Body<IdBody>) -> ApiResult<Json<Value>> {
    st.store.update::<Cluster, _>(KIND, &b.cluster_id, "Cluster", |c| {
        c.pinned = true;
        Ok(())
    })
    .await?;
    Ok(empty())
}

async fn unpin(State(st): State<S>, Body(b): Body<IdBody>) -> ApiResult<Json<Value>> {
    st.store.update::<Cluster, _>(KIND, &b.cluster_id, "Cluster", |c| {
        c.pinned = false;
        Ok(())
    })
    .await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct EventsBody {
    cluster_id: String,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    offset: Option<i64>,
}

async fn events(State(st): State<S>, Body(b): Body<EventsBody>) -> ApiResult<Json<Value>> {
    let evs: Vec<Doc<Value>> = st
        .store
        .list(KIND_EVENT, st.ws(), Filter { parent_id: Some(&b.cluster_id), limit: b.limit.or(Some(50)), offset: b.offset, newest_first: true, ..Default::default() })
        .await?;
    let total = st.store.count(KIND_EVENT, st.ws(), Some(&b.cluster_id)).await?;
    Ok(Json(json!({ "events": evs.into_iter().map(|e| e.data).collect::<Vec<_>>(), "total_count": total })))
}

async fn list_node_types() -> Json<Value> {
    Json(json!({ "node_types": node_types() }))
}

async fn list_spark_versions() -> Json<Value> {
    Json(json!({ "versions": spark_versions() }))
}

async fn list_zones() -> Json<Value> {
    Json(json!({ "zones": ["auto"], "default_zone": "auto" }))
}

/// Forge-level view of a running cluster (executors, jobs) for the UI.
async fn forge_status(State(st): State<S>, Query(q): Query<IdQuery>) -> ApiResult<Json<Value>> {
    let id = q.cluster_id.ok_or_else(|| ApiError::invalid("cluster_id is required"))?;
    let c = st.get_cluster(&id).await?.data;
    let Some(h) = &c.handle else {
        return Ok(Json(json!({ "state": c.state, "executors": [], "jobs": [] })));
    };
    let client = st.forge.client(&h.driver_addr).await?;
    let status = client.status().await?;
    let execs = client.list_executors().await?;
    let jobs = client.list_jobs(50).await?;
    Ok(Json(json!({
        "state": c.state,
        "driver": { "id": status.driver_id, "uptime_ms": status.uptime_ms, "version": status.version, "total_slots": status.total_slots, "free_slots": status.free_slots, "running_jobs": status.running_jobs },
        "executors": execs.executors.iter().map(|e| {
            let m = e.metadata.clone().unwrap_or_default();
            let r = e.resources.unwrap_or_default();
            json!({ "id": m.id, "host": m.host, "port": m.port, "slots": m.task_slots, "free_slots": r.free_task_slots, "memory_bytes": r.total_memory_bytes, "cpu_cores": r.cpu_cores, "running": e.running_tasks, "completed": e.completed_tasks, "failed": e.failed_tasks, "last_heartbeat_ms": e.last_heartbeat_ms })
        }).collect::<Vec<_>>(),
        "jobs": jobs.iter().map(|j| {
            let p = j.progress.unwrap_or_default();
            json!({ "job_id": j.job_id, "sql": j.sql, "state": j.state, "stages_total": p.total_stages, "stages_done": p.completed_stages, "tasks_total": p.total_tasks, "tasks_done": p.completed_tasks, "tasks_running": p.running_tasks, "tasks_failed": p.failed_tasks, "submitted_ms": j.submitted_ms, "finished_ms": j.finished_ms, "error": j.error })
        }).collect::<Vec<_>>(),
    })))
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/clusters/create", post(create))
        .route("/api/2.1/clusters/create", post(create))
        .route("/api/2.0/clusters/get", get(get_cluster))
        .route("/api/2.1/clusters/get", get(get_cluster))
        .route("/api/2.0/clusters/list", get(list).post(list))
        .route("/api/2.1/clusters/list", get(list).post(list))
        .route("/api/2.0/clusters/edit", post(edit))
        .route("/api/2.1/clusters/edit", post(edit))
        .route("/api/2.0/clusters/start", post(start))
        .route("/api/2.1/clusters/start", post(start))
        .route("/api/2.0/clusters/restart", post(restart))
        .route("/api/2.1/clusters/restart", post(restart))
        .route("/api/2.0/clusters/delete", post(delete))
        .route("/api/2.1/clusters/delete", post(delete))
        .route("/api/2.0/clusters/permanent-delete", post(permanent_delete))
        .route("/api/2.1/clusters/permanent-delete", post(permanent_delete))
        .route("/api/2.0/clusters/resize", post(resize))
        .route("/api/2.1/clusters/resize", post(resize))
        .route("/api/2.0/clusters/pin", post(pin))
        .route("/api/2.0/clusters/unpin", post(unpin))
        .route("/api/2.0/clusters/events", post(events))
        .route("/api/2.1/clusters/events", post(events))
        .route("/api/2.0/clusters/list-node-types", get(list_node_types))
        .route("/api/2.1/clusters/list-node-types", get(list_node_types))
        .route("/api/2.0/clusters/spark-versions", get(list_spark_versions))
        .route("/api/2.1/clusters/spark-versions", get(list_spark_versions))
        .route("/api/2.0/clusters/list-zones", get(list_zones))
        .route("/api/2.0/lakeforge/clusters/forge-status", get(forge_status))
}
