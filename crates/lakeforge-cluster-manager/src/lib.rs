//! Cluster manager backends for Lakeforge.
//!
//! A backend knows how to turn a [`LaunchSpec`] into a running Forge cluster
//! (one driver + N executors) and how to observe / tear it down again.
//! Two backends ship today:
//!
//! * [`local::LocalProcessBackend`] — spawns `forge driver` / `forge executor`
//!   processes on the control-plane host. Used for dev and single-node installs.
//! * [`kubernetes::KubernetesBackend`] — creates a driver Deployment+Service and
//!   an executor Deployment in a namespace using the `forge` container image.

pub mod kubernetes;
pub mod local;

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("launch failed: {0}")]
    Launch(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("cluster not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ClusterError>;

/// Everything a backend needs to start a Forge cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchSpec {
    pub cluster_id: String,
    pub cluster_name: String,
    pub num_workers: u32,
    pub slots_per_worker: u32,
    /// Executor memory limit in MiB (advisory for local, requests/limits on k8s).
    pub worker_memory_mb: u64,
    pub driver_memory_mb: u64,
    /// Engine configuration passed through to the driver as `forge.*` settings.
    pub conf: BTreeMap<String, String>,
    pub env: BTreeMap<String, String>,
    /// Container image for Kubernetes launches.
    pub image: Option<String>,
}

/// Opaque per-backend handle persisted with the cluster record.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterHandle {
    pub backend: String,
    /// gRPC address of the Forge driver, e.g. `http://10.0.0.5:50051`.
    pub driver_addr: String,
    /// Backend-specific state (pids, k8s object names...).
    pub state: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BackendState {
    Pending,
    Running,
    Terminated,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendStatus {
    pub state: BackendState,
    pub message: Option<String>,
    pub ready_workers: u32,
}

#[async_trait]
pub trait ClusterBackend: Send + Sync {
    fn name(&self) -> &'static str;
    async fn launch(&self, spec: &LaunchSpec) -> Result<ClusterHandle>;
    async fn status(&self, handle: &ClusterHandle) -> Result<BackendStatus>;
    async fn resize(&self, spec: &LaunchSpec, handle: &ClusterHandle) -> Result<ClusterHandle>;
    async fn terminate(&self, handle: &ClusterHandle) -> Result<()>;
}

/// Build the backend named by `LAKEFORGE_CLUSTER_BACKEND` (`local` | `kubernetes`).
pub async fn backend_from_env() -> Result<Box<dyn ClusterBackend>> {
    let which = std::env::var("LAKEFORGE_CLUSTER_BACKEND").unwrap_or_else(|_| "local".into());
    match which.as_str() {
        "local" | "process" => Ok(Box::new(local::LocalProcessBackend::from_env())),
        "kubernetes" | "k8s" => Ok(Box::new(kubernetes::KubernetesBackend::from_env().await?)),
        other => Err(ClusterError::Backend(format!("unknown cluster backend `{other}`"))),
    }
}
