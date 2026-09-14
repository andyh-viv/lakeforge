//! Forge driver: SQL front door, catalog owner and scheduler host.

pub mod server;
pub mod session;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use forge_common::{ForgeError, Result};
use forge_proto::driver_service_server::DriverServiceServer;
use forge_scheduler::{Scheduler, SchedulerConfig};

pub use server::DriverServer;
pub use session::SessionManager;

#[derive(Debug, Clone)]
pub struct DriverConfig {
    pub id: String,
    pub bind: SocketAddr,
    pub advertise_host: String,
    pub work_dir: String,
    pub executor_timeout: Duration,
    /// Run queries in-process when no executors are registered.
    pub local_fallback: bool,
    pub max_jobs_retained: usize,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            id: format!("driver-{}", &forge_common::new_id()[..8]),
            bind: "0.0.0.0:50051".parse().unwrap(),
            advertise_host: "127.0.0.1".into(),
            work_dir: "/tmp/forge/driver".into(),
            executor_timeout: Duration::from_secs(30),
            local_fallback: true,
            max_jobs_retained: 1000,
        }
    }
}

impl DriverConfig {
    pub fn advertise_addr(&self) -> String {
        format!("http://{}:{}", self.advertise_host, self.bind.port())
    }
}

/// Build the driver server (without binding it), for embedding.
pub fn build(cfg: DriverConfig) -> Arc<DriverServer> {
    let scheduler = Scheduler::new(SchedulerConfig {
        driver_id: cfg.id.clone(),
        driver_addr: cfg.advertise_addr(),
        work_dir: cfg.work_dir.clone(),
        executor_timeout: cfg.executor_timeout,
        max_jobs_retained: cfg.max_jobs_retained,
    });
    Arc::new(DriverServer::new(cfg, scheduler))
}

/// Serve the driver until stopped.
pub async fn run(cfg: DriverConfig) -> Result<()> {
    let bind = cfg.bind;
    let server = build(cfg);
    serve(server, bind).await
}

pub async fn serve(server: Arc<DriverServer>, bind: SocketAddr) -> Result<()> {
    tracing::info!(id = %server.config().id, %bind, "driver listening");
    tonic::transport::Server::builder()
        .add_service(
            DriverServiceServer::from_arc(server)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
        .serve(bind)
        .await
        .map_err(|e| ForgeError::Transport(e.to_string()))
}
