//! Forge executor: runs tasks handed out by the driver, serves shuffle data.

pub mod runner;
pub mod server;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use forge_common::{ForgeError, Result};
use forge_proto::driver_service_client::DriverServiceClient;
use forge_proto::executor_service_server::ExecutorServiceServer;
use forge_proto::{ExecutorMetadata, ExecutorResources, HeartbeatRequest, RegisterExecutorRequest};
use tonic::transport::{Channel, Endpoint};

pub use runner::TaskRunner;
pub use server::ExecutorServer;

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub id: String,
    /// Address the executor binds to.
    pub bind: SocketAddr,
    /// Host other nodes use to reach this executor (defaults to bind host).
    pub advertise_host: String,
    pub driver_addr: String,
    pub task_slots: usize,
    pub work_dir: String,
    pub heartbeat_interval: Duration,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            id: format!("exec-{}", &forge_common::new_id()[..8]),
            bind: "0.0.0.0:50052".parse().unwrap(),
            advertise_host: "127.0.0.1".into(),
            driver_addr: "http://127.0.0.1:50051".into(),
            task_slots: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            work_dir: "/tmp/forge/executor".into(),
            heartbeat_interval: Duration::from_secs(5),
        }
    }
}

impl ExecutorConfig {
    pub fn advertise_addr(&self) -> String {
        format!("http://{}:{}", self.advertise_host, self.bind.port())
    }

    pub fn metadata(&self) -> ExecutorMetadata {
        ExecutorMetadata {
            id: self.id.clone(),
            host: self.advertise_host.clone(),
            port: self.bind.port() as u32,
            task_slots: self.task_slots as u32,
            labels: Default::default(),
        }
    }
}

pub async fn connect_driver(addr: &str) -> Result<DriverServiceClient<Channel>> {
    let channel = Endpoint::from_shared(addr.to_string())
        .map_err(|e| ForgeError::Transport(e.to_string()))?
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        .connect()
        .await
        .map_err(|e| ForgeError::Transport(format!("connect driver {addr}: {e}")))?;
    Ok(DriverServiceClient::new(channel)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX))
}

fn resources(cfg: &ExecutorConfig, running: usize) -> ExecutorResources {
    ExecutorResources {
        total_memory_bytes: 0,
        available_memory_bytes: 0,
        cpu_cores: std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1),
        free_task_slots: cfg.task_slots.saturating_sub(running) as u32,
    }
}

/// Run an executor until the process is stopped: serves gRPC, registers with
/// the driver and heartbeats (re-registering if the driver forgets us).
pub async fn run(cfg: ExecutorConfig) -> Result<()> {
    let cfg = Arc::new(cfg);
    forge_shuffle::set_local_executor_addr(cfg.advertise_addr());
    let runner = Arc::new(TaskRunner::new(Arc::clone(&cfg)));
    let server = ExecutorServer::new(Arc::clone(&runner));

    let bind = cfg.bind;
    let serve = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                ExecutorServiceServer::new(server)
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX),
            )
            .serve(bind)
            .await
    });
    tracing::info!(id = %cfg.id, %bind, driver = %cfg.driver_addr, slots = cfg.task_slots, "executor listening");

    // Registration / heartbeat loop.
    let hb_cfg = Arc::clone(&cfg);
    let hb_runner = Arc::clone(&runner);
    tokio::spawn(async move {
        let mut registered = false;
        let mut backoff = Duration::from_millis(500);
        loop {
            let client = match connect_driver(&hb_cfg.driver_addr).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("driver unreachable: {e}; retrying in {backoff:?}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                    registered = false;
                    continue;
                }
            };
            backoff = Duration::from_millis(500);
            let mut client = client;
            loop {
                if !registered {
                    let req = RegisterExecutorRequest {
                        metadata: Some(hb_cfg.metadata()),
                        resources: Some(resources(&hb_cfg, hb_runner.running_count())),
                    };
                    match client.register_executor(req).await {
                        Ok(r) if r.get_ref().accepted => {
                            tracing::info!(driver = %r.get_ref().driver_id, "registered with driver");
                            registered = true;
                        }
                        Ok(_) => tracing::warn!("driver rejected registration"),
                        Err(e) => {
                            tracing::warn!("register failed: {e}");
                            break;
                        }
                    }
                }
                tokio::time::sleep(hb_cfg.heartbeat_interval).await;
                let req = HeartbeatRequest {
                    executor_id: hb_cfg.id.clone(),
                    resources: Some(resources(&hb_cfg, hb_runner.running_count())),
                    timestamp_ms: forge_common::now_ms(),
                    running_tasks: hb_runner.running_statuses(),
                };
                match client.heartbeat(req).await {
                    Ok(r) => {
                        let r = r.into_inner();
                        if !r.known {
                            registered = false;
                        }
                        for job in r.completed_jobs {
                            hb_runner.remove_job_data(&job);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("heartbeat failed: {e}");
                        registered = false;
                        break;
                    }
                }
            }
        }
    });

    serve
        .await
        .map_err(|e| ForgeError::Internal(e.to_string()))?
        .map_err(|e| ForgeError::Transport(e.to_string()))
}
