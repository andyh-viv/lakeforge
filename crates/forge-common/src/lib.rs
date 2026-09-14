//! Shared types, errors and utilities for the Forge engine.

use std::time::{SystemTime, UNIX_EPOCH};

pub mod config;

pub type Result<T> = std::result::Result<T, ForgeError>;

#[derive(Debug, thiserror::Error)]
pub enum ForgeError {
    #[error("planning error: {0}")]
    Planning(String),
    #[error("execution error: {0}")]
    Execution(String),
    #[error("shuffle error: {0}")]
    Shuffle(String),
    #[error("scheduler error: {0}")]
    Scheduler(String),
    #[error("catalog error: {0}")]
    Catalog(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("cancelled")]
    Cancelled,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<tonic::Status> for ForgeError {
    fn from(s: tonic::Status) -> Self {
        ForgeError::Transport(format!("{}: {}", s.code(), s.message()))
    }
}

impl From<tonic::transport::Error> for ForgeError {
    fn from(e: tonic::transport::Error) -> Self {
        ForgeError::Transport(e.to_string())
    }
}

impl From<ForgeError> for tonic::Status {
    fn from(e: ForgeError) -> Self {
        match e {
            ForgeError::NotFound(m) => tonic::Status::not_found(m),
            ForgeError::InvalidArgument(m) => tonic::Status::invalid_argument(m),
            ForgeError::Cancelled => tonic::Status::cancelled("cancelled"),
            ForgeError::Planning(m) => tonic::Status::invalid_argument(m),
            other => tonic::Status::internal(other.to_string()),
        }
    }
}

impl From<ForgeError> for datafusion::error::DataFusionError {
    fn from(e: ForgeError) -> Self {
        datafusion::error::DataFusionError::External(Box::new(e))
    }
}

impl From<serde_json::Error> for ForgeError {
    fn from(e: serde_json::Error) -> Self {
        ForgeError::Internal(e.to_string())
    }
}

/// Current wall-clock time in epoch milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Generate a short unique id (UUIDv7, time-ordered).
pub fn new_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Initialise tracing with RUST_LOG (default `info`).
pub fn init_tracing(service: &str) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = std::env::var("FORGE_LOG_FORMAT").map(|v| v == "json").unwrap_or(false);
    let registry = tracing_subscriber::registry().with(filter);
    if json {
        let _ = registry.with(fmt::layer().json().with_target(false)).try_init();
    } else {
        let _ = registry.with(fmt::layer().with_target(false)).try_init();
    }
    tracing::info!(service, version = env!("CARGO_PKG_VERSION"), "starting");
}

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
