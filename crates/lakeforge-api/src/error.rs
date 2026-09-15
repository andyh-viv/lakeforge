//! Databricks-compatible error envelope: `{"error_code": "...", "message": "..."}`.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    InvalidParameter(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    AlreadyExists(String),
    #[error("{0}")]
    Unauthenticated(String),
    #[error("{0}")]
    PermissionDenied(String),
    #[error("{0}")]
    InvalidState(String),
    #[error("{0}")]
    Internal(String),
    #[error("{0}")]
    Unavailable(String),
}

impl ApiError {
    pub fn code(&self) -> (&'static str, StatusCode) {
        match self {
            ApiError::InvalidParameter(_) => ("INVALID_PARAMETER_VALUE", StatusCode::BAD_REQUEST),
            ApiError::NotFound(_) => ("RESOURCE_DOES_NOT_EXIST", StatusCode::NOT_FOUND),
            ApiError::AlreadyExists(_) => ("RESOURCE_ALREADY_EXISTS", StatusCode::CONFLICT),
            ApiError::Unauthenticated(_) => ("UNAUTHENTICATED", StatusCode::UNAUTHORIZED),
            ApiError::PermissionDenied(_) => ("PERMISSION_DENIED", StatusCode::FORBIDDEN),
            ApiError::InvalidState(_) => ("INVALID_STATE", StatusCode::BAD_REQUEST),
            ApiError::Internal(_) => ("INTERNAL_ERROR", StatusCode::INTERNAL_SERVER_ERROR),
            ApiError::Unavailable(_) => ("TEMPORARILY_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE),
        }
    }

    pub fn invalid(msg: impl Into<String>) -> Self {
        ApiError::InvalidParameter(msg.into())
    }
    pub fn not_found(what: &str, id: &str) -> Self {
        ApiError::NotFound(format!("{what} {id} does not exist."))
    }
    pub fn internal(e: impl std::fmt::Display) -> Self {
        ApiError::Internal(e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, status) = self.code();
        if status.is_server_error() {
            tracing::error!(error = %self, "request failed");
        }
        (status, Json(json!({ "error_code": code, "message": self.to_string() }))).into_response()
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        ApiError::Internal(format!("database error: {e}"))
    }
}
impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        ApiError::InvalidParameter(format!("malformed JSON: {e}"))
    }
}
impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self {
        ApiError::Internal(format!("io error: {e}"))
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError::Internal(e.to_string())
    }
}
impl From<lakeforge_cluster_manager::ClusterError> for ApiError {
    fn from(e: lakeforge_cluster_manager::ClusterError) -> Self {
        ApiError::Internal(e.to_string())
    }
}
impl From<forge_common::ForgeError> for ApiError {
    fn from(e: forge_common::ForgeError) -> Self {
        ApiError::InvalidState(format!("query failed: {e}"))
    }
}
impl From<object_store::Error> for ApiError {
    fn from(e: object_store::Error) -> Self {
        match e {
            object_store::Error::NotFound { path, .. } => ApiError::NotFound(format!("No file or directory exists on path {path}.")),
            other => ApiError::Internal(format!("storage error: {other}")),
        }
    }
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;
