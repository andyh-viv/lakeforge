//! REST surface. Each submodule owns one Databricks API family and exposes a
//! `router()` mounted here.

pub mod catalog;
pub mod clusters;
pub mod commands;
pub mod dbfs;
pub mod jobs;
pub mod misc;
pub mod mlflow;
pub mod notebooks;
pub mod permissions;
pub mod pipelines;
pub mod repos;
pub mod scim;
pub mod secrets;
pub mod serving;
pub mod sql;
pub mod tokens;
pub mod workspace;

use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::FromRequest;
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{middleware, Router};
use serde::de::DeserializeOwned;
use serde_json::json;

use crate::auth::auth_middleware;
use crate::error::ApiError;
use crate::state::AppState;

pub type S = Arc<AppState>;

/// JSON body extractor that reports malformed input in the Databricks error shape.
pub struct Body<T>(pub T);

impl<T: DeserializeOwned, St: Send + Sync> FromRequest<St> for Body<T> {
    type Rejection = ApiError;

    async fn from_request(req: axum::extract::Request, state: &St) -> Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req, state).await {
            Ok(axum::Json(v)) => Ok(Body(v)),
            Err(JsonRejection::MissingJsonContentType(_)) => Err(ApiError::invalid("Expected application/json body.")),
            Err(e) => Err(ApiError::invalid(format!("Invalid JSON body: {}", e.body_text()))),
        }
    }
}

/// Clients (curl `-d`, some SDKs) send JSON bodies with no `Content-Type` or
/// with `application/x-www-form-urlencoded`; treat those as JSON like
/// Databricks does. Multipart/octet-stream uploads are untouched.
async fn default_json_content_type(mut req: axum::extract::Request, next: middleware::Next) -> axum::response::Response {
    let has_body_method = matches!(*req.method(), Method::POST | Method::PUT | Method::PATCH | Method::DELETE);
    let ct = req.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    if has_body_method && (ct.is_empty() || ct.starts_with("application/x-www-form-urlencoded")) {
        req.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    next.run(req).await
}

pub fn ok_json<T: serde::Serialize>(v: T) -> axum::Json<T> {
    axum::Json(v)
}

pub fn empty() -> axum::Json<serde_json::Value> {
    axum::Json(json!({}))
}

pub fn router(state: S) -> Router {
    let api = Router::new()
        .merge(clusters::router())
        .merge(jobs::router())
        .merge(workspace::router())
        .merge(notebooks::router())
        .merge(sql::router())
        .merge(catalog::router())
        .merge(secrets::router())
        .merge(tokens::router())
        .merge(dbfs::router())
        .merge(mlflow::router())
        .merge(repos::router())
        .merge(pipelines::router())
        .merge(serving::router())
        .merge(scim::router())
        .merge(permissions::router())
        .merge(commands::router())
        .merge(misc::router())
        .layer(middleware::from_fn_with_state(Arc::clone(&state), auth_middleware))
        .layer(middleware::from_fn(default_json_content_type))
        .with_state(Arc::clone(&state));

    Router::new()
        .route("/health", get(|| async { (StatusCode::OK, axum::Json(json!({"status": "ok"}))).into_response() }))
        .merge(api)
}
