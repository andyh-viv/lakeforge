//! Lakeforge control plane: Databricks-compatible REST API over the Forge
//! engine, workspace object storage and a cluster-manager backend.

pub mod api;
pub mod auth;
pub mod config;
pub mod error;
pub mod forge;
pub mod kernel;
pub mod state;
pub mod storage;
pub mod store;
pub mod uc;
pub mod workers;

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Router;
use tower::ServiceExt;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

pub use config::Config;
pub use state::AppState;

/// Build the full application router (API + optional static UI).
pub fn router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);
    let mut app = api::router(Arc::clone(&state));
    if let Some(ui) = &state.config.ui_dir {
        let ui = ui.clone();
        app = app.fallback(move |req: axum::extract::Request| serve_spa(ui.clone(), req));
    }
    app.layer(cors).layer(TraceLayer::new_for_http())
}

/// Serve static assets from the UI bundle; any unknown non-API path gets
/// `index.html` with 200 so client-side routes deep-link correctly.
async fn serve_spa(ui: String, req: axum::extract::Request) -> Response {
    let path = req.uri().path().to_string();
    if path.starts_with("/api/") || path.starts_with("/ajax-api/") {
        return (StatusCode::NOT_FOUND, axum::Json(serde_json::json!({ "error_code": "ENDPOINT_NOT_FOUND", "message": format!("No API found for '{} {}'", req.method(), path) }))).into_response();
    }
    let index = std::path::Path::new(&ui).join("index.html");
    match ServeDir::new(&ui).oneshot(req).await {
        Ok(res) if res.status() != StatusCode::NOT_FOUND => res.into_response(),
        _ => {
            let idx = axum::extract::Request::builder().uri("/").body(axum::body::Body::empty()).expect("static request");
            match ServeFile::new(index).oneshot(idx).await {
                Ok(res) => res.into_response(),
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
    }
}

pub async fn run(config: Config) -> anyhow::Result<()> {
    let state = AppState::new(config.clone()).await?;
    workers::spawn_all(Arc::clone(&state));
    match state.admin_principal().await {
        Ok(admin) => {
            if let Err(e) = state.ensure_starter_warehouse(&admin).await {
                tracing::warn!(error = %e, "starter warehouse provisioning failed");
            }
        }
        Err(e) => tracing::warn!(error = %e, "admin principal unavailable; skipping starter warehouse"),
    }
    let app = router(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    tracing::info!(bind = %config.bind, db = %redact(&config.database_url), storage = %state.storage.root_url, backend = state.backend.name(), "lakeforge control plane listening");
    axum::serve(listener, app).with_graceful_shutdown(shutdown()).await?;
    Ok(())
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}

fn redact(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut u) if u.password().is_some() => {
            let _ = u.set_password(Some("***"));
            u.to_string()
        }
        _ => url.to_string(),
    }
}
