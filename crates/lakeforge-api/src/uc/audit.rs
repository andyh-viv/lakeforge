//! Audit log: every mutating REST call (and every SQL statement) is recorded
//! as a `system.access.audit`-shaped event. Events are persisted as docs and
//! exposed through `system.access.audit` and `/api/2.0/lakeforge/audit`.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::auth::Principal;
use crate::error::ApiResult;
use crate::state::AppState;
use crate::store::{now_ms, Filter};

pub const KIND_AUDIT: &str = "uc_audit";
/// Keep at most this many audit events in the store (oldest evicted).
pub const MAX_EVENTS: i64 = 200_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_id: String,
    pub event_time: i64,
    pub workspace_id: String,
    pub service_name: String,
    pub action_name: String,
    pub request_id: String,
    pub request_params: Map<String, Value>,
    pub user_identity: UserIdentity,
    pub source_ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub session_id: Option<String>,
    pub response: AuditResponse,
    pub audit_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserIdentity {
    pub email: String,
    pub subject_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditResponse {
    pub status_code: i64,
    pub error_message: Option<String>,
    pub result: Option<String>,
}

/// Map a request path to Databricks' `(service_name, action_name)`.
pub fn classify(method: &Method, path: &str) -> Option<(String, String)> {
    let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    // /api/<ver>/<service>/...
    if segs.len() < 3 || segs[0] != "api" {
        return None;
    }
    let service = segs[2];
    let rest: Vec<&str> = segs[3..].to_vec();
    let verb = match *method {
        Method::POST => "create",
        Method::PATCH | Method::PUT => "update",
        Method::DELETE => "delete",
        _ => "get",
    };
    let (svc, action) = match service {
        "unity-catalog" => {
            let obj = rest.first().copied().unwrap_or("metastore");
            let singular = obj.trim_end_matches('s').replace('-', "_");
            let action = match (method, rest.as_slice()) {
                (&Method::PATCH, ["permissions", ..]) => "updatePermissions".to_string(),
                (&Method::GET, ["permissions", ..]) => "getPermissions".to_string(),
                (&Method::GET, ["effective-permissions", ..]) => "getEffectivePermissions".to_string(),
                (&Method::POST, [_, _, "versions", ..]) => "createModelVersion".to_string(),
                (&Method::GET, [_]) => format!("list{}", camel(obj)),
                (&Method::GET, _) => format!("get{}", camel(&singular)),
                _ => format!("{verb}{}", camel(&singular)),
            };
            ("unityCatalog", action)
        }
        "clusters" => ("clusters", rest.first().copied().unwrap_or("list").to_string()),
        "jobs" => ("jobs", rest.join("/").replace('/', "_")),
        "sql" => match rest.as_slice() {
            ["statements", ..] if *method == Method::POST => ("sqlStatements", "executeStatement".to_string()),
            ["warehouses", .., action] if *method == Method::POST => ("databrickssql", action.to_string()),
            _ => ("databrickssql", format!("{verb}{}", camel(rest.first().copied().unwrap_or("")))),
        },
        "workspace" => ("workspace", rest.first().copied().unwrap_or("").to_string()),
        "secrets" => ("secrets", rest.join("_")),
        "token" | "token-management" => ("accounts", format!("{verb}Token")),
        "preview" => match rest.as_slice() {
            ["scim", "v2", obj, ..] => ("accounts", format!("{verb}{}", camel(obj))),
            _ => ("preview", rest.join("_")),
        },
        "database" => ("database", format!("{verb}{}", camel(rest.first().copied().unwrap_or("instance").trim_end_matches('s')))),
        "mlflow" => ("mlflow", rest.join("_").replace('-', "_")),
        "serving-endpoints" => ("serverlessRealTimeInference", format!("{verb}ServingEndpoint")),
        "pipelines" => ("deltaPipelines", format!("{verb}Pipeline")),
        "repos" => ("repos", format!("{verb}Repo")),
        "dbfs" | "fs" => ("dbfs", rest.first().copied().unwrap_or(verb).to_string()),
        "permissions" => ("accounts", format!("{verb}Permissions")),
        "lakeforge" => ("lakeforge", rest.join("_")),
        other => (other, format!("{verb}{}", camel(rest.first().copied().unwrap_or("")))),
    };
    Some((svc.to_string(), action))
}

fn camel(s: &str) -> String {
    let mut out = String::new();
    let mut up = true;
    for c in s.chars() {
        if c == '-' || c == '_' {
            up = true;
        } else if up {
            out.extend(c.to_uppercase());
            up = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn header(h: &HeaderMap, name: &str) -> Option<String> {
    h.get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}

/// Axum middleware: record every non-GET API call (and GETs that fail with
/// 403) after the handler has run.
pub async fn audit_middleware(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(str::to_string);
    let headers = req.headers().clone();
    let principal = req.extensions().get::<Principal>().cloned();
    let interesting = method != Method::GET && method != Method::HEAD && method != Method::OPTIONS;
    // Statement execution and command execution carry the SQL/code in the body; they are
    // audited by the SQL guard / commands layer with richer parameters.
    let skip = path.starts_with("/api/2.0/sql/statements") || path.starts_with("/api/1.2/commands") || path.starts_with("/api/2.0/lakeforge/notebooks") || path.starts_with("/api/2.0/lakeforge/audit") || path.starts_with("/api/2.0/dbfs/add-block") || path.ends_with("/heartbeat");
    let (req, body_params) = if interesting && !skip && is_json(&headers) {
        let (parts, body) = req.into_parts();
        match axum::body::to_bytes(body, 4 * 1024 * 1024).await {
            Ok(bytes) => {
                let params = serde_json::from_slice::<Value>(&bytes).ok().and_then(|v| v.as_object().cloned()).map(redact).unwrap_or_default();
                (Request::from_parts(parts, Body::from(bytes)), params)
            }
            Err(_) => (Request::from_parts(parts, Body::empty()), Map::new()),
        }
    } else {
        (req, Map::new())
    };
    let resp = next.run(req).await;
    let status = resp.status();
    if !interesting && status != StatusCode::FORBIDDEN {
        return resp;
    }
    if skip {
        return resp;
    }
    let Some((service_name, action_name)) = classify(&method, &path) else { return resp };
    let mut params = body_params;
    params.insert("path".into(), json!(path));
    params.insert("method".into(), json!(method.as_str()));
    if let Some(q) = query {
        params.insert("query".into(), json!(q));
    }
    let (email, subject) = principal.as_ref().map(|p| (p.user_name.clone(), p.user_id.clone())).unwrap_or_else(|| ("anonymous".into(), "".into()));
    let ev = AuditEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        event_time: now_ms(),
        workspace_id: state.ws().to_string(),
        service_name,
        action_name,
        request_id: header(&headers, "x-request-id").unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        request_params: params,
        user_identity: UserIdentity { email, subject_name: subject },
        source_ip_address: header(&headers, "x-forwarded-for").or_else(|| header(&headers, "x-real-ip")),
        user_agent: header(&headers, "user-agent"),
        session_id: None,
        response: AuditResponse { status_code: status.as_u16() as i64, error_message: None, result: None },
        audit_level: "WORKSPACE_LEVEL".into(),
    };
    let st = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(e) = st.record_audit(ev).await {
            tracing::debug!(error = %e, "audit write failed");
        }
    });
    resp
}

fn is_json(h: &HeaderMap) -> bool {
    header(h, "content-type").map(|c| c.starts_with("application/json")).unwrap_or(false)
}

const SECRET_KEYS: &[&str] = &["password", "string_value", "bytes_value", "token", "token_value", "secret", "client_secret", "private_key", "personal_access_token", "new_password", "aws_secret_access_key"];

/// Never persist secret material from request bodies.
pub fn redact(mut m: Map<String, Value>) -> Map<String, Value> {
    for (k, v) in m.iter_mut() {
        if SECRET_KEYS.iter().any(|s| k.eq_ignore_ascii_case(s)) {
            *v = json!("***");
        } else if let Value::Object(inner) = v {
            *v = Value::Object(redact(inner.clone()));
        }
    }
    m
}

impl AppState {
    pub async fn record_audit(&self, ev: AuditEvent) -> ApiResult<()> {
        self.store.insert(KIND_AUDIT, self.ws(), &ev.event_id, Some(&ev.service_name), Some(&ev.action_name), &ev).await?;
        // opportunistic eviction
        if ev.event_time % 97 == 0 {
            let n = self.store.count(KIND_AUDIT, self.ws(), None).await?;
            if n > MAX_EVENTS {
                let _ = self.store.delete_oldest(KIND_AUDIT, self.ws(), (n - MAX_EVENTS) as u64).await;
            }
        }
        Ok(())
    }

    /// Record an event produced by an internal path (SQL guard, commands).
    pub async fn audit(&self, p: &Principal, service: &str, action: &str, params: Value, status: i64, error: Option<String>) {
        let ev = AuditEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_time: now_ms(),
            workspace_id: self.ws().to_string(),
            service_name: service.into(),
            action_name: action.into(),
            request_id: uuid::Uuid::new_v4().to_string(),
            request_params: params.as_object().cloned().map(redact).unwrap_or_default(),
            user_identity: UserIdentity { email: p.user_name.clone(), subject_name: p.user_id.clone() },
            source_ip_address: None,
            user_agent: None,
            session_id: None,
            response: AuditResponse { status_code: status, error_message: error, result: None },
            audit_level: "WORKSPACE_LEVEL".into(),
        };
        if let Err(e) = self.record_audit(ev).await {
            tracing::debug!(error = %e, "audit write failed");
        }
    }

    pub async fn audit_events(&self, limit: i64, service: Option<&str>, action: Option<&str>, user: Option<&str>, since_ms: Option<i64>) -> ApiResult<Vec<AuditEvent>> {
        let docs: Vec<crate::store::Doc<AuditEvent>> = self.store.list(KIND_AUDIT, self.ws(), Filter { parent_id: service, name: action, newest_first: true, limit: Some(limit.max(1) * 4), ..Default::default() }).await?;
        Ok(docs
            .into_iter()
            .map(|d| d.data)
            .filter(|e| user.map(|u| e.user_identity.email.eq_ignore_ascii_case(u)).unwrap_or(true))
            .filter(|e| since_ms.map(|s| e.event_time >= s).unwrap_or(true))
            .take(limit as usize)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_uc_calls() {
        assert_eq!(classify(&Method::POST, "/api/2.1/unity-catalog/tables"), Some(("unityCatalog".into(), "createTable".into())));
        assert_eq!(classify(&Method::DELETE, "/api/2.1/unity-catalog/schemas/main.x"), Some(("unityCatalog".into(), "deleteSchema".into())));
        assert_eq!(classify(&Method::PATCH, "/api/2.1/unity-catalog/permissions/table/main.default.t"), Some(("unityCatalog".into(), "updatePermissions".into())));
        assert_eq!(classify(&Method::POST, "/api/2.0/clusters/create"), Some(("clusters".into(), "create".into())));
        assert_eq!(classify(&Method::POST, "/api/2.0/database/instances"), Some(("database".into(), "createInstance".into())));
        assert!(classify(&Method::GET, "/healthz").is_none());
    }

    #[test]
    fn redacts_secrets() {
        let m = redact(json!({ "name": "x", "password": "p", "nested": { "token": "t" } }).as_object().cloned().unwrap());
        assert_eq!(m["password"], "***");
        assert_eq!(m["nested"]["token"], "***");
        assert_eq!(m["name"], "x");
    }
}
