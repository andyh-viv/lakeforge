//! Secrets API (`/api/2.0/secrets/*`). Values are stored encrypted-at-rest in
//! the kv table (AES-GCM keyed from the JWT secret); metadata lives in docs.

use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{empty, Body, S};
use crate::auth::Who;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Filter};

pub const KIND_SCOPE: &str = "secret_scope";
pub const KIND_SECRET: &str = "secret";
pub const KIND_ACL: &str = "secret_acl";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scope {
    pub name: String,
    pub backend_type: String,
    #[serde(default)]
    pub keyvault_metadata: Option<Value>,
    #[serde(default)]
    pub created_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretMeta {
    pub key: String,
    pub last_updated_timestamp: i64,
}

fn secret_id(scope: &str, key: &str) -> String {
    format!("{scope}/{key}")
}

impl AppState {
    fn secret_cipher(&self) -> ApiResult<crate::auth::Sealer> {
        Ok(self.auth.sealer())
    }

    pub async fn get_secret(&self, scope: &str, key: &str) -> ApiResult<Option<Vec<u8>>> {
        let Some(sealed) = self.store.kv_get(&format!("secret:{scope}/{key}")).await? else { return Ok(None) };
        Ok(Some(self.secret_cipher()?.open(&sealed)?))
    }

    pub async fn put_secret(&self, scope: &str, key: &str, value: &[u8]) -> ApiResult<()> {
        self.store.require::<Scope>(KIND_SCOPE, scope, "Secret scope").await?;
        let sealed = self.secret_cipher()?.seal(value)?;
        self.store.kv_set(&format!("secret:{scope}/{key}"), &sealed).await?;
        self.store.upsert(KIND_SECRET, self.ws(), &secret_id(scope, key), Some(scope), Some(key), &SecretMeta { key: key.into(), last_updated_timestamp: now_ms() }).await
    }

    async fn check_scope_access(&self, p: &crate::auth::Principal, scope: &str, need: &str) -> ApiResult<()> {
        if p.is_admin {
            return Ok(());
        }
        let acls: Vec<crate::store::Doc<Value>> = self.store.list(KIND_ACL, self.ws(), Filter { parent_id: Some(scope), ..Default::default() }).await?;
        let mine: Vec<&str> = acls.iter().filter(|a| a.data["principal"].as_str().map(|pr| pr == p.user_name || p.groups.iter().any(|g| g == pr)).unwrap_or(false)).filter_map(|a| a.data["permission"].as_str()).collect();
        let ok = match need {
            "READ" => mine.iter().any(|m| matches!(*m, "READ" | "WRITE" | "MANAGE")),
            "WRITE" => mine.iter().any(|m| matches!(*m, "WRITE" | "MANAGE")),
            _ => mine.contains(&"MANAGE"),
        };
        if ok {
            Ok(())
        } else {
            Err(ApiError::PermissionDenied(format!("User {} does not have {need} permission on scope {scope}", p.user_name)))
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateScope {
    scope: String,
    #[serde(default)]
    initial_manage_principal: Option<String>,
    #[serde(default)]
    scope_backend_type: Option<String>,
    #[serde(default)]
    backend_azure_keyvault: Option<Value>,
}

async fn create_scope(State(st): State<S>, Who(p): Who, Body(b): Body<CreateScope>) -> ApiResult<Json<Value>> {
    if b.scope.is_empty() || b.scope.len() > 128 || !b.scope.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.') {
        return Err(ApiError::invalid("Scope name must consist of alphanumeric characters, dashes, underscores, and periods, and may not exceed 128 characters."));
    }
    if st.store.get::<Scope>(KIND_SCOPE, &b.scope).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("Scope {} already exists!", b.scope)));
    }
    let scope = Scope { name: b.scope.clone(), backend_type: b.scope_backend_type.unwrap_or_else(|| "DATABRICKS".into()), keyvault_metadata: b.backend_azure_keyvault, created_by: p.user_name.clone() };
    st.store.insert(KIND_SCOPE, st.ws(), &b.scope, None, Some(&b.scope), &scope).await?;
    let manager = b.initial_manage_principal.unwrap_or_else(|| p.user_name.clone());
    let acl = json!({ "principal": manager, "permission": "MANAGE" });
    st.store.insert(KIND_ACL, st.ws(), &format!("{}/{manager}", b.scope), Some(&b.scope), Some(&manager), &acl).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct ScopeBody {
    scope: String,
}

async fn delete_scope(State(st): State<S>, Who(p): Who, Body(b): Body<ScopeBody>) -> ApiResult<Json<Value>> {
    st.store.require::<Scope>(KIND_SCOPE, &b.scope, "Secret scope").await?;
    st.check_scope_access(&p, &b.scope, "MANAGE").await?;
    let secrets: Vec<crate::store::Doc<SecretMeta>> = st.store.list(KIND_SECRET, st.ws(), Filter { parent_id: Some(&b.scope), ..Default::default() }).await?;
    for s in secrets {
        st.store.kv_delete(&format!("secret:{}/{}", b.scope, s.data.key)).await?;
    }
    st.store.delete_children(KIND_SECRET, &b.scope).await?;
    st.store.delete_children(KIND_ACL, &b.scope).await?;
    st.store.delete(KIND_SCOPE, &b.scope).await?;
    Ok(empty())
}

async fn list_scopes(State(st): State<S>, Who(p): Who) -> ApiResult<Json<Value>> {
    let scopes: Vec<crate::store::Doc<Scope>> = st.store.list(KIND_SCOPE, st.ws(), Filter::default()).await?;
    let mut out = vec![];
    for s in scopes {
        if st.check_scope_access(&p, &s.data.name, "READ").await.is_ok() {
            out.push(json!({ "name": s.data.name, "backend_type": s.data.backend_type, "keyvault_metadata": s.data.keyvault_metadata }));
        }
    }
    Ok(Json(json!({ "scopes": out })))
}

#[derive(Debug, Deserialize)]
struct PutSecret {
    scope: String,
    key: String,
    #[serde(default)]
    string_value: Option<String>,
    #[serde(default)]
    bytes_value: Option<String>,
}

async fn put_secret(State(st): State<S>, Who(p): Who, Body(b): Body<PutSecret>) -> ApiResult<Json<Value>> {
    st.check_scope_access(&p, &b.scope, "WRITE").await?;
    if b.key.is_empty() || b.key.len() > 128 || !b.key.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.') {
        return Err(ApiError::invalid("Secret key must consist of alphanumeric characters, dashes, underscores, and periods, and may not exceed 128 characters."));
    }
    let value = match (&b.string_value, &b.bytes_value) {
        (Some(s), None) => s.as_bytes().to_vec(),
        (None, Some(bb)) => base64::engine::general_purpose::STANDARD.decode(bb).map_err(|e| ApiError::invalid(format!("bytes_value must be base64: {e}")))?,
        _ => return Err(ApiError::invalid("Exactly one of string_value or bytes_value must be specified.")),
    };
    st.put_secret(&b.scope, &b.key, &value).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct KeyBody {
    scope: String,
    key: String,
}

async fn delete_secret(State(st): State<S>, Who(p): Who, Body(b): Body<KeyBody>) -> ApiResult<Json<Value>> {
    st.check_scope_access(&p, &b.scope, "WRITE").await?;
    if !st.store.delete(KIND_SECRET, &secret_id(&b.scope, &b.key)).await? {
        return Err(ApiError::NotFound(format!("Secret {} does not exist in scope {}", b.key, b.scope)));
    }
    st.store.kv_delete(&format!("secret:{}/{}", b.scope, b.key)).await?;
    Ok(empty())
}

async fn get_secret(State(st): State<S>, Who(p): Who, Query(q): Query<KeyBody>) -> ApiResult<Json<Value>> {
    st.check_scope_access(&p, &q.scope, "READ").await?;
    let v = st.get_secret(&q.scope, &q.key).await?.ok_or_else(|| ApiError::NotFound(format!("Secret {} does not exist in scope {}", q.key, q.scope)))?;
    Ok(Json(json!({ "key": q.key, "value": base64::engine::general_purpose::STANDARD.encode(&v) })))
}

async fn list_secrets(State(st): State<S>, Who(p): Who, Query(q): Query<ScopeBody>) -> ApiResult<Json<Value>> {
    st.store.require::<Scope>(KIND_SCOPE, &q.scope, "Secret scope").await?;
    st.check_scope_access(&p, &q.scope, "READ").await?;
    let secrets: Vec<crate::store::Doc<SecretMeta>> = st.store.list(KIND_SECRET, st.ws(), Filter { parent_id: Some(&q.scope), ..Default::default() }).await?;
    Ok(Json(json!({ "secrets": secrets.iter().map(|s| &s.data).collect::<Vec<_>>() })))
}

#[derive(Debug, Deserialize)]
struct AclBody {
    scope: String,
    principal: String,
    permission: String,
}

async fn put_acl(State(st): State<S>, Who(p): Who, Body(b): Body<AclBody>) -> ApiResult<Json<Value>> {
    st.check_scope_access(&p, &b.scope, "MANAGE").await?;
    if !matches!(b.permission.as_str(), "READ" | "WRITE" | "MANAGE") {
        return Err(ApiError::invalid("permission must be READ, WRITE or MANAGE"));
    }
    st.store.upsert(KIND_ACL, st.ws(), &format!("{}/{}", b.scope, b.principal), Some(&b.scope), Some(&b.principal), &json!({ "principal": b.principal, "permission": b.permission })).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct AclKey {
    scope: String,
    principal: String,
}

async fn delete_acl(State(st): State<S>, Who(p): Who, Body(b): Body<AclKey>) -> ApiResult<Json<Value>> {
    st.check_scope_access(&p, &b.scope, "MANAGE").await?;
    st.store.delete(KIND_ACL, &format!("{}/{}", b.scope, b.principal)).await?;
    Ok(empty())
}

async fn get_acl(State(st): State<S>, Who(p): Who, Query(q): Query<AclKey>) -> ApiResult<Json<Value>> {
    st.check_scope_access(&p, &q.scope, "MANAGE").await?;
    Ok(Json(st.store.require::<Value>(KIND_ACL, &format!("{}/{}", q.scope, q.principal), "ACL").await?.data))
}

async fn list_acls(State(st): State<S>, Who(p): Who, Query(q): Query<ScopeBody>) -> ApiResult<Json<Value>> {
    st.check_scope_access(&p, &q.scope, "MANAGE").await?;
    let acls: Vec<crate::store::Doc<Value>> = st.store.list(KIND_ACL, st.ws(), Filter { parent_id: Some(&q.scope), ..Default::default() }).await?;
    Ok(Json(json!({ "items": acls.iter().map(|a| &a.data).collect::<Vec<_>>() })))
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/secrets/scopes/create", post(create_scope))
        .route("/api/2.0/secrets/scopes/delete", post(delete_scope))
        .route("/api/2.0/secrets/scopes/list", get(list_scopes))
        .route("/api/2.0/secrets/put", post(put_secret))
        .route("/api/2.0/secrets/delete", post(delete_secret))
        .route("/api/2.0/secrets/get", get(get_secret))
        .route("/api/2.0/secrets/list", get(list_secrets))
        .route("/api/2.0/secrets/acls/put", post(put_acl))
        .route("/api/2.0/secrets/acls/delete", post(delete_acl))
        .route("/api/2.0/secrets/acls/get", get(get_acl))
        .route("/api/2.0/secrets/acls/list", get(list_acls))
}
