//! Token API (`/api/2.0/token/*`), Token Management (`/api/2.0/token-management/*`),
//! and the login endpoint used by the UI (`/api/2.0/lakeforge/login`).

use axum::extract::{Path, Query, State};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{empty, Body, S};
use crate::auth::{Token, Who, KIND_TOKEN};
use crate::error::{ApiError, ApiResult};
use crate::store::{Doc, Filter};

fn token_info(t: &Token) -> Value {
    json!({ "token_id": t.token_id, "comment": t.comment, "creation_time": t.creation_time, "expiry_time": t.expiry_time, "created_by_id": t.created_by_id, "created_by_username": t.created_by_username, "owner_id": t.owner_id })
}

#[derive(Debug, Deserialize)]
struct CreateBody {
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    lifetime_seconds: Option<i64>,
}

async fn create(State(st): State<S>, Who(p): Who, Body(b): Body<CreateBody>) -> ApiResult<Json<Value>> {
    let (value, tok) = st.create_token(&p, b.comment.as_deref().unwrap_or(""), b.lifetime_seconds).await?;
    Ok(Json(json!({ "token_value": value, "token_info": token_info(&tok) })))
}

async fn list(State(st): State<S>, Who(p): Who) -> ApiResult<Json<Value>> {
    let toks: Vec<Doc<Token>> = st.store.list(KIND_TOKEN, st.ws(), Filter::default()).await?;
    Ok(Json(json!({ "token_infos": toks.iter().filter(|t| t.data.owner_id == p.user_id).map(|t| token_info(&t.data)).collect::<Vec<_>>() })))
}

#[derive(Debug, Deserialize)]
struct DeleteBody {
    token_id: String,
}

async fn delete_h(State(st): State<S>, Who(p): Who, Body(b): Body<DeleteBody>) -> ApiResult<Json<Value>> {
    let tok: Doc<Token> = st.store.require(KIND_TOKEN, &b.token_id, "Token").await?;
    if tok.data.owner_id != p.user_id && !p.is_admin {
        return Err(ApiError::PermissionDenied("Cannot revoke a token you do not own.".into()));
    }
    st.store.delete(KIND_TOKEN, &b.token_id).await?;
    Ok(empty())
}

// Token management (admin)

#[derive(Debug, Deserialize)]
struct MgmtQ {
    #[serde(default)]
    created_by_id: Option<String>,
    #[serde(default)]
    created_by_username: Option<String>,
}

async fn mgmt_list(State(st): State<S>, Who(p): Who, Query(q): Query<MgmtQ>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let toks: Vec<Doc<Token>> = st.store.list(KIND_TOKEN, st.ws(), Filter::default()).await?;
    Ok(Json(json!({ "token_infos": toks.iter().filter(|t| q.created_by_id.as_deref().map(|c| c == t.data.created_by_id).unwrap_or(true) && q.created_by_username.as_deref().map(|c| c == t.data.created_by_username).unwrap_or(true)).map(|t| token_info(&t.data)).collect::<Vec<_>>() })))
}

async fn mgmt_get(State(st): State<S>, Who(p): Who, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let tok: Doc<Token> = st.store.require(KIND_TOKEN, &id, "Token").await?;
    Ok(Json(json!({ "token_info": token_info(&tok.data) })))
}

async fn mgmt_delete(State(st): State<S>, Who(p): Who, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    st.store.delete(KIND_TOKEN, &id).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct OboBody {
    application_id: String,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    lifetime_seconds: Option<i64>,
}

async fn create_obo(State(st): State<S>, Who(p): Who, Body(b): Body<OboBody>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let sp = st.service_principal_by_app_id(&b.application_id).await?.ok_or_else(|| ApiError::NotFound(format!("Service principal {} not found", b.application_id)))?;
    let sp_principal = st.principal_for_user(&sp).await?;
    let (value, tok) = st.create_token(&sp_principal, b.comment.as_deref().unwrap_or(""), b.lifetime_seconds).await?;
    Ok(Json(json!({ "token_value": value, "token_info": token_info(&tok) })))
}

// Login (UI) — password -> JWT session

#[derive(Debug, Deserialize)]
struct LoginBody {
    username: String,
    password: String,
}

async fn login(State(st): State<S>, Json(b): Json<LoginBody>) -> ApiResult<Json<Value>> {
    let p = st.authenticate_password(&b.username, &b.password).await?;
    let jwt = st.auth.issue_jwt(&p.user_id, &p.user_name, 12 * 3600)?;
    Ok(Json(json!({ "access_token": jwt, "token_type": "Bearer", "expires_in": 12 * 3600, "user": { "user_id": p.user_id, "user_name": p.user_name, "is_admin": p.is_admin, "groups": p.groups } })))
}

async fn me(State(st): State<S>, Who(p): Who) -> ApiResult<Json<Value>> {
    let user = st.user_by_name(&p.user_name).await?;
    Ok(Json(json!({ "user_id": p.user_id, "user_name": p.user_name, "display_name": user.as_ref().map(|u| u.data.display_name.clone()).unwrap_or_default(), "is_admin": p.is_admin, "groups": p.groups, "workspace_id": st.ws(), "cloud": st.config.cloud, "version": env!("CARGO_PKG_VERSION") })))
}

#[derive(Debug, Deserialize)]
struct PasswordBody {
    #[serde(default)]
    user_name: Option<String>,
    #[serde(default)]
    old_password: Option<String>,
    new_password: String,
}

async fn change_password(State(st): State<S>, Who(p): Who, Body(b): Body<PasswordBody>) -> ApiResult<Json<Value>> {
    let target = b.user_name.as_deref().unwrap_or(&p.user_name);
    if target != p.user_name {
        p.require_admin()?;
    } else if !p.is_admin {
        let old = b.old_password.as_deref().ok_or_else(|| ApiError::invalid("old_password is required"))?;
        st.authenticate_password(&p.user_name, old).await?;
    }
    if b.new_password.len() < 8 {
        return Err(ApiError::invalid("Password must be at least 8 characters"));
    }
    let mut user = st.user_by_name(target).await?.ok_or_else(|| ApiError::NotFound(format!("User {target} not found")))?;
    user.data.password_hash = Some(crate::auth::hash_password(&b.new_password)?);
    st.store.put(crate::auth::KIND_USER, &user.id, None, Some(&user.data.user_name), &user.data).await?;
    Ok(empty())
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/token/create", post(create))
        .route("/api/2.0/token/list", get(list))
        .route("/api/2.0/token/delete", post(delete_h))
        .route("/api/2.0/token-management/tokens", get(mgmt_list))
        .route("/api/2.0/token-management/tokens/{id}", get(mgmt_get).delete(mgmt_delete))
        .route("/api/2.0/token-management/on-behalf-of/tokens", post(create_obo))
        .route("/api/2.0/lakeforge/login", post(login))
        .route("/api/2.0/lakeforge/me", get(me))
        .route("/api/2.0/lakeforge/password", post(change_password))
        .route("/api/2.0/preview/scim/v2/Me", get(scim_me))
        .route("/api/2.0/lakeforge/tokens/{id}", delete(mgmt_delete))
}

async fn scim_me(State(st): State<S>, Who(p): Who) -> ApiResult<Json<Value>> {
    let user = st.user_by_name(&p.user_name).await?.ok_or_else(|| ApiError::NotFound("User not found".into()))?;
    Ok(Json(super::scim::user_resource(&st, &user).await?))
}
