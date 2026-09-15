//! SCIM 2.0 (`/api/2.0/preview/scim/v2/{Users,Groups,ServicePrincipals}`).

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::{Body, S};
use crate::auth::{Group, User, Who, ADMINS_GROUP, KIND_GROUP, KIND_USER, USERS_GROUP};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{Doc, Filter};

const USER_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:User";
const GROUP_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:Group";
const SP_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal";
const LIST_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:ListResponse";
const PATCH_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:PatchOp";

pub async fn user_resource(st: &AppState, doc: &Doc<User>) -> ApiResult<Value> {
    let groups: Vec<Doc<Group>> = st.store.list(KIND_GROUP, st.ws(), Filter::default()).await?;
    let member_of: Vec<Value> = groups.iter().filter(|g| g.data.members.contains(&doc.id) || doc.data.groups.contains(&g.data.display_name)).map(|g| json!({ "value": g.id, "display": g.data.display_name, "type": "direct", "$ref": format!("Groups/{}", g.id) })).collect();
    let u = &doc.data;
    let (given, family) = u.display_name.split_once(' ').map(|(a, b)| (a.to_string(), b.to_string())).unwrap_or((u.display_name.clone(), String::new()));
    if u.application_id.is_some() {
        return Ok(json!({ "schemas": [SP_SCHEMA], "id": doc.id, "applicationId": u.application_id, "displayName": u.display_name, "active": u.active, "groups": member_of, "entitlements": u.entitlements.iter().map(|e| json!({ "value": e })).collect::<Vec<_>>() }));
    }
    Ok(json!({ "schemas": [USER_SCHEMA], "id": doc.id, "userName": u.user_name, "displayName": u.display_name, "name": { "givenName": given, "familyName": family }, "emails": [{ "value": u.user_name, "type": "work", "primary": true }], "active": u.active, "groups": member_of, "entitlements": u.entitlements.iter().map(|e| json!({ "value": e })).collect::<Vec<_>>(), "externalId": Value::Null }))
}

async fn group_resource(st: &AppState, doc: &Doc<Group>) -> ApiResult<Value> {
    let mut members = vec![];
    for m in &doc.data.members {
        if let Some(u) = st.store.get::<User>(KIND_USER, m).await? {
            members.push(json!({ "value": m, "display": u.data.user_name, "$ref": format!("Users/{m}") }));
        } else if let Some(g) = st.store.get::<Group>(KIND_GROUP, m).await? {
            members.push(json!({ "value": m, "display": g.data.display_name, "$ref": format!("Groups/{m}") }));
        }
    }
    // Users may also carry group names directly (bootstrap admin).
    let users: Vec<Doc<User>> = st.store.list(KIND_USER, st.ws(), Filter::default()).await?;
    for u in users.iter().filter(|u| u.data.groups.contains(&doc.data.display_name) && !doc.data.members.contains(&u.id)) {
        members.push(json!({ "value": u.id, "display": u.data.user_name, "$ref": format!("Users/{}", u.id) }));
    }
    Ok(json!({ "schemas": [GROUP_SCHEMA], "id": doc.id, "displayName": doc.data.display_name, "members": members, "entitlements": doc.data.entitlements.iter().map(|e| json!({ "value": e })).collect::<Vec<_>>(), "meta": { "resourceType": "Group" } }))
}

impl AppState {
    pub async fn service_principal_by_app_id(&self, app_id: &str) -> ApiResult<Option<Doc<User>>> {
        let users: Vec<Doc<User>> = self.store.list(KIND_USER, self.ws(), Filter::default()).await?;
        Ok(users.into_iter().find(|u| u.data.application_id.as_deref() == Some(app_id)))
    }
}

fn scim_error(status: StatusCode, detail: &str) -> Response {
    (status, Json(json!({ "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"], "status": status.as_u16().to_string(), "detail": detail }))).into_response()
}

fn list_response(resources: Vec<Value>, start: usize, count: usize) -> Value {
    let total = resources.len();
    let page: Vec<Value> = resources.into_iter().skip(start.saturating_sub(1)).take(count).collect();
    json!({ "schemas": [LIST_SCHEMA], "totalResults": total, "startIndex": start, "itemsPerPage": page.len(), "Resources": page })
}

/// Minimal SCIM filter: `attr eq "value"`, `attr co "value"`, `attr sw "value"`.
fn filter_match(filter: Option<&str>, res: &Value) -> bool {
    let Some(f) = filter else { return true };
    let parts: Vec<&str> = f.splitn(3, ' ').collect();
    if parts.len() != 3 {
        return true;
    }
    let (attr, op, val) = (parts[0], parts[1].to_ascii_lowercase(), parts[2].trim_matches('"'));
    let actual = res.get(attr).map(|v| match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }).unwrap_or_default();
    match op.as_str() {
        "eq" => actual.eq_ignore_ascii_case(val),
        "ne" => !actual.eq_ignore_ascii_case(val),
        "co" => actual.to_lowercase().contains(&val.to_lowercase()),
        "sw" => actual.to_lowercase().starts_with(&val.to_lowercase()),
        _ => true,
    }
}

#[derive(Debug, Deserialize)]
struct ListQ {
    #[serde(default)]
    filter: Option<String>,
    #[serde(default, rename = "startIndex")]
    start_index: Option<usize>,
    #[serde(default)]
    count: Option<usize>,
    #[serde(default, rename = "sortBy")]
    sort_by: Option<String>,
    #[serde(default, rename = "sortOrder")]
    sort_order: Option<String>,
}

fn apply_sort(mut items: Vec<Value>, q: &ListQ) -> Vec<Value> {
    if let Some(k) = &q.sort_by {
        items.sort_by(|a, b| a[k].to_string().cmp(&b[k].to_string()));
        if q.sort_order.as_deref() == Some("descending") {
            items.reverse();
        }
    }
    items
}

// ------------------------------------------------------------------ Users

async fn list_users(State(st): State<S>, Query(q): Query<ListQ>) -> ApiResult<Json<Value>> {
    let users: Vec<Doc<User>> = st.store.list(KIND_USER, st.ws(), Filter::default()).await?;
    let mut out = vec![];
    for u in users.iter().filter(|u| u.data.application_id.is_none()) {
        let r = user_resource(&st, u).await?;
        if filter_match(q.filter.as_deref(), &r) {
            out.push(r);
        }
    }
    let out = apply_sort(out, &q);
    Ok(Json(list_response(out, q.start_index.unwrap_or(1), q.count.unwrap_or(100))))
}

async fn get_user(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let doc: Doc<User> = st.store.require(KIND_USER, &id, "User").await?;
    Ok(Json(user_resource(&st, &doc).await?))
}

fn entitlements_of(v: &Value) -> Vec<String> {
    v["entitlements"].as_array().map(|a| a.iter().filter_map(|e| e["value"].as_str().map(|s| s.to_string())).collect()).unwrap_or_default()
}

async fn create_user(State(st): State<S>, Who(p): Who, Body(b): Body<Value>) -> ApiResult<Response> {
    p.require_admin()?;
    let user_name = b["userName"].as_str().ok_or_else(|| ApiError::invalid("userName is required"))?.to_string();
    if st.user_by_name(&user_name).await?.is_some() {
        return Ok(scim_error(StatusCode::CONFLICT, &format!("User with username {user_name} already exists.")));
    }
    let display = b["displayName"].as_str().map(|s| s.to_string()).unwrap_or_else(|| {
        let given = b["name"]["givenName"].as_str().unwrap_or("");
        let family = b["name"]["familyName"].as_str().unwrap_or("");
        format!("{given} {family}").trim().to_string()
    });
    let id = uuid::Uuid::new_v4().simple().to_string();
    let password_hash = b["password"].as_str().map(crate::auth::hash_password).transpose()?;
    let mut entitlements = entitlements_of(&b);
    if entitlements.is_empty() {
        entitlements = vec!["workspace-access".into(), "databricks-sql-access".into()];
    }
    let user = User { user_name: user_name.clone(), display_name: if display.is_empty() { user_name.clone() } else { display }, password_hash, active: b["active"].as_bool().unwrap_or(true), groups: vec![USERS_GROUP.into()], application_id: None, entitlements };
    st.store.insert(KIND_USER, st.ws(), &id, None, Some(&user_name), &user).await?;
    if let Some(groups) = b["groups"].as_array() {
        for g in groups {
            if let Some(gid) = g["value"].as_str() {
                add_member(&st, gid, &id).await?;
            }
        }
    }
    let doc: Doc<User> = st.store.require(KIND_USER, &id, "User").await?;
    Ok((StatusCode::CREATED, Json(user_resource(&st, &doc).await?)).into_response())
}

async fn replace_user(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let mut doc: Doc<User> = st.store.require(KIND_USER, &id, "User").await?;
    if let Some(n) = b["userName"].as_str() {
        doc.data.user_name = n.to_string();
    }
    if let Some(n) = b["displayName"].as_str() {
        doc.data.display_name = n.to_string();
    }
    if let Some(a) = b["active"].as_bool() {
        doc.data.active = a;
    }
    if b.get("entitlements").is_some() {
        doc.data.entitlements = entitlements_of(&b);
    }
    if let Some(pw) = b["password"].as_str() {
        doc.data.password_hash = Some(crate::auth::hash_password(pw)?);
    }
    let name = doc.data.user_name.clone();
    st.store.put(KIND_USER, &id, None, Some(&name), &doc.data).await?;
    Ok(Json(user_resource(&st, &doc).await?))
}

async fn patch_user(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let mut doc: Doc<User> = st.store.require(KIND_USER, &id, "User").await?;
    for op in b["Operations"].as_array().cloned().unwrap_or_default() {
        let kind = op["op"].as_str().unwrap_or("replace").to_ascii_lowercase();
        let path = op["path"].as_str().unwrap_or("");
        let value = &op["value"];
        match (kind.as_str(), path) {
            ("replace" | "add", "active") => doc.data.active = value.as_bool().or_else(|| value.as_str().map(|s| s == "true")).unwrap_or(doc.data.active),
            ("replace" | "add", "displayName") => doc.data.display_name = value.as_str().unwrap_or(&doc.data.display_name).to_string(),
            ("replace" | "add", "userName") => doc.data.user_name = value.as_str().unwrap_or(&doc.data.user_name).to_string(),
            ("replace" | "add", "password") => doc.data.password_hash = Some(crate::auth::hash_password(value.as_str().unwrap_or(""))?),
            ("add", "entitlements") => {
                for e in value.as_array().cloned().unwrap_or_default() {
                    if let Some(v) = e["value"].as_str() {
                        if !doc.data.entitlements.iter().any(|x| x == v) {
                            doc.data.entitlements.push(v.to_string());
                        }
                    }
                }
            }
            ("remove", p2) if p2.starts_with("entitlements") => {
                if let Some(v) = p2.split("value eq \"").nth(1).and_then(|s| s.split('"').next()) {
                    doc.data.entitlements.retain(|x| x != v);
                } else {
                    doc.data.entitlements.clear();
                }
            }
            ("replace" | "add", "") => {
                if let Some(o) = value.as_object() {
                    if let Some(a) = o.get("active").and_then(|v| v.as_bool()) {
                        doc.data.active = a;
                    }
                    if let Some(d) = o.get("displayName").and_then(|v| v.as_str()) {
                        doc.data.display_name = d.to_string();
                    }
                    if let Some(u) = o.get("userName").and_then(|v| v.as_str()) {
                        doc.data.user_name = u.to_string();
                    }
                }
            }
            _ => {}
        }
    }
    let name = doc.data.user_name.clone();
    st.store.put(KIND_USER, &id, None, Some(&name), &doc.data).await?;
    Ok(Json(user_resource(&st, &doc).await?))
}

async fn delete_user(State(st): State<S>, Who(p): Who, Path(id): Path<String>) -> ApiResult<Response> {
    p.require_admin()?;
    if id == p.user_id {
        return Err(ApiError::invalid("Cannot delete the current user."));
    }
    let groups: Vec<Doc<Group>> = st.store.list(KIND_GROUP, st.ws(), Filter::default()).await?;
    for mut g in groups {
        if g.data.members.contains(&id) {
            g.data.members.retain(|m| m != &id);
            st.store.put(KIND_GROUP, &g.id, None, Some(&g.data.display_name), &g.data).await?;
        }
    }
    if !st.store.delete(KIND_USER, &id).await? {
        return Err(ApiError::NotFound(format!("User {id} not found")));
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

// ----------------------------------------------------------------- Groups

async fn add_member(st: &AppState, gid: &str, member: &str) -> ApiResult<()> {
    let mut g: Doc<Group> = st.store.require(KIND_GROUP, gid, "Group").await?;
    if !g.data.members.contains(&member.to_string()) {
        g.data.members.push(member.to_string());
        st.store.put(KIND_GROUP, gid, None, Some(&g.data.display_name), &g.data).await?;
    }
    Ok(())
}

async fn list_groups(State(st): State<S>, Query(q): Query<ListQ>) -> ApiResult<Json<Value>> {
    let groups: Vec<Doc<Group>> = st.store.list(KIND_GROUP, st.ws(), Filter::default()).await?;
    let mut out = vec![];
    for g in &groups {
        let r = group_resource(&st, g).await?;
        if filter_match(q.filter.as_deref(), &r) {
            out.push(r);
        }
    }
    let out = apply_sort(out, &q);
    Ok(Json(list_response(out, q.start_index.unwrap_or(1), q.count.unwrap_or(100))))
}

async fn get_group(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let doc: Doc<Group> = st.store.require(KIND_GROUP, &id, "Group").await?;
    Ok(Json(group_resource(&st, &doc).await?))
}

async fn create_group(State(st): State<S>, Who(p): Who, Body(b): Body<Value>) -> ApiResult<Response> {
    p.require_admin()?;
    let name = b["displayName"].as_str().ok_or_else(|| ApiError::invalid("displayName is required"))?.to_string();
    if st.store.find_by_name::<Group>(KIND_GROUP, st.ws(), None, &name).await?.is_some() {
        return Ok(scim_error(StatusCode::CONFLICT, &format!("Group with name {name} already exists.")));
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    let members: Vec<String> = b["members"].as_array().map(|a| a.iter().filter_map(|m| m["value"].as_str().map(|s| s.to_string())).collect()).unwrap_or_default();
    let g = Group { display_name: name.clone(), members, entitlements: entitlements_of(&b) };
    st.store.insert(KIND_GROUP, st.ws(), &id, None, Some(&name), &g).await?;
    let doc: Doc<Group> = st.store.require(KIND_GROUP, &id, "Group").await?;
    Ok((StatusCode::CREATED, Json(group_resource(&st, &doc).await?)).into_response())
}

async fn replace_group(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let mut doc: Doc<Group> = st.store.require(KIND_GROUP, &id, "Group").await?;
    if let Some(n) = b["displayName"].as_str() {
        doc.data.display_name = n.to_string();
    }
    if let Some(m) = b["members"].as_array() {
        doc.data.members = m.iter().filter_map(|m| m["value"].as_str().map(|s| s.to_string())).collect();
    }
    if b.get("entitlements").is_some() {
        doc.data.entitlements = entitlements_of(&b);
    }
    st.store.put(KIND_GROUP, &id, None, Some(&doc.data.display_name), &doc.data).await?;
    Ok(Json(group_resource(&st, &doc).await?))
}

async fn patch_group(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let mut doc: Doc<Group> = st.store.require(KIND_GROUP, &id, "Group").await?;
    if !b["schemas"].as_array().map(|s| s.iter().any(|x| x == PATCH_SCHEMA)).unwrap_or(true) {
        return Err(ApiError::invalid("Expected a PatchOp document"));
    }
    for op in b["Operations"].as_array().cloned().unwrap_or_default() {
        let kind = op["op"].as_str().unwrap_or("replace").to_ascii_lowercase();
        let path = op["path"].as_str().unwrap_or("");
        let value = &op["value"];
        match kind.as_str() {
            "add" if path == "members" || path.is_empty() => {
                let vals = value.get("members").and_then(|m| m.as_array()).cloned().or_else(|| value.as_array().cloned()).unwrap_or_default();
                for m in vals {
                    if let Some(v) = m["value"].as_str() {
                        if !doc.data.members.iter().any(|x| x == v) {
                            doc.data.members.push(v.to_string());
                        }
                    }
                }
            }
            "add" if path == "entitlements" => {
                for e in value.as_array().cloned().unwrap_or_default() {
                    if let Some(v) = e["value"].as_str() {
                        if !doc.data.entitlements.iter().any(|x| x == v) {
                            doc.data.entitlements.push(v.to_string());
                        }
                    }
                }
            }
            "remove" if path.starts_with("members") => {
                if let Some(v) = path.split("value eq \"").nth(1).and_then(|s| s.split('"').next()) {
                    doc.data.members.retain(|x| x != v);
                } else if let Some(arr) = value.as_array() {
                    for m in arr {
                        if let Some(v) = m["value"].as_str() {
                            doc.data.members.retain(|x| x != v);
                        }
                    }
                }
            }
            "remove" if path.starts_with("entitlements") => {
                if let Some(v) = path.split("value eq \"").nth(1).and_then(|s| s.split('"').next()) {
                    doc.data.entitlements.retain(|x| x != v);
                }
            }
            "replace" if path == "displayName" => doc.data.display_name = value.as_str().unwrap_or(&doc.data.display_name).to_string(),
            "replace" if path == "members" => doc.data.members = value.as_array().map(|a| a.iter().filter_map(|m| m["value"].as_str().map(|s| s.to_string())).collect()).unwrap_or_default(),
            _ => {}
        }
    }
    st.store.put(KIND_GROUP, &id, None, Some(&doc.data.display_name), &doc.data).await?;
    Ok(Json(group_resource(&st, &doc).await?))
}

async fn delete_group(State(st): State<S>, Who(p): Who, Path(id): Path<String>) -> ApiResult<Response> {
    p.require_admin()?;
    let doc: Doc<Group> = st.store.require(KIND_GROUP, &id, "Group").await?;
    if doc.data.display_name == ADMINS_GROUP || doc.data.display_name == USERS_GROUP {
        return Err(ApiError::invalid(format!("Cannot delete built-in group {}", doc.data.display_name)));
    }
    st.store.delete(KIND_GROUP, &id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

// ------------------------------------------------------- ServicePrincipals

async fn list_sps(State(st): State<S>, Query(q): Query<ListQ>) -> ApiResult<Json<Value>> {
    let users: Vec<Doc<User>> = st.store.list(KIND_USER, st.ws(), Filter::default()).await?;
    let mut out = vec![];
    for u in users.iter().filter(|u| u.data.application_id.is_some()) {
        let r = user_resource(&st, u).await?;
        if filter_match(q.filter.as_deref(), &r) {
            out.push(r);
        }
    }
    let out = apply_sort(out, &q);
    Ok(Json(list_response(out, q.start_index.unwrap_or(1), q.count.unwrap_or(100))))
}

async fn create_sp(State(st): State<S>, Who(p): Who, Body(b): Body<Value>) -> ApiResult<Response> {
    p.require_admin()?;
    let app_id = b["applicationId"].as_str().map(|s| s.to_string()).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let display = b["displayName"].as_str().unwrap_or(&app_id).to_string();
    if st.service_principal_by_app_id(&app_id).await?.is_some() {
        return Ok(scim_error(StatusCode::CONFLICT, "Service principal already exists"));
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    let mut entitlements = entitlements_of(&b);
    if entitlements.is_empty() {
        entitlements = vec!["workspace-access".into(), "databricks-sql-access".into()];
    }
    let user = User { user_name: format!("sp:{app_id}"), display_name: display, password_hash: None, active: b["active"].as_bool().unwrap_or(true), groups: vec![], application_id: Some(app_id.clone()), entitlements };
    let name = user.user_name.clone();
    st.store.insert(KIND_USER, st.ws(), &id, None, Some(&name), &user).await?;
    if let Some(groups) = b["groups"].as_array() {
        for g in groups {
            if let Some(gid) = g["value"].as_str() {
                add_member(&st, gid, &id).await?;
            }
        }
    }
    let doc: Doc<User> = st.store.require(KIND_USER, &id, "ServicePrincipal").await?;
    Ok((StatusCode::CREATED, Json(user_resource(&st, &doc).await?)).into_response())
}

async fn get_sp(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let doc: Doc<User> = st.store.require(KIND_USER, &id, "ServicePrincipal").await?;
    if doc.data.application_id.is_none() {
        return Err(ApiError::NotFound(format!("ServicePrincipal {id} not found")));
    }
    Ok(Json(user_resource(&st, &doc).await?))
}

// ------------------------------------------------------------ Bootstrap SP token

async fn sp_secret(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    p.require_admin()?;
    let doc: Doc<User> = st.store.require(KIND_USER, &id, "ServicePrincipal").await?;
    let sp = st.principal_for_user(&doc).await?;
    let lifetime = b.get("lifetime").and_then(|v| v.as_str()).and_then(|s| s.trim_end_matches('s').parse::<i64>().ok());
    let (value, tok) = st.create_token(&sp, "oauth-secret", lifetime).await?;
    Ok(Json(json!({ "id": tok.token_id, "secret": value, "create_time": chrono::Utc::now().to_rfc3339(), "status": "ACTIVE", "expire_time": if tok.expiry_time > 0 { json!(chrono::DateTime::from_timestamp_millis(tok.expiry_time).map(|d| d.to_rfc3339())) } else { Value::Null } })))
}

pub fn router() -> Router<S> {
    let mut r = Router::new();
    for base in ["/api/2.0/preview/scim/v2", "/api/2.0/account/scim/v2"] {
        r = r
            .route(&format!("{base}/Users"), get(list_users).post(create_user))
            .route(&format!("{base}/Users/{{id}}"), get(get_user).put(replace_user).patch(patch_user).delete(delete_user))
            .route(&format!("{base}/Groups"), get(list_groups).post(create_group))
            .route(&format!("{base}/Groups/{{id}}"), get(get_group).put(replace_group).patch(patch_group).delete(delete_group))
            .route(&format!("{base}/ServicePrincipals"), get(list_sps).post(create_sp))
            .route(&format!("{base}/ServicePrincipals/{{id}}"), get(get_sp).put(replace_user).patch(patch_user).delete(delete_user));
    }
    r.route("/api/2.0/accounts/servicePrincipals/{id}/credentials/secrets", axum::routing::post(sp_secret))
}
