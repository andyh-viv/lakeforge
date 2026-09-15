//! Permissions API (`/api/2.0/permissions/{type}/{id}`) plus permission
//! levels, and workspace-object ACL checks used by other modules.

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use super::{Body, S};
use crate::auth::{Who, ADMINS_GROUP};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

pub const KIND_PERMS: &str = "permissions";

/// Permission levels per object type, ordered weakest -> strongest.
pub fn levels(object_type: &str) -> &'static [&'static str] {
    match object_type {
        "clusters" => &["CAN_ATTACH_TO", "CAN_RESTART", "CAN_MANAGE"],
        "cluster-policies" => &["CAN_USE"],
        "instance-pools" => &["CAN_ATTACH_TO", "CAN_MANAGE"],
        "jobs" => &["CAN_VIEW", "CAN_MANAGE_RUN", "IS_OWNER", "CAN_MANAGE"],
        "pipelines" => &["CAN_VIEW", "CAN_RUN", "IS_OWNER", "CAN_MANAGE"],
        "notebooks" | "directories" | "files" => &["CAN_READ", "CAN_RUN", "CAN_EDIT", "CAN_MANAGE"],
        "repos" => &["CAN_READ", "CAN_RUN", "CAN_EDIT", "CAN_MANAGE"],
        "experiments" | "registered-models" => &["CAN_READ", "CAN_EDIT", "CAN_MANAGE_STAGING_VERSIONS", "CAN_MANAGE_PRODUCTION_VERSIONS", "CAN_MANAGE"],
        "sql/warehouses" | "warehouses" => &["CAN_USE", "CAN_MONITOR", "CAN_MANAGE"],
        "sql/queries" | "sql/alerts" | "sql/dashboards" | "dashboards" => &["CAN_VIEW", "CAN_RUN", "CAN_EDIT", "CAN_MANAGE"],
        "serving-endpoints" => &["CAN_VIEW", "CAN_QUERY", "CAN_MANAGE"],
        "tokens" | "authorization" => &["CAN_USE"],
        "passwords" => &["CAN_USE"],
        _ => &["CAN_VIEW", "CAN_MANAGE"],
    }
}

fn level_rank(object_type: &str, level: &str) -> Option<usize> {
    levels(object_type).iter().position(|l| *l == level)
}

fn doc_id(object_type: &str, object_id: &str) -> String {
    format!("{object_type}/{object_id}")
}

impl AppState {
    pub async fn get_permissions(&self, object_type: &str, object_id: &str) -> ApiResult<Vec<Value>> {
        Ok(self.store.get::<Value>(KIND_PERMS, &doc_id(object_type, object_id)).await?.and_then(|d| d.data["access_control_list"].as_array().cloned()).unwrap_or_default())
    }

    /// Replace the ACL. Entries: `{user_name|group_name|service_principal_name, permission_level}`.
    pub async fn set_permissions(&self, object_type: &str, object_id: &str, acl: &Value, by: &str) -> ApiResult<Value> {
        let entries = acl.as_array().ok_or_else(|| ApiError::invalid("access_control_list must be an array"))?;
        let mut out: Vec<Value> = vec![];
        for e in entries {
            let level = e["permission_level"].as_str().ok_or_else(|| ApiError::invalid("permission_level is required"))?;
            if level_rank(object_type, level).is_none() {
                return Err(ApiError::invalid(format!("Invalid permission_level {level} for {object_type}; allowed: {:?}", levels(object_type))));
            }
            let principal = ["user_name", "group_name", "service_principal_name"].iter().find_map(|k| e[k].as_str().map(|v| (k.to_string(), v.to_string()))).ok_or_else(|| ApiError::invalid("user_name, group_name or service_principal_name is required"))?;
            let mut entry = json!({ "permission_level": level });
            entry[principal.0] = json!(principal.1);
            out.push(entry);
        }
        let v = json!({ "object_type": object_type, "object_id": object_id, "access_control_list": out, "updated_by": by, "updated_at": crate::store::now_ms() });
        self.store.upsert(KIND_PERMS, self.ws(), &doc_id(object_type, object_id), Some(object_type), Some(object_id), &v).await?;
        Ok(v)
    }

    pub async fn update_permissions(&self, object_type: &str, object_id: &str, acl: &Value, by: &str) -> ApiResult<Value> {
        let mut cur = self.get_permissions(object_type, object_id).await?;
        for e in acl.as_array().cloned().unwrap_or_default() {
            let key = ["user_name", "group_name", "service_principal_name"].iter().find_map(|k| e[k].as_str().map(|v| (k.to_string(), v.to_string())));
            if let Some((k, v)) = key {
                cur.retain(|c| c[&k] != v);
                cur.push(e);
            }
        }
        self.set_permissions(object_type, object_id, &Value::Array(cur), by).await
    }

    /// Effective level (highest) for a principal, `None` if no explicit grant.
    pub async fn effective_level(&self, p: &crate::auth::Principal, object_type: &str, object_id: &str) -> ApiResult<Option<String>> {
        if p.is_admin {
            return Ok(Some(levels(object_type).last().copied().unwrap_or("CAN_MANAGE").to_string()));
        }
        let acl = self.get_permissions(object_type, object_id).await?;
        let mut best: Option<(usize, String)> = None;
        for e in acl {
            let matches = e["user_name"].as_str() == Some(&p.user_name) || e["service_principal_name"].as_str() == Some(&p.user_name) || e["group_name"].as_str().map(|g| p.groups.iter().any(|x| x == g) || g == "users").unwrap_or(false);
            if !matches {
                continue;
            }
            if let Some(l) = e["permission_level"].as_str() {
                if let Some(r) = level_rank(object_type, l) {
                    if best.as_ref().map(|(br, _)| r > *br).unwrap_or(true) {
                        best = Some((r, l.to_string()));
                    }
                }
            }
        }
        Ok(best.map(|(_, l)| l))
    }

    /// Enforce `need` on an object. Owners (creator) and admins always pass;
    /// objects with no ACL are open to all workspace users (Databricks default
    /// for unrestricted workspaces).
    pub async fn check_permission(&self, p: &crate::auth::Principal, object_type: &str, object_id: &str, need: &str, owner: Option<&str>) -> ApiResult<()> {
        if p.is_admin || owner == Some(p.user_name.as_str()) {
            return Ok(());
        }
        let acl = self.get_permissions(object_type, object_id).await?;
        if acl.is_empty() {
            return Ok(());
        }
        let have = self.effective_level(p, object_type, object_id).await?;
        let ok = match (have, level_rank(object_type, need)) {
            (Some(h), Some(n)) => level_rank(object_type, &h).map(|r| r >= n).unwrap_or(false),
            _ => false,
        };
        if ok {
            Ok(())
        } else {
            Err(ApiError::PermissionDenied(format!("User {} does not have {need} on {object_type}/{object_id}", p.user_name)))
        }
    }
}

fn expand(object_type: &str, object_id: &str, acl: &[Value], owner: Option<&str>) -> Value {
    let mut grouped: std::collections::BTreeMap<(String, String), Vec<Value>> = Default::default();
    // Admins always have the top level.
    let top = levels(object_type).last().copied().unwrap_or("CAN_MANAGE");
    grouped.entry(("group_name".into(), ADMINS_GROUP.into())).or_default().push(json!({ "permission_level": top, "inherited": true, "inherited_from_object": ["/authorization/admins"] }));
    if let Some(o) = owner {
        grouped.entry(("user_name".into(), o.into())).or_default().push(json!({ "permission_level": if object_type == "jobs" || object_type == "pipelines" { "IS_OWNER" } else { top }, "inherited": false }));
    }
    for e in acl {
        let key = ["user_name", "group_name", "service_principal_name"].iter().find_map(|k| e[k].as_str().map(|v| (k.to_string(), v.to_string())));
        if let Some(k) = key {
            grouped.entry(k).or_default().push(json!({ "permission_level": e["permission_level"], "inherited": false }));
        }
    }
    let list: Vec<Value> = grouped
        .into_iter()
        .map(|((k, v), perms)| {
            let mut o = json!({ "all_permissions": perms, "display_name": v });
            o[k] = json!(v);
            o
        })
        .collect();
    json!({ "object_id": format!("/{object_type}/{object_id}"), "object_type": object_type_singular(object_type), "access_control_list": list })
}

fn object_type_singular(t: &str) -> &str {
    match t {
        "clusters" => "cluster",
        "jobs" => "job",
        "pipelines" => "pipeline",
        "notebooks" => "notebook",
        "directories" => "directory",
        "repos" => "repo",
        "experiments" => "mlflowExperiment",
        "registered-models" => "registered-model",
        "sql/warehouses" | "warehouses" => "warehouses",
        "serving-endpoints" => "serving-endpoint",
        "instance-pools" => "instance-pool",
        "cluster-policies" => "cluster-policy",
        other => other,
    }
}

async fn owner_of(st: &AppState, object_type: &str, id: &str) -> Option<String> {
    match object_type {
        "clusters" => st.get_cluster(id).await.ok().map(|c| c.data.creator_user_name),
        "jobs" => st.get_job(id.parse().ok()?).await.ok().map(|j| j.data.creator_user_name),
        _ => None,
    }
}

async fn get_perms(State(st): State<S>, Path((t, id)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let t = t.trim_start_matches('/').to_string();
    let acl = st.get_permissions(&t, &id).await?;
    let owner = owner_of(&st, &t, &id).await;
    Ok(Json(expand(&t, &id, &acl, owner.as_deref())))
}

async fn put_perms(State(st): State<S>, Who(p): Who, Path((t, id)): Path<(String, String)>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    let owner = owner_of(&st, &t, &id).await;
    st.check_permission(&p, &t, &id, levels(&t).last().copied().unwrap_or("CAN_MANAGE"), owner.as_deref()).await?;
    st.set_permissions(&t, &id, &b["access_control_list"], &p.user_name).await?;
    let acl = st.get_permissions(&t, &id).await?;
    Ok(Json(expand(&t, &id, &acl, owner.as_deref())))
}

async fn patch_perms(State(st): State<S>, Who(p): Who, Path((t, id)): Path<(String, String)>, Body(b): Body<Value>) -> ApiResult<Json<Value>> {
    let owner = owner_of(&st, &t, &id).await;
    st.check_permission(&p, &t, &id, levels(&t).last().copied().unwrap_or("CAN_MANAGE"), owner.as_deref()).await?;
    st.update_permissions(&t, &id, &b["access_control_list"], &p.user_name).await?;
    let acl = st.get_permissions(&t, &id).await?;
    Ok(Json(expand(&t, &id, &acl, owner.as_deref())))
}

async fn perm_levels(Path((t, _id)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "permission_levels": levels(&t).iter().map(|l| json!({ "permission_level": l, "description": describe(l) })).collect::<Vec<_>>() })))
}

fn describe(l: &str) -> &'static str {
    match l {
        "CAN_ATTACH_TO" => "Can attach to the cluster",
        "CAN_RESTART" => "Can restart the cluster",
        "CAN_MANAGE" => "Can manage the object",
        "CAN_VIEW" => "Can view the object",
        "CAN_MANAGE_RUN" => "Can manage runs",
        "IS_OWNER" => "Is owner",
        "CAN_READ" => "Can read",
        "CAN_RUN" => "Can run",
        "CAN_EDIT" => "Can edit",
        "CAN_USE" => "Can use",
        "CAN_MONITOR" => "Can monitor",
        "CAN_QUERY" => "Can query",
        _ => "",
    }
}

// Two-segment object types (sql/warehouses etc.)
async fn get_perms2(State(st): State<S>, Path((a, b, id)): Path<(String, String, String)>) -> ApiResult<Json<Value>> {
    get_perms(State(st), Path((format!("{a}/{b}"), id))).await
}
async fn put_perms2(State(st): State<S>, w: Who, Path((a, b, id)): Path<(String, String, String)>, body: Body<Value>) -> ApiResult<Json<Value>> {
    put_perms(State(st), w, Path((format!("{a}/{b}"), id)), body).await
}
async fn patch_perms2(State(st): State<S>, w: Who, Path((a, b, id)): Path<(String, String, String)>, body: Body<Value>) -> ApiResult<Json<Value>> {
    patch_perms(State(st), w, Path((format!("{a}/{b}"), id)), body).await
}
async fn perm_levels2(Path((a, b, id)): Path<(String, String, String)>) -> ApiResult<Json<Value>> {
    perm_levels(Path((format!("{a}/{b}"), id))).await
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/permissions/{type}/{id}", get(get_perms).put(put_perms).patch(patch_perms))
        .route("/api/2.0/permissions/{type}/{id}/permissionLevels", get(perm_levels))
        .route("/api/2.0/permissions/{a}/{b}/{id}", get(get_perms2).put(put_perms2).patch(patch_perms2))
        .route("/api/2.0/permissions/{a}/{b}/{id}/permissionLevels", get(perm_levels2))
        .route("/api/2.0/preview/permissions/{type}/{id}", get(get_perms).put(put_perms).patch(patch_perms))
}
