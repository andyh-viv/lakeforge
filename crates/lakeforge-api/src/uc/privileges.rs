//! Unity Catalog privilege model: securable hierarchy, ownership, grants with
//! inheritance, and the checks enforced by the REST API and the SQL guard.
//!
//! Grants are stored per securable as `{ privilege_assignments: [{principal,
//! privileges}] }` (the Databricks `permissions` shape). A privilege granted on
//! a securable applies to every descendant, and owners hold every privilege on
//! the objects they own and everything below them. Workspace admins act as the
//! metastore admin.

use std::collections::HashMap;

use serde_json::{json, Value};

use crate::api::catalog::{KIND_CATALOG, KIND_CONNECTION, KIND_EXT_LOCATION, KIND_FUNCTION, KIND_GRANTS, KIND_SCHEMA, KIND_STORAGE_CRED, KIND_TABLE, KIND_VOLUME, METASTORE_ID};
use crate::auth::{Principal, USERS_GROUP};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

pub const KIND_SHARE: &str = "uc_share";
pub const KIND_RECIPIENT: &str = "uc_recipient";
pub const KIND_PROVIDER: &str = "uc_provider";
pub const KIND_MODEL: &str = "uc_registered_model";

/// `account users` is the Databricks name for "everyone"; `users` is ours.
pub const ACCOUNT_USERS: &str = "account users";

/// Every privilege Unity Catalog knows about.
pub const ALL_PRIVILEGES: &[&str] = &[
    "ALL_PRIVILEGES",
    "APPLY_TAG",
    "BROWSE",
    "CREATE_CATALOG",
    "CREATE_CONNECTION",
    "CREATE_EXTERNAL_LOCATION",
    "CREATE_EXTERNAL_TABLE",
    "CREATE_EXTERNAL_VOLUME",
    "CREATE_FOREIGN_CATALOG",
    "CREATE_FOREIGN_SECURABLE",
    "CREATE_FUNCTION",
    "CREATE_MANAGED_STORAGE",
    "CREATE_MATERIALIZED_VIEW",
    "CREATE_MODEL",
    "CREATE_PROVIDER",
    "CREATE_RECIPIENT",
    "CREATE_SCHEMA",
    "CREATE_SERVICE_CREDENTIAL",
    "CREATE_SHARE",
    "CREATE_STORAGE_CREDENTIAL",
    "CREATE_TABLE",
    "CREATE_VIEW",
    "CREATE_VOLUME",
    "EXECUTE",
    "MANAGE",
    "MANAGE_ALLOWLIST",
    "MODIFY",
    "READ_FILES",
    "READ_PRIVATE_FILES",
    "READ_VOLUME",
    "REFRESH",
    "SELECT",
    "SET_SHARE_PERMISSION",
    "USE_CATALOG",
    "USE_CONNECTION",
    "USE_MARKETPLACE_ASSETS",
    "USE_PROVIDER",
    "USE_RECIPIENT",
    "USE_SCHEMA",
    "USE_SHARE",
    "WRITE_FILES",
    "WRITE_PRIVATE_FILES",
    "WRITE_VOLUME",
];

/// Normalise `all privileges`, `use catalog`, `select` ... to the API spelling.
pub fn normalize_privilege(p: &str) -> String {
    p.trim().to_ascii_uppercase().replace(' ', "_")
}

pub fn is_known_privilege(p: &str) -> bool {
    ALL_PRIVILEGES.contains(&p)
}

/// Securable types accepted in `permissions/{securable_type}/{full_name}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Securable {
    Metastore,
    Catalog,
    Schema,
    Table,
    Volume,
    Function,
    ExternalLocation,
    StorageCredential,
    Connection,
    Share,
    Recipient,
    Provider,
}

impl Securable {
    pub fn parse(s: &str) -> ApiResult<Self> {
        Ok(match s.to_ascii_lowercase().replace('-', "_").as_str() {
            "metastore" => Self::Metastore,
            "catalog" => Self::Catalog,
            "schema" => Self::Schema,
            "table" | "view" | "materialized_view" | "streaming_table" => Self::Table,
            "volume" => Self::Volume,
            "function" | "registered_model" | "model" => Self::Function,
            "external_location" => Self::ExternalLocation,
            "storage_credential" | "credential" => Self::StorageCredential,
            "connection" => Self::Connection,
            "share" => Self::Share,
            "recipient" => Self::Recipient,
            "provider" => Self::Provider,
            other => return Err(ApiError::invalid(format!("Unknown securable type '{other}'"))),
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Metastore => "metastore",
            Self::Catalog => "catalog",
            Self::Schema => "schema",
            Self::Table => "table",
            Self::Volume => "volume",
            Self::Function => "function",
            Self::ExternalLocation => "external_location",
            Self::StorageCredential => "storage_credential",
            Self::Connection => "connection",
            Self::Share => "share",
            Self::Recipient => "recipient",
            Self::Provider => "provider",
        }
    }

    pub fn api_type(self) -> &'static str {
        match self {
            Self::Metastore => "METASTORE",
            Self::Catalog => "CATALOG",
            Self::Schema => "SCHEMA",
            Self::Table => "TABLE",
            Self::Volume => "VOLUME",
            Self::Function => "FUNCTION",
            Self::ExternalLocation => "EXTERNAL_LOCATION",
            Self::StorageCredential => "STORAGE_CREDENTIAL",
            Self::Connection => "CONNECTION",
            Self::Share => "SHARE",
            Self::Recipient => "RECIPIENT",
            Self::Provider => "PROVIDER",
        }
    }

    /// Document kind holding the securable itself (for owner lookup).
    pub fn kind(self) -> Option<&'static str> {
        Some(match self {
            Self::Metastore => return None,
            Self::Catalog => KIND_CATALOG,
            Self::Schema => KIND_SCHEMA,
            Self::Table => KIND_TABLE,
            Self::Volume => KIND_VOLUME,
            Self::Function => KIND_FUNCTION,
            Self::ExternalLocation => KIND_EXT_LOCATION,
            Self::StorageCredential => KIND_STORAGE_CRED,
            Self::Connection => KIND_CONNECTION,
            Self::Share => KIND_SHARE,
            Self::Recipient => KIND_RECIPIENT,
            Self::Provider => KIND_PROVIDER,
        })
    }
}

/// The securable plus its ancestors, nearest first, ending at the metastore.
pub fn lineage_of(sec: Securable, full_name: &str) -> Vec<(Securable, String)> {
    let mut out = vec![(sec, full_name.to_string())];
    match sec {
        Securable::Table | Securable::Volume | Securable::Function => {
            let parts: Vec<&str> = full_name.split('.').collect();
            if parts.len() >= 3 {
                out.push((Securable::Schema, format!("{}.{}", parts[0], parts[1])));
                out.push((Securable::Catalog, parts[0].to_string()));
            } else if parts.len() == 2 {
                out.push((Securable::Catalog, parts[0].to_string()));
            }
        }
        Securable::Schema => {
            if let Some((c, _)) = full_name.split_once('.') {
                out.push((Securable::Catalog, c.to_string()));
            }
        }
        _ => {}
    }
    if sec != Securable::Metastore {
        out.push((Securable::Metastore, METASTORE_ID.to_string()));
    }
    out
}

pub fn grants_key(sec: Securable, full_name: &str) -> String {
    format!("{}:{full_name}", sec.as_str())
}

pub fn grants_doc_id(sec: Securable, full_name: &str) -> String {
    format!("{KIND_GRANTS}:{}", grants_key(sec, full_name))
}

#[derive(Debug, Clone, Default)]
pub struct Assignment {
    pub principal: String,
    pub privileges: Vec<String>,
}

pub fn parse_assignments(v: &Value) -> Vec<Assignment> {
    v["privilege_assignments"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|x| Assignment {
                    principal: x["principal"].as_str().unwrap_or("").to_string(),
                    privileges: x["privileges"].as_array().map(|p| p.iter().filter_map(|s| s.as_str().map(normalize_privilege)).collect()).unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn assignments_json(a: &[Assignment]) -> Value {
    json!({ "privilege_assignments": a.iter().filter(|x| !x.privileges.is_empty()).map(|x| json!({ "principal": x.principal, "privileges": x.privileges })).collect::<Vec<_>>() })
}

/// Does `principal` (a user or group name) refer to `p`?
pub fn principal_matches(p: &Principal, principal: &str) -> bool {
    if principal.eq_ignore_ascii_case(&p.user_name) || principal == p.user_id {
        return true;
    }
    if principal.eq_ignore_ascii_case(ACCOUNT_USERS) || principal.eq_ignore_ascii_case(USERS_GROUP) {
        return true;
    }
    p.groups.iter().any(|g| g.eq_ignore_ascii_case(principal))
}

/// Per-request authorizer with a cache of grants and owners so a statement
/// touching many tables does not re-read the same catalog grants repeatedly.
pub struct Authorizer<'a> {
    st: &'a AppState,
    pub principal: &'a Principal,
    grants: HashMap<String, Vec<Assignment>>,
    owners: HashMap<String, Option<String>>,
}

impl<'a> Authorizer<'a> {
    pub fn new(st: &'a AppState, principal: &'a Principal) -> Self {
        Self { st, principal, grants: HashMap::new(), owners: HashMap::new() }
    }

    pub fn is_admin(&self) -> bool {
        self.principal.is_admin
    }

    pub async fn grants(&mut self, sec: Securable, full_name: &str) -> ApiResult<Vec<Assignment>> {
        let key = grants_key(sec, full_name);
        if let Some(g) = self.grants.get(&key) {
            return Ok(g.clone());
        }
        let g = self.st.store.get::<Value>(KIND_GRANTS, &grants_doc_id(sec, full_name)).await?.map(|d| parse_assignments(&d.data)).unwrap_or_default();
        self.grants.insert(key, g.clone());
        Ok(g)
    }

    /// Owner of a securable (`None` if it does not exist or has no owner).
    pub async fn owner(&mut self, sec: Securable, full_name: &str) -> ApiResult<Option<String>> {
        let key = grants_key(sec, full_name);
        if let Some(o) = self.owners.get(&key) {
            return Ok(o.clone());
        }
        let owner = match sec {
            Securable::Metastore => Some(self.st.config.admin_user.clone()),
            _ => {
                let kind = sec.kind().unwrap_or(KIND_TABLE);
                self.st.store.get::<Value>(kind, &format!("{kind}:{full_name}")).await?.and_then(|d| d.data["owner"].as_str().map(str::to_string))
            }
        };
        self.owners.insert(key, owner.clone());
        Ok(owner)
    }

    /// True when the principal owns the securable or any ancestor.
    pub async fn is_owner(&mut self, sec: Securable, full_name: &str) -> ApiResult<bool> {
        if self.is_admin() {
            return Ok(true);
        }
        for (s, n) in lineage_of(sec, full_name) {
            if let Some(o) = self.owner(s, &n).await? {
                if principal_matches_owner(self.principal, &o) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Direct owner only (used for ownership transfer / destructive ops on the
    /// object itself; admins and ancestor owners also qualify).
    pub async fn can_manage(&mut self, sec: Securable, full_name: &str) -> ApiResult<bool> {
        if self.is_owner(sec, full_name).await? {
            return Ok(true);
        }
        self.has_privilege(sec, full_name, "MANAGE").await
    }

    /// Privilege check with inheritance and `ALL_PRIVILEGES`.
    pub async fn has_privilege(&mut self, sec: Securable, full_name: &str, privilege: &str) -> ApiResult<bool> {
        if self.is_admin() {
            return Ok(true);
        }
        let privilege = normalize_privilege(privilege);
        for (s, n) in lineage_of(sec, full_name) {
            if let Some(o) = self.owner(s, &n).await? {
                if principal_matches_owner(self.principal, &o) {
                    return Ok(true);
                }
            }
            for a in self.grants(s, &n).await? {
                if principal_matches(self.principal, &a.principal) && (a.privileges.contains(&privilege) || a.privileges.iter().any(|p| p == "ALL_PRIVILEGES")) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub async fn has_any_privilege(&mut self, sec: Securable, full_name: &str) -> ApiResult<bool> {
        if self.is_admin() {
            return Ok(true);
        }
        for (s, n) in lineage_of(sec, full_name) {
            if let Some(o) = self.owner(s, &n).await? {
                if principal_matches_owner(self.principal, &o) {
                    return Ok(true);
                }
            }
            for a in self.grants(s, &n).await? {
                if principal_matches(self.principal, &a.principal) && !a.privileges.is_empty() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub async fn require(&mut self, sec: Securable, full_name: &str, privilege: &str) -> ApiResult<()> {
        if self.has_privilege(sec, full_name, privilege).await? {
            Ok(())
        } else {
            Err(denied(self.principal, privilege, sec, full_name))
        }
    }

    pub async fn require_owner(&mut self, sec: Securable, full_name: &str) -> ApiResult<()> {
        if self.can_manage(sec, full_name).await? {
            Ok(())
        } else {
            Err(ApiError::PermissionDenied(format!("User does not own {} '{full_name}' and does not have MANAGE on it.", sec.api_type())))
        }
    }

    /// `USE CATALOG` + `USE SCHEMA` on the ancestors of a schema-level object,
    /// then `privilege` on the object itself.
    pub async fn require_on_object(&mut self, sec: Securable, full_name: &str, privilege: &str) -> ApiResult<()> {
        self.require_use_path(full_name).await?;
        self.require(sec, full_name, privilege).await
    }

    /// `USE CATALOG` on the catalog and `USE SCHEMA` on the schema of `full_name`
    /// (whichever parts are present).
    pub async fn require_use_path(&mut self, full_name: &str) -> ApiResult<()> {
        let parts: Vec<&str> = full_name.split('.').collect();
        if let Some(c) = parts.first() {
            self.require(Securable::Catalog, c, "USE_CATALOG").await?;
        }
        if parts.len() >= 3 {
            self.require(Securable::Schema, &format!("{}.{}", parts[0], parts[1]), "USE_SCHEMA").await?;
        }
        Ok(())
    }

    /// Can the principal see the object in listings / `GET`? Owners, any
    /// direct or inherited privilege, or a grant on any descendant qualify
    /// (so a user with SELECT on one table can browse to it).
    pub async fn can_browse(&mut self, sec: Securable, full_name: &str) -> ApiResult<bool> {
        if self.has_any_privilege(sec, full_name).await? {
            return Ok(true);
        }
        if matches!(sec, Securable::Catalog | Securable::Schema) {
            return self.has_grant_below(full_name).await;
        }
        Ok(false)
    }

    /// Any grant to the principal on a securable whose name starts with `prefix.`.
    async fn has_grant_below(&mut self, prefix: &str) -> ApiResult<bool> {
        let all = self.st.all_grants().await?;
        let want = format!("{prefix}.");
        for (key, assignments) in all {
            let Some((_, name)) = key.split_once(':') else { continue };
            if name.starts_with(&want) && assignments.iter().any(|a| principal_matches(self.principal, &a.principal) && !a.privileges.is_empty()) {
                return Ok(true);
            }
        }
        // ownership of a descendant also makes the parent browsable
        for kind in [KIND_SCHEMA, KIND_TABLE, KIND_VOLUME, KIND_FUNCTION] {
            let docs: Vec<crate::store::Doc<Value>> = self.st.store.list(kind, self.st.ws(), crate::store::Filter { name_prefix: Some(&want), ..Default::default() }).await?;
            if docs.iter().any(|d| d.data["owner"].as_str().map(|o| principal_matches_owner(self.principal, o)).unwrap_or(false)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Effective privileges on a securable: direct grants plus everything
    /// inherited from ancestors, in the `effective-permissions` shape.
    pub async fn effective(&mut self, sec: Securable, full_name: &str, only_principal: Option<&str>) -> ApiResult<Value> {
        let mut per_principal: Vec<(String, Vec<Value>)> = vec![];
        for (i, (s, n)) in lineage_of(sec, full_name).into_iter().enumerate() {
            for a in self.grants(s, &n).await? {
                if let Some(p) = only_principal {
                    if !a.principal.eq_ignore_ascii_case(p) {
                        continue;
                    }
                }
                let entry = match per_principal.iter_mut().find(|(p, _)| *p == a.principal) {
                    Some(e) => e,
                    None => {
                        per_principal.push((a.principal.clone(), vec![]));
                        per_principal.last_mut().expect("just pushed")
                    }
                };
                for priv_ in a.privileges {
                    if entry.1.iter().any(|v| v["privilege"] == priv_) {
                        continue;
                    }
                    let mut v = json!({ "privilege": priv_ });
                    if i > 0 {
                        v["inherited_from_type"] = json!(s.api_type());
                        v["inherited_from_name"] = json!(n);
                    }
                    entry.1.push(v);
                }
            }
        }
        Ok(json!({ "privilege_assignments": per_principal.into_iter().map(|(p, privs)| json!({ "principal": p, "privileges": privs })).collect::<Vec<_>>() }))
    }
}

fn principal_matches_owner(p: &Principal, owner: &str) -> bool {
    principal_matches(p, owner)
}

pub fn denied(p: &Principal, privilege: &str, sec: Securable, full_name: &str) -> ApiError {
    let sec_name = match sec {
        Securable::Metastore => "Metastore".to_string(),
        other => {
            let t = other.api_type();
            let mut c = t.chars();
            match c.next() {
                Some(f) => f.to_uppercase().collect::<String>() + &c.as_str().to_ascii_lowercase().replace('_', " "),
                None => t.to_string(),
            }
        }
    };
    ApiError::PermissionDenied(format!("[INSUFFICIENT_PERMISSIONS] User {} does not have {} on {sec_name} '{full_name}'.", p.user_name, normalize_privilege(privilege).replace('_', " ")))
}

impl AppState {
    /// Every grants document as `(key, assignments)`.
    pub async fn all_grants(&self) -> ApiResult<Vec<(String, Vec<Assignment>)>> {
        let docs: Vec<crate::store::Doc<Value>> = self.store.list(KIND_GRANTS, self.ws(), crate::store::Filter::default()).await?;
        Ok(docs.into_iter().map(|d| (d.name.unwrap_or_default(), parse_assignments(&d.data))).collect())
    }

    pub async fn grants_for(&self, sec: Securable, full_name: &str) -> ApiResult<Vec<Assignment>> {
        Ok(self.store.get::<Value>(KIND_GRANTS, &grants_doc_id(sec, full_name)).await?.map(|d| parse_assignments(&d.data)).unwrap_or_default())
    }

    /// Apply `add`/`remove` privilege changes for `principal` on a securable.
    pub async fn change_grants(&self, sec: Securable, full_name: &str, principal: &str, add: &[String], remove: &[String]) -> ApiResult<Vec<Assignment>> {
        let mut assignments = self.grants_for(sec, full_name).await?;
        let idx = match assignments.iter().position(|a| a.principal.eq_ignore_ascii_case(principal)) {
            Some(i) => i,
            None => {
                assignments.push(Assignment { principal: principal.to_string(), privileges: vec![] });
                assignments.len() - 1
            }
        };
        for a in add {
            let a = normalize_privilege(a);
            if !is_known_privilege(&a) {
                return Err(ApiError::invalid(format!("Privilege {a} is not supported")));
            }
            if !assignments[idx].privileges.contains(&a) {
                assignments[idx].privileges.push(a);
            }
        }
        for r in remove {
            let r = normalize_privilege(r);
            if r == "ALL_PRIVILEGES" {
                assignments[idx].privileges.clear();
            } else {
                assignments[idx].privileges.retain(|p| *p != r);
            }
        }
        assignments.retain(|a| !a.privileges.is_empty());
        let key = grants_key(sec, full_name);
        let v = assignments_json(&assignments);
        if assignments.is_empty() {
            self.store.delete(KIND_GRANTS, &grants_doc_id(sec, full_name)).await?;
        } else {
            self.store.upsert(KIND_GRANTS, self.ws(), &grants_doc_id(sec, full_name), None, Some(&key), &v).await?;
        }
        Ok(assignments)
    }

    /// Drop all grants on a securable (and, for containers, on descendants).
    pub async fn drop_grants(&self, sec: Securable, full_name: &str) -> ApiResult<()> {
        self.store.delete(KIND_GRANTS, &grants_doc_id(sec, full_name)).await?;
        if matches!(sec, Securable::Catalog | Securable::Schema) {
            let want = format!("{full_name}.");
            for (key, _) in self.all_grants().await? {
                if let Some((t, name)) = key.split_once(':') {
                    if name.starts_with(&want) {
                        if let Ok(s) = Securable::parse(t) {
                            self.store.delete(KIND_GRANTS, &grants_doc_id(s, name)).await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Rename grants when a securable is renamed.
    pub async fn move_grants(&self, sec: Securable, from: &str, to: &str) -> ApiResult<()> {
        let a = self.grants_for(sec, from).await?;
        if a.is_empty() {
            return Ok(());
        }
        self.store.delete(KIND_GRANTS, &grants_doc_id(sec, from)).await?;
        let key = grants_key(sec, to);
        self.store.upsert(KIND_GRANTS, self.ws(), &grants_doc_id(sec, to), None, Some(&key), &assignments_json(&a)).await?;
        Ok(())
    }

    /// Databricks-style defaults for the workspace catalog: every workspace
    /// user can use `main`, browse it, and create/read/write in `main.default`.
    pub async fn ensure_default_grants(&self) -> ApiResult<()> {
        if self.store.kv_get("uc_default_grants_v1").await?.is_some() {
            return Ok(());
        }
        let cat = crate::api::catalog::DEFAULT_CATALOG;
        let want_cat = ["USE_CATALOG", "USE_SCHEMA", "BROWSE", "CREATE_SCHEMA"].map(String::from);
        let want_sch = ["CREATE_TABLE", "CREATE_VOLUME", "CREATE_FUNCTION", "CREATE_MODEL", "SELECT", "MODIFY", "READ_VOLUME", "WRITE_VOLUME", "EXECUTE"].map(String::from);
        self.change_grants(Securable::Catalog, cat, ACCOUNT_USERS, &want_cat, &[]).await?;
        self.change_grants(Securable::Schema, &format!("{cat}.default"), ACCOUNT_USERS, &want_sch, &[]).await?;
        self.store.kv_set("uc_default_grants_v1", "1").await?;
        Ok(())
    }

    /// Validate that a principal name refers to an existing user or group
    /// (or the well-known everyone group).
    pub async fn principal_exists(&self, name: &str) -> ApiResult<bool> {
        if name.eq_ignore_ascii_case(ACCOUNT_USERS) || name.eq_ignore_ascii_case(USERS_GROUP) {
            return Ok(true);
        }
        if self.user_by_name(name).await?.is_some() {
            return Ok(true);
        }
        let groups: Vec<crate::store::Doc<crate::auth::Group>> = self.store.list(crate::auth::KIND_GROUP, self.ws(), crate::store::Filter::default()).await?;
        Ok(groups.iter().any(|g| g.data.display_name.eq_ignore_ascii_case(name)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lineage_walks_to_metastore() {
        let l = lineage_of(Securable::Table, "main.default.t");
        assert_eq!(l.len(), 4);
        assert_eq!(l[1], (Securable::Schema, "main.default".to_string()));
        assert_eq!(l[2], (Securable::Catalog, "main".to_string()));
        assert_eq!(l[3].0, Securable::Metastore);
        assert_eq!(lineage_of(Securable::ExternalLocation, "loc").len(), 2);
    }

    #[test]
    fn principal_matching() {
        let p = Principal { user_id: "u1".into(), user_name: "a@b.c".into(), is_admin: false, groups: vec!["analysts".into()] };
        assert!(principal_matches(&p, "A@B.C"));
        assert!(principal_matches(&p, "analysts"));
        assert!(principal_matches(&p, ACCOUNT_USERS));
        assert!(!principal_matches(&p, "eng"));
        assert!(principal_matches_owner(&p, "analysts"));
    }

    #[test]
    fn privilege_normalisation() {
        assert_eq!(normalize_privilege("use catalog"), "USE_CATALOG");
        assert_eq!(normalize_privilege(" all privileges "), "ALL_PRIVILEGES");
        assert!(is_known_privilege("SELECT"));
        assert!(!is_known_privilege("FLY"));
    }
}
