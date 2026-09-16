//! Lakebase (`/api/2.0/database/*`): database instances, roles, temporary
//! credentials, database catalogs, synced tables and database tables in the
//! shapes of the Databricks Database API.
//!
//! Backends:
//! * `emulated` (default) — instances, roles, catalogs and synced tables are
//!   persisted control-plane metadata only; there is no reachable Postgres
//!   endpoint. `read_write_dns` is a placeholder host and credentials are
//!   short-lived Lakeforge tokens.
//! * `external` — `--lakebase-postgres-url` points at a PostgreSQL server; the
//!   instance advertises that host as `read_write_dns`. Provisioning roles and
//!   databases on that server and running synced-table pipelines are not
//!   implemented (see docs/issues).

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::catalog::{id_of, split_name, KIND_CATALOG, KIND_TABLE};
use super::{empty, Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};
use crate::uc::privileges::{Authorizer, Securable};

pub const KIND_INSTANCE: &str = "lakebase_instance";
pub const KIND_ROLE: &str = "lakebase_role";
pub const KIND_CATALOG_LINK: &str = "lakebase_catalog";
pub const KIND_SYNCED_TABLE: &str = "lakebase_synced_table";
pub const KIND_DATABASE_TABLE: &str = "lakebase_table";

pub const CAPACITIES: &[&str] = &["CU_1", "CU_2", "CU_4", "CU_8"];
pub const PG_VERSION: &str = "PG_VERSION_16";
pub const STARTING_SECS: i64 = 3;
pub const CREDENTIAL_TTL_SECS: i64 = 3600;
pub const SCHEDULING_POLICIES: &[&str] = &["SNAPSHOT", "TRIGGERED", "CONTINUOUS"];

pub const BACKEND_EMULATED: &str = "emulated";
pub const BACKEND_EXTERNAL: &str = "external";

fn ts(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms).map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)).unwrap_or_default()
}

fn ts_ms(s: &Value) -> Option<i64> {
    s.as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|d| d.timestamp_millis())
}

fn validate_instance_name(name: &str) -> ApiResult<()> {
    let ok = !name.is_empty() && name.len() <= 63 && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-') && !name.starts_with('-') && !name.ends_with('-');
    if !ok {
        return Err(ApiError::invalid(format!("Invalid instance name '{name}': use 1-63 lowercase letters, digits and hyphens")));
    }
    Ok(())
}

pub struct Backend {
    pub kind: &'static str,
    pub host: String,
    pub port: u16,
}

impl AppState {
    pub fn lakebase_backend(&self) -> Backend {
        match self.config.lakebase_postgres_url.as_deref().and_then(|u| url::Url::parse(u).ok()) {
            Some(u) => Backend { kind: BACKEND_EXTERNAL, host: u.host_str().unwrap_or("localhost").to_string(), port: u.port().unwrap_or(5432) },
            None => Backend { kind: BACKEND_EMULATED, host: "lakebase.invalid".into(), port: 5432 },
        }
    }

    fn instance_dns(&self, name: &str, read_only: bool) -> String {
        let b = self.lakebase_backend();
        match b.kind {
            BACKEND_EXTERNAL => b.host,
            _ => format!("{name}{}.{}", if read_only { "-ro" } else { "" }, b.host),
        }
    }

    /// Apply time-based lifecycle transitions (STARTING → AVAILABLE,
    /// UPDATING → AVAILABLE, DELETING → gone) and persist them.
    async fn settle_instance(&self, mut doc: Doc<Value>) -> ApiResult<Option<Doc<Value>>> {
        let now = now_ms();
        let since = ts_ms(&doc.data["updated_time"]).unwrap_or(now);
        let state = doc.data["state"].as_str().unwrap_or("").to_string();
        let elapsed = now - since >= STARTING_SECS * 1000;
        let next = match state.as_str() {
            "STARTING" | "UPDATING" | "FAILING_OVER" if elapsed => Some(if doc.data["stopped"].as_bool().unwrap_or(false) { "STOPPED" } else { "AVAILABLE" }),
            "DELETING" if elapsed => None,
            _ => Some(state.as_str()),
        };
        match next {
            None => {
                self.store.delete(KIND_INSTANCE, &doc.id).await?;
                self.store.delete_children(KIND_ROLE, &doc.id).await?;
                Ok(None)
            }
            Some(s) if s != state => {
                doc.data["state"] = json!(s);
                doc.data["effective_stopped"] = json!(s == "STOPPED");
                self.store.upsert(KIND_INSTANCE, self.ws(), &doc.id, None, doc.name.as_deref(), &doc.data).await?;
                Ok(Some(doc))
            }
            Some(_) => Ok(Some(doc)),
        }
    }

    pub async fn lakebase_instance(&self, name: &str) -> ApiResult<Value> {
        let doc = self.store.require::<Value>(KIND_INSTANCE, &id_of(KIND_INSTANCE, name), "Database instance").await?;
        match self.settle_instance(doc).await? {
            Some(d) => Ok(d.data),
            None => Err(ApiError::NotFound(format!("Database instance {name} does not exist."))),
        }
    }

    pub async fn lakebase_instances(&self) -> ApiResult<Vec<Value>> {
        let docs: Vec<Doc<Value>> = self.store.list(KIND_INSTANCE, self.ws(), Filter::default()).await?;
        let mut out = vec![];
        for d in docs {
            if let Some(d) = self.settle_instance(d).await? {
                out.push(d.data);
            }
        }
        Ok(out)
    }

    async fn require_instance_owner(&self, p: &Principal, inst: &Value) -> ApiResult<()> {
        if p.is_admin || inst["creator"] == p.user_name {
            return Ok(());
        }
        Err(ApiError::PermissionDenied(format!("User does not own database instance '{}'", inst["name"].as_str().unwrap_or_default())))
    }

    pub async fn lakebase_create_instance(&self, p: &Principal, o: Map<String, Value>) -> ApiResult<Value> {
        let name = o.get("name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("name is required"))?.to_string();
        validate_instance_name(&name)?;
        if self.store.get::<Value>(KIND_INSTANCE, &id_of(KIND_INSTANCE, &name)).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("Database instance '{name}' already exists")));
        }
        let capacity = o.get("capacity").and_then(|v| v.as_str()).unwrap_or("CU_1").to_string();
        if !CAPACITIES.contains(&capacity.as_str()) {
            return Err(ApiError::invalid(format!("capacity must be one of {}", CAPACITIES.join(", "))));
        }
        let node_count = o.get("node_count").and_then(|v| v.as_i64()).unwrap_or(1);
        if !(1..=4).contains(&node_count) {
            return Err(ApiError::invalid("node_count must be between 1 and 4"));
        }
        let readable_secondaries = o.get("enable_readable_secondaries").and_then(|v| v.as_bool()).unwrap_or(node_count > 1);
        let retention = o.get("retention_window_in_days").and_then(|v| v.as_i64()).unwrap_or(7);
        if !(2..=35).contains(&retention) {
            return Err(ApiError::invalid("retention_window_in_days must be between 2 and 35"));
        }
        let parent = match o.get("parent_instance_ref") {
            Some(Value::Object(r)) => {
                let pname = r.get("name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("parent_instance_ref.name is required"))?;
                let parent = self.lakebase_instance(pname).await?;
                let mut r = r.clone();
                r.insert("uid".into(), parent["uid"].clone());
                r.entry("branch_time").or_insert(json!(ts(now_ms())));
                r.entry("lsn").or_insert(Value::Null);
                Some(Value::Object(r))
            }
            None | Some(Value::Null) => None,
            Some(_) => return Err(ApiError::invalid("parent_instance_ref must be an object")),
        };
        let now = now_ms();
        let uid = uuid::Uuid::new_v4().to_string();
        let backend = self.lakebase_backend();
        let stopped = o.get("stopped").and_then(|v| v.as_bool()).unwrap_or(false);
        let doc = json!({
            "name": name,
            "uid": uid,
            "creator": p.user_name,
            "state": "STARTING",
            "stopped": stopped,
            "effective_stopped": stopped,
            "capacity": capacity,
            "effective_capacity": capacity,
            "node_count": node_count,
            "effective_node_count": node_count,
            "enable_readable_secondaries": readable_secondaries,
            "effective_enable_readable_secondaries": readable_secondaries,
            "retention_window_in_days": retention,
            "effective_retention_window_in_days": retention,
            "enable_pg_native_login": o.get("enable_pg_native_login").and_then(|v| v.as_bool()).unwrap_or(false),
            "effective_enable_pg_native_login": o.get("enable_pg_native_login").and_then(|v| v.as_bool()).unwrap_or(false),
            "usage_policy_id": o.get("usage_policy_id").cloned().unwrap_or(Value::Null),
            "effective_usage_policy_id": o.get("usage_policy_id").cloned().unwrap_or(Value::Null),
            "custom_tags": o.get("custom_tags").cloned().unwrap_or_else(|| json!([])),
            "effective_custom_tags": o.get("custom_tags").cloned().unwrap_or_else(|| json!([])),
            "pg_version": PG_VERSION,
            "read_write_dns": self.instance_dns(&name, false),
            "read_only_dns": if readable_secondaries { json!(self.instance_dns(&name, true)) } else { Value::Null },
            "port": backend.port,
            "creation_time": ts(now),
            "updated_time": ts(now),
            "parent_instance_ref": parent,
            "child_instance_refs": [],
            "backend": { "kind": backend.kind, "host": backend.host, "emulated": backend.kind == BACKEND_EMULATED },
        });
        self.store.insert(KIND_INSTANCE, self.ws(), &id_of(KIND_INSTANCE, &name), None, Some(&name), &doc).await?;
        if let Some(pname) = doc["parent_instance_ref"]["name"].as_str() {
            let child = json!({ "name": name, "uid": uid, "branch_time": doc["parent_instance_ref"]["branch_time"] });
            let _ = self
                .store
                .update::<Value, _>(KIND_INSTANCE, &id_of(KIND_INSTANCE, pname), "Database instance", |v| {
                    let mut kids = v["child_instance_refs"].as_array().cloned().unwrap_or_default();
                    kids.push(child);
                    v["child_instance_refs"] = Value::Array(kids);
                    Ok(())
                })
                .await;
        }
        self.audit(p, "database", "createDatabaseInstance", json!({ "name": name, "capacity": doc["capacity"], "backend": doc["backend"]["kind"] }), 200, None).await;
        Ok(doc)
    }

    pub async fn lakebase_update_instance(&self, p: &Principal, name: &str, o: Map<String, Value>, mask: Option<&str>) -> ApiResult<Value> {
        let inst = self.lakebase_instance(name).await?;
        self.require_instance_owner(p, &inst).await?;
        if inst["state"] == "DELETING" {
            return Err(ApiError::InvalidState(format!("Database instance '{name}' is being deleted")));
        }
        let allowed: Vec<&str> = match mask {
            Some(m) => m.split(',').map(str::trim).filter(|s| !s.is_empty()).collect(),
            None => vec!["capacity", "node_count", "stopped", "enable_readable_secondaries", "retention_window_in_days", "enable_pg_native_login", "usage_policy_id", "custom_tags"],
        };
        let doc = self
            .store
            .update::<Value, _>(KIND_INSTANCE, &id_of(KIND_INSTANCE, name), "Database instance", |v| {
                let mut changed_compute = false;
                for k in &allowed {
                    let Some(val) = o.get(*k) else { continue };
                    match *k {
                        "capacity" => {
                            let c = val.as_str().unwrap_or_default();
                            if !CAPACITIES.contains(&c) {
                                return Err(ApiError::invalid(format!("capacity must be one of {}", CAPACITIES.join(", "))));
                            }
                            changed_compute = true;
                        }
                        "node_count" => {
                            if !(1..=4).contains(&val.as_i64().unwrap_or(0)) {
                                return Err(ApiError::invalid("node_count must be between 1 and 4"));
                            }
                            changed_compute = true;
                        }
                        "retention_window_in_days" => {
                            if !(2..=35).contains(&val.as_i64().unwrap_or(0)) {
                                return Err(ApiError::invalid("retention_window_in_days must be between 2 and 35"));
                            }
                        }
                        "stopped" => changed_compute = true,
                        _ => {}
                    }
                    v[*k] = val.clone();
                    v[format!("effective_{k}")] = val.clone();
                }
                if changed_compute {
                    v["state"] = json!("UPDATING");
                }
                if v["enable_readable_secondaries"].as_bool().unwrap_or(false) && v["read_only_dns"].is_null() {
                    v["read_only_dns"] = json!(format!("{name}-ro.{}", self.lakebase_backend().host));
                }
                v["updated_time"] = json!(ts(now_ms()));
                Ok(())
            })
            .await?;
        self.audit(p, "database", "updateDatabaseInstance", json!({ "name": name, "update_mask": mask }), 200, None).await;
        Ok(doc.data)
    }

    pub async fn lakebase_delete_instance(&self, p: &Principal, name: &str, force: bool, purge: bool) -> ApiResult<()> {
        let inst = self.lakebase_instance(name).await?;
        self.require_instance_owner(p, &inst).await?;
        let kids = inst["child_instance_refs"].as_array().map(|a| a.len()).unwrap_or(0);
        if kids > 0 && !force {
            return Err(ApiError::InvalidState(format!("Database instance '{name}' has {kids} child instance(s); pass force=true")));
        }
        let links: Vec<Doc<Value>> = self.store.list(KIND_CATALOG_LINK, self.ws(), Filter::default()).await?;
        let dependent: Vec<String> = links.into_iter().filter(|l| l.data["database_instance_name"] == name).filter_map(|l| l.name).collect();
        if !dependent.is_empty() && !force {
            return Err(ApiError::InvalidState(format!("Database instance '{name}' backs catalog(s) {}; pass force=true", dependent.join(", "))));
        }
        for c in dependent {
            let _ = self.lakebase_delete_catalog(p, &c).await;
        }
        if force {
            for k in inst["child_instance_refs"].as_array().into_iter().flatten() {
                if let Some(child) = k["name"].as_str() {
                    let _ = self.store.delete(KIND_INSTANCE, &id_of(KIND_INSTANCE, child)).await;
                }
            }
        }
        if purge {
            self.store.delete(KIND_INSTANCE, &id_of(KIND_INSTANCE, name)).await?;
            self.store.delete_children(KIND_ROLE, &id_of(KIND_INSTANCE, name)).await?;
        } else {
            self.store
                .update::<Value, _>(KIND_INSTANCE, &id_of(KIND_INSTANCE, name), "Database instance", |v| {
                    v["state"] = json!("DELETING");
                    v["updated_time"] = json!(ts(now_ms()));
                    Ok(())
                })
                .await?;
        }
        self.audit(p, "database", "deleteDatabaseInstance", json!({ "name": name, "force": force, "purge": purge }), 200, None).await;
        Ok(())
    }

    // ------------------------------------------------------------ roles

    pub async fn lakebase_roles(&self, instance: &str) -> ApiResult<Vec<Value>> {
        self.lakebase_instance(instance).await?;
        let docs: Vec<Doc<Value>> = self.store.list(KIND_ROLE, self.ws(), Filter { parent_id: Some(&id_of(KIND_INSTANCE, instance)), ..Default::default() }).await?;
        Ok(docs.into_iter().map(|d| d.data).collect())
    }

    pub async fn lakebase_create_role(&self, p: &Principal, instance: &str, o: Map<String, Value>) -> ApiResult<Value> {
        let inst = self.lakebase_instance(instance).await?;
        self.require_instance_owner(p, &inst).await?;
        let name = o.get("name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("name is required"))?.to_string();
        if name.is_empty() || name.contains(char::is_whitespace) {
            return Err(ApiError::invalid("role name must be non-empty without whitespace"));
        }
        let identity_type = o.get("identity_type").and_then(|v| v.as_str()).unwrap_or("USER").to_string();
        if !matches!(identity_type.as_str(), "USER" | "GROUP" | "SERVICE_PRINCIPAL" | "PG_ONLY") {
            return Err(ApiError::invalid("identity_type must be USER, GROUP, SERVICE_PRINCIPAL or PG_ONLY"));
        }
        if identity_type != "PG_ONLY" && !self.principal_exists(&name).await? {
            return Err(ApiError::NotFound(format!("Principal '{name}' does not exist")));
        }
        let membership = o.get("membership_role").and_then(|v| v.as_str()).unwrap_or("DATABRICKS_SUPERUSER").to_string();
        let id = format!("{}:{name}", id_of(KIND_ROLE, instance));
        if self.store.get::<Value>(KIND_ROLE, &id).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("Role '{name}' already exists on '{instance}'")));
        }
        let doc = json!({ "name": name, "identity_type": identity_type, "membership_role": membership, "attributes": o.get("attributes").cloned().unwrap_or_else(|| json!({ "createdb": false, "createrole": false, "bypassrls": false })), "instance_name": instance, "created_time": ts(now_ms()), "created_by": p.user_name });
        self.store.insert(KIND_ROLE, self.ws(), &id, Some(&id_of(KIND_INSTANCE, instance)), Some(&name), &doc).await?;
        Ok(doc)
    }

    pub async fn lakebase_role(&self, instance: &str, name: &str) -> ApiResult<Value> {
        self.lakebase_instance(instance).await?;
        Ok(self.store.require::<Value>(KIND_ROLE, &format!("{}:{name}", id_of(KIND_ROLE, instance)), "Database instance role").await?.data)
    }

    pub async fn lakebase_delete_role(&self, p: &Principal, instance: &str, name: &str) -> ApiResult<()> {
        let inst = self.lakebase_instance(instance).await?;
        self.require_instance_owner(p, &inst).await?;
        if !self.store.delete(KIND_ROLE, &format!("{}:{name}", id_of(KIND_ROLE, instance))).await? {
            return Err(ApiError::NotFound(format!("Role '{name}' does not exist on '{instance}'")));
        }
        Ok(())
    }

    // ------------------------------------------------------------ credentials

    /// Short-lived credential for the named instances. The token is a
    /// Lakeforge PAT (usable against this API); it is not a Postgres password
    /// unless an external backend accepts Lakeforge tokens.
    pub async fn lakebase_credential(&self, p: &Principal, instance_names: &[String], request_id: Option<&str>) -> ApiResult<Value> {
        if instance_names.is_empty() {
            return Err(ApiError::invalid("instance_names must contain at least one instance"));
        }
        for n in instance_names {
            let inst = self.lakebase_instance(n).await?;
            if inst["state"] != "AVAILABLE" && inst["state"] != "STARTING" && inst["state"] != "UPDATING" {
                return Err(ApiError::InvalidState(format!("Database instance '{n}' is {}", inst["state"].as_str().unwrap_or("unavailable"))));
            }
            let has_role = self.lakebase_roles(n).await?.iter().any(|r| r["name"] == p.user_name || p.groups.iter().any(|g| r["name"] == *g));
            if !(p.is_admin || inst["creator"] == p.user_name || has_role) {
                return Err(ApiError::PermissionDenied(format!("User has no role on database instance '{n}'")));
            }
        }
        let (value, _) = self.create_token(p, &format!("lakebase credential for {}", instance_names.join(",")), Some(CREDENTIAL_TTL_SECS)).await?;
        let expiration = now_ms() + CREDENTIAL_TTL_SECS * 1000;
        self.audit(p, "database", "generateDatabaseCredential", json!({ "instance_names": instance_names, "request_id": request_id }), 200, None).await;
        Ok(json!({ "token": value, "expiration_time": ts(expiration), "request_id": request_id, "backend": self.lakebase_backend().kind }))
    }

    // ------------------------------------------------------------ database catalogs

    pub async fn lakebase_create_catalog(&self, p: &Principal, o: Map<String, Value>) -> ApiResult<Value> {
        let name = o.get("name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("name is required"))?.to_string();
        let instance = o.get("database_instance_name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("database_instance_name is required"))?.to_string();
        let database = o.get("database_name").and_then(|v| v.as_str()).unwrap_or("databricks_postgres").to_string();
        let inst = self.lakebase_instance(&instance).await?;
        self.require_instance_owner(p, &inst).await?;
        if self.store.get::<Value>(KIND_CATALOG_LINK, &id_of(KIND_CATALOG_LINK, &name)).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("Database catalog '{name}' already exists")));
        }
        let mut c = Map::new();
        c.insert("name".into(), json!(name));
        c.insert("catalog_type".into(), json!("MANAGED_CATALOG"));
        c.insert("comment".into(), json!(format!("Lakebase database '{database}' on instance '{instance}'")));
        c.insert("properties".into(), json!({ "lakebase.instance": instance, "lakebase.database": database }));
        c.insert("options".into(), json!({ "database_instance_name": instance, "database_name": database }));
        let cat = self.uc_create_catalog(p, c).await?;
        let _ = self
            .store
            .update::<Value, _>(KIND_CATALOG, &id_of(KIND_CATALOG, &name), "Catalog", |v| {
                v["securable_kind"] = json!("CATALOG_DATABASE");
                Ok(())
            })
            .await;
        let doc = json!({ "name": name, "database_instance_name": instance, "database_name": database, "create_database_if_not_exists": o.get("create_database_if_not_exists").and_then(|v| v.as_bool()).unwrap_or(true), "uid": uuid::Uuid::new_v4().to_string(), "creator": p.user_name, "created_time": ts(now_ms()), "backend": inst["backend"]["kind"], "catalog": cat });
        self.store.insert(KIND_CATALOG_LINK, self.ws(), &id_of(KIND_CATALOG_LINK, &name), Some(&id_of(KIND_INSTANCE, &instance)), Some(&name), &doc).await?;
        Ok(doc)
    }

    pub async fn lakebase_catalog(&self, name: &str) -> ApiResult<Value> {
        Ok(self.store.require::<Value>(KIND_CATALOG_LINK, &id_of(KIND_CATALOG_LINK, name), "Database catalog").await?.data)
    }

    pub async fn lakebase_catalogs(&self) -> ApiResult<Vec<Value>> {
        let docs: Vec<Doc<Value>> = self.store.list(KIND_CATALOG_LINK, self.ws(), Filter::default()).await?;
        Ok(docs.into_iter().map(|d| d.data).collect())
    }

    pub async fn lakebase_delete_catalog(&self, p: &Principal, name: &str) -> ApiResult<()> {
        self.lakebase_catalog(name).await?;
        if self.uc_get(KIND_CATALOG, name).await?.is_some() {
            self.uc_delete_catalog(p, name, true).await?;
        }
        self.store.delete(KIND_CATALOG_LINK, &id_of(KIND_CATALOG_LINK, name)).await?;
        Ok(())
    }

    // ------------------------------------------------------------ synced tables

    pub async fn lakebase_create_synced_table(&self, p: &Principal, o: Map<String, Value>) -> ApiResult<Value> {
        let name = o.get("name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("name is required"))?.to_string();
        let (cat, sch, tbl) = split_name(&name);
        if name.split('.').count() != 3 {
            return Err(ApiError::invalid("name must be a three-level catalog.schema.table"));
        }
        let spec = o.get("spec").and_then(|v| v.as_object()).ok_or_else(|| ApiError::invalid("spec is required"))?;
        let source = spec.get("source_table_full_name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("spec.source_table_full_name is required"))?.to_string();
        let policy = spec.get("scheduling_policy").and_then(|v| v.as_str()).unwrap_or("SNAPSHOT").to_string();
        if !SCHEDULING_POLICIES.contains(&policy.as_str()) {
            return Err(ApiError::invalid(format!("spec.scheduling_policy must be one of {}", SCHEDULING_POLICIES.join(", "))));
        }
        let pks: Vec<String> = spec.get("primary_key_columns").and_then(|v| v.as_array()).into_iter().flatten().filter_map(|v| v.as_str().map(str::to_string)).collect();
        if pks.is_empty() {
            return Err(ApiError::invalid("spec.primary_key_columns must contain at least one column"));
        }
        if spec.contains_key("existing_pipeline_id") && spec.contains_key("new_pipeline_spec") {
            return Err(ApiError::invalid("only one of spec.existing_pipeline_id and spec.new_pipeline_spec may be set"));
        }
        let src = self.uc_require(KIND_TABLE, &source, "Source table").await?;
        let mut az = Authorizer::new(self, p);
        az.require_on_object(Securable::Table, &source, "SELECT").await?;
        let cols: Vec<String> = src["columns"].as_array().into_iter().flatten().filter_map(|c| c["name"].as_str().map(str::to_string)).collect();
        if !cols.is_empty() {
            for k in &pks {
                if !cols.iter().any(|c| c.eq_ignore_ascii_case(k)) {
                    return Err(ApiError::invalid(format!("primary key column '{k}' not found in {source}")));
                }
            }
            if let Some(tk) = spec.get("timeseries_key").and_then(|v| v.as_str()) {
                if !cols.iter().any(|c| c.eq_ignore_ascii_case(tk)) {
                    return Err(ApiError::invalid(format!("timeseries_key '{tk}' not found in {source}")));
                }
            }
        }
        let link = self.lakebase_catalog(&cat).await.map_err(|_| ApiError::invalid(format!("Catalog '{cat}' is not a Lakebase database catalog")))?;
        let instance = link["database_instance_name"].as_str().unwrap_or_default().to_string();
        let inst = self.lakebase_instance(&instance).await?;
        az.require_use_path(&name).await?;
        az.require(Securable::Schema, &format!("{cat}.{sch}"), "CREATE_TABLE").await?;
        if self.store.get::<Value>(KIND_SYNCED_TABLE, &id_of(KIND_SYNCED_TABLE, &name)).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("Synced table '{name}' already exists")));
        }
        let pipeline_id = spec.get("existing_pipeline_id").and_then(|v| v.as_str()).map(str::to_string).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let now = now_ms();
        let doc = json!({
            "name": name,
            "table_serving_url": format!("postgresql://{}:{}/{}", inst["read_write_dns"].as_str().unwrap_or_default(), inst["port"].as_i64().unwrap_or(5432), link["database_name"].as_str().unwrap_or_default()),
            "database_instance_name": instance,
            "effective_database_instance_name": instance,
            "logical_database_name": link["database_name"],
            "effective_logical_database_name": link["database_name"],
            "unity_catalog_provisioning_state": "ACTIVE",
            "spec": {
                "source_table_full_name": source,
                "scheduling_policy": policy,
                "primary_key_columns": pks,
                "timeseries_key": spec.get("timeseries_key").cloned().unwrap_or(Value::Null),
                "create_database_objects_if_missing": spec.get("create_database_objects_if_missing").and_then(|v| v.as_bool()).unwrap_or(true),
                "existing_pipeline_id": spec.get("existing_pipeline_id").cloned().unwrap_or(Value::Null),
                "new_pipeline_spec": spec.get("new_pipeline_spec").cloned().unwrap_or(Value::Null),
            },
            "data_synchronization_status": {
                "detailed_state": if policy == "SNAPSHOT" { "PROVISIONING_INITIAL_SNAPSHOT" } else { "PROVISIONING_PIPELINE_RESOURCES" },
                "message": format!("Synced table registered on {} backend; data movement pipelines are not implemented", inst["backend"]["kind"].as_str().unwrap_or(BACKEND_EMULATED)),
                "pipeline_id": pipeline_id,
                "synced_rows": 0,
                "last_sync": Value::Null,
                "provisioning_status": { "initial_pipeline_sync_progress": { "sync_progress_completion": 0.0, "synced_row_count": 0, "total_row_count": Value::Null } },
            },
            "creator": p.user_name,
            "created_time": ts(now),
            "updated_time": ts(now),
            "backend": inst["backend"]["kind"],
        });
        self.store.insert(KIND_SYNCED_TABLE, self.ws(), &id_of(KIND_SYNCED_TABLE, &name), Some(&id_of(KIND_INSTANCE, &instance)), Some(&name), &doc).await?;
        let mut t = Map::new();
        t.insert("name".into(), json!(tbl));
        t.insert("catalog_name".into(), json!(cat));
        t.insert("schema_name".into(), json!(sch));
        t.insert("full_name".into(), json!(name));
        t.insert("table_type".into(), json!("MANAGED"));
        t.insert("data_source_format".into(), json!("POSTGRESQL"));
        t.insert("securable_type".into(), json!("TABLE"));
        t.insert("columns".into(), src["columns"].clone());
        t.insert("properties".into(), json!({ "lakebase.synced_table": true, "lakebase.source_table": source, "lakebase.scheduling_policy": policy }));
        t.insert("comment".into(), json!(format!("Synced from {source} ({policy})")));
        if let Err(e) = self.upsert_table(t, &p.user_name).await {
            tracing::warn!(error = %e, "synced table UC mirror failed");
        }
        self.audit(p, "database", "createSyncedDatabaseTable", json!({ "name": name, "source": source, "scheduling_policy": policy }), 200, None).await;
        Ok(doc)
    }

    /// Advance a synced table's emulated lifecycle to the steady state for
    /// its policy on read.
    pub async fn lakebase_synced_table(&self, name: &str) -> ApiResult<Value> {
        let mut doc = self.store.require::<Value>(KIND_SYNCED_TABLE, &id_of(KIND_SYNCED_TABLE, name), "Synced table").await?;
        let created = ts_ms(&doc.data["created_time"]).unwrap_or(0);
        let state = doc.data["data_synchronization_status"]["detailed_state"].as_str().unwrap_or("").to_string();
        if state.starts_with("PROVISIONING") && now_ms() - created >= STARTING_SECS * 1000 {
            let policy = doc.data["spec"]["scheduling_policy"].as_str().unwrap_or("SNAPSHOT");
            let steady = match policy {
                "CONTINUOUS" => "ONLINE_CONTINUOUS_UPDATE",
                "TRIGGERED" => "ONLINE_TRIGGERED_UPDATE",
                _ => "ONLINE_NO_PENDING_UPDATE",
            };
            doc.data["data_synchronization_status"]["detailed_state"] = json!(steady);
            doc.data["data_synchronization_status"]["last_sync"] = json!({ "timestamp": ts(now_ms()), "delta_table_version": 0 });
            doc.data["updated_time"] = json!(ts(now_ms()));
            self.store.upsert(KIND_SYNCED_TABLE, self.ws(), &doc.id, doc.parent_id.as_deref(), doc.name.as_deref(), &doc.data).await?;
        }
        Ok(doc.data)
    }

    pub async fn lakebase_synced_tables(&self) -> ApiResult<Vec<Value>> {
        let docs: Vec<Doc<Value>> = self.store.list(KIND_SYNCED_TABLE, self.ws(), Filter::default()).await?;
        let mut out = vec![];
        for d in docs {
            if let Some(n) = d.name.as_deref() {
                out.push(self.lakebase_synced_table(n).await?);
            }
        }
        Ok(out)
    }

    pub async fn lakebase_delete_synced_table(&self, p: &Principal, name: &str) -> ApiResult<()> {
        self.lakebase_synced_table(name).await?;
        let mut az = Authorizer::new(self, p);
        az.require_owner(Securable::Table, name).await?;
        self.store.delete(KIND_SYNCED_TABLE, &id_of(KIND_SYNCED_TABLE, name)).await?;
        if self.uc_get(KIND_TABLE, name).await?.is_some() {
            let _ = self.uc_delete_table(p, name).await;
        }
        self.audit(p, "database", "deleteSyncedDatabaseTable", json!({ "name": name }), 200, None).await;
        Ok(())
    }

    // ------------------------------------------------------------ database tables

    pub async fn lakebase_create_table(&self, p: &Principal, o: Map<String, Value>) -> ApiResult<Value> {
        let name = o.get("name").and_then(|v| v.as_str()).ok_or_else(|| ApiError::invalid("name is required"))?.to_string();
        if name.split('.').count() != 3 {
            return Err(ApiError::invalid("name must be a three-level catalog.schema.table"));
        }
        let (cat, sch, tbl) = split_name(&name);
        let link = self.lakebase_catalog(&cat).await.map_err(|_| ApiError::invalid(format!("Catalog '{cat}' is not a Lakebase database catalog")))?;
        let instance = o.get("database_instance_name").and_then(|v| v.as_str()).map(str::to_string).unwrap_or_else(|| link["database_instance_name"].as_str().unwrap_or_default().to_string());
        self.lakebase_instance(&instance).await?;
        let mut az = Authorizer::new(self, p);
        az.require_use_path(&name).await?;
        az.require(Securable::Schema, &format!("{cat}.{sch}"), "CREATE_TABLE").await?;
        if self.store.get::<Value>(KIND_DATABASE_TABLE, &id_of(KIND_DATABASE_TABLE, &name)).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("Database table '{name}' already exists")));
        }
        let doc = json!({ "name": name, "database_instance_name": instance, "logical_database_name": o.get("logical_database_name").cloned().unwrap_or(link["database_name"].clone()), "table_serving_url": format!("postgresql://{}/{}", self.instance_dns(&instance, false), link["database_name"].as_str().unwrap_or_default()), "creator": p.user_name, "created_time": ts(now_ms()) });
        self.store.insert(KIND_DATABASE_TABLE, self.ws(), &id_of(KIND_DATABASE_TABLE, &name), Some(&id_of(KIND_INSTANCE, &instance)), Some(&name), &doc).await?;
        let mut t = Map::new();
        t.insert("name".into(), json!(tbl));
        t.insert("catalog_name".into(), json!(cat));
        t.insert("schema_name".into(), json!(sch));
        t.insert("full_name".into(), json!(name));
        t.insert("table_type".into(), json!("EXTERNAL"));
        t.insert("data_source_format".into(), json!("POSTGRESQL"));
        t.insert("securable_type".into(), json!("TABLE"));
        t.insert("columns".into(), o.get("columns").cloned().unwrap_or_else(|| json!([])));
        t.insert("properties".into(), json!({ "lakebase.database_table": true }));
        if let Err(e) = self.upsert_table(t, &p.user_name).await {
            tracing::warn!(error = %e, "database table UC mirror failed");
        }
        Ok(doc)
    }

    pub async fn lakebase_table(&self, name: &str) -> ApiResult<Value> {
        Ok(self.store.require::<Value>(KIND_DATABASE_TABLE, &id_of(KIND_DATABASE_TABLE, name), "Database table").await?.data)
    }

    pub async fn lakebase_delete_table(&self, p: &Principal, name: &str) -> ApiResult<()> {
        self.lakebase_table(name).await?;
        let mut az = Authorizer::new(self, p);
        az.require_owner(Securable::Table, name).await?;
        self.store.delete(KIND_DATABASE_TABLE, &id_of(KIND_DATABASE_TABLE, name)).await?;
        if self.uc_get(KIND_TABLE, name).await?.is_some() {
            let _ = self.uc_delete_table(p, name).await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- handlers

#[derive(Debug, Deserialize, Default)]
struct PageQ {
    #[serde(default)]
    page_size: Option<usize>,
}

fn page(mut items: Vec<Value>, q: &PageQ, key: &str) -> Value {
    if let Some(n) = q.page_size {
        items.truncate(n.max(1));
    }
    json!({ key: items })
}

async fn list_instances(State(st): State<S>, Who(p): Who, Query(q): Query<PageQ>) -> ApiResult<Json<Value>> {
    let all = st.lakebase_instances().await?;
    let visible = if p.is_admin { all } else { all.into_iter().filter(|i| i["creator"] == p.user_name).collect() };
    Ok(Json(page(visible, &q, "database_instances")))
}

async fn create_instance(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_create_instance(&p, o).await?))
}

async fn get_instance(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_instance(&name).await?))
}

#[derive(Debug, Deserialize)]
struct UidQ {
    uid: String,
}

async fn find_by_uid(State(st): State<S>, Query(q): Query<UidQ>) -> ApiResult<Json<Value>> {
    st.lakebase_instances().await?.into_iter().find(|i| i["uid"] == q.uid).map(Json).ok_or_else(|| ApiError::NotFound(format!("Database instance with uid {} does not exist.", q.uid)))
}

#[derive(Debug, Deserialize, Default)]
struct MaskQ {
    update_mask: Option<String>,
}

async fn update_instance(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Query(q): Query<MaskQ>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_update_instance(&p, &name, o, q.update_mask.as_deref()).await?))
}

#[derive(Debug, Deserialize, Default)]
struct DeleteQ {
    #[serde(default)]
    force: bool,
    #[serde(default)]
    purge: bool,
}

async fn delete_instance(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Query(q): Query<DeleteQ>) -> ApiResult<Json<Value>> {
    st.lakebase_delete_instance(&p, &name, q.force, q.purge).await?;
    Ok(empty())
}

async fn list_roles(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "database_instance_roles": st.lakebase_roles(&name).await? })))
}

async fn create_role(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_create_role(&p, &name, o).await?))
}

async fn get_role(State(st): State<S>, Path((name, role)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_role(&name, &role).await?))
}

async fn delete_role(State(st): State<S>, Who(p): Who, Path((name, role)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    st.lakebase_delete_role(&p, &name, &role).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct CredentialBody {
    #[serde(default)]
    instance_names: Vec<String>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    claims: Vec<Value>,
}

async fn create_credential(State(st): State<S>, Who(p): Who, Body(b): Body<CredentialBody>) -> ApiResult<Json<Value>> {
    let mut names = b.instance_names;
    for c in &b.claims {
        if let Some(n) = c["instance_name"].as_str() {
            names.push(n.to_string());
        }
    }
    Ok(Json(st.lakebase_credential(&p, &names, b.request_id.as_deref()).await?))
}

async fn create_catalog(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_create_catalog(&p, o).await?))
}

async fn list_catalogs(State(st): State<S>) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "database_catalogs": st.lakebase_catalogs().await? })))
}

async fn get_catalog(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_catalog(&name).await?))
}

async fn delete_catalog(State(st): State<S>, Who(p): Who, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    st.lakebase_delete_catalog(&p, &name).await?;
    Ok(empty())
}

async fn create_synced_table(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_create_synced_table(&p, o).await?))
}

async fn list_synced_tables(State(st): State<S>) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "synced_tables": st.lakebase_synced_tables().await? })))
}

async fn get_synced_table(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_synced_table(&name).await?))
}

async fn delete_synced_table(State(st): State<S>, Who(p): Who, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    st.lakebase_delete_synced_table(&p, &name).await?;
    Ok(empty())
}

async fn create_table(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_create_table(&p, o).await?))
}

async fn get_table(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.lakebase_table(&name).await?))
}

async fn delete_table(State(st): State<S>, Who(p): Who, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    st.lakebase_delete_table(&p, &name).await?;
    Ok(empty())
}

async fn backend_info(State(st): State<S>) -> Json<Value> {
    let b = st.lakebase_backend();
    Json(json!({ "backend": b.kind, "host": b.host, "port": b.port, "emulated": b.kind == BACKEND_EMULATED, "pg_version": PG_VERSION, "capacities": CAPACITIES, "scheduling_policies": SCHEDULING_POLICIES }))
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/database/instances", get(list_instances).post(create_instance))
        .route("/api/2.0/database/instances:findByUid", get(find_by_uid))
        .route("/api/2.0/database/instances/{name}", get(get_instance).patch(update_instance).delete(delete_instance))
        .route("/api/2.0/database/instances/{name}/roles", get(list_roles).post(create_role))
        .route("/api/2.0/database/instances/{name}/roles/{role}", get(get_role).delete(delete_role))
        .route("/api/2.0/database/credentials", post(create_credential))
        .route("/api/2.0/database/catalogs", get(list_catalogs).post(create_catalog))
        .route("/api/2.0/database/catalogs/{name}", get(get_catalog).delete(delete_catalog))
        .route("/api/2.0/database/synced_tables", get(list_synced_tables).post(create_synced_table))
        .route("/api/2.0/database/synced_tables/{name}", get(get_synced_table).delete(delete_synced_table))
        .route("/api/2.0/database/tables", post(create_table))
        .route("/api/2.0/database/tables/{name}", get(get_table).delete(delete_table))
        .route("/api/2.0/lakeforge/lakebase/backend", get(backend_info))
}
