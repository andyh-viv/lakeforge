//! Unity-Catalog-compatible metastore (`/api/2.1/unity-catalog/*`).
//!
//! Securables are stored as JSON docs keyed by full name. Tables with a
//! storage location are pushed to every running Forge cluster so the engine's
//! `catalog.schema.table` namespace mirrors the metastore. Every handler is
//! authorised against the UC privilege model (`crate::uc::privileges`):
//! owners and admins manage objects, `USE_*`/`CREATE_*`/`SELECT`/... are
//! inherited down the catalog → schema → object hierarchy, and listings only
//! return what the caller can browse.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::routing::{get, patch};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::clusters::{Cluster, ClusterState, KIND as CLUSTER_KIND};
use super::{empty, Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};
use crate::uc::privileges::{normalize_privilege, Authorizer, Securable};
use crate::uc::sqlguard::{Analysis, StmtKind};
use crate::uc::system_tables;

pub const KIND_CATALOG: &str = "uc_catalog";
pub const KIND_SCHEMA: &str = "uc_schema";
pub const KIND_TABLE: &str = "uc_table";
pub const KIND_VOLUME: &str = "uc_volume";
pub const KIND_FUNCTION: &str = "uc_function";
pub const KIND_EXT_LOCATION: &str = "uc_external_location";
pub const KIND_STORAGE_CRED: &str = "uc_storage_credential";
pub const KIND_CONNECTION: &str = "uc_connection";
pub const KIND_GRANTS: &str = "uc_grants";
pub const METASTORE_ID: &str = "lakeforge-metastore";
pub const DEFAULT_CATALOG: &str = "main";
pub const SYSTEM_CATALOG: &str = "system";

pub fn id_of(kind: &str, full_name: &str) -> String {
    format!("{kind}:{full_name}")
}

pub fn base_fields(o: &mut Map<String, Value>, owner: &str) {
    let now = now_ms();
    o.entry("owner").or_insert(json!(owner));
    o.entry("created_at").or_insert(json!(now));
    o.entry("created_by").or_insert(json!(owner));
    o.insert("updated_at".into(), json!(now));
    o.insert("updated_by".into(), json!(owner));
    o.entry("metastore_id").or_insert(json!(METASTORE_ID));
    o.entry("comment").or_insert(json!(""));
    o.entry("properties").or_insert(json!({}));
}

/// Data source format named by `USING <fmt>` / `STORED AS <fmt>` in a
/// `CREATE TABLE` statement, if any.
pub fn ddl_format(sql: &str) -> Option<String> {
    let toks: Vec<String> = sql.split_whitespace().map(|t| t.trim_end_matches(';').to_ascii_uppercase()).collect();
    let after = |word: &str| toks.iter().position(|t| t == word).and_then(|i| toks.get(i + 1)).cloned();
    if let Some(f) = after("USING") {
        return Some(f);
    }
    let i = toks.iter().position(|t| t == "STORED")?;
    (toks.get(i + 1).map(String::as_str) == Some("AS")).then(|| toks.get(i + 2).cloned()).flatten()
}

pub fn str_field(o: &Map<String, Value>, k: &str) -> ApiResult<String> {
    o.get(k).and_then(|v| v.as_str()).map(str::to_string).ok_or_else(|| ApiError::invalid(format!("{k} is required")))
}

fn validate_name(name: &str, what: &str) -> ApiResult<()> {
    if name.is_empty() || name.contains('.') || name.contains('/') || name.contains(char::is_whitespace) {
        return Err(ApiError::invalid(format!("Invalid {what} name '{name}': must be non-empty and contain no '.', '/' or whitespace")));
    }
    Ok(())
}

/// Split `catalog.schema.name` into its three parts.
pub fn split_name(name: &str) -> (String, String, String) {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.as_slice() {
        [c, s, t] => (c.to_string(), s.to_string(), t.to_string()),
        [s, t] => (DEFAULT_CATALOG.into(), s.to_string(), t.to_string()),
        [t] => (DEFAULT_CATALOG.into(), "default".into(), t.to_string()),
        _ => (DEFAULT_CATALOG.into(), "default".into(), name.to_string()),
    }
}

fn schema_of(full: &str) -> ApiResult<(String, String)> {
    let parts: Vec<&str> = full.split('.').collect();
    match parts.as_slice() {
        [c, s] => Ok((c.to_string(), s.to_string())),
        _ => Err(ApiError::invalid(format!("Invalid schema name '{full}': expected catalog.schema"))),
    }
}

fn three_parts(full: &str, what: &str) -> ApiResult<(String, String, String)> {
    let parts: Vec<&str> = full.split('.').collect();
    match parts.as_slice() {
        [c, s, t] if !c.is_empty() && !s.is_empty() && !t.is_empty() => Ok((c.to_string(), s.to_string(), t.to_string())),
        _ => Err(ApiError::invalid(format!("Invalid {what} name '{full}': expected catalog.schema.name"))),
    }
}

// ---------------------------------------------------------------------------
// Metastore bootstrap and engine sync
// ---------------------------------------------------------------------------

impl AppState {
    pub async fn ensure_default_catalog(&self) -> ApiResult<()> {
        if self.store.get::<Value>(KIND_CATALOG, &id_of(KIND_CATALOG, DEFAULT_CATALOG)).await?.is_none() {
            let mut o = Map::new();
            o.insert("name".into(), json!(DEFAULT_CATALOG));
            o.insert("catalog_type".into(), json!("MANAGED_CATALOG"));
            o.insert("full_name".into(), json!(DEFAULT_CATALOG));
            o.insert("isolation_mode".into(), json!("OPEN"));
            o.insert("securable_type".into(), json!("CATALOG"));
            o.insert("securable_kind".into(), json!("CATALOG_STANDARD"));
            base_fields(&mut o, &self.config.admin_user);
            self.store.insert(KIND_CATALOG, self.ws(), &id_of(KIND_CATALOG, DEFAULT_CATALOG), None, Some(DEFAULT_CATALOG), &Value::Object(o)).await?;
        }
        for sch in ["default", "information_schema"] {
            let full = format!("{DEFAULT_CATALOG}.{sch}");
            if self.store.get::<Value>(KIND_SCHEMA, &id_of(KIND_SCHEMA, &full)).await?.is_none() {
                let mut o = Map::new();
                o.insert("name".into(), json!(sch));
                o.insert("catalog_name".into(), json!(DEFAULT_CATALOG));
                o.insert("full_name".into(), json!(full));
                o.insert("catalog_type".into(), json!("MANAGED_CATALOG"));
                o.insert("securable_type".into(), json!("SCHEMA"));
                base_fields(&mut o, &self.config.admin_user);
                self.store.insert(KIND_SCHEMA, self.ws(), &id_of(KIND_SCHEMA, &full), Some(DEFAULT_CATALOG), Some(&full), &Value::Object(o)).await?;
            }
        }
        self.ensure_default_grants().await?;
        Ok(())
    }

    pub async fn running_clusters(&self) -> ApiResult<Vec<Cluster>> {
        let docs: Vec<Doc<Cluster>> = self.store.list(CLUSTER_KIND, self.ws(), Filter::default()).await?;
        Ok(docs.into_iter().map(|d| d.data).filter(|c| c.state == ClusterState::Running && c.handle.is_some()).collect())
    }

    /// Run a statement on every running cluster's driver (best effort).
    pub async fn broadcast_sql(&self, sql: &str) {
        for c in self.running_clusters().await.unwrap_or_default() {
            if let Some(h) = &c.handle {
                match self.forge.client(&h.driver_addr).await {
                    Ok(cl) => {
                        if let Err(e) = cl.sql(sql).await {
                            tracing::debug!(cluster = %c.cluster_id, error = %e, sql, "broadcast statement failed");
                        }
                    }
                    Err(e) => tracing::debug!(cluster = %c.cluster_id, error = %e, "driver unreachable"),
                }
            }
        }
    }

    /// Register one metastore table on a driver; returns the engine schema.
    pub async fn register_table_on(&self, driver_addr: &str, table: &Value) -> ApiResult<Option<Value>> {
        let (Some(cat), Some(sch), Some(name)) = (table.get("catalog_name").and_then(|v| v.as_str()), table.get("schema_name").and_then(|v| v.as_str()), table.get("name").and_then(|v| v.as_str())) else {
            return Ok(None);
        };
        let client = self.forge.client(driver_addr).await?;
        if table.get("table_type").and_then(|v| v.as_str()) == Some("VIEW") {
            if let Some(def) = table.get("view_definition").and_then(|v| v.as_str()) {
                let _ = client.sql(&format!("CREATE SCHEMA IF NOT EXISTS {cat}.{sch}")).await;
                client.sql(&format!("CREATE OR REPLACE VIEW {cat}.{sch}.{name} AS {def}")).await?;
            }
            return Ok(None);
        }
        let Some(loc) = table.get("storage_location").and_then(|v| v.as_str()) else { return Ok(None) };
        let format = table.get("data_source_format").and_then(|v| v.as_str()).unwrap_or("DELTA").to_ascii_lowercase();
        let options: HashMap<String, String> = table
            .get("properties")
            .and_then(|p| p.as_object())
            .map(|p| p.iter().filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string()))).collect())
            .unwrap_or_default();
        let schema_json = client.register_table_in(cat, sch, name, &format, loc, options).await?;
        Ok(serde_json::from_str(&schema_json).ok())
    }

    /// Push every table in the metastore to `cluster`'s driver.
    pub async fn sync_metastore_to_cluster(&self, cluster: &Cluster) -> ApiResult<()> {
        let Some(h) = &cluster.handle else { return Ok(()) };
        let tables: Vec<Doc<Value>> = self.store.list(KIND_TABLE, self.ws(), Filter::default()).await?;
        let mut ok = 0;
        // tables first, then views (views depend on tables)
        let (views, base): (Vec<_>, Vec<_>) = tables.into_iter().partition(|t| t.data["table_type"] == "VIEW");
        for t in base.into_iter().chain(views) {
            match self.register_table_on(&h.driver_addr, &t.data).await {
                Ok(Some(schema)) => {
                    ok += 1;
                    let cols = merge_columns(t.data.get("columns"), columns_from_engine_schema(&schema));
                    let _ = self
                        .store
                        .update::<Value, _>(KIND_TABLE, &t.id, "Table", |v| {
                            if let Some(o) = v.as_object_mut() {
                                o.insert("columns".into(), Value::Array(cols));
                            }
                            Ok(())
                        })
                        .await;
                }
                Ok(None) => {}
                Err(e) => tracing::warn!(table = ?t.name, error = %e, "could not register table on cluster"),
            }
        }
        tracing::info!(cluster = %cluster.cluster_id, tables = ok, "metastore synced to cluster");
        Ok(())
    }

    /// Push one table to all running clusters (after create/update).
    pub async fn broadcast_table(&self, table: &Value) -> Vec<Value> {
        let mut schema = None;
        if let Ok(clusters) = self.running_clusters().await {
            for c in clusters {
                if let Some(h) = &c.handle {
                    match self.register_table_on(&h.driver_addr, table).await {
                        Ok(Some(s)) => schema = Some(s),
                        Ok(None) => {}
                        Err(e) => tracing::warn!(cluster = %c.cluster_id, error = %e, "table broadcast failed"),
                    }
                }
            }
        }
        schema.map(|s| columns_from_engine_schema(&s)).unwrap_or_default()
    }

    /// Root under which Forge drivers write managed tables
    /// (`forge.sql.warehouse.dir`); tables live at `<root>/<catalog>/<schema>/<table>`.
    pub fn warehouse_dir(&self) -> String {
        self.storage.url_for("/tables")
    }

    pub fn managed_table_location(&self, cat: &str, sch: &str, name: &str) -> String {
        format!("{}/{cat}/{sch}/{name}", self.warehouse_dir())
    }

    /// Mirror DDL that ran successfully through the SQL path into the
    /// metastore so `CREATE TABLE`/`CREATE SCHEMA`/`DROP ...` issued from
    /// notebooks or the SQL editor show up in Unity Catalog.
    pub async fn observe_ddl(&self, an: &Analysis, sql: &str, user: &str) {
        match an.kind {
            StmtKind::CreateTable | StmtKind::CreateView => {
                for (sec, full) in &an.creates {
                    if *sec != Securable::Table {
                        continue;
                    }
                    let (cat, sch, tbl) = split_name(full);
                    let mut o = Map::new();
                    o.insert("name".into(), json!(tbl));
                    o.insert("catalog_name".into(), json!(cat));
                    o.insert("schema_name".into(), json!(sch));
                    o.insert("full_name".into(), json!(format!("{cat}.{sch}.{tbl}")));
                    let table_type = if an.kind == StmtKind::CreateView {
                        "VIEW"
                    } else if an.location.is_some() {
                        "EXTERNAL"
                    } else {
                        "MANAGED"
                    };
                    o.insert("table_type".into(), json!(table_type));
                    o.insert("data_source_format".into(), json!(ddl_format(sql).unwrap_or_else(|| "DELTA".into())));
                    if an.kind == StmtKind::CreateView {
                        o.insert("view_definition".into(), json!(sql));
                    } else {
                        let loc = an.location.clone().unwrap_or_else(|| self.managed_table_location(&cat, &sch, &tbl));
                        o.insert("storage_location".into(), json!(loc));
                    }
                    o.insert("securable_type".into(), json!("TABLE"));
                    if let Err(e) = self.upsert_table(o, user).await {
                        tracing::warn!(error = %e, "ddl mirror failed");
                    }
                }
            }
            StmtKind::CreateSchema => {
                for (sec, full) in &an.creates {
                    if *sec != Securable::Schema {
                        continue;
                    }
                    let (cat, sch) = full.split_once('.').map(|(c, s)| (c.to_string(), s.to_string())).unwrap_or_else(|| (DEFAULT_CATALOG.to_string(), full.clone()));
                    let mut o = Map::new();
                    o.insert("name".into(), json!(sch));
                    o.insert("catalog_name".into(), json!(cat));
                    o.insert("full_name".into(), json!(format!("{cat}.{sch}")));
                    o.insert("securable_type".into(), json!("SCHEMA"));
                    base_fields(&mut o, user);
                    let full = format!("{cat}.{sch}");
                    let _ = self.store.upsert(KIND_SCHEMA, self.ws(), &id_of(KIND_SCHEMA, &full), Some(&cat), Some(&full), &Value::Object(o)).await;
                }
            }
            StmtKind::Drop => {
                for (sec, full) in &an.owned {
                    match sec {
                        Securable::Table => {
                            let (cat, sch, tbl) = split_name(full);
                            let _ = self.store.delete(KIND_TABLE, &id_of(KIND_TABLE, &format!("{cat}.{sch}.{tbl}"))).await;
                        }
                        Securable::Schema => {
                            let _ = self.store.delete(KIND_SCHEMA, &id_of(KIND_SCHEMA, full)).await;
                        }
                        Securable::Function => {
                            let _ = self.store.delete(KIND_FUNCTION, &id_of(KIND_FUNCTION, full)).await;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    // ------------------------------------------------------------ lookups

    pub async fn uc_get(&self, kind: &str, full: &str) -> ApiResult<Option<Value>> {
        Ok(self.store.get::<Value>(kind, &id_of(kind, full)).await?.map(|d| d.data))
    }

    pub async fn uc_require(&self, kind: &str, full: &str, what: &str) -> ApiResult<Value> {
        if let Some(v) = self.uc_get(kind, full).await? {
            return Ok(v);
        }
        if let Some(v) = self.uc_virtual(kind, full) {
            return Ok(v);
        }
        Err(ApiError::NotFound(format!("{what} '{full}' does not exist.")))
    }

    /// Virtual objects: the `system` catalog, its schemas and tables, and each
    /// catalog's `information_schema` tables.
    fn uc_virtual(&self, kind: &str, full: &str) -> Option<Value> {
        let parts: Vec<&str> = full.split('.').collect();
        match (kind, parts.as_slice()) {
            (KIND_CATALOG, [SYSTEM_CATALOG]) => Some(system_tables::system_catalog_doc(&self.config.admin_user)),
            (KIND_SCHEMA, [SYSTEM_CATALOG, s]) if system_tables::system_schemas().contains(s) => Some(system_tables::virtual_schema_doc(SYSTEM_CATALOG, s, &self.config.admin_user)),
            (KIND_SCHEMA, [c, "information_schema"]) => Some(system_tables::virtual_schema_doc(c, "information_schema", &self.config.admin_user)),
            (KIND_TABLE, [c, s, t]) => system_tables::virtual_table_doc(c, s, t, &self.config.admin_user),
            _ => None,
        }
    }

    /// Fully-qualified securable exists (real or virtual)?
    pub async fn uc_exists(&self, sec: Securable, full: &str) -> ApiResult<bool> {
        let kind = match sec {
            Securable::Catalog => KIND_CATALOG,
            Securable::Schema => KIND_SCHEMA,
            Securable::Table => KIND_TABLE,
            Securable::Volume => KIND_VOLUME,
            Securable::Function => KIND_FUNCTION,
            Securable::ExternalLocation => KIND_EXT_LOCATION,
            Securable::StorageCredential => KIND_STORAGE_CRED,
            Securable::Connection => KIND_CONNECTION,
            Securable::Share => crate::uc::privileges::KIND_SHARE,
            Securable::Recipient => crate::uc::privileges::KIND_RECIPIENT,
            Securable::Provider => crate::uc::privileges::KIND_PROVIDER,
            Securable::Metastore => return Ok(true),
        };
        Ok(self.uc_get(kind, full).await?.is_some() || self.uc_virtual(kind, full).is_some())
    }

    // ------------------------------------------------------------ catalogs

    pub async fn uc_create_catalog(&self, p: &Principal, mut o: Map<String, Value>) -> ApiResult<Value> {
        let name = str_field(&o, "name")?;
        validate_name(&name, "catalog")?;
        if name == SYSTEM_CATALOG {
            return Err(ApiError::AlreadyExists("Catalog 'system' is reserved".into()));
        }
        let mut az = Authorizer::new(self, p);
        az.require(Securable::Metastore, METASTORE_ID, "CREATE_CATALOG").await?;
        if self.uc_get(KIND_CATALOG, &name).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("Catalog '{name}' already exists")));
        }
        o.insert("full_name".into(), json!(name));
        let mut kind = "CATALOG_STANDARD";
        if let Some(conn) = o.get("connection_name").and_then(|v| v.as_str()) {
            az.require(Securable::Connection, conn, "CREATE_FOREIGN_CATALOG").await?;
            self.uc_require(KIND_CONNECTION, conn, "Connection").await?;
            o.insert("catalog_type".into(), json!("FOREIGN_CATALOG"));
            kind = "CATALOG_FOREIGN_POSTGRESQL";
        } else if o.contains_key("provider_name") || o.contains_key("share_name") {
            o.insert("catalog_type".into(), json!("DELTASHARING_CATALOG"));
            kind = "CATALOG_DELTASHARING";
        } else {
            o.entry("catalog_type").or_insert(json!("MANAGED_CATALOG"));
        }
        o.entry("isolation_mode").or_insert(json!("OPEN"));
        o.insert("securable_type".into(), json!("CATALOG"));
        o.insert("securable_kind".into(), json!(kind));
        base_fields(&mut o, &p.user_name);
        let v = Value::Object(o);
        self.store.insert(KIND_CATALOG, self.ws(), &id_of(KIND_CATALOG, &name), None, Some(&name), &v).await?;
        if v["catalog_type"] == "MANAGED_CATALOG" {
            // every catalog gets a default schema, like Databricks
            let mut s = Map::new();
            s.insert("name".into(), json!("default"));
            s.insert("catalog_name".into(), json!(name));
            self.uc_insert_schema(p, s).await?;
        }
        Ok(v)
    }

    pub async fn uc_delete_catalog(&self, p: &Principal, name: &str, force: bool) -> ApiResult<()> {
        let mut az = Authorizer::new(self, p);
        if name == SYSTEM_CATALOG {
            return Err(ApiError::PermissionDenied("Catalog 'system' cannot be dropped".into()));
        }
        self.uc_require(KIND_CATALOG, name, "Catalog").await?;
        az.require_owner(Securable::Catalog, name).await?;
        let schemas: Vec<Doc<Value>> = self.store.list(KIND_SCHEMA, self.ws(), Filter { parent_id: Some(name), ..Default::default() }).await?;
        let non_default: Vec<_> = schemas.iter().filter(|s| s.data["name"] != "default" && s.data["name"] != "information_schema").collect();
        if !non_default.is_empty() && !force {
            return Err(ApiError::InvalidState(format!("Catalog '{name}' is not empty; use force=true (or CASCADE)")));
        }
        for s in schemas {
            let full = s.name.clone().unwrap_or_default();
            self.uc_delete_schema_contents(&full).await?;
            self.store.delete(KIND_SCHEMA, &s.id).await?;
        }
        if !self.store.delete(KIND_CATALOG, &id_of(KIND_CATALOG, name)).await? {
            return Err(ApiError::NotFound(format!("Catalog '{name}' does not exist")));
        }
        self.drop_grants(Securable::Catalog, name).await?;
        Ok(())
    }

    // ------------------------------------------------------------ schemas

    /// Insert a schema doc without privilege checks (caller has verified).
    async fn uc_insert_schema(&self, p: &Principal, mut o: Map<String, Value>) -> ApiResult<Value> {
        let name = str_field(&o, "name")?;
        let cat = str_field(&o, "catalog_name")?;
        validate_name(&name, "schema")?;
        let full = format!("{cat}.{name}");
        if self.uc_get(KIND_SCHEMA, &full).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("Schema '{full}' already exists")));
        }
        o.insert("full_name".into(), json!(full));
        o.insert("securable_type".into(), json!("SCHEMA"));
        o.entry("catalog_type").or_insert(json!("MANAGED_CATALOG"));
        o.entry("storage_root").or_insert(json!(self.storage.url_for(&format!("/tables/{cat}/{name}"))));
        o.entry("schema_id").or_insert(json!(uuid::Uuid::new_v4().to_string()));
        base_fields(&mut o, &p.user_name);
        let v = Value::Object(o);
        self.store.insert(KIND_SCHEMA, self.ws(), &id_of(KIND_SCHEMA, &full), Some(&cat), Some(&full), &v).await?;
        self.broadcast_sql(&format!("CREATE SCHEMA IF NOT EXISTS {cat}.{name}")).await;
        Ok(v)
    }

    pub async fn uc_create_schema(&self, p: &Principal, o: Map<String, Value>) -> ApiResult<Value> {
        let cat = str_field(&o, "catalog_name")?;
        self.uc_require(KIND_CATALOG, &cat, "Catalog").await?;
        if cat == SYSTEM_CATALOG {
            return Err(ApiError::PermissionDenied("Catalog 'system' is read-only".into()));
        }
        let mut az = Authorizer::new(self, p);
        az.require(Securable::Catalog, &cat, "USE_CATALOG").await?;
        az.require(Securable::Catalog, &cat, "CREATE_SCHEMA").await?;
        self.uc_insert_schema(p, o).await
    }

    async fn uc_delete_schema_contents(&self, full: &str) -> ApiResult<()> {
        let tables: Vec<Doc<Value>> = self.store.list(KIND_TABLE, self.ws(), Filter { parent_id: Some(full), ..Default::default() }).await?;
        for t in tables {
            self.uc_remove_table_doc(&t.data).await?;
        }
        for (kind, sec) in [(KIND_VOLUME, Securable::Volume), (KIND_FUNCTION, Securable::Function)] {
            let docs: Vec<Doc<Value>> = self.store.list(kind, self.ws(), Filter { parent_id: Some(full), ..Default::default() }).await?;
            for d in docs {
                self.drop_grants(sec, d.name.as_deref().unwrap_or("")).await?;
            }
            self.store.delete_children(kind, full).await?;
        }
        self.store.delete_children(crate::uc::privileges::KIND_MODEL, full).await?;
        self.drop_grants(Securable::Schema, full).await?;
        Ok(())
    }

    pub async fn uc_delete_schema(&self, p: &Principal, full: &str, force: bool) -> ApiResult<()> {
        schema_of(full)?;
        self.uc_require(KIND_SCHEMA, full, "Schema").await?;
        if full.ends_with(".information_schema") || full.starts_with("system.") {
            return Err(ApiError::PermissionDenied(format!("Schema '{full}' is read-only")));
        }
        let mut az = Authorizer::new(self, p);
        az.require_owner(Securable::Schema, full).await?;
        let n = self.store.count(KIND_TABLE, self.ws(), Some(full)).await? + self.store.count(KIND_VOLUME, self.ws(), Some(full)).await? + self.store.count(KIND_FUNCTION, self.ws(), Some(full)).await?;
        if n > 0 && !force {
            return Err(ApiError::InvalidState(format!("Schema '{full}' is not empty; use force=true (or CASCADE)")));
        }
        self.uc_delete_schema_contents(full).await?;
        if !self.store.delete(KIND_SCHEMA, &id_of(KIND_SCHEMA, full)).await? {
            return Err(ApiError::NotFound(format!("Schema '{full}' does not exist")));
        }
        self.broadcast_sql(&format!("DROP SCHEMA IF EXISTS {full} CASCADE")).await;
        Ok(())
    }

    // ------------------------------------------------------------ tables

    /// Insert or update a table doc, register it on running clusters and
    /// capture the engine schema as UC columns.
    pub async fn upsert_table(&self, mut o: Map<String, Value>, user: &str) -> ApiResult<Value> {
        let full = str_field(&o, "full_name")?;
        let existing = self.store.get::<Value>(KIND_TABLE, &id_of(KIND_TABLE, &full)).await?;
        if let Some(prev) = existing.as_ref().and_then(|d| d.data.as_object()) {
            for (k, v) in prev {
                o.entry(k.clone()).or_insert(v.clone());
            }
        }
        base_fields(&mut o, user);
        o.entry("table_id").or_insert(json!(uuid::Uuid::new_v4().to_string()));
        o.entry("columns").or_insert(json!([]));
        o.entry("table_constraints").or_insert(json!([]));
        let v = Value::Object(o);
        let cols = self.broadcast_table(&v).await;
        let mut v = v;
        if !cols.is_empty() {
            v["columns"] = Value::Array(merge_columns(v.get("columns"), cols));
        }
        let parent = format!("{}.{}", v["catalog_name"].as_str().unwrap_or(""), v["schema_name"].as_str().unwrap_or(""));
        self.store.upsert(KIND_TABLE, self.ws(), &id_of(KIND_TABLE, &full), Some(&parent), Some(&full), &v).await?;
        Ok(v)
    }

    pub async fn uc_create_table(&self, p: &Principal, mut o: Map<String, Value>) -> ApiResult<Value> {
        let name = str_field(&o, "name")?;
        let cat = str_field(&o, "catalog_name")?;
        let sch = str_field(&o, "schema_name")?;
        validate_name(&name, "table")?;
        let schema_full = format!("{cat}.{sch}");
        self.uc_require(KIND_SCHEMA, &schema_full, "Schema").await?;
        if cat == SYSTEM_CATALOG || sch == "information_schema" {
            return Err(ApiError::PermissionDenied(format!("Schema '{schema_full}' is read-only")));
        }
        let mut az = Authorizer::new(self, p);
        az.require_use_path(&format!("{schema_full}.{name}")).await?;
        let table_type = o.get("table_type").and_then(|v| v.as_str()).unwrap_or("MANAGED").to_string();
        match table_type.as_str() {
            "VIEW" | "MATERIALIZED_VIEW" => az.require(Securable::Schema, &schema_full, "CREATE_TABLE").await?,
            "EXTERNAL" => {
                az.require(Securable::Schema, &schema_full, "CREATE_TABLE").await?;
                if let Some(loc) = o.get("storage_location").and_then(|v| v.as_str()) {
                    self.require_path_privilege(&mut az, loc, "CREATE_EXTERNAL_TABLE").await?;
                }
            }
            _ => az.require(Securable::Schema, &schema_full, "CREATE_TABLE").await?,
        }
        let full = format!("{cat}.{sch}.{name}");
        if self.uc_get(KIND_TABLE, &full).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("Table '{full}' already exists")));
        }
        o.insert("full_name".into(), json!(full));
        o.insert("table_type".into(), json!(table_type));
        o.entry("data_source_format").or_insert(json!("DELTA"));
        o.insert("securable_type".into(), json!("TABLE"));
        if table_type == "MANAGED" && !o.contains_key("storage_location") {
            o.insert("storage_location".into(), json!(self.managed_table_location(&cat, &sch, &name)));
        }
        normalise_columns(&mut o);
        self.upsert_table(o, &p.user_name).await
    }

    async fn uc_remove_table_doc(&self, table: &Value) -> ApiResult<()> {
        let full = table["full_name"].as_str().unwrap_or_default().to_string();
        self.store.delete(KIND_TABLE, &id_of(KIND_TABLE, &full)).await?;
        if table["table_type"] == "VIEW" {
            self.broadcast_sql(&format!("DROP VIEW IF EXISTS {full}")).await;
        } else {
            self.broadcast_sql(&format!("DROP TABLE IF EXISTS {full}")).await;
        }
        if table["table_type"] == "MANAGED" {
            if let Some(path) = table["storage_location"].as_str().and_then(|l| self.storage.path_of(l)) {
                let _ = self.storage.delete_prefix(&path).await;
            }
        }
        self.drop_grants(Securable::Table, &full).await?;
        Ok(())
    }

    pub async fn uc_delete_table(&self, p: &Principal, full: &str) -> ApiResult<()> {
        three_parts(full, "table")?;
        let doc = self.uc_require(KIND_TABLE, full, "Table").await?;
        if full.starts_with("system.") || doc["table_type"] == "SYSTEM" {
            return Err(ApiError::PermissionDenied(format!("Table '{full}' is read-only")));
        }
        let mut az = Authorizer::new(self, p);
        az.require_owner(Securable::Table, full).await?;
        self.uc_remove_table_doc(&doc).await
    }

    /// `READ_FILES`/`WRITE_FILES`/`CREATE_EXTERNAL_TABLE` on the external
    /// location covering `url`. Paths under the workspace storage root are
    /// governed by the metastore itself (admins / owners only).
    pub async fn require_path_privilege(&self, az: &mut Authorizer<'_>, url: &str, privilege: &str) -> ApiResult<()> {
        if az.is_admin() {
            return Ok(());
        }
        let locs: Vec<Doc<Value>> = self.store.list(KIND_EXT_LOCATION, self.ws(), Filter::default()).await?;
        let mut best: Option<(usize, String)> = None;
        for l in locs {
            let Some(u) = l.data["url"].as_str() else { continue };
            let prefix = u.trim_end_matches('/');
            let matches = url == prefix || url.starts_with(&format!("{prefix}/"));
            if matches && best.as_ref().map(|(n, _)| prefix.len() > *n).unwrap_or(true) {
                best = Some((prefix.len(), l.name.unwrap_or_default()));
            }
        }
        match best {
            Some((_, name)) => az.require(Securable::ExternalLocation, &name, privilege).await,
            None => Err(ApiError::PermissionDenied(format!("[INSUFFICIENT_PERMISSIONS] No external location covers '{url}'; {} requires an external location grant.", normalize_privilege(privilege).replace('_', " ")))),
        }
    }

    // ------------------------------------------------------------ ownership / patch

    /// Apply a PATCH to a securable: comment/properties/owner/new_name.
    /// Owner changes require ownership (or MANAGE) and an existing principal.
    #[allow(clippy::too_many_arguments)]
    pub async fn uc_patch(&self, p: &Principal, sec: Securable, kind: &str, full: &str, o: &Map<String, Value>, allowed: &[&str], what: &str) -> ApiResult<Value> {
        let mut az = Authorizer::new(self, p);
        self.uc_require(kind, full, what).await?;
        if full.starts_with("system.") || (sec == Securable::Catalog && full == SYSTEM_CATALOG) {
            return Err(ApiError::PermissionDenied(format!("{what} '{full}' is read-only")));
        }
        let is_owner = az.can_manage(sec, full).await?;
        if !is_owner {
            // Non-owners may only edit comments when they can browse (like Databricks' MANAGE requirement) -> deny.
            return Err(ApiError::PermissionDenied(format!("User does not own {what} '{full}' and does not have MANAGE on it.")));
        }
        if let Some(owner) = o.get("owner").and_then(|v| v.as_str()) {
            if !self.principal_exists(owner).await? {
                return Err(ApiError::invalid(format!("Principal '{owner}' does not exist")));
            }
        }
        let new_name = o.get("new_name").and_then(|v| v.as_str()).map(str::to_string);
        if let Some(n) = &new_name {
            validate_name(n, sec.as_str())?;
        }
        let doc = self
            .store
            .update::<Value, _>(kind, &id_of(kind, full), what, |v| {
                merge_patch(v, o, allowed, &p.user_name);
                Ok(())
            })
            .await?;
        if let Some(n) = new_name.filter(|n| Some(n.as_str()) != full.rsplit('.').next()) {
            return self.uc_rename(sec, kind, full, &n, doc.data).await;
        }
        if kind == KIND_TABLE {
            self.broadcast_table(&doc.data).await;
        }
        Ok(doc.data)
    }

    /// Rename a securable (doc id, name, grants, children, engine registration).
    async fn uc_rename(&self, sec: Securable, kind: &str, from: &str, new_name: &str, mut data: Value) -> ApiResult<Value> {
        let to = match from.rsplit_once('.') {
            Some((prefix, _)) => format!("{prefix}.{new_name}"),
            None => new_name.to_string(),
        };
        if self.uc_get(kind, &to).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("{} '{to}' already exists", sec.api_type())));
        }
        data["name"] = json!(new_name);
        data["full_name"] = json!(to);
        let parent = from.rsplit_once('.').map(|(p, _)| p.to_string());
        self.store.delete(kind, &id_of(kind, from)).await?;
        self.store.insert(kind, self.ws(), &id_of(kind, &to), parent.as_deref(), Some(&to), &data).await?;
        self.move_grants(sec, from, &to).await?;
        match sec {
            Securable::Table => {
                self.broadcast_sql(&format!("DROP TABLE IF EXISTS {from}")).await;
                self.broadcast_sql(&format!("DROP VIEW IF EXISTS {from}")).await;
                self.broadcast_table(&data).await;
            }
            Securable::Schema | Securable::Catalog => {
                // re-key children
                let prefix = format!("{from}.");
                for child_kind in [KIND_SCHEMA, KIND_TABLE, KIND_VOLUME, KIND_FUNCTION, crate::uc::privileges::KIND_MODEL] {
                    let docs: Vec<Doc<Value>> = self.store.list(child_kind, self.ws(), Filter { name_prefix: Some(&prefix), ..Default::default() }).await?;
                    for d in docs {
                        let old = d.name.clone().unwrap_or_default();
                        let new_full = format!("{to}.{}", &old[prefix.len()..]);
                        let mut v = d.data.clone();
                        v["full_name"] = json!(new_full);
                        if child_kind == KIND_SCHEMA {
                            v["catalog_name"] = json!(to);
                        } else {
                            let (c, s, _) = split_name(&new_full);
                            v["catalog_name"] = json!(c);
                            v["schema_name"] = json!(s);
                        }
                        let new_parent = new_full.rsplit_once('.').map(|(p, _)| p.to_string());
                        self.store.delete(child_kind, &d.id).await?;
                        self.store.insert(child_kind, self.ws(), &id_of(child_kind, &new_full), new_parent.as_deref(), Some(&new_full), &v).await?;
                        let child_sec = match child_kind {
                            KIND_SCHEMA => Securable::Schema,
                            KIND_TABLE => Securable::Table,
                            KIND_VOLUME => Securable::Volume,
                            _ => Securable::Function,
                        };
                        self.move_grants(child_sec, &old, &new_full).await?;
                        if child_kind == KIND_TABLE {
                            self.broadcast_sql(&format!("DROP TABLE IF EXISTS {old}")).await;
                            self.broadcast_table(&v).await;
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(data)
    }

    // ------------------------------------------------------------ volumes / functions

    pub async fn uc_create_in_schema(&self, p: &Principal, kind: &str, securable: &str, mut o: Map<String, Value>) -> ApiResult<Value> {
        let name = str_field(&o, "name")?;
        let cat = str_field(&o, "catalog_name")?;
        let sch = str_field(&o, "schema_name")?;
        validate_name(&name, securable)?;
        let schema_full = format!("{cat}.{sch}");
        self.uc_require(KIND_SCHEMA, &schema_full, "Schema").await?;
        if cat == SYSTEM_CATALOG {
            return Err(ApiError::PermissionDenied("Catalog 'system' is read-only".into()));
        }
        let full = format!("{cat}.{sch}.{name}");
        let mut az = Authorizer::new(self, p);
        az.require_use_path(&full).await?;
        let (sec, create_priv) = match kind {
            KIND_VOLUME => (Securable::Volume, "CREATE_VOLUME"),
            KIND_FUNCTION => (Securable::Function, "CREATE_FUNCTION"),
            _ => (Securable::Function, "CREATE_MODEL"),
        };
        az.require(Securable::Schema, &schema_full, create_priv).await?;
        if self.uc_get(kind, &full).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("{securable} '{full}' already exists")));
        }
        o.insert("full_name".into(), json!(full));
        o.insert("securable_type".into(), json!(sec.api_type()));
        if kind == KIND_VOLUME {
            let vt = o.get("volume_type").and_then(|v| v.as_str()).unwrap_or("MANAGED").to_string();
            o.insert("volume_type".into(), json!(vt));
            o.entry("volume_id").or_insert(json!(uuid::Uuid::new_v4().to_string()));
            if vt == "EXTERNAL" {
                let loc = str_field(&o, "storage_location")?;
                self.require_path_privilege(&mut az, &loc, "CREATE_EXTERNAL_VOLUME").await?;
            } else if !o.contains_key("storage_location") {
                o.insert("storage_location".into(), json!(self.storage.url_for(&format!("/Volumes/{cat}/{sch}/{name}"))));
                self.storage.mkdirs(&format!("/Volumes/{cat}/{sch}/{name}")).await?;
            }
        }
        if kind == KIND_FUNCTION {
            o.entry("function_id").or_insert(json!(uuid::Uuid::new_v4().to_string()));
            o.entry("routine_body").or_insert(json!("SQL"));
            o.entry("routine_definition").or_insert(json!(""));
            o.entry("data_type").or_insert(json!("STRING"));
            o.entry("full_data_type").or_insert(json!("STRING"));
            o.entry("is_deterministic").or_insert(json!(true));
            o.entry("sql_data_access").or_insert(json!("CONTAINS_SQL"));
            o.entry("parameter_style").or_insert(json!("S"));
            o.entry("security_type").or_insert(json!("DEFINER"));
            o.entry("specific_name").or_insert(json!(name));
            o.entry("is_null_call").or_insert(json!(false));
            o.entry("input_params").or_insert(json!({ "parameters": [] }));
            if o.get("routine_body") == Some(&json!("SQL")) {
                if let Some(def) = o.get("routine_definition").and_then(|v| v.as_str()) {
                    // must be an expression we can inline
                    if !def.trim().is_empty() {
                        sqlparser::parser::Parser::new(&sqlparser::dialect::GenericDialect {})
                            .try_with_sql(def)
                            .and_then(|mut p| p.parse_expr())
                            .map_err(|e| ApiError::invalid(format!("routine_definition is not a valid SQL expression: {e}")))?;
                    }
                }
            }
        }
        base_fields(&mut o, &p.user_name);
        let v = Value::Object(o);
        self.store.insert(kind, self.ws(), &id_of(kind, &full), Some(&schema_full), Some(&full), &v).await?;
        Ok(v)
    }

    pub async fn uc_delete_in_schema(&self, p: &Principal, kind: &str, sec: Securable, full: &str, what: &str) -> ApiResult<()> {
        three_parts(full, what)?;
        self.uc_require(kind, full, what).await?;
        let mut az = Authorizer::new(self, p);
        az.require_owner(sec, full).await?;
        self.store.delete(kind, &id_of(kind, full)).await?;
        self.drop_grants(sec, full).await?;
        Ok(())
    }

    // ------------------------------------------------------------ top-level named securables

    pub async fn uc_create_named(&self, p: &Principal, kind: &str, sec: Securable, securable: &str, mut o: Map<String, Value>) -> ApiResult<Value> {
        let name = str_field(&o, "name")?;
        validate_name(&name, securable)?;
        let mut az = Authorizer::new(self, p);
        let create_priv = match sec {
            Securable::ExternalLocation => "CREATE_EXTERNAL_LOCATION",
            Securable::StorageCredential => "CREATE_STORAGE_CREDENTIAL",
            Securable::Connection => "CREATE_CONNECTION",
            Securable::Share => "CREATE_SHARE",
            Securable::Recipient => "CREATE_RECIPIENT",
            Securable::Provider => "CREATE_PROVIDER",
            _ => "CREATE_CATALOG",
        };
        az.require(Securable::Metastore, METASTORE_ID, create_priv).await?;
        if self.uc_get(kind, &name).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("{securable} '{name}' already exists")));
        }
        o.insert("id".into(), json!(uuid::Uuid::new_v4().to_string()));
        o.insert("securable_type".into(), json!(sec.api_type()));
        if kind == KIND_EXT_LOCATION {
            if let Some(cred) = o.get("credential_name").and_then(|v| v.as_str()) {
                self.uc_require(KIND_STORAGE_CRED, cred, "Storage Credential").await?;
                az.require(Securable::StorageCredential, cred, "CREATE_EXTERNAL_LOCATION").await?;
            }
            o.entry("read_only").or_insert(json!(false));
        }
        if kind == KIND_STORAGE_CRED {
            redact_credential(&mut o);
        }
        if kind == KIND_CONNECTION {
            o.entry("connection_type").or_insert(json!("POSTGRESQL"));
            o.entry("read_only").or_insert(json!(false));
            o.entry("credential_type").or_insert(json!("USERNAME_PASSWORD"));
            o.entry("connection_id").or_insert(json!(uuid::Uuid::new_v4().to_string()));
            // connection secrets are sealed into the kv store, never kept in the doc
            if let Some(Value::Object(opts)) = o.get_mut("options") {
                if let Some(pw) = opts.remove("password").and_then(|v| v.as_str().map(str::to_string)) {
                    self.store.kv_set(&connection_secret_key(&name), &self.auth.sealer().seal(pw.as_bytes())?).await?;
                    opts.insert("password".into(), json!("****"));
                }
            }
        }
        base_fields(&mut o, &p.user_name);
        let v = Value::Object(o);
        self.store.insert(kind, self.ws(), &id_of(kind, &name), None, Some(&name), &v).await?;
        Ok(v)
    }

    pub async fn uc_delete_named(&self, p: &Principal, kind: &str, sec: Securable, name: &str, what: &str, force: bool) -> ApiResult<()> {
        self.uc_require(kind, name, what).await?;
        let mut az = Authorizer::new(self, p);
        az.require_owner(sec, name).await?;
        if kind == KIND_STORAGE_CRED && !force {
            let locs: Vec<Doc<Value>> = self.store.list(KIND_EXT_LOCATION, self.ws(), Filter::default()).await?;
            if locs.iter().any(|l| l.data["credential_name"] == name) {
                return Err(ApiError::InvalidState(format!("Storage credential '{name}' is used by external locations; use force=true")));
            }
        }
        if kind == KIND_CONNECTION {
            let cats: Vec<Doc<Value>> = self.store.list(KIND_CATALOG, self.ws(), Filter::default()).await?;
            if cats.iter().any(|c| c.data["connection_name"] == name) && !force {
                return Err(ApiError::InvalidState(format!("Connection '{name}' is used by foreign catalogs; use force=true")));
            }
            self.store.kv_delete(&connection_secret_key(name)).await?;
        }
        self.store.delete(kind, &id_of(kind, name)).await?;
        self.drop_grants(sec, name).await?;
        Ok(())
    }

    // ------------------------------------------------------------ listings

    /// Filter docs down to those the principal can browse.
    pub async fn uc_visible(&self, p: &Principal, sec: Securable, docs: Vec<Value>) -> ApiResult<Vec<Value>> {
        if p.is_admin {
            return Ok(docs);
        }
        let mut az = Authorizer::new(self, p);
        let mut out = vec![];
        for d in docs {
            let full = d["full_name"].as_str().or_else(|| d["name"].as_str()).unwrap_or("");
            if az.can_browse(sec, full).await? {
                out.push(d);
            }
        }
        Ok(out)
    }

    pub async fn uc_list_catalogs(&self, p: &Principal) -> ApiResult<Vec<Value>> {
        self.ensure_default_catalog().await?;
        let docs: Vec<Doc<Value>> = self.store.list(KIND_CATALOG, self.ws(), Filter::default()).await?;
        let mut all: Vec<Value> = docs.into_iter().map(|d| d.data).collect();
        all.push(system_tables::system_catalog_doc(&self.config.admin_user));
        self.uc_visible(p, Securable::Catalog, all).await
    }

    pub async fn uc_list_schemas(&self, p: &Principal, cat: &str) -> ApiResult<Vec<Value>> {
        self.ensure_default_catalog().await?;
        self.uc_require(KIND_CATALOG, cat, "Catalog").await?;
        let mut all: Vec<Value> = if cat == SYSTEM_CATALOG {
            system_tables::system_schemas().iter().map(|s| system_tables::virtual_schema_doc(SYSTEM_CATALOG, s, &self.config.admin_user)).collect()
        } else {
            let docs: Vec<Doc<Value>> = self.store.list(KIND_SCHEMA, self.ws(), Filter { parent_id: Some(cat), ..Default::default() }).await?;
            let mut v: Vec<Value> = docs.into_iter().map(|d| d.data).collect();
            if !v.iter().any(|s| s["name"] == "information_schema") {
                v.push(system_tables::virtual_schema_doc(cat, "information_schema", &self.config.admin_user));
            }
            v
        };
        all.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        self.uc_visible(p, Securable::Schema, all).await
    }

    pub async fn uc_list_tables(&self, p: &Principal, cat: &str, sch: &str, limit: Option<i64>) -> ApiResult<Vec<Value>> {
        let parent = format!("{cat}.{sch}");
        let all: Vec<Value> = if cat == SYSTEM_CATALOG || sch == "information_schema" {
            system_tables::virtual_tables_in(cat, sch, &self.config.admin_user)
        } else {
            self.uc_require(KIND_SCHEMA, &parent, "Schema").await?;
            let docs: Vec<Doc<Value>> = self.store.list(KIND_TABLE, self.ws(), Filter { parent_id: Some(&parent), limit, ..Default::default() }).await?;
            docs.into_iter().map(|d| d.data).collect()
        };
        self.uc_visible(p, Securable::Table, all).await
    }

    pub async fn uc_list_in_schema(&self, p: &Principal, kind: &str, sec: Securable, cat: &str, sch: &str) -> ApiResult<Vec<Value>> {
        let parent = format!("{cat}.{sch}");
        self.uc_require(KIND_SCHEMA, &parent, "Schema").await?;
        let docs: Vec<Doc<Value>> = self.store.list(kind, self.ws(), Filter { parent_id: Some(&parent), ..Default::default() }).await?;
        self.uc_visible(p, sec, docs.into_iter().map(|d| d.data).collect()).await
    }

    pub async fn uc_list_named(&self, p: &Principal, kind: &str, sec: Securable) -> ApiResult<Vec<Value>> {
        let docs: Vec<Doc<Value>> = self.store.list(kind, self.ws(), Filter::default()).await?;
        self.uc_visible(p, sec, docs.into_iter().map(|d| d.data).collect()).await
    }

    /// GET on a single securable: must exist and be browsable.
    pub async fn uc_get_visible(&self, p: &Principal, kind: &str, sec: Securable, full: &str, what: &str) -> ApiResult<Value> {
        if kind == KIND_SCHEMA {
            schema_of(full)?;
        }
        let v = self.uc_require(kind, full, what).await?;
        let mut az = Authorizer::new(self, p);
        if !az.can_browse(sec, full).await? {
            return Err(ApiError::PermissionDenied(format!("[INSUFFICIENT_PERMISSIONS] User {} does not have permission to browse {what} '{full}'.", p.user_name)));
        }
        Ok(v)
    }
}

pub fn connection_secret_key(name: &str) -> String {
    format!("uc_connection_secret:{name}")
}

impl AppState {
    /// Plaintext password for a UC connection (owner/`USE_CONNECTION` checked by caller).
    pub async fn connection_password(&self, name: &str) -> ApiResult<Option<String>> {
        let Some(sealed) = self.store.kv_get(&connection_secret_key(name)).await? else { return Ok(None) };
        let bytes = self.auth.sealer().open(&sealed)?;
        Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
    }
}

fn redact_credential(o: &mut Map<String, Value>) {
    for k in ["aws_iam_role", "azure_service_principal", "azure_managed_identity", "gcp_service_account_key", "databricks_gcp_service_account", "cloudflare_api_token"] {
        if let Some(Value::Object(c)) = o.get_mut(k) {
            c.remove("client_secret");
            c.remove("private_key");
            c.remove("secret_access_key");
            c.remove("private_key_id");
        }
    }
}

fn normalise_columns(o: &mut Map<String, Value>) {
    if let Some(cols) = o.get_mut("columns").and_then(|c| c.as_array_mut()) {
        for (i, c) in cols.iter_mut().enumerate() {
            if let Some(co) = c.as_object_mut() {
                co.entry("position").or_insert(json!(i));
                co.entry("nullable").or_insert(json!(true));
                if !co.contains_key("type_name") {
                    let tt = co.get("type_text").and_then(|v| v.as_str()).unwrap_or("string").to_ascii_uppercase();
                    co.insert("type_name".into(), json!(tt));
                }
                if !co.contains_key("type_text") {
                    let tt = co.get("type_name").and_then(|v| v.as_str()).unwrap_or("string").to_ascii_lowercase();
                    co.insert("type_text".into(), json!(tt));
                }
            }
        }
    }
}

/// Keep user-supplied metadata (comment, mask, tags) while taking the type
/// information from the engine schema.
pub fn merge_columns(existing: Option<&Value>, engine: Vec<Value>) -> Vec<Value> {
    let prev: HashMap<String, &Value> = existing.and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|c| c["name"].as_str().map(|n| (n.to_string(), c))).collect()).unwrap_or_default();
    engine
        .into_iter()
        .map(|mut c| {
            if let Some(old) = c["name"].as_str().and_then(|n| prev.get(n)) {
                if let (Some(co), Some(oo)) = (c.as_object_mut(), old.as_object()) {
                    for k in ["comment", "mask", "tags", "type_precision", "type_scale", "partition_index"] {
                        if let Some(v) = oo.get(k) {
                            co.entry(k).or_insert(v.clone());
                        }
                    }
                }
            }
            c
        })
        .collect()
}

pub fn columns_from_engine_schema(schema: &Value) -> Vec<Value> {
    let Some(fields) = schema.as_array() else { return vec![] };
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let name = f.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let dt = f.get("data_type").or_else(|| f.get("type")).map(|v| v.to_string()).unwrap_or_default();
            let type_name = map_arrow_type_text(&dt);
            json!({ "name": name, "type_text": type_name.to_ascii_lowercase(), "type_name": type_name, "position": i, "nullable": f.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true), "type_json": dt })
        })
        .collect()
}

fn map_arrow_type_text(dt: &str) -> String {
    let d = dt.trim_matches('"');
    let up = d.to_ascii_uppercase();
    if up.contains("UTF8") {
        "STRING".into()
    } else if up.contains("INT64") {
        "LONG".into()
    } else if up.contains("INT32") {
        "INT".into()
    } else if up.contains("INT16") {
        "SHORT".into()
    } else if up.contains("INT8") {
        "BYTE".into()
    } else if up.contains("FLOAT64") {
        "DOUBLE".into()
    } else if up.contains("FLOAT32") {
        "FLOAT".into()
    } else if up.contains("BOOLEAN") {
        "BOOLEAN".into()
    } else if up.contains("TIMESTAMP") {
        "TIMESTAMP".into()
    } else if up.contains("DATE") {
        "DATE".into()
    } else if up.contains("DECIMAL") {
        "DECIMAL".into()
    } else if up.contains("BINARY") {
        "BINARY".into()
    } else if up.contains("LIST") {
        "ARRAY".into()
    } else if up.contains("STRUCT") {
        "STRUCT".into()
    } else if up.contains("MAP") {
        "MAP".into()
    } else {
        up
    }
}

pub fn merge_patch(v: &mut Value, o: &Map<String, Value>, allowed: &[&str], user: &str) {
    if let Some(obj) = v.as_object_mut() {
        for (k, val) in o {
            if allowed.contains(&k.as_str()) || allowed.contains(&"*") {
                if k == "new_name" {
                    continue;
                }
                if k == "properties" {
                    if let (Some(Value::Object(cur)), Some(new)) = (obj.get_mut("properties"), val.as_object()) {
                        for (pk, pv) in new {
                            if pv.is_null() {
                                cur.remove(pk);
                            } else {
                                cur.insert(pk.clone(), pv.clone());
                            }
                        }
                        continue;
                    }
                }
                obj.insert(k.clone(), val.clone());
            }
        }
        obj.insert("updated_at".into(), json!(now_ms()));
        obj.insert("updated_by".into(), json!(user));
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn metastores(State(st): State<S>) -> Json<Value> {
    Json(json!({ "metastores": [metastore_summary(&st)] }))
}

pub fn metastore_summary(st: &AppState) -> Value {
    json!({
        "metastore_id": METASTORE_ID,
        "name": "lakeforge",
        "owner": st.config.admin_user,
        "region": st.config.cloud,
        "cloud": st.config.cloud,
        "default_data_access_config_id": null,
        "storage_root": st.storage.url_for("/metastore"),
        "created_at": st.started_at.timestamp_millis(),
        "created_by": st.config.admin_user,
        "global_metastore_id": format!("{}:{}", st.config.cloud, METASTORE_ID),
        "delta_sharing_scope": "INTERNAL_AND_EXTERNAL",
        "delta_sharing_recipient_token_lifetime_in_seconds": 86400 * 30,
        "delta_sharing_organization_name": "lakeforge",
        "privilege_model_version": "1.0",
        "external_access_enabled": true,
    })
}

async fn metastore_summary_h(State(st): State<S>) -> Json<Value> {
    Json(metastore_summary(&st))
}

async fn get_metastore(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    if id != METASTORE_ID {
        return Err(ApiError::NotFound(format!("Metastore '{id}' does not exist.")));
    }
    Ok(Json(metastore_summary(&st)))
}

async fn current_assignment(State(st): State<S>) -> Json<Value> {
    Json(json!({ "metastore_id": METASTORE_ID, "workspace_id": st.ws(), "default_catalog_name": DEFAULT_CATALOG }))
}

async fn workspace_assignments(State(st): State<S>) -> Json<Value> {
    Json(json!({ "workspace_ids": [st.ws()] }))
}

// Catalogs

async fn list_catalogs(State(st): State<S>, Who(p): Who) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "catalogs": st.uc_list_catalogs(&p).await? })))
}

async fn create_catalog(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_create_catalog(&p, o).await?))
}

async fn get_catalog(State(st): State<S>, Who(p): Who, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    st.ensure_default_catalog().await?;
    Ok(Json(st.uc_get_visible(&p, KIND_CATALOG, Securable::Catalog, &name, "Catalog").await?))
}

async fn update_catalog(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_patch(&p, Securable::Catalog, KIND_CATALOG, &name, &o, &["comment", "owner", "properties", "isolation_mode", "enable_predictive_optimization", "options", "new_name"], "Catalog").await?))
}

#[derive(Debug, Deserialize)]
struct ForceQ {
    #[serde(default)]
    force: bool,
}

async fn delete_catalog(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Query(q): Query<ForceQ>) -> ApiResult<Json<Value>> {
    st.uc_delete_catalog(&p, &name, q.force).await?;
    Ok(empty())
}

// Schemas

#[derive(Debug, Deserialize)]
struct SchemaListQ {
    catalog_name: String,
}

async fn list_schemas(State(st): State<S>, Who(p): Who, Query(q): Query<SchemaListQ>) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "schemas": st.uc_list_schemas(&p, &q.catalog_name).await? })))
}

async fn create_schema(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_create_schema(&p, o).await?))
}

async fn get_schema(State(st): State<S>, Who(p): Who, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    st.ensure_default_catalog().await?;
    Ok(Json(st.uc_get_visible(&p, KIND_SCHEMA, Securable::Schema, &full, "Schema").await?))
}

async fn update_schema(State(st): State<S>, Who(p): Who, Path(full): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_patch(&p, Securable::Schema, KIND_SCHEMA, &full, &o, &["comment", "owner", "properties", "enable_predictive_optimization", "new_name"], "Schema").await?))
}

async fn delete_schema(State(st): State<S>, Who(p): Who, Path(full): Path<String>, Query(q): Query<ForceQ>) -> ApiResult<Json<Value>> {
    st.uc_delete_schema(&p, &full, q.force).await?;
    Ok(empty())
}

// Tables

#[derive(Debug, Deserialize)]
struct TableListQ {
    catalog_name: String,
    schema_name: String,
    #[serde(default)]
    max_results: Option<i64>,
    #[serde(default)]
    omit_columns: Option<bool>,
    #[serde(default)]
    omit_properties: Option<bool>,
}

async fn list_tables(State(st): State<S>, Who(p): Who, Query(q): Query<TableListQ>) -> ApiResult<Json<Value>> {
    let mut items = st.uc_list_tables(&p, &q.catalog_name, &q.schema_name, q.max_results).await?;
    for it in &mut items {
        if let Some(o) = it.as_object_mut() {
            if q.omit_columns == Some(true) {
                o.remove("columns");
            }
            if q.omit_properties == Some(true) {
                o.remove("properties");
            }
        }
    }
    Ok(Json(json!({ "tables": items })))
}

#[derive(Debug, Deserialize)]
struct TableSummaryQ {
    catalog_name: String,
    #[serde(default)]
    schema_name_pattern: Option<String>,
    #[serde(default)]
    table_name_pattern: Option<String>,
}

async fn table_summaries(State(st): State<S>, Who(p): Who, Query(q): Query<TableSummaryQ>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_TABLE, st.ws(), Filter { name_prefix: Some(&format!("{}.", q.catalog_name)), ..Default::default() }).await?;
    let like = |pat: &Option<String>, s: &str| pat.as_ref().map(|p| glob_like(p, s)).unwrap_or(true);
    let visible = st.uc_visible(&p, Securable::Table, docs.into_iter().map(|d| d.data).collect()).await?;
    let items: Vec<Value> = visible
        .into_iter()
        .filter(|d| like(&q.schema_name_pattern, d["schema_name"].as_str().unwrap_or("")) && like(&q.table_name_pattern, d["name"].as_str().unwrap_or("")))
        .map(|d| json!({ "full_name": d["full_name"], "table_type": d["table_type"] }))
        .collect();
    Ok(Json(json!({ "tables": items })))
}

pub fn glob_like(pat: &str, s: &str) -> bool {
    let p = pat.replace('%', "*");
    if !p.contains('*') {
        return p.eq_ignore_ascii_case(s);
    }
    let parts: Vec<&str> = p.split('*').collect();
    let mut pos = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        match s[pos..].to_ascii_lowercase().find(&part.to_ascii_lowercase()) {
            Some(k) => {
                if i == 0 && k != 0 {
                    return false;
                }
                pos += k + part.len();
            }
            None => return false,
        }
    }
    parts.last().map(|l| l.is_empty()).unwrap_or(true) || pos == s.len()
}

async fn create_table(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_create_table(&p, o).await?))
}

async fn get_table(State(st): State<S>, Who(p): Who, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_get_visible(&p, KIND_TABLE, Securable::Table, &full, "Table").await?))
}

async fn table_exists(State(st): State<S>, Who(p): Who, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    let exists = match st.uc_get_visible(&p, KIND_TABLE, Securable::Table, &full, "Table").await {
        Ok(_) => true,
        Err(ApiError::NotFound(_)) | Err(ApiError::PermissionDenied(_)) => false,
        Err(e) => return Err(e),
    };
    Ok(Json(json!({ "table_exists": exists })))
}

async fn update_table(State(st): State<S>, Who(p): Who, Path(full): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_patch(&p, Securable::Table, KIND_TABLE, &full, &o, &["comment", "owner", "properties", "columns", "storage_location", "data_source_format", "new_name", "row_filter"], "Table").await?))
}

async fn delete_table(State(st): State<S>, Who(p): Who, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    st.uc_delete_table(&p, &full).await?;
    Ok(empty())
}

// Volumes / functions

#[derive(Debug, Deserialize)]
struct SchemaScopedQ {
    catalog_name: String,
    schema_name: String,
}

async fn list_volumes(State(st): State<S>, Who(p): Who, Query(q): Query<SchemaScopedQ>) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "volumes": st.uc_list_in_schema(&p, KIND_VOLUME, Securable::Volume, &q.catalog_name, &q.schema_name).await? })))
}
async fn create_volume(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_create_in_schema(&p, KIND_VOLUME, "Volume", o).await?))
}
async fn get_volume(State(st): State<S>, Who(p): Who, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_get_visible(&p, KIND_VOLUME, Securable::Volume, &full, "Volume").await?))
}
async fn update_volume(State(st): State<S>, Who(p): Who, Path(full): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_patch(&p, Securable::Volume, KIND_VOLUME, &full, &o, &["comment", "owner", "new_name"], "Volume").await?))
}
async fn delete_volume(State(st): State<S>, Who(p): Who, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    st.uc_delete_in_schema(&p, KIND_VOLUME, Securable::Volume, &full, "Volume").await?;
    Ok(empty())
}

async fn list_functions(State(st): State<S>, Who(p): Who, Query(q): Query<SchemaScopedQ>) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "functions": st.uc_list_in_schema(&p, KIND_FUNCTION, Securable::Function, &q.catalog_name, &q.schema_name).await? })))
}
async fn create_function(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let inner = o.get("function_info").and_then(|v| v.as_object()).cloned().unwrap_or(o);
    Ok(Json(st.uc_create_in_schema(&p, KIND_FUNCTION, "Function", inner).await?))
}
async fn get_function(State(st): State<S>, Who(p): Who, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_get_visible(&p, KIND_FUNCTION, Securable::Function, &full, "Function").await?))
}
async fn update_function(State(st): State<S>, Who(p): Who, Path(full): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_patch(&p, Securable::Function, KIND_FUNCTION, &full, &o, &["comment", "owner"], "Function").await?))
}
async fn delete_function(State(st): State<S>, Who(p): Who, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    st.uc_delete_in_schema(&p, KIND_FUNCTION, Securable::Function, &full, "Function").await?;
    Ok(empty())
}

// Top-level named securables

async fn list_ext_locations(State(st): State<S>, Who(p): Who) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "external_locations": st.uc_list_named(&p, KIND_EXT_LOCATION, Securable::ExternalLocation).await? })))
}
async fn create_ext_location(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    str_field(&o, "url")?;
    Ok(Json(st.uc_create_named(&p, KIND_EXT_LOCATION, Securable::ExternalLocation, "External Location", o).await?))
}
async fn get_ext_location(State(st): State<S>, Who(p): Who, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_get_visible(&p, KIND_EXT_LOCATION, Securable::ExternalLocation, &name, "External Location").await?))
}
async fn update_ext_location(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_patch(&p, Securable::ExternalLocation, KIND_EXT_LOCATION, &name, &o, &["*"], "External Location").await?))
}
async fn delete_ext_location(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Query(q): Query<ForceQ>) -> ApiResult<Json<Value>> {
    st.uc_delete_named(&p, KIND_EXT_LOCATION, Securable::ExternalLocation, &name, "External Location", q.force).await?;
    Ok(empty())
}

async fn list_storage_creds(State(st): State<S>, Who(p): Who) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "storage_credentials": st.uc_list_named(&p, KIND_STORAGE_CRED, Securable::StorageCredential).await? })))
}
async fn create_storage_cred(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_create_named(&p, KIND_STORAGE_CRED, Securable::StorageCredential, "Storage Credential", o).await?))
}
async fn get_storage_cred(State(st): State<S>, Who(p): Who, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_get_visible(&p, KIND_STORAGE_CRED, Securable::StorageCredential, &name, "Storage Credential").await?))
}
async fn update_storage_cred(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(mut o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    redact_credential(&mut o);
    Ok(Json(st.uc_patch(&p, Securable::StorageCredential, KIND_STORAGE_CRED, &name, &o, &["*"], "Storage Credential").await?))
}
async fn delete_storage_cred(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Query(q): Query<ForceQ>) -> ApiResult<Json<Value>> {
    st.uc_delete_named(&p, KIND_STORAGE_CRED, Securable::StorageCredential, &name, "Storage Credential", q.force).await?;
    Ok(empty())
}

async fn list_connections(State(st): State<S>, Who(p): Who) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "connections": st.uc_list_named(&p, KIND_CONNECTION, Securable::Connection).await? })))
}
async fn create_connection(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_create_named(&p, KIND_CONNECTION, Securable::Connection, "Connection", o).await?))
}
async fn get_connection(State(st): State<S>, Who(p): Who, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_get_visible(&p, KIND_CONNECTION, Securable::Connection, &name, "Connection").await?))
}
async fn update_connection(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_patch(&p, Securable::Connection, KIND_CONNECTION, &name, &o, &["comment", "owner", "options", "new_name"], "Connection").await?))
}
async fn delete_connection(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Query(q): Query<ForceQ>) -> ApiResult<Json<Value>> {
    st.uc_delete_named(&p, KIND_CONNECTION, Securable::Connection, &name, "Connection", q.force).await?;
    Ok(empty())
}

// Grants

#[derive(Debug, Deserialize)]
struct GrantsQ {
    #[serde(default)]
    principal: Option<String>,
}

async fn get_grants(State(st): State<S>, Who(p): Who, Path((securable_type, full)): Path<(String, String)>, Query(q): Query<GrantsQ>) -> ApiResult<Json<Value>> {
    let sec = Securable::parse(&securable_type)?;
    if !st.uc_exists(sec, &full).await? {
        return Err(ApiError::NotFound(format!("{} '{full}' does not exist.", sec.api_type())));
    }
    let mut az = Authorizer::new(&st, &p);
    if !az.can_browse(sec, &full).await? && sec != Securable::Metastore {
        return Err(ApiError::PermissionDenied(format!("[INSUFFICIENT_PERMISSIONS] User {} cannot view grants on {} '{full}'.", p.user_name, sec.api_type())));
    }
    let a = st.grants_for(sec, &full).await?;
    let filtered: Vec<_> = a.into_iter().filter(|x| q.principal.as_ref().map(|pp| x.principal.eq_ignore_ascii_case(pp)).unwrap_or(true)).collect();
    Ok(Json(crate::uc::privileges::assignments_json(&filtered)))
}

async fn get_effective(State(st): State<S>, Who(p): Who, Path((securable_type, full)): Path<(String, String)>, Query(q): Query<GrantsQ>) -> ApiResult<Json<Value>> {
    let sec = Securable::parse(&securable_type)?;
    if !st.uc_exists(sec, &full).await? {
        return Err(ApiError::NotFound(format!("{} '{full}' does not exist.", sec.api_type())));
    }
    let mut az = Authorizer::new(&st, &p);
    if !az.can_browse(sec, &full).await? && sec != Securable::Metastore {
        return Err(ApiError::PermissionDenied(format!("[INSUFFICIENT_PERMISSIONS] User {} cannot view grants on {} '{full}'.", p.user_name, sec.api_type())));
    }
    Ok(Json(az.effective(sec, &full, q.principal.as_deref()).await?))
}

#[derive(Debug, Deserialize)]
pub struct GrantChange {
    pub principal: String,
    #[serde(default)]
    pub add: Vec<String>,
    #[serde(default)]
    pub remove: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GrantPatch {
    #[serde(default)]
    pub changes: Vec<GrantChange>,
}

impl AppState {
    /// Grant/revoke with authorization: the caller must own the securable
    /// (or hold MANAGE); privileges must be valid; principals must exist.
    pub async fn uc_update_grants(&self, p: &Principal, sec: Securable, full: &str, changes: Vec<GrantChange>) -> ApiResult<Value> {
        if !self.uc_exists(sec, full).await? {
            return Err(ApiError::NotFound(format!("{} '{full}' does not exist.", sec.api_type())));
        }
        let mut az = Authorizer::new(self, p);
        if sec == Securable::Metastore {
            if !p.is_admin {
                return Err(ApiError::PermissionDenied("Only metastore admins can grant on the metastore.".into()));
            }
        } else {
            az.require_owner(sec, full).await?;
        }
        for ch in &changes {
            if !self.principal_exists(&ch.principal).await? {
                return Err(ApiError::invalid(format!("Principal '{}' does not exist", ch.principal)));
            }
            for a in ch.add.iter().chain(ch.remove.iter()) {
                let n = normalize_privilege(a);
                if !crate::uc::privileges::is_known_privilege(&n) {
                    return Err(ApiError::invalid(format!("Privilege {a} is not supported")));
                }
            }
        }
        let mut out = vec![];
        for ch in changes {
            out = self.change_grants(sec, full, &ch.principal, &ch.add, &ch.remove).await?;
        }
        Ok(crate::uc::privileges::assignments_json(&out))
    }
}

async fn update_grants(State(st): State<S>, Who(p): Who, Path((securable_type, full)): Path<(String, String)>, Body(b): Body<GrantPatch>) -> ApiResult<Json<Value>> {
    let sec = Securable::parse(&securable_type)?;
    Ok(Json(st.uc_update_grants(&p, sec, &full, b.changes).await?))
}

/// Databricks-style `SHOW`-like browse helper for the UI: everything under a schema.
#[derive(Debug, Deserialize)]
struct BrowseQ {
    #[serde(default)]
    catalog_name: Option<String>,
    #[serde(default)]
    schema_name: Option<String>,
}

async fn browse(State(st): State<S>, Who(p): Who, Query(q): Query<BrowseQ>) -> ApiResult<Json<Value>> {
    st.ensure_default_catalog().await?;
    match (q.catalog_name, q.schema_name) {
        (None, _) => Ok(Json(json!({ "catalogs": st.uc_list_catalogs(&p).await? }))),
        (Some(c), None) => Ok(Json(json!({ "schemas": st.uc_list_schemas(&p, &c).await? }))),
        (Some(c), Some(s)) => {
            let tables = st.uc_list_tables(&p, &c, &s, None).await?;
            let (volumes, functions, models) = if c == SYSTEM_CATALOG || s == "information_schema" {
                (vec![], vec![], vec![])
            } else {
                (
                    st.uc_list_in_schema(&p, KIND_VOLUME, Securable::Volume, &c, &s).await?,
                    st.uc_list_in_schema(&p, KIND_FUNCTION, Securable::Function, &c, &s).await?,
                    st.uc_list_in_schema(&p, crate::uc::privileges::KIND_MODEL, Securable::Function, &c, &s).await?,
                )
            };
            Ok(Json(json!({ "tables": tables, "volumes": volumes, "functions": functions, "models": models })))
        }
    }
}

pub fn router() -> Router<S> {
    let uc = Router::new()
        .route("/metastores", get(metastores))
        .route("/metastores/{id}", get(get_metastore))
        .route("/metastore_summary", get(metastore_summary_h))
        .route("/current-metastore-assignment", get(current_assignment))
        .route("/workspaces/{ws}/metastore", get(current_assignment))
        .route("/metastores/{id}/workspaces", get(workspace_assignments))
        .route("/catalogs", get(list_catalogs).post(create_catalog))
        .route("/catalogs/{name}", get(get_catalog).patch(update_catalog).delete(delete_catalog))
        .route("/schemas", get(list_schemas).post(create_schema))
        .route("/schemas/{full_name}", get(get_schema).patch(update_schema).delete(delete_schema))
        .route("/tables", get(list_tables).post(create_table))
        .route("/table-summaries", get(table_summaries))
        .route("/tables/{full_name}", get(get_table).patch(update_table).delete(delete_table))
        .route("/tables/{full_name}/exists", get(table_exists))
        .route("/volumes", get(list_volumes).post(create_volume))
        .route("/volumes/{full_name}", get(get_volume).patch(update_volume).delete(delete_volume))
        .route("/functions", get(list_functions).post(create_function))
        .route("/functions/{full_name}", get(get_function).patch(update_function).delete(delete_function))
        .route("/external-locations", get(list_ext_locations).post(create_ext_location))
        .route("/external-locations/{name}", get(get_ext_location).patch(update_ext_location).delete(delete_ext_location))
        .route("/storage-credentials", get(list_storage_creds).post(create_storage_cred))
        .route("/storage-credentials/{name}", get(get_storage_cred).patch(update_storage_cred).delete(delete_storage_cred))
        .route("/connections", get(list_connections).post(create_connection))
        .route("/connections/{name}", get(get_connection).patch(update_connection).delete(delete_connection))
        .route("/permissions/{securable_type}/{full_name}", get(get_grants).patch(update_grants))
        .route("/effective-permissions/{securable_type}/{full_name}", get(get_effective))
        .merge(super::catalog_ext::router());
    Router::new()
        .nest("/api/2.1/unity-catalog", uc.clone())
        .nest("/api/2.0/unity-catalog", uc)
        .route("/api/2.0/lakeforge/catalog/browse", get(browse))
        .route("/api/2.0/lakeforge/catalog/grants/{securable_type}/{full_name}", patch(update_grants).post(update_grants))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_and_glob() {
        assert_eq!(split_name("a.b.c"), ("a".into(), "b".into(), "c".into()));
        assert_eq!(split_name("t"), ("main".into(), "default".into(), "t".into()));
        assert!(glob_like("sal%", "sales"));
        assert!(glob_like("*orders", "raw_orders"));
        assert!(!glob_like("x%", "sales"));
    }

    #[test]
    fn merge_keeps_user_metadata() {
        let existing = json!([{ "name": "id", "type_name": "INT", "comment": "pk", "mask": { "function_name": "m" } }]);
        let engine = vec![json!({ "name": "id", "type_name": "LONG", "position": 0 }), json!({ "name": "x", "type_name": "STRING", "position": 1 })];
        let merged = merge_columns(Some(&existing), engine);
        assert_eq!(merged[0]["type_name"], "LONG");
        assert_eq!(merged[0]["comment"], "pk");
        assert_eq!(merged[0]["mask"]["function_name"], "m");
        assert!(merged[1].get("comment").is_none());
    }
}
