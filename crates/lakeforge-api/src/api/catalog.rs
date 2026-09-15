//! Unity-Catalog-compatible metastore (`/api/2.1/unity-catalog/*`).
//!
//! Securables are stored as JSON docs keyed by full name. Tables with a
//! storage location are pushed to every running Forge cluster so the engine's
//! `catalog.schema.table` namespace mirrors the metastore.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::routing::{get, patch};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::clusters::{Cluster, ClusterState, KIND as CLUSTER_KIND};
use super::{empty, Body, S};
use crate::auth::Who;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};

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

fn id_of(kind: &str, full_name: &str) -> String {
    format!("{kind}:{full_name}")
}

fn base_fields(o: &mut Map<String, Value>, owner: &str) {
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

fn str_field(o: &Map<String, Value>, k: &str) -> ApiResult<String> {
    o.get(k).and_then(|v| v.as_str()).map(str::to_string).ok_or_else(|| ApiError::invalid(format!("{k} is required")))
}

impl AppState {
    pub async fn ensure_default_catalog(&self) -> ApiResult<()> {
        if self.store.get::<Value>(KIND_CATALOG, &id_of(KIND_CATALOG, DEFAULT_CATALOG)).await?.is_none() {
            let mut o = Map::new();
            o.insert("name".into(), json!(DEFAULT_CATALOG));
            o.insert("catalog_type".into(), json!("MANAGED_CATALOG"));
            o.insert("full_name".into(), json!(DEFAULT_CATALOG));
            o.insert("isolation_mode".into(), json!("OPEN"));
            o.insert("securable_type".into(), json!("CATALOG"));
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
        Ok(())
    }

    async fn running_clusters(&self) -> ApiResult<Vec<Cluster>> {
        let docs: Vec<Doc<Cluster>> = self.store.list(CLUSTER_KIND, self.ws(), Filter::default()).await?;
        Ok(docs.into_iter().map(|d| d.data).filter(|c| c.state == ClusterState::Running && c.handle.is_some()).collect())
    }

    /// Register one metastore table on a driver; returns the engine schema.
    pub async fn register_table_on(&self, driver_addr: &str, table: &Value) -> ApiResult<Option<Value>> {
        let (Some(loc), Some(cat), Some(sch), Some(name)) = (
            table.get("storage_location").and_then(|v| v.as_str()),
            table.get("catalog_name").and_then(|v| v.as_str()),
            table.get("schema_name").and_then(|v| v.as_str()),
            table.get("name").and_then(|v| v.as_str()),
        ) else {
            return Ok(None);
        };
        if table.get("table_type").and_then(|v| v.as_str()) == Some("VIEW") {
            return Ok(None);
        }
        let format = table.get("data_source_format").and_then(|v| v.as_str()).unwrap_or("DELTA").to_ascii_lowercase();
        let options: HashMap<String, String> = table
            .get("properties")
            .and_then(|p| p.as_object())
            .map(|p| p.iter().filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string()))).collect())
            .unwrap_or_default();
        let client = self.forge.client(driver_addr).await?;
        let schema_json = client.register_table_in(cat, sch, name, &format, loc, options).await?;
        Ok(serde_json::from_str(&schema_json).ok())
    }

    /// Push every table in the metastore to `cluster`'s driver.
    pub async fn sync_metastore_to_cluster(&self, cluster: &Cluster) -> ApiResult<()> {
        let Some(h) = &cluster.handle else { return Ok(()) };
        let tables: Vec<Doc<Value>> = self.store.list(KIND_TABLE, self.ws(), Filter::default()).await?;
        let mut ok = 0;
        for t in tables {
            match self.register_table_on(&h.driver_addr, &t.data).await {
                Ok(Some(schema)) => {
                    ok += 1;
                    let cols = columns_from_engine_schema(&schema);
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

    /// Observe DDL executed through the SQL path and mirror it into the metastore.
    pub async fn observe_ddl(&self, sql: &str, user: &str) {
        let Some(ddl) = parse_ddl(sql) else { return };
        match ddl {
            Ddl::CreateTable { name, format, location } => {
                let (cat, sch, tbl) = split_name(&name);
                let mut o = Map::new();
                o.insert("name".into(), json!(tbl));
                o.insert("catalog_name".into(), json!(cat));
                o.insert("schema_name".into(), json!(sch));
                o.insert("full_name".into(), json!(format!("{cat}.{sch}.{tbl}")));
                o.insert("table_type".into(), json!(if location.is_some() { "EXTERNAL" } else { "MANAGED" }));
                o.insert("data_source_format".into(), json!(format.unwrap_or_else(|| "DELTA".into())));
                let loc = location.unwrap_or_else(|| self.managed_table_location(&cat, &sch, &tbl));
                o.insert("storage_location".into(), json!(loc));
                o.insert("securable_type".into(), json!("TABLE"));
                if let Err(e) = self.upsert_table(o, user).await {
                    tracing::warn!(error = %e, "ddl mirror failed");
                }
            }
            Ddl::DropTable(name) => {
                let (cat, sch, tbl) = split_name(&name);
                let _ = self.store.delete(KIND_TABLE, &id_of(KIND_TABLE, &format!("{cat}.{sch}.{tbl}"))).await;
            }
            Ddl::CreateSchema(name) => {
                let (cat, sch) = match name.split_once('.') {
                    Some((c, s)) => (c.to_string(), s.to_string()),
                    None => (DEFAULT_CATALOG.to_string(), name),
                };
                let mut o = Map::new();
                o.insert("name".into(), json!(sch));
                o.insert("catalog_name".into(), json!(cat));
                o.insert("full_name".into(), json!(format!("{cat}.{sch}")));
                o.insert("securable_type".into(), json!("SCHEMA"));
                base_fields(&mut o, user);
                let full = format!("{cat}.{sch}");
                let _ = self.store.upsert(KIND_SCHEMA, self.ws(), &id_of(KIND_SCHEMA, &full), Some(&cat), Some(&full), &Value::Object(o)).await;
            }
            Ddl::DropSchema(name) => {
                let full = if name.contains('.') { name } else { format!("{DEFAULT_CATALOG}.{name}") };
                let _ = self.store.delete(KIND_SCHEMA, &id_of(KIND_SCHEMA, &full)).await;
            }
        }
    }

    async fn upsert_table(&self, mut o: Map<String, Value>, user: &str) -> ApiResult<Value> {
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
        let v = Value::Object(o);
        let cols = self.broadcast_table(&v).await;
        let mut v = v;
        if !cols.is_empty() {
            v["columns"] = Value::Array(cols);
        }
        let parent = format!("{}.{}", v["catalog_name"].as_str().unwrap_or(""), v["schema_name"].as_str().unwrap_or(""));
        self.store.upsert(KIND_TABLE, self.ws(), &id_of(KIND_TABLE, &full), Some(&parent), Some(&full), &v).await?;
        Ok(v)
    }
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

pub enum Ddl {
    CreateTable { name: String, format: Option<String>, location: Option<String> },
    DropTable(String),
    CreateSchema(String),
    DropSchema(String),
}

fn unquote(s: &str) -> String {
    s.split('.').map(|p| p.trim_matches(|c| c == '`' || c == '"')).collect::<Vec<_>>().join(".")
}

/// Best-effort parse of table/schema DDL; not a full SQL parser.
pub fn parse_ddl(sql: &str) -> Option<Ddl> {
    let toks: Vec<String> = sql.split_whitespace().map(|t| t.trim_end_matches(';').to_string()).collect();
    let up: Vec<String> = toks.iter().map(|t| t.to_ascii_uppercase()).collect();
    if up.is_empty() {
        return None;
    }
    let idx = |word: &str| up.iter().position(|t| t == word);
    match up[0].as_str() {
        "CREATE" => {
            if let Some(i) = idx("TABLE") {
                let mut j = i + 1;
                if up.get(j).map(|s| s.as_str()) == Some("IF") {
                    j += 2;
                }
                let name = unquote(toks.get(j)?.split('(').next()?);
                let format = idx("USING").or_else(|| idx("AS").filter(|&a| up.get(a + 1).is_some() && a > 0 && up[a - 1] == "STORED")).and_then(|k| toks.get(k + 1)).map(|f| f.to_ascii_uppercase());
                let format = format.or_else(|| idx("STORED").and_then(|k| if up.get(k + 1).map(|s| s.as_str()) == Some("AS") { toks.get(k + 2).map(|f| f.to_ascii_uppercase()) } else { None }));
                let location = idx("LOCATION").and_then(|k| toks.get(k + 1)).map(|l| l.trim_matches('\'').to_string());
                Some(Ddl::CreateTable { name, format, location })
            } else if let Some(i) = idx("SCHEMA").or_else(|| idx("DATABASE")) {
                let mut j = i + 1;
                if up.get(j).map(|s| s.as_str()) == Some("IF") {
                    j += 2;
                }
                Some(Ddl::CreateSchema(unquote(toks.get(j)?)))
            } else {
                None
            }
        }
        "DROP" => {
            if let Some(i) = idx("TABLE") {
                let mut j = i + 1;
                if up.get(j).map(|s| s.as_str()) == Some("IF") {
                    j += 2;
                }
                Some(Ddl::DropTable(unquote(toks.get(j)?)))
            } else if let Some(i) = idx("SCHEMA").or_else(|| idx("DATABASE")) {
                let mut j = i + 1;
                if up.get(j).map(|s| s.as_str()) == Some("IF") {
                    j += 2;
                }
                Some(Ddl::DropSchema(unquote(toks.get(j)?)))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub fn split_name(name: &str) -> (String, String, String) {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.as_slice() {
        [c, s, t] => (c.to_string(), s.to_string(), t.to_string()),
        [s, t] => (DEFAULT_CATALOG.into(), s.to_string(), t.to_string()),
        [t] => (DEFAULT_CATALOG.into(), "default".into(), t.to_string()),
        _ => (DEFAULT_CATALOG.into(), "default".into(), name.to_string()),
    }
}

// ---------------------------------------------------------------- handlers

async fn metastores(State(st): State<S>) -> Json<Value> {
    Json(json!({ "metastores": [metastore_summary(&st)] }))
}

fn metastore_summary(st: &AppState) -> Value {
    json!({
        "metastore_id": METASTORE_ID,
        "name": "lakeforge",
        "owner": st.config.admin_user,
        "region": st.config.cloud,
        "cloud": st.config.cloud,
        "default_data_access_config_id": null,
        "storage_root": st.storage.url_for("/metastore"),
        "created_at": st.started_at.timestamp_millis(),
        "global_metastore_id": format!("{}:{}", st.config.cloud, METASTORE_ID),
        "delta_sharing_scope": "INTERNAL",
        "privilege_model_version": "1.0",
    })
}

async fn metastore_summary_h(State(st): State<S>) -> Json<Value> {
    Json(metastore_summary(&st))
}

async fn current_assignment(State(st): State<S>) -> Json<Value> {
    Json(json!({ "metastore_id": METASTORE_ID, "workspace_id": st.ws(), "default_catalog_name": DEFAULT_CATALOG }))
}

// Catalogs

async fn list_catalogs(State(st): State<S>) -> ApiResult<Json<Value>> {
    st.ensure_default_catalog().await?;
    let docs: Vec<Doc<Value>> = st.store.list(KIND_CATALOG, st.ws(), Filter::default()).await?;
    Ok(Json(json!({ "catalogs": docs.into_iter().map(|d| d.data).collect::<Vec<_>>() })))
}

async fn create_catalog(State(st): State<S>, Who(p): Who, Body(mut o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let name = str_field(&o, "name")?;
    if st.store.get::<Value>(KIND_CATALOG, &id_of(KIND_CATALOG, &name)).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("Catalog '{name}' already exists")));
    }
    o.insert("full_name".into(), json!(name));
    o.entry("catalog_type").or_insert(json!("MANAGED_CATALOG"));
    o.entry("isolation_mode").or_insert(json!("OPEN"));
    o.insert("securable_type".into(), json!("CATALOG"));
    base_fields(&mut o, &p.user_name);
    let v = Value::Object(o);
    st.store.insert(KIND_CATALOG, st.ws(), &id_of(KIND_CATALOG, &name), None, Some(&name), &v).await?;
    // every catalog gets a default schema, like Databricks
    let full = format!("{name}.default");
    let mut s = Map::new();
    s.insert("name".into(), json!("default"));
    s.insert("catalog_name".into(), json!(name));
    s.insert("full_name".into(), json!(full));
    s.insert("securable_type".into(), json!("SCHEMA"));
    base_fields(&mut s, &p.user_name);
    st.store.insert(KIND_SCHEMA, st.ws(), &id_of(KIND_SCHEMA, &full), Some(&name), Some(&full), &Value::Object(s)).await?;
    Ok(Json(v))
}

async fn get_catalog(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    st.ensure_default_catalog().await?;
    Ok(Json(st.store.require::<Value>(KIND_CATALOG, &id_of(KIND_CATALOG, &name), "Catalog").await?.data))
}

async fn update_catalog(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let doc = st
        .store
        .update::<Value, _>(KIND_CATALOG, &id_of(KIND_CATALOG, &name), "Catalog", |v| {
            merge_patch(v, &o, &["comment", "owner", "properties", "isolation_mode", "enable_predictive_optimization", "new_name"], &p.user_name);
            Ok(())
        })
        .await?;
    Ok(Json(doc.data))
}

#[derive(Debug, Deserialize)]
struct ForceQ {
    #[serde(default)]
    force: bool,
}

async fn delete_catalog(State(st): State<S>, Path(name): Path<String>, Query(q): Query<ForceQ>) -> ApiResult<Json<Value>> {
    let schemas: Vec<Doc<Value>> = st.store.list(KIND_SCHEMA, st.ws(), Filter { parent_id: Some(&name), ..Default::default() }).await?;
    let non_default: Vec<_> = schemas.iter().filter(|s| s.data["name"] != "default" && s.data["name"] != "information_schema").collect();
    if !non_default.is_empty() && !q.force {
        return Err(ApiError::InvalidState(format!("Catalog '{name}' is not empty; use force=true")));
    }
    for s in schemas {
        st.store.delete_children(KIND_TABLE, s.name.as_deref().unwrap_or("")).await?;
        st.store.delete_children(KIND_VOLUME, s.name.as_deref().unwrap_or("")).await?;
        st.store.delete_children(KIND_FUNCTION, s.name.as_deref().unwrap_or("")).await?;
        st.store.delete(KIND_SCHEMA, &s.id).await?;
    }
    if !st.store.delete(KIND_CATALOG, &id_of(KIND_CATALOG, &name)).await? {
        return Err(ApiError::NotFound(format!("Catalog '{name}' does not exist")));
    }
    Ok(empty())
}

fn merge_patch(v: &mut Value, o: &Map<String, Value>, allowed: &[&str], user: &str) {
    if let Some(obj) = v.as_object_mut() {
        for (k, val) in o {
            if allowed.contains(&k.as_str()) || allowed.contains(&"*") {
                if k == "new_name" {
                    obj.insert("name".into(), val.clone());
                } else {
                    obj.insert(k.clone(), val.clone());
                }
            }
        }
        obj.insert("updated_at".into(), json!(now_ms()));
        obj.insert("updated_by".into(), json!(user));
    }
}

// Schemas

#[derive(Debug, Deserialize)]
struct SchemaListQ {
    catalog_name: String,
}

async fn list_schemas(State(st): State<S>, Query(q): Query<SchemaListQ>) -> ApiResult<Json<Value>> {
    st.ensure_default_catalog().await?;
    let docs: Vec<Doc<Value>> = st.store.list(KIND_SCHEMA, st.ws(), Filter { parent_id: Some(&q.catalog_name), ..Default::default() }).await?;
    Ok(Json(json!({ "schemas": docs.into_iter().map(|d| d.data).collect::<Vec<_>>() })))
}

async fn create_schema(State(st): State<S>, Who(p): Who, Body(mut o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let name = str_field(&o, "name")?;
    let cat = str_field(&o, "catalog_name")?;
    st.store.require::<Value>(KIND_CATALOG, &id_of(KIND_CATALOG, &cat), "Catalog").await?;
    let full = format!("{cat}.{name}");
    if st.store.get::<Value>(KIND_SCHEMA, &id_of(KIND_SCHEMA, &full)).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("Schema '{full}' already exists")));
    }
    o.insert("full_name".into(), json!(full));
    o.insert("securable_type".into(), json!("SCHEMA"));
    o.entry("catalog_type").or_insert(json!("MANAGED_CATALOG"));
    o.entry("storage_root").or_insert(json!(st.storage.url_for(&format!("/tables/{cat}/{name}"))));
    base_fields(&mut o, &p.user_name);
    let v = Value::Object(o);
    st.store.insert(KIND_SCHEMA, st.ws(), &id_of(KIND_SCHEMA, &full), Some(&cat), Some(&full), &v).await?;
    for c in st.running_clusters().await.unwrap_or_default() {
        if let Some(h) = &c.handle {
            if let Ok(cl) = st.forge.client(&h.driver_addr).await {
                let _ = cl.sql(&format!("CREATE SCHEMA IF NOT EXISTS {cat}.{name}")).await;
            }
        }
    }
    Ok(Json(v))
}

async fn get_schema(State(st): State<S>, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    st.ensure_default_catalog().await?;
    Ok(Json(st.store.require::<Value>(KIND_SCHEMA, &id_of(KIND_SCHEMA, &full), "Schema").await?.data))
}

async fn update_schema(State(st): State<S>, Who(p): Who, Path(full): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let doc = st
        .store
        .update::<Value, _>(KIND_SCHEMA, &id_of(KIND_SCHEMA, &full), "Schema", |v| {
            merge_patch(v, &o, &["comment", "owner", "properties", "enable_predictive_optimization"], &p.user_name);
            Ok(())
        })
        .await?;
    Ok(Json(doc.data))
}

async fn delete_schema(State(st): State<S>, Path(full): Path<String>, Query(q): Query<ForceQ>) -> ApiResult<Json<Value>> {
    let n = st.store.count(KIND_TABLE, st.ws(), Some(&full)).await?;
    if n > 0 && !q.force {
        return Err(ApiError::InvalidState(format!("Schema '{full}' is not empty; use force=true")));
    }
    st.store.delete_children(KIND_TABLE, &full).await?;
    st.store.delete_children(KIND_VOLUME, &full).await?;
    st.store.delete_children(KIND_FUNCTION, &full).await?;
    if !st.store.delete(KIND_SCHEMA, &id_of(KIND_SCHEMA, &full)).await? {
        return Err(ApiError::NotFound(format!("Schema '{full}' does not exist")));
    }
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
}

async fn list_tables(State(st): State<S>, Query(q): Query<TableListQ>) -> ApiResult<Json<Value>> {
    let parent = format!("{}.{}", q.catalog_name, q.schema_name);
    let docs: Vec<Doc<Value>> = st.store.list(KIND_TABLE, st.ws(), Filter { parent_id: Some(&parent), limit: q.max_results, ..Default::default() }).await?;
    let mut items: Vec<Value> = docs.into_iter().map(|d| d.data).collect();
    if q.omit_columns == Some(true) {
        for it in &mut items {
            if let Some(o) = it.as_object_mut() {
                o.remove("columns");
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

async fn table_summaries(State(st): State<S>, Query(q): Query<TableSummaryQ>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_TABLE, st.ws(), Filter { name_prefix: Some(&format!("{}.", q.catalog_name)), ..Default::default() }).await?;
    let like = |pat: &Option<String>, s: &str| pat.as_ref().map(|p| glob_like(p, s)).unwrap_or(true);
    let items: Vec<Value> = docs
        .into_iter()
        .filter(|d| like(&q.schema_name_pattern, d.data["schema_name"].as_str().unwrap_or("")) && like(&q.table_name_pattern, d.data["name"].as_str().unwrap_or("")))
        .map(|d| json!({ "full_name": d.data["full_name"], "table_type": d.data["table_type"] }))
        .collect();
    Ok(Json(json!({ "tables": items })))
}

fn glob_like(pat: &str, s: &str) -> bool {
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

async fn create_table(State(st): State<S>, Who(p): Who, Body(mut o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let name = str_field(&o, "name")?;
    let cat = str_field(&o, "catalog_name")?;
    let sch = str_field(&o, "schema_name")?;
    st.store.require::<Value>(KIND_SCHEMA, &id_of(KIND_SCHEMA, &format!("{cat}.{sch}")), "Schema").await?;
    let full = format!("{cat}.{sch}.{name}");
    if st.store.get::<Value>(KIND_TABLE, &id_of(KIND_TABLE, &full)).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("Table '{full}' already exists")));
    }
    o.insert("full_name".into(), json!(full));
    o.entry("table_type").or_insert(json!("MANAGED"));
    o.entry("data_source_format").or_insert(json!("DELTA"));
    o.insert("securable_type".into(), json!("TABLE"));
    if o.get("table_type").and_then(|v| v.as_str()) == Some("MANAGED") && !o.contains_key("storage_location") {
        o.insert("storage_location".into(), json!(st.managed_table_location(&cat, &sch, &name)));
    }
    if let Some(cols) = o.get_mut("columns").and_then(|c| c.as_array_mut()) {
        for (i, c) in cols.iter_mut().enumerate() {
            if let Some(co) = c.as_object_mut() {
                co.entry("position").or_insert(json!(i));
                co.entry("nullable").or_insert(json!(true));
                if !co.contains_key("type_name") {
                    let tt = co.get("type_text").and_then(|v| v.as_str()).unwrap_or("string").to_ascii_uppercase();
                    co.insert("type_name".into(), json!(tt));
                }
            }
        }
    }
    let v = st.upsert_table(o, &p.user_name).await?;
    Ok(Json(v))
}

async fn get_table(State(st): State<S>, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_TABLE, &id_of(KIND_TABLE, &full), "Table").await?.data))
}

async fn table_exists(State(st): State<S>, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(json!({ "table_exists": st.store.get::<Value>(KIND_TABLE, &id_of(KIND_TABLE, &full)).await?.is_some() })))
}

async fn update_table(State(st): State<S>, Who(p): Who, Path(full): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let doc = st
        .store
        .update::<Value, _>(KIND_TABLE, &id_of(KIND_TABLE, &full), "Table", |v| {
            merge_patch(v, &o, &["comment", "owner", "properties", "columns", "storage_location", "data_source_format"], &p.user_name);
            Ok(())
        })
        .await?;
    st.broadcast_table(&doc.data).await;
    Ok(Json(doc.data))
}

async fn delete_table(State(st): State<S>, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    let doc = st.store.require::<Value>(KIND_TABLE, &id_of(KIND_TABLE, &full), "Table").await?;
    st.store.delete(KIND_TABLE, &doc.id).await?;
    for c in st.running_clusters().await.unwrap_or_default() {
        if let Some(h) = &c.handle {
            if let Ok(cl) = st.forge.client(&h.driver_addr).await {
                let _ = cl.sql(&format!("DROP TABLE IF EXISTS {full}")).await;
            }
        }
    }
    if doc.data["table_type"] == "MANAGED" {
        if let Some(path) = doc.data["storage_location"].as_str().and_then(|l| st.storage.path_of(l)) {
            let _ = st.storage.delete_prefix(&path).await;
        }
    }
    Ok(empty())
}

// Generic schema-scoped securables (volumes, functions)

async fn list_in_schema(st: &AppState, kind: &str, key: &str, cat: &str, sch: &str) -> ApiResult<Json<Value>> {
    let parent = format!("{cat}.{sch}");
    let docs: Vec<Doc<Value>> = st.store.list(kind, st.ws(), Filter { parent_id: Some(&parent), ..Default::default() }).await?;
    Ok(Json(json!({ key: docs.into_iter().map(|d| d.data).collect::<Vec<_>>() })))
}

async fn create_in_schema(st: &AppState, kind: &str, securable: &str, user: &str, mut o: Map<String, Value>) -> ApiResult<Json<Value>> {
    let name = str_field(&o, "name")?;
    let cat = str_field(&o, "catalog_name")?;
    let sch = str_field(&o, "schema_name")?;
    st.store.require::<Value>(KIND_SCHEMA, &id_of(KIND_SCHEMA, &format!("{cat}.{sch}")), "Schema").await?;
    let full = format!("{cat}.{sch}.{name}");
    if st.store.get::<Value>(kind, &id_of(kind, &full)).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("{securable} '{full}' already exists")));
    }
    o.insert("full_name".into(), json!(full));
    o.insert("securable_type".into(), json!(securable.to_ascii_uppercase()));
    if kind == KIND_VOLUME {
        o.entry("volume_type").or_insert(json!("MANAGED"));
        o.entry("volume_id").or_insert(json!(uuid::Uuid::new_v4().to_string()));
        if !o.contains_key("storage_location") {
            o.insert("storage_location".into(), json!(st.storage.url_for(&format!("/Volumes/{cat}/{sch}/{name}"))));
            st.storage.mkdirs(&format!("/Volumes/{cat}/{sch}/{name}")).await?;
        }
    }
    if kind == KIND_FUNCTION {
        o.entry("function_id").or_insert(json!(uuid::Uuid::new_v4().to_string()));
        o.entry("routine_body").or_insert(json!("SQL"));
    }
    base_fields(&mut o, user);
    let v = Value::Object(o);
    st.store.insert(kind, st.ws(), &id_of(kind, &full), Some(&format!("{cat}.{sch}")), Some(&full), &v).await?;
    Ok(Json(v))
}

#[derive(Debug, Deserialize)]
struct SchemaScopedQ {
    catalog_name: String,
    schema_name: String,
}

async fn list_volumes(State(st): State<S>, Query(q): Query<SchemaScopedQ>) -> ApiResult<Json<Value>> {
    list_in_schema(&st, KIND_VOLUME, "volumes", &q.catalog_name, &q.schema_name).await
}
async fn create_volume(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    create_in_schema(&st, KIND_VOLUME, "Volume", &p.user_name, o).await
}
async fn get_volume(State(st): State<S>, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_VOLUME, &id_of(KIND_VOLUME, &full), "Volume").await?.data))
}
async fn update_volume(State(st): State<S>, Who(p): Who, Path(full): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let doc = st
        .store
        .update::<Value, _>(KIND_VOLUME, &id_of(KIND_VOLUME, &full), "Volume", |v| {
            merge_patch(v, &o, &["comment", "owner", "new_name"], &p.user_name);
            Ok(())
        })
        .await?;
    Ok(Json(doc.data))
}
async fn delete_volume(State(st): State<S>, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    let doc = st.store.require::<Value>(KIND_VOLUME, &id_of(KIND_VOLUME, &full), "Volume").await?;
    st.store.delete(KIND_VOLUME, &doc.id).await?;
    Ok(empty())
}

async fn list_functions(State(st): State<S>, Query(q): Query<SchemaScopedQ>) -> ApiResult<Json<Value>> {
    list_in_schema(&st, KIND_FUNCTION, "functions", &q.catalog_name, &q.schema_name).await
}
async fn create_function(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let inner = o.get("function_info").and_then(|v| v.as_object()).cloned().unwrap_or(o);
    create_in_schema(&st, KIND_FUNCTION, "Function", &p.user_name, inner).await
}
async fn get_function(State(st): State<S>, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_FUNCTION, &id_of(KIND_FUNCTION, &full), "Function").await?.data))
}
async fn delete_function(State(st): State<S>, Path(full): Path<String>) -> ApiResult<Json<Value>> {
    let doc = st.store.require::<Value>(KIND_FUNCTION, &id_of(KIND_FUNCTION, &full), "Function").await?;
    st.store.delete(KIND_FUNCTION, &doc.id).await?;
    Ok(empty())
}

// Top-level named securables (external locations, storage credentials, connections)

async fn list_named(st: &AppState, kind: &str, key: &str) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(kind, st.ws(), Filter::default()).await?;
    Ok(Json(json!({ key: docs.into_iter().map(|d| d.data).collect::<Vec<_>>() })))
}

async fn create_named(st: &AppState, kind: &str, securable: &str, user: &str, mut o: Map<String, Value>) -> ApiResult<Json<Value>> {
    let name = str_field(&o, "name")?;
    if st.store.get::<Value>(kind, &id_of(kind, &name)).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("{securable} '{name}' already exists")));
    }
    o.insert("id".into(), json!(uuid::Uuid::new_v4().to_string()));
    o.insert("securable_type".into(), json!(securable.to_ascii_uppercase().replace(' ', "_")));
    if kind == KIND_STORAGE_CRED {
        // never echo secrets back
        for k in ["aws_iam_role", "azure_service_principal", "gcp_service_account_key", "databricks_gcp_service_account"] {
            if let Some(Value::Object(c)) = o.get_mut(k) {
                c.remove("client_secret");
                c.remove("private_key");
            }
        }
    }
    base_fields(&mut o, user);
    let v = Value::Object(o);
    st.store.insert(kind, st.ws(), &id_of(kind, &name), None, Some(&name), &v).await?;
    Ok(Json(v))
}

async fn list_ext_locations(State(st): State<S>) -> ApiResult<Json<Value>> {
    list_named(&st, KIND_EXT_LOCATION, "external_locations").await
}
async fn create_ext_location(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    str_field(&o, "url")?;
    create_named(&st, KIND_EXT_LOCATION, "External Location", &p.user_name, o).await
}
async fn get_ext_location(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_EXT_LOCATION, &id_of(KIND_EXT_LOCATION, &name), "External Location").await?.data))
}
async fn update_ext_location(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let doc = st
        .store
        .update::<Value, _>(KIND_EXT_LOCATION, &id_of(KIND_EXT_LOCATION, &name), "External Location", |v| {
            merge_patch(v, &o, &["*"], &p.user_name);
            Ok(())
        })
        .await?;
    Ok(Json(doc.data))
}
async fn delete_ext_location(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    st.store.delete(KIND_EXT_LOCATION, &id_of(KIND_EXT_LOCATION, &name)).await?;
    Ok(empty())
}

async fn list_storage_creds(State(st): State<S>) -> ApiResult<Json<Value>> {
    list_named(&st, KIND_STORAGE_CRED, "storage_credentials").await
}
async fn create_storage_cred(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    create_named(&st, KIND_STORAGE_CRED, "Storage Credential", &p.user_name, o).await
}
async fn get_storage_cred(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_STORAGE_CRED, &id_of(KIND_STORAGE_CRED, &name), "Storage Credential").await?.data))
}
async fn update_storage_cred(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    let doc = st
        .store
        .update::<Value, _>(KIND_STORAGE_CRED, &id_of(KIND_STORAGE_CRED, &name), "Storage Credential", |v| {
            merge_patch(v, &o, &["*"], &p.user_name);
            Ok(())
        })
        .await?;
    Ok(Json(doc.data))
}
async fn delete_storage_cred(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    st.store.delete(KIND_STORAGE_CRED, &id_of(KIND_STORAGE_CRED, &name)).await?;
    Ok(empty())
}

async fn list_connections(State(st): State<S>) -> ApiResult<Json<Value>> {
    list_named(&st, KIND_CONNECTION, "connections").await
}
async fn create_connection(State(st): State<S>, Who(p): Who, Body(o): Body<Map<String, Value>>) -> ApiResult<Json<Value>> {
    create_named(&st, KIND_CONNECTION, "Connection", &p.user_name, o).await
}
async fn get_connection(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.store.require::<Value>(KIND_CONNECTION, &id_of(KIND_CONNECTION, &name), "Connection").await?.data))
}
async fn delete_connection(State(st): State<S>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    st.store.delete(KIND_CONNECTION, &id_of(KIND_CONNECTION, &name)).await?;
    Ok(empty())
}

// Grants

async fn get_grants(State(st): State<S>, Path((securable_type, full)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let id = id_of(KIND_GRANTS, &format!("{}:{full}", securable_type.to_ascii_lowercase()));
    let v = st.store.get::<Value>(KIND_GRANTS, &id).await?.map(|d| d.data).unwrap_or_else(|| json!({ "privilege_assignments": [] }));
    Ok(Json(v))
}

#[derive(Debug, Deserialize)]
struct GrantChange {
    principal: String,
    #[serde(default)]
    add: Vec<String>,
    #[serde(default)]
    remove: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct GrantPatch {
    #[serde(default)]
    changes: Vec<GrantChange>,
}

async fn update_grants(State(st): State<S>, Path((securable_type, full)): Path<(String, String)>, Body(b): Body<GrantPatch>) -> ApiResult<Json<Value>> {
    let key = format!("{}:{full}", securable_type.to_ascii_lowercase());
    let id = id_of(KIND_GRANTS, &key);
    let mut assignments: Vec<(String, Vec<String>)> = st
        .store
        .get::<Value>(KIND_GRANTS, &id)
        .await?
        .and_then(|d| d.data["privilege_assignments"].as_array().cloned())
        .unwrap_or_default()
        .iter()
        .map(|a| (a["principal"].as_str().unwrap_or("").to_string(), a["privileges"].as_array().map(|p| p.iter().filter_map(|x| x.as_str().map(str::to_string)).collect()).unwrap_or_default()))
        .collect();
    for ch in b.changes {
        let entry = match assignments.iter_mut().find(|(p, _)| *p == ch.principal) {
            Some(e) => e,
            None => {
                assignments.push((ch.principal.clone(), vec![]));
                assignments.last_mut().unwrap()
            }
        };
        for a in ch.add {
            let a = a.to_ascii_uppercase();
            if !entry.1.contains(&a) {
                entry.1.push(a);
            }
        }
        for r in ch.remove {
            let r = r.to_ascii_uppercase();
            entry.1.retain(|p| *p != r);
        }
    }
    assignments.retain(|(_, p)| !p.is_empty());
    let v = json!({ "privilege_assignments": assignments.iter().map(|(p, privs)| json!({ "principal": p, "privileges": privs })).collect::<Vec<_>>() });
    st.store.upsert(KIND_GRANTS, st.ws(), &id, None, Some(&key), &v).await?;
    Ok(Json(v))
}

/// Databricks-style `SHOW`-like browse helper for the UI: everything under a schema.
#[derive(Debug, Deserialize)]
struct BrowseQ {
    #[serde(default)]
    catalog_name: Option<String>,
    #[serde(default)]
    schema_name: Option<String>,
}

async fn browse(State(st): State<S>, Query(q): Query<BrowseQ>) -> ApiResult<Json<Value>> {
    st.ensure_default_catalog().await?;
    match (q.catalog_name, q.schema_name) {
        (None, _) => {
            let docs: Vec<Doc<Value>> = st.store.list(KIND_CATALOG, st.ws(), Filter::default()).await?;
            Ok(Json(json!({ "catalogs": docs.into_iter().map(|d| d.data).collect::<Vec<_>>() })))
        }
        (Some(c), None) => {
            let docs: Vec<Doc<Value>> = st.store.list(KIND_SCHEMA, st.ws(), Filter { parent_id: Some(&c), ..Default::default() }).await?;
            Ok(Json(json!({ "schemas": docs.into_iter().map(|d| d.data).collect::<Vec<_>>() })))
        }
        (Some(c), Some(s)) => {
            let parent = format!("{c}.{s}");
            let tables: Vec<Doc<Value>> = st.store.list(KIND_TABLE, st.ws(), Filter { parent_id: Some(&parent), ..Default::default() }).await?;
            let volumes: Vec<Doc<Value>> = st.store.list(KIND_VOLUME, st.ws(), Filter { parent_id: Some(&parent), ..Default::default() }).await?;
            let functions: Vec<Doc<Value>> = st.store.list(KIND_FUNCTION, st.ws(), Filter { parent_id: Some(&parent), ..Default::default() }).await?;
            Ok(Json(json!({
                "tables": tables.into_iter().map(|d| d.data).collect::<Vec<_>>(),
                "volumes": volumes.into_iter().map(|d| d.data).collect::<Vec<_>>(),
                "functions": functions.into_iter().map(|d| d.data).collect::<Vec<_>>(),
            })))
        }
    }
}

pub fn router() -> Router<S> {
    let uc = Router::new()
        .route("/metastores", get(metastores))
        .route("/metastore_summary", get(metastore_summary_h))
        .route("/current-metastore-assignment", get(current_assignment))
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
        .route("/functions/{full_name}", get(get_function).delete(delete_function))
        .route("/external-locations", get(list_ext_locations).post(create_ext_location))
        .route("/external-locations/{name}", get(get_ext_location).patch(update_ext_location).delete(delete_ext_location))
        .route("/storage-credentials", get(list_storage_creds).post(create_storage_cred))
        .route("/storage-credentials/{name}", get(get_storage_cred).patch(update_storage_cred).delete(delete_storage_cred))
        .route("/connections", get(list_connections).post(create_connection))
        .route("/connections/{name}", get(get_connection).delete(delete_connection))
        .route("/permissions/{securable_type}/{full_name}", get(get_grants).patch(update_grants))
        .route("/effective-permissions/{securable_type}/{full_name}", get(get_grants));
    Router::new()
        .nest("/api/2.1/unity-catalog", uc.clone())
        .nest("/api/2.0/unity-catalog", uc)
        .route("/api/2.0/lakeforge/catalog/browse", get(browse))
        .route("/api/2.0/lakeforge/catalog/grants/{securable_type}/{full_name}", patch(update_grants).post(update_grants))
}
