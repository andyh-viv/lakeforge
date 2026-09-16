//! Unity Catalog enforcement for the SQL path.
//!
//! Every statement executed through [`crate::state::AppState::execute_sql`]
//! (SQL editor, Statement Execution API, notebooks, jobs, pipelines) is
//! analysed with [`super::sqlguard`], authorised against the privilege model
//! and rewritten so that row filters, column masks, SQL UDFs and session
//! functions are applied before the text reaches the Forge engine.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use serde_json::{json, Value};
use sqlparser::ast::Statement;

use crate::api::catalog::{KIND_FUNCTION, KIND_SCHEMA, KIND_TABLE, SYSTEM_CATALOG};
use crate::auth::Principal;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{Doc, Filter};
use crate::uc::privileges::{Authorizer, Securable};
use crate::uc::sqlguard::{self, Analysis, SessionFacts, SqlFunction, TablePolicy};
use crate::uc::system_tables;

pub const CONF_DEFAULT_CATALOG: &str = "forge.sql.defaultCatalog";
pub const CONF_DEFAULT_SCHEMA: &str = "forge.sql.defaultSchema";

/// A statement that passed authorisation and is ready for the engine.
#[derive(Debug)]
pub struct PreparedSql {
    /// Text to send to Forge (rewritten if any policy / UDF applied).
    pub sql: String,
    pub analysis: Analysis,
    pub default_catalog: String,
    pub default_schema: String,
    /// `full_name -> table_type` for every governed table the statement touches.
    pub table_types: HashMap<String, String>,
    /// Set when the statement is served entirely by the metastore (SQL UDF
    /// DDL) and must not be sent to the engine.
    pub metastore_op: Option<MetastoreOp>,
}

/// Statements Forge cannot execute that Unity Catalog owns instead.
#[derive(Debug, Clone)]
pub enum MetastoreOp {
    CreateFunction { doc: serde_json::Map<String, Value>, replace: bool },
    DropFunction { names: Vec<String>, if_exists: bool },
}

pub fn defaults(conf: &HashMap<String, String>) -> (String, String) {
    let cat = conf.get(CONF_DEFAULT_CATALOG).filter(|s| !s.is_empty()).cloned().unwrap_or_else(|| crate::api::catalog::DEFAULT_CATALOG.to_string());
    let sch = conf.get(CONF_DEFAULT_SCHEMA).filter(|s| !s.is_empty()).cloned().unwrap_or_else(|| "default".to_string());
    (cat, sch)
}

fn metastore_op(stmts: &[Statement], cat: &str, sch: &str) -> Option<MetastoreOp> {
    let [stmt] = stmts else { return None };
    match stmt {
        Statement::CreateFunction(_) => {
            let (f, return_type, params, replace) = sqlguard::parse_sql_function(stmt, cat, sch)?;
            let (c, s, name) = {
                let parts: Vec<&str> = f.full_name.split('.').collect();
                (parts.first()?.to_string(), parts.get(1)?.to_string(), parts.get(2)?.to_string())
            };
            let mut doc = serde_json::Map::new();
            doc.insert("name".into(), json!(name));
            doc.insert("catalog_name".into(), json!(c));
            doc.insert("schema_name".into(), json!(s));
            doc.insert("routine_body".into(), json!("SQL"));
            doc.insert("routine_definition".into(), json!(f.body));
            doc.insert("data_type".into(), json!(return_type.to_ascii_uppercase()));
            doc.insert("full_data_type".into(), json!(return_type.to_ascii_uppercase()));
            doc.insert(
                "input_params".into(),
                json!({ "parameters": params.iter().enumerate().map(|(i, (n, t))| json!({ "name": n, "type_text": t, "type_name": t.to_ascii_uppercase(), "position": i })).collect::<Vec<_>>() }),
            );
            Some(MetastoreOp::CreateFunction { doc, replace })
        }
        Statement::DropFunction(d) => Some(MetastoreOp::DropFunction {
            names: d.func_desc.iter().map(|f| sqlguard::resolve_name(&f.name, cat, sch)).collect(),
            if_exists: d.if_exists,
        }),
        _ => None,
    }
}

fn parent_schema(full: &str) -> Option<String> {
    let parts: Vec<&str> = full.split('.').collect();
    (parts.len() == 3).then(|| format!("{}.{}", parts[0], parts[1]))
}

fn parent_catalog(full: &str) -> Option<String> {
    full.split('.').next().map(str::to_string)
}

impl AppState {
    /// Analyse, authorise and rewrite `sql` for `p`.
    pub async fn prepare_sql(self: &Arc<Self>, p: &Principal, sql: &str, conf: &HashMap<String, String>) -> ApiResult<PreparedSql> {
        let (cat, sch) = defaults(conf);
        let (stmts, analysis) = sqlguard::analyze(sql, &cat, &sch);
        let table_types = self.authorize_sql(p, &analysis).await?;
        let metastore_op = stmts.as_deref().and_then(|s| metastore_op(s, &cat, &sch));
        let rewritten = match &stmts {
            Some(stmts) if metastore_op.is_none() && analysis.kind.is_rewritable() => self.rewrite_sql(p, stmts, &analysis, &cat, &sch).await?,
            _ => None,
        };
        Ok(PreparedSql { sql: rewritten.unwrap_or_else(|| sql.to_string()), analysis, default_catalog: cat, default_schema: sch, table_types, metastore_op })
    }

    /// Apply a metastore-only statement (SQL UDF create/drop) after it has
    /// been authorised by `authorize_sql`.
    pub async fn apply_metastore_op(&self, p: &Principal, op: &MetastoreOp) -> ApiResult<()> {
        match op {
            MetastoreOp::CreateFunction { doc, replace } => {
                let full = doc.get("full_name").and_then(|v| v.as_str()).unwrap_or_default().to_string();
                if *replace && self.uc_get(KIND_FUNCTION, &full).await?.is_some() {
                    self.uc_delete_in_schema(p, KIND_FUNCTION, Securable::Function, &full, "Function").await?;
                }
                self.uc_create_in_schema(p, KIND_FUNCTION, "Function", doc.clone()).await?;
            }
            MetastoreOp::DropFunction { names, if_exists } => {
                for full in names {
                    if *if_exists && self.uc_get(KIND_FUNCTION, full).await?.is_none() {
                        continue;
                    }
                    self.uc_delete_in_schema(p, KIND_FUNCTION, Securable::Function, full, "Function").await?;
                }
            }
        }
        Ok(())
    }

    /// Enforce UC privileges for a statement. Objects unknown to the
    /// metastore (temp views, engine-only names) are only checked at the
    /// catalog/schema level so the engine can still report its own errors.
    pub async fn authorize_sql(&self, p: &Principal, an: &Analysis) -> ApiResult<HashMap<String, String>> {
        let mut az = Authorizer::new(self, p);
        let mut types = HashMap::new();

        for full in &an.reads {
            if let Some(t) = self.governed_table(full).await? {
                types.insert(full.clone(), t["table_type"].as_str().unwrap_or("MANAGED").to_string());
                if full.starts_with(&format!("{SYSTEM_CATALOG}.")) {
                    self.require_system_read(&mut az, full).await?;
                } else {
                    az.require_on_object(Securable::Table, full, "SELECT").await?;
                }
            } else {
                self.require_visible_parents(&mut az, full).await?;
            }
        }
        for full in &an.writes {
            if system_tables::is_system_table(full) {
                return Err(ApiError::PermissionDenied(format!("Table '{full}' is read-only")));
            }
            if let Some(t) = self.governed_table(full).await? {
                types.insert(full.clone(), t["table_type"].as_str().unwrap_or("MANAGED").to_string());
                az.require_on_object(Securable::Table, full, "MODIFY").await?;
            } else {
                self.require_visible_parents(&mut az, full).await?;
            }
        }
        for (sec, full) in &an.creates {
            if full.starts_with(&format!("{SYSTEM_CATALOG}.")) {
                return Err(ApiError::PermissionDenied(format!("Catalog '{SYSTEM_CATALOG}' is read-only")));
            }
            if an.replace && self.uc_exists(*sec, full).await? {
                az.require_owner(*sec, full).await?;
                continue;
            }
            match sec {
                Securable::Table => {
                    if let Some(s) = parent_schema(full) {
                        az.require_use_path(full).await?;
                        az.require(Securable::Schema, &s, "CREATE_TABLE").await?;
                    }
                }
                Securable::Function => {
                    if let Some(s) = parent_schema(full) {
                        az.require_use_path(full).await?;
                        az.require(Securable::Schema, &s, "CREATE_FUNCTION").await?;
                    }
                }
                Securable::Schema => {
                    if let Some(c) = parent_catalog(full) {
                        az.require(Securable::Catalog, &c, "USE_CATALOG").await?;
                        az.require(Securable::Catalog, &c, "CREATE_SCHEMA").await?;
                    }
                }
                other => az.require_owner(*other, full).await?,
            }
            if let Some(loc) = &an.location {
                self.require_path_privilege(&mut az, loc, "CREATE_EXTERNAL_TABLE").await?;
            }
        }
        for (sec, full) in &an.owned {
            if full.starts_with(&format!("{SYSTEM_CATALOG}.")) {
                return Err(ApiError::PermissionDenied(format!("Catalog '{SYSTEM_CATALOG}' is read-only")));
            }
            if self.uc_exists(*sec, full).await? {
                az.require_owner(*sec, full).await?;
            } else {
                self.require_visible_parents(&mut az, full).await?;
            }
        }
        for path in &an.paths {
            let privilege = if an.kind.is_write() && an.writes.is_empty() && an.creates.is_empty() { "WRITE_FILES" } else { "READ_FILES" };
            self.require_path_privilege(&mut az, path, privilege).await?;
        }
        Ok(types)
    }

    async fn governed_table(&self, full: &str) -> ApiResult<Option<Value>> {
        if let Some(t) = self.uc_get(KIND_TABLE, full).await? {
            return Ok(Some(t));
        }
        if system_tables::is_system_table(full) {
            let parts: Vec<&str> = full.split('.').collect();
            return Ok(system_tables::virtual_table_doc(parts[0], parts[1], parts[2], &self.config.admin_user));
        }
        Ok(None)
    }

    /// `information_schema` is readable by every workspace user; the other
    /// system schemas need the schema to be enabled and either admin rights
    /// or an explicit grant on `system.<schema>`.
    async fn require_system_read(&self, az: &mut Authorizer<'_>, full: &str) -> ApiResult<()> {
        let parts: Vec<&str> = full.split('.').collect();
        let schema = parts.get(1).copied().unwrap_or_default();
        if schema == "information_schema" {
            return Ok(());
        }
        if !self.enabled_system_schemas().await?.iter().any(|s| s == schema) {
            return Err(ApiError::PermissionDenied(format!("System schema '{schema}' is not enabled; enable it via PUT /api/2.1/unity-catalog/metastores/{{id}}/systemschemas/{schema}")));
        }
        if az.is_admin() {
            return Ok(());
        }
        az.require(Securable::Schema, &format!("{SYSTEM_CATALOG}.{schema}"), "SELECT").await
    }

    /// For names the metastore does not know: require `USE CATALOG` /
    /// `USE SCHEMA` on whichever ancestors do exist.
    async fn require_visible_parents(&self, az: &mut Authorizer<'_>, full: &str) -> ApiResult<()> {
        let parts: Vec<&str> = full.split('.').collect();
        if let Some(c) = parts.first() {
            if self.uc_exists(Securable::Catalog, c).await? {
                az.require(Securable::Catalog, c, "USE_CATALOG").await?;
            }
        }
        if parts.len() >= 3 {
            let s = format!("{}.{}", parts[0], parts[1]);
            if self.uc_get(KIND_SCHEMA, &s).await?.is_some() {
                az.require(Securable::Schema, &s, "USE_SCHEMA").await?;
            }
        }
        Ok(())
    }

    /// Row filters and column masks attached to the tables the statement reads.
    pub async fn table_policies(&self, tables: &BTreeSet<String>) -> ApiResult<HashMap<String, TablePolicy>> {
        let mut out = HashMap::new();
        for full in tables {
            let Some(t) = self.uc_get(KIND_TABLE, full).await? else { continue };
            let columns: Vec<String> = t["columns"].as_array().into_iter().flatten().filter_map(|c| c["name"].as_str().map(str::to_string)).collect();
            let row_filter = t["row_filter"]["function_name"].as_str().map(|f| (f.to_string(), t["row_filter"]["input_column_names"].as_array().into_iter().flatten().filter_map(|c| c.as_str().map(str::to_string)).collect()));
            let mut masks = HashMap::new();
            for c in t["columns"].as_array().into_iter().flatten() {
                if let (Some(name), Some(f)) = (c["name"].as_str(), c["mask"]["function_name"].as_str()) {
                    masks.insert(name.to_string(), (f.to_string(), c["mask"]["using_column_names"].as_array().into_iter().flatten().filter_map(|u| u.as_str().map(str::to_string)).collect()));
                }
            }
            if row_filter.is_some() || !masks.is_empty() {
                out.insert(full.clone(), TablePolicy { full_name: full.clone(), columns, row_filter, masks });
            }
        }
        Ok(out)
    }

    /// Every SQL-bodied UDF in the metastore, keyed by full name.
    pub async fn sql_functions(&self) -> ApiResult<HashMap<String, SqlFunction>> {
        let docs: Vec<Doc<Value>> = self.store.list(KIND_FUNCTION, self.ws(), Filter::default()).await?;
        let mut out = HashMap::new();
        for d in docs {
            let f = d.data;
            if f["routine_body"] != "SQL" {
                continue;
            }
            let Some(body) = f["routine_definition"].as_str().filter(|b| !b.trim().is_empty()) else { continue };
            let Some(full) = f["full_name"].as_str() else { continue };
            let params = f["input_params"]["parameters"].as_array().into_iter().flatten().filter_map(|p| p["name"].as_str().map(str::to_string)).collect();
            out.insert(full.to_string(), SqlFunction { full_name: full.to_string(), params, body: body.to_string() });
        }
        Ok(out)
    }

    async fn rewrite_sql(&self, p: &Principal, stmts: &[Statement], an: &Analysis, cat: &str, sch: &str) -> ApiResult<Option<String>> {
        let policies = self.table_policies(&an.reads).await?;
        let functions = self.sql_functions().await?;
        if policies.is_empty() && functions.is_empty() && !an.kind.is_rewritable() {
            return Ok(None);
        }
        let facts = SessionFacts { user: p.user_name.clone(), groups: p.groups.clone() };
        Ok(sqlguard::rewrite(stmts, &policies, &functions, &facts, cat, sch))
    }
}
