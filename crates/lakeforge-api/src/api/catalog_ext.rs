//! Unity Catalog surfaces beyond plain CRUD: tags, constraints, row filters and
//! column masks, comments, workspace bindings, temporary table credentials,
//! artifact allowlists, system schemas, lineage tracking and the audit log.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::catalog::{id_of, metastore_summary, split_name, KIND_CATALOG, KIND_FUNCTION, KIND_SCHEMA, KIND_TABLE, KIND_VOLUME, METASTORE_ID, SYSTEM_CATALOG};
use super::{empty, Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};
use crate::uc::privileges::{Authorizer, Securable, KIND_MODEL};
use crate::uc::system_tables;

pub const KV_SYSTEM_SCHEMAS: &str = "uc_system_schemas";
pub const TEMP_CREDENTIAL_TTL_SECS: i64 = 3600;

/// `information_schema` plus the system schemas enabled out of the box.
pub const DEFAULT_ENABLED_SYSTEM_SCHEMAS: &[&str] = &["information_schema", "access", "query", "compute", "lakeflow", "billing"];

pub fn kind_of(sec: Securable) -> ApiResult<&'static str> {
    sec.kind().ok_or_else(|| ApiError::invalid(format!("{} does not support this operation", sec.api_type())))
}

fn what_of(sec: Securable) -> &'static str {
    match sec {
        Securable::Catalog => "Catalog",
        Securable::Schema => "Schema",
        Securable::Table => "Table",
        Securable::Volume => "Volume",
        Securable::Function => "Function",
        Securable::ExternalLocation => "External Location",
        Securable::StorageCredential => "Storage Credential",
        Securable::Connection => "Connection",
        Securable::Share => "Share",
        Securable::Recipient => "Recipient",
        Securable::Provider => "Provider",
        Securable::Metastore => "Metastore",
    }
}

fn column_mut<'a>(doc: &'a mut Value, column: &str) -> ApiResult<&'a mut Map<String, Value>> {
    let cols = doc.get_mut("columns").and_then(|c| c.as_array_mut()).ok_or_else(|| ApiError::NotFound(format!("Column '{column}' not found")))?;
    cols.iter_mut()
        .filter_map(|c| c.as_object_mut())
        .find(|c| c.get("name").and_then(|n| n.as_str()).map(|n| n.eq_ignore_ascii_case(column)).unwrap_or(false))
        .ok_or_else(|| ApiError::NotFound(format!("Column '{column}' not found")))
}

fn column_names(doc: &Value) -> Vec<String> {
    doc["columns"].as_array().map(|a| a.iter().filter_map(|c| c["name"].as_str().map(str::to_string)).collect()).unwrap_or_default()
}

impl AppState {
    /// Owner/`MANAGE`-gated in-place edit of a securable document.
    pub async fn uc_mutate<F>(&self, p: &Principal, sec: Securable, full: &str, f: F) -> ApiResult<Value>
    where
        F: FnOnce(&mut Value) -> ApiResult<()>,
    {
        let kind = kind_of(sec)?;
        let what = what_of(sec);
        self.uc_require(kind, full, what).await?;
        if full.starts_with("system.") || (sec == Securable::Catalog && full == SYSTEM_CATALOG) || full.contains(".information_schema") {
            return Err(ApiError::PermissionDenied(format!("{what} '{full}' is read-only")));
        }
        let mut az = Authorizer::new(self, p);
        if !az.can_manage(sec, full).await? {
            return Err(ApiError::PermissionDenied(format!("[INSUFFICIENT_PERMISSIONS] User {} does not own {what} '{full}' and does not have MANAGE on it.", p.user_name)));
        }
        let user = p.user_name.clone();
        let doc = self
            .store
            .update::<Value, _>(kind, &id_of(kind, full), what, |v| {
                f(v)?;
                v["updated_at"] = json!(now_ms());
                v["updated_by"] = json!(user);
                Ok(())
            })
            .await?;
        if kind == KIND_TABLE {
            self.broadcast_table(&doc.data).await;
        }
        Ok(doc.data)
    }

    // ------------------------------------------------------------ tags

    pub async fn uc_set_tags(&self, p: &Principal, sec: Securable, full: &str, column: Option<&str>, tags: &[(String, String)]) -> ApiResult<Value> {
        self.uc_mutate(p, sec, full, |v| {
            let target = match column {
                Some(c) => column_mut(v, c)?,
                None => v.as_object_mut().ok_or_else(|| ApiError::Internal("bad document".into()))?,
            };
            let entry = target.entry("tags").or_insert_with(|| json!({}));
            if !entry.is_object() {
                *entry = json!({});
            }
            let o = entry.as_object_mut().ok_or_else(|| ApiError::Internal("bad tags".into()))?;
            for (k, val) in tags {
                if k.trim().is_empty() {
                    return Err(ApiError::invalid("tag key must not be empty"));
                }
                o.insert(k.clone(), json!(val));
            }
            Ok(())
        })
        .await
    }

    pub async fn uc_unset_tags(&self, p: &Principal, sec: Securable, full: &str, column: Option<&str>, keys: &[String]) -> ApiResult<Value> {
        self.uc_mutate(p, sec, full, |v| {
            let target = match column {
                Some(c) => column_mut(v, c)?,
                None => v.as_object_mut().ok_or_else(|| ApiError::Internal("bad document".into()))?,
            };
            if let Some(o) = target.get_mut("tags").and_then(|t| t.as_object_mut()) {
                for k in keys {
                    o.remove(k);
                }
            }
            Ok(())
        })
        .await
    }

    /// Tag assignments of a securable (and, for tables, of its columns).
    pub async fn uc_tag_assignments(&self, p: &Principal, sec: Securable, full: &str) -> ApiResult<Vec<Value>> {
        let doc = self.uc_get_visible(p, kind_of(sec)?, sec, full, what_of(sec)).await?;
        let mut out = vec![];
        let et = format!("{}s", sec.api_type().to_ascii_lowercase());
        if let Some(t) = doc["tags"].as_object() {
            for (k, v) in t {
                out.push(json!({ "entity_type": et, "entity_name": full, "tag_key": k, "tag_value": v }));
            }
        }
        if sec == Securable::Table {
            for c in doc["columns"].as_array().into_iter().flatten() {
                if let (Some(n), Some(t)) = (c["name"].as_str(), c["tags"].as_object()) {
                    for (k, v) in t {
                        out.push(json!({ "entity_type": "columns", "entity_name": format!("{full}.{n}"), "tag_key": k, "tag_value": v }));
                    }
                }
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------------ comments

    pub async fn uc_set_comment(&self, p: &Principal, sec: Securable, full: &str, column: Option<&str>, comment: Option<&str>) -> ApiResult<Value> {
        self.uc_mutate(p, sec, full, |v| {
            let target = match column {
                Some(c) => column_mut(v, c)?,
                None => v.as_object_mut().ok_or_else(|| ApiError::Internal("bad document".into()))?,
            };
            match comment {
                Some(c) => target.insert("comment".into(), json!(c)),
                None => target.remove("comment"),
            };
            Ok(())
        })
        .await
    }

    // ------------------------------------------------------------ row filters / column masks

    async fn require_policy_function(&self, p: &Principal, func: &str) -> ApiResult<Value> {
        let f = self.uc_require(KIND_FUNCTION, func, "Function").await?;
        let mut az = Authorizer::new(self, p);
        az.require(Securable::Function, func, "EXECUTE").await?;
        if f["routine_body"] != "SQL" {
            return Err(ApiError::invalid(format!("Function '{func}' must be a SQL function to be used as a policy")));
        }
        Ok(f)
    }

    pub async fn uc_set_row_filter(&self, p: &Principal, table: &str, func: &str, input_columns: &[String]) -> ApiResult<Value> {
        let f = self.require_policy_function(p, func).await?;
        let n_params = f["input_params"]["parameters"].as_array().map(|a| a.len()).unwrap_or(0);
        if n_params != input_columns.len() {
            return Err(ApiError::invalid(format!("Row filter '{func}' takes {n_params} argument(s) but {} column(s) were given", input_columns.len())));
        }
        self.uc_mutate(p, Securable::Table, table, |v| {
            let names = column_names(v);
            for c in input_columns {
                if !names.iter().any(|n| n.eq_ignore_ascii_case(c)) {
                    return Err(ApiError::invalid(format!("Column '{c}' not found in table '{table}'")));
                }
            }
            v["row_filter"] = json!({ "function_name": func, "input_column_names": input_columns });
            Ok(())
        })
        .await
    }

    pub async fn uc_drop_row_filter(&self, p: &Principal, table: &str) -> ApiResult<Value> {
        self.uc_mutate(p, Securable::Table, table, |v| {
            if let Some(o) = v.as_object_mut() {
                o.remove("row_filter");
            }
            Ok(())
        })
        .await
    }

    pub async fn uc_set_column_mask(&self, p: &Principal, table: &str, column: &str, func: &str, using: &[String]) -> ApiResult<Value> {
        let f = self.require_policy_function(p, func).await?;
        let n_params = f["input_params"]["parameters"].as_array().map(|a| a.len()).unwrap_or(0);
        if n_params != using.len() + 1 {
            return Err(ApiError::invalid(format!("Mask '{func}' takes {n_params} argument(s); the masked column plus {} USING column(s) were given", using.len())));
        }
        self.uc_mutate(p, Securable::Table, table, |v| {
            let names = column_names(v);
            for c in using {
                if !names.iter().any(|n| n.eq_ignore_ascii_case(c)) {
                    return Err(ApiError::invalid(format!("Column '{c}' not found in table '{table}'")));
                }
            }
            let col = column_mut(v, column)?;
            col.insert("mask".into(), json!({ "function_name": func, "using_column_names": using }));
            Ok(())
        })
        .await
    }

    pub async fn uc_drop_column_mask(&self, p: &Principal, table: &str, column: &str) -> ApiResult<Value> {
        self.uc_mutate(p, Securable::Table, table, |v| {
            column_mut(v, column)?.remove("mask");
            Ok(())
        })
        .await
    }

    // ------------------------------------------------------------ constraints

    pub async fn uc_add_constraint(&self, p: &Principal, table: &str, constraint: Value) -> ApiResult<Value> {
        let name = constraint_name(&constraint)?;
        if let Some(fk) = constraint.get("foreign_key_constraint") {
            let parent = fk["parent_table"].as_str().ok_or_else(|| ApiError::invalid("foreign_key_constraint.parent_table is required"))?;
            let parent_doc = self.uc_require(KIND_TABLE, parent, "Table").await?;
            let mut az = Authorizer::new(self, p);
            az.require(Securable::Table, parent, "SELECT").await?;
            let pcols = column_names(&parent_doc);
            for c in fk["parent_columns"].as_array().into_iter().flatten().filter_map(|c| c.as_str()) {
                if !pcols.iter().any(|n| n.eq_ignore_ascii_case(c)) {
                    return Err(ApiError::invalid(format!("Column '{c}' not found in parent table '{parent}'")));
                }
            }
        }
        self.uc_mutate(p, Securable::Table, table, |v| {
            let names = column_names(v);
            let own_cols: Vec<&str> = constraint
                .get("primary_key_constraint")
                .map(|c| &c["child_columns"])
                .or_else(|| constraint.get("foreign_key_constraint").map(|c| &c["child_columns"]))
                .and_then(|c| c.as_array())
                .map(|a| a.iter().filter_map(|c| c.as_str()).collect())
                .unwrap_or_default();
            for c in own_cols {
                if !names.iter().any(|n| n.eq_ignore_ascii_case(c)) {
                    return Err(ApiError::invalid(format!("Column '{c}' not found in table '{table}'")));
                }
            }
            let arr = v["table_constraints"].as_array().cloned().unwrap_or_default();
            if arr.iter().any(|c| constraint_name(c).ok().as_deref() == Some(name.as_str())) {
                return Err(ApiError::AlreadyExists(format!("Constraint '{name}' already exists on '{table}'")));
            }
            if constraint.get("primary_key_constraint").is_some() && arr.iter().any(|c| c.get("primary_key_constraint").is_some()) {
                return Err(ApiError::AlreadyExists(format!("Table '{table}' already has a primary key")));
            }
            let mut arr = arr;
            arr.push(constraint.clone());
            v["table_constraints"] = Value::Array(arr);
            if let Some(chk) = constraint.get("check_constraint") {
                let expr = chk["expression"].as_str().unwrap_or_default().to_string();
                let props = v.as_object_mut().and_then(|o| o.entry("properties").or_insert_with(|| json!({})).as_object_mut());
                if let Some(props) = props {
                    props.insert(format!("delta.constraints.{name}"), json!(expr));
                }
            }
            Ok(())
        })
        .await
    }

    pub async fn uc_drop_constraint(&self, p: &Principal, table: &str, name: &str, cascade: bool) -> ApiResult<Value> {
        // referencing FKs elsewhere
        let referencing: Vec<(String, String)> = {
            let docs: Vec<Doc<Value>> = self.store.list(KIND_TABLE, self.ws(), Filter::default()).await?;
            docs.iter()
                .flat_map(|d| {
                    let full = d.name.clone().unwrap_or_default();
                    d.data["table_constraints"].as_array().cloned().unwrap_or_default().into_iter().filter_map(move |c| {
                        let fk = c.get("foreign_key_constraint")?;
                        (fk["parent_table"] == table).then(|| (full.clone(), fk["name"].as_str().unwrap_or_default().to_string()))
                    })
                })
                .collect()
        };
        let doc = self.uc_require(KIND_TABLE, table, "Table").await?;
        let is_pk = doc["table_constraints"].as_array().into_iter().flatten().any(|c| c["primary_key_constraint"]["name"] == name);
        if is_pk && !referencing.is_empty() {
            if !cascade {
                return Err(ApiError::InvalidState(format!("Primary key '{name}' is referenced by foreign keys ({}); use cascade=true", referencing.iter().map(|(t, c)| format!("{t}.{c}")).collect::<Vec<_>>().join(", "))));
            }
            for (t, c) in referencing {
                self.uc_mutate(p, Securable::Table, &t, |v| {
                    if let Some(a) = v["table_constraints"].as_array_mut() {
                        a.retain(|x| x["foreign_key_constraint"]["name"] != c);
                    }
                    Ok(())
                })
                .await?;
            }
        }
        self.uc_mutate(p, Securable::Table, table, |v| {
            let before = v["table_constraints"].as_array().map(|a| a.len()).unwrap_or(0);
            if let Some(a) = v["table_constraints"].as_array_mut() {
                a.retain(|x| constraint_name(x).ok().as_deref() != Some(name));
            }
            if v["table_constraints"].as_array().map(|a| a.len()).unwrap_or(0) == before {
                return Err(ApiError::NotFound(format!("Constraint '{name}' not found on '{table}'")));
            }
            if let Some(props) = v["properties"].as_object_mut() {
                props.remove(&format!("delta.constraints.{name}"));
            }
            Ok(())
        })
        .await
    }

    // ------------------------------------------------------------ workspace bindings

    /// Whether a catalog is usable from this workspace (isolation mode + bindings).
    pub fn catalog_bound_here(&self, cat: &Value) -> bool {
        if cat["isolation_mode"] != "ISOLATED" {
            return true;
        }
        cat["workspace_bindings"].as_array().into_iter().flatten().any(|b| b["workspace_id"].as_str() == Some(self.ws()) || b["workspace_id"].as_i64().map(|i| i.to_string()).as_deref() == Some(self.ws()))
    }

    pub async fn uc_bindings(&self, p: &Principal, sec: Securable, name: &str) -> ApiResult<Vec<Value>> {
        let doc = self.uc_get_visible(p, kind_of(sec)?, sec, name, what_of(sec)).await?;
        Ok(doc["workspace_bindings"].as_array().cloned().unwrap_or_default())
    }

    pub async fn uc_update_bindings(&self, p: &Principal, sec: Securable, name: &str, add: Vec<Value>, remove: Vec<Value>) -> ApiResult<Vec<Value>> {
        let default_type = match sec {
            Securable::Catalog => "BINDING_TYPE_READ_WRITE",
            _ => "BINDING_TYPE_READ_WRITE",
        };
        let doc = self
            .uc_mutate(p, sec, name, |v| {
                let mut list = v["workspace_bindings"].as_array().cloned().unwrap_or_default();
                for r in &remove {
                    let id = r["workspace_id"].clone();
                    list.retain(|b| b["workspace_id"] != id);
                }
                for a in add {
                    let id = a["workspace_id"].clone();
                    if id.is_null() {
                        return Err(ApiError::invalid("workspace_id is required"));
                    }
                    let bt = a["binding_type"].as_str().unwrap_or(default_type).to_string();
                    if !matches!(bt.as_str(), "BINDING_TYPE_READ_WRITE" | "BINDING_TYPE_READ_ONLY") {
                        return Err(ApiError::invalid(format!("unknown binding_type '{bt}'")));
                    }
                    list.retain(|b| b["workspace_id"] != id);
                    list.push(json!({ "workspace_id": id, "binding_type": bt }));
                }
                v["workspace_bindings"] = Value::Array(list);
                Ok(())
            })
            .await?;
        Ok(doc["workspace_bindings"].as_array().cloned().unwrap_or_default())
    }

    // ------------------------------------------------------------ system schemas

    pub async fn enabled_system_schemas(&self) -> ApiResult<Vec<String>> {
        match self.store.kv_get(KV_SYSTEM_SCHEMAS).await? {
            Some(b) => Ok(serde_json::from_str(&b).unwrap_or_default()),
            None => Ok(DEFAULT_ENABLED_SYSTEM_SCHEMAS.iter().map(|s| s.to_string()).collect()),
        }
    }

    pub async fn set_system_schema(&self, p: &Principal, schema: &str, enabled: bool) -> ApiResult<()> {
        if !p.is_admin {
            return Err(ApiError::PermissionDenied("Only metastore admins can enable system schemas".into()));
        }
        if !system_tables::SYSTEM_SCHEMAS.contains(&schema) {
            return Err(ApiError::NotFound(format!("System schema '{schema}' does not exist")));
        }
        if schema == "information_schema" && !enabled {
            return Err(ApiError::invalid("information_schema cannot be disabled"));
        }
        let mut cur = self.enabled_system_schemas().await?;
        cur.retain(|s| s != schema);
        if enabled {
            cur.push(schema.to_string());
        }
        self.store.kv_set(KV_SYSTEM_SCHEMAS, &serde_json::to_string(&cur)?).await
    }

    // ------------------------------------------------------------ temporary table credentials

    /// Short-lived credential for direct access to a table's storage. Local
    /// and single-tenant deployments mint a workspace bearer token scoped in
    /// time (the Files API enforces UC volumes/table paths); cloud deployments
    /// need vended cloud credentials, which are not implemented.
    pub async fn temporary_table_credential(&self, p: &Principal, table_id: &str, operation: &str) -> ApiResult<Value> {
        let docs: Vec<Doc<Value>> = self.store.list(KIND_TABLE, self.ws(), Filter::default()).await?;
        let table = docs.into_iter().map(|d| d.data).find(|t| t["table_id"] == table_id).ok_or_else(|| ApiError::NotFound(format!("Table with id '{table_id}' not found")))?;
        let full = table["full_name"].as_str().unwrap_or_default().to_string();
        let mut az = Authorizer::new(self, p);
        let privilege = match operation {
            "READ" => "SELECT",
            "READ_WRITE" => "MODIFY",
            other => return Err(ApiError::invalid(format!("unknown operation '{other}'; expected READ or READ_WRITE"))),
        };
        az.require_use_path(&full).await?;
        az.require(Securable::Table, &full, privilege).await?;
        if privilege == "MODIFY" {
            az.require(Securable::Table, &full, "SELECT").await?;
        }
        let url = table["storage_location"].as_str().unwrap_or_default().to_string();
        let expiration_time = now_ms() + TEMP_CREDENTIAL_TTL_SECS * 1000;
        let (value, _) = self.create_token(p, &format!("temporary table credential for {full}"), Some(TEMP_CREDENTIAL_TTL_SECS)).await?;
        let mut out = json!({ "url": url, "expiration_time": expiration_time });
        match self.config.cloud.as_str() {
            "aws" | "gcp" | "azure" => {
                out["lakeforge_credentials"] = json!({ "bearer_token": value, "files_api": format!("{}/api/2.0/fs/files", self.config.public_url), "note": "cloud credential vending is not implemented; use the Files API with this token" });
            }
            _ => {
                out["lakeforge_credentials"] = json!({ "bearer_token": value, "files_api": format!("{}/api/2.0/fs/files", self.config.public_url) });
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------------ artifact allowlists

    pub async fn artifact_allowlist(&self, artifact_type: &str) -> ApiResult<Value> {
        validate_artifact_type(artifact_type)?;
        let key = format!("uc_artifact_allowlist:{artifact_type}");
        Ok(match self.store.kv_get(&key).await? {
            Some(b) => serde_json::from_str(&b)?,
            None => json!({ "artifact_matchers": [], "metastore_id": METASTORE_ID }),
        })
    }

    pub async fn set_artifact_allowlist(&self, p: &Principal, artifact_type: &str, matchers: Vec<Value>) -> ApiResult<Value> {
        validate_artifact_type(artifact_type)?;
        if !p.is_admin {
            return Err(ApiError::PermissionDenied("Only metastore admins can edit artifact allowlists".into()));
        }
        for m in &matchers {
            let a = m["artifact"].as_str().ok_or_else(|| ApiError::invalid("artifact_matchers[].artifact is required"))?;
            if a.trim().is_empty() {
                return Err(ApiError::invalid("artifact must not be empty"));
            }
            let mt = m["match_type"].as_str().unwrap_or("PREFIX_MATCH");
            if mt != "PREFIX_MATCH" {
                return Err(ApiError::invalid(format!("unknown match_type '{mt}'")));
            }
        }
        let v = json!({
            "artifact_matchers": matchers.into_iter().map(|m| json!({ "artifact": m["artifact"], "match_type": m["match_type"].as_str().unwrap_or("PREFIX_MATCH") })).collect::<Vec<_>>(),
            "metastore_id": METASTORE_ID,
            "created_at": now_ms(),
            "created_by": p.user_name,
        });
        self.store.kv_set(&format!("uc_artifact_allowlist:{artifact_type}"), &serde_json::to_string(&v)?).await?;
        Ok(v)
    }

    /// `true` if `artifact` (a path / Maven coordinate / init-script path) is
    /// allowed for `artifact_type` on shared clusters.
    pub async fn artifact_allowed(&self, artifact_type: &str, artifact: &str) -> ApiResult<bool> {
        let list = self.artifact_allowlist(artifact_type).await?;
        Ok(list["artifact_matchers"].as_array().into_iter().flatten().any(|m| m["artifact"].as_str().map(|a| artifact.starts_with(a)).unwrap_or(false)))
    }
}

fn validate_artifact_type(t: &str) -> ApiResult<()> {
    if matches!(t, "INIT_SCRIPT" | "LIBRARY_JAR" | "LIBRARY_MAVEN") {
        Ok(())
    } else {
        Err(ApiError::invalid(format!("unknown artifact_type '{t}'; expected INIT_SCRIPT, LIBRARY_JAR or LIBRARY_MAVEN")))
    }
}

pub fn constraint_name(c: &Value) -> ApiResult<String> {
    for k in ["primary_key_constraint", "foreign_key_constraint", "named_table_constraint", "check_constraint"] {
        if let Some(n) = c.get(k).and_then(|x| x["name"].as_str()) {
            return Ok(n.to_string());
        }
    }
    Err(ApiError::invalid("constraint must be one of primary_key_constraint, foreign_key_constraint, named_table_constraint, check_constraint with a name"))
}

/// Databricks `entity_type` (plural) -> securable + optional column.
fn entity(entity_type: &str, entity_name: &str) -> ApiResult<(Securable, String, Option<String>)> {
    Ok(match entity_type {
        "catalogs" => (Securable::Catalog, entity_name.to_string(), None),
        "schemas" => (Securable::Schema, entity_name.to_string(), None),
        "tables" => (Securable::Table, entity_name.to_string(), None),
        "volumes" => (Securable::Volume, entity_name.to_string(), None),
        "functions" => (Securable::Function, entity_name.to_string(), None),
        "columns" => {
            let (tbl, col) = entity_name.rsplit_once('.').ok_or_else(|| ApiError::invalid("column entity_name must be catalog.schema.table.column"))?;
            (Securable::Table, tbl.to_string(), Some(col.to_string()))
        }
        other => return Err(ApiError::invalid(format!("unknown entity_type '{other}'"))),
    })
}

// ------------------------------------------------------------------ handlers

#[derive(Debug, Deserialize)]
struct TagAssignment {
    entity_type: String,
    entity_name: String,
    tag_key: String,
    #[serde(default)]
    tag_value: String,
}

async fn create_tag(State(st): State<S>, Who(p): Who, Body(b): Body<TagAssignment>) -> ApiResult<Json<Value>> {
    let (sec, full, col) = entity(&b.entity_type, &b.entity_name)?;
    st.uc_set_tags(&p, sec, &full, col.as_deref(), &[(b.tag_key.clone(), b.tag_value.clone())]).await?;
    Ok(Json(json!({ "entity_type": b.entity_type, "entity_name": b.entity_name, "tag_key": b.tag_key, "tag_value": b.tag_value })))
}

#[derive(Debug, Deserialize)]
struct TagListQ {
    entity_type: String,
    entity_name: String,
}

async fn list_tags(State(st): State<S>, Who(p): Who, Query(q): Query<TagListQ>) -> ApiResult<Json<Value>> {
    let (sec, full, col) = entity(&q.entity_type, &q.entity_name)?;
    let mut all = st.uc_tag_assignments(&p, sec, &full).await?;
    all.retain(|t| t["entity_type"] == q.entity_type && t["entity_name"] == q.entity_name);
    let _ = col;
    Ok(Json(json!({ "tag_assignments": all })))
}

#[derive(Debug, Deserialize)]
struct TagPatch {
    #[serde(default)]
    tag_value: String,
}

async fn update_tag(State(st): State<S>, Who(p): Who, Path((et, en, key)): Path<(String, String, String)>, Body(b): Body<TagPatch>) -> ApiResult<Json<Value>> {
    let (sec, full, col) = entity(&et, &en)?;
    st.uc_set_tags(&p, sec, &full, col.as_deref(), &[(key.clone(), b.tag_value.clone())]).await?;
    Ok(Json(json!({ "entity_type": et, "entity_name": en, "tag_key": key, "tag_value": b.tag_value })))
}

async fn delete_tag(State(st): State<S>, Who(p): Who, Path((et, en, key)): Path<(String, String, String)>) -> ApiResult<Json<Value>> {
    let (sec, full, col) = entity(&et, &en)?;
    st.uc_unset_tags(&p, sec, &full, col.as_deref(), &[key]).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct ConstraintBody {
    full_name_arg: String,
    constraint: Value,
}

async fn create_constraint(State(st): State<S>, Who(p): Who, Body(b): Body<ConstraintBody>) -> ApiResult<Json<Value>> {
    let doc = st.uc_add_constraint(&p, &b.full_name_arg, b.constraint).await?;
    Ok(Json(doc))
}

#[derive(Debug, Deserialize)]
struct ConstraintDelQ {
    constraint_name: String,
    #[serde(default)]
    cascade: bool,
}

async fn delete_constraint(State(st): State<S>, Who(p): Who, Path(full): Path<String>, Query(q): Query<ConstraintDelQ>) -> ApiResult<Json<Value>> {
    st.uc_drop_constraint(&p, &full, &q.constraint_name, q.cascade).await?;
    Ok(empty())
}

async fn get_catalog_bindings(State(st): State<S>, Who(p): Who, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    let b = st.uc_bindings(&p, Securable::Catalog, &name).await?;
    Ok(Json(json!({ "workspaces": b.iter().filter_map(|x| x["workspace_id"].as_i64().or_else(|| x["workspace_id"].as_str().and_then(|s| s.parse().ok()))).collect::<Vec<i64>>() })))
}

#[derive(Debug, Deserialize, Default)]
struct CatalogBindingPatch {
    #[serde(default)]
    assign_workspaces: Vec<Value>,
    #[serde(default)]
    unassign_workspaces: Vec<Value>,
}

async fn update_catalog_bindings(State(st): State<S>, Who(p): Who, Path(name): Path<String>, Body(b): Body<CatalogBindingPatch>) -> ApiResult<Json<Value>> {
    let add = b.assign_workspaces.into_iter().map(|w| json!({ "workspace_id": w, "binding_type": "BINDING_TYPE_READ_WRITE" })).collect();
    let remove = b.unassign_workspaces.into_iter().map(|w| json!({ "workspace_id": w })).collect();
    let out = st.uc_update_bindings(&p, Securable::Catalog, &name, add, remove).await?;
    Ok(Json(json!({ "workspaces": out.iter().map(|x| x["workspace_id"].clone()).collect::<Vec<_>>() })))
}

fn binding_securable(t: &str) -> ApiResult<Securable> {
    Ok(match t {
        "catalog" => Securable::Catalog,
        "external_location" | "external-location" => Securable::ExternalLocation,
        "storage_credential" | "storage-credential" => Securable::StorageCredential,
        "credential" => Securable::StorageCredential,
        other => return Err(ApiError::invalid(format!("unknown securable_type '{other}'"))),
    })
}

async fn get_bindings(State(st): State<S>, Who(p): Who, Path((t, name)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let b = st.uc_bindings(&p, binding_securable(&t)?, &name).await?;
    Ok(Json(json!({ "bindings": b })))
}

#[derive(Debug, Deserialize, Default)]
struct BindingPatch {
    #[serde(default)]
    add: Vec<Value>,
    #[serde(default)]
    remove: Vec<Value>,
}

async fn update_bindings(State(st): State<S>, Who(p): Who, Path((t, name)): Path<(String, String)>, Body(b): Body<BindingPatch>) -> ApiResult<Json<Value>> {
    let out = st.uc_update_bindings(&p, binding_securable(&t)?, &name, b.add, b.remove).await?;
    Ok(Json(json!({ "bindings": out })))
}

async fn list_system_schemas(State(st): State<S>, Path(_id): Path<String>) -> ApiResult<Json<Value>> {
    let enabled = st.enabled_system_schemas().await?;
    let schemas: Vec<Value> = system_tables::SYSTEM_SCHEMAS
        .iter()
        .map(|s| json!({ "schema": s, "state": if enabled.iter().any(|e| e == s) { "ENABLE_COMPLETED" } else { "AVAILABLE" } }))
        .collect();
    Ok(Json(json!({ "schemas": schemas })))
}

async fn enable_system_schema(State(st): State<S>, Who(p): Who, Path((_id, schema)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    st.set_system_schema(&p, &schema, true).await?;
    Ok(empty())
}

async fn disable_system_schema(State(st): State<S>, Who(p): Who, Path((_id, schema)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    st.set_system_schema(&p, &schema, false).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct TempCredBody {
    table_id: String,
    #[serde(default = "default_read")]
    operation: String,
}

fn default_read() -> String {
    "READ".into()
}

async fn temp_table_credentials(State(st): State<S>, Who(p): Who, Body(b): Body<TempCredBody>) -> ApiResult<Json<Value>> {
    Ok(Json(st.temporary_table_credential(&p, &b.table_id, &b.operation).await?))
}

#[derive(Debug, Deserialize)]
struct RowFilterBody {
    function_name: String,
    #[serde(default)]
    input_columns: Vec<String>,
}

async fn set_row_filter(State(st): State<S>, Who(p): Who, Path(table): Path<String>, Body(b): Body<RowFilterBody>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_set_row_filter(&p, &table, &b.function_name, &b.input_columns).await?))
}

async fn drop_row_filter(State(st): State<S>, Who(p): Who, Path(table): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_drop_row_filter(&p, &table).await?))
}

#[derive(Debug, Deserialize)]
struct ColumnMaskBody {
    column: String,
    function_name: String,
    #[serde(default)]
    using_columns: Vec<String>,
}

async fn set_column_mask(State(st): State<S>, Who(p): Who, Path(table): Path<String>, Body(b): Body<ColumnMaskBody>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_set_column_mask(&p, &table, &b.column, &b.function_name, &b.using_columns).await?))
}

async fn drop_column_mask(State(st): State<S>, Who(p): Who, Path((table, column)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    Ok(Json(st.uc_drop_column_mask(&p, &table, &column).await?))
}

async fn get_allowlist(State(st): State<S>, Path(t): Path<String>) -> ApiResult<Json<Value>> {
    Ok(Json(st.artifact_allowlist(&t).await?))
}

#[derive(Debug, Deserialize, Default)]
struct AllowlistBody {
    #[serde(default)]
    artifact_matchers: Vec<Value>,
}

async fn put_allowlist(State(st): State<S>, Who(p): Who, Path(t): Path<String>, Body(b): Body<AllowlistBody>) -> ApiResult<Json<Value>> {
    Ok(Json(st.set_artifact_allowlist(&p, &t, b.artifact_matchers).await?))
}

// ------------------------------------------------------------------ lineage / audit / system tables

#[derive(Debug, Deserialize)]
struct TableLineageQ {
    table_name: String,
    #[serde(default)]
    include_entity_lineage: bool,
}

async fn table_lineage(State(st): State<S>, Who(p): Who, Query(q): Query<TableLineageQ>) -> ApiResult<Json<Value>> {
    let mut az = Authorizer::new(&st, &p);
    if !az.can_browse(Securable::Table, &q.table_name).await? {
        return Err(ApiError::PermissionDenied(format!("[INSUFFICIENT_PERMISSIONS] User {} cannot browse table '{}'", p.user_name, q.table_name)));
    }
    let mut v = st.table_lineage_for(&q.table_name).await?;
    if !q.include_entity_lineage {
        for side in ["upstreams", "downstreams"] {
            if let Some(a) = v[side].as_array_mut() {
                for e in a {
                    if let Some(o) = e.as_object_mut() {
                        o.remove("notebookInfos");
                        o.remove("jobInfos");
                        o.remove("pipelineInfos");
                    }
                }
            }
        }
    }
    Ok(Json(v))
}

#[derive(Debug, Deserialize)]
struct TableLineageBody {
    table_name: String,
    #[serde(default)]
    include_entity_lineage: bool,
}

async fn table_lineage_post(st: State<S>, who: Who, Body(b): Body<TableLineageBody>) -> ApiResult<Json<Value>> {
    table_lineage(st, who, Query(TableLineageQ { table_name: b.table_name, include_entity_lineage: b.include_entity_lineage })).await
}

#[derive(Debug, Deserialize)]
struct ColumnLineageQ {
    table_name: String,
    column_name: String,
}

async fn column_lineage(State(st): State<S>, Who(p): Who, Query(q): Query<ColumnLineageQ>) -> ApiResult<Json<Value>> {
    let mut az = Authorizer::new(&st, &p);
    if !az.can_browse(Securable::Table, &q.table_name).await? {
        return Err(ApiError::PermissionDenied(format!("[INSUFFICIENT_PERMISSIONS] User {} cannot browse table '{}'", p.user_name, q.table_name)));
    }
    Ok(Json(st.column_lineage_for(&q.table_name, &q.column_name).await?))
}

async fn column_lineage_post(st: State<S>, who: Who, Body(b): Body<ColumnLineageQ>) -> ApiResult<Json<Value>> {
    column_lineage(st, who, Query(b)).await
}

#[derive(Debug, Deserialize, Default)]
struct AuditQ {
    limit: Option<i64>,
    service_name: Option<String>,
    action_name: Option<String>,
    user_name: Option<String>,
    since: Option<i64>,
}

async fn audit_log(State(st): State<S>, Who(p): Who, Query(q): Query<AuditQ>) -> ApiResult<Json<Value>> {
    // non-admins may only see their own events (like system.access.audit governed by SELECT)
    let user = if p.is_admin { q.user_name.clone() } else { Some(p.user_name.clone()) };
    let events = st.audit_events(q.limit.unwrap_or(200).clamp(1, 5000), q.service_name.as_deref(), q.action_name.as_deref(), user.as_deref(), q.since).await?;
    Ok(Json(json!({ "events": events })))
}

async fn list_system_tables(State(st): State<S>) -> ApiResult<Json<Value>> {
    let enabled = st.enabled_system_schemas().await?;
    let mut by_schema: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for t in system_tables::tables() {
        by_schema.entry(t.schema.to_string()).or_default().push(json!({
            "name": t.name,
            "full_name": format!("system.{}.{}", t.schema, t.name),
            "comment": t.comment,
            "columns": t.columns.iter().map(|(n, ty)| json!({ "name": n, "type_text": ty.type_text(), "type_name": ty.type_name() })).collect::<Vec<_>>(),
            "enabled": enabled.iter().any(|e| e == t.schema),
        }));
    }
    Ok(Json(json!({ "schemas": by_schema.into_iter().map(|(s, tables)| json!({ "schema": s, "enabled": enabled.iter().any(|e| e == &s), "tables": tables })).collect::<Vec<_>>() })))
}

#[derive(Debug, Deserialize, Default)]
struct RowsQ {
    limit: Option<usize>,
}

async fn system_table_rows(State(st): State<S>, Who(p): Who, Path((schema, name)): Path<(String, String)>, Query(q): Query<RowsQ>) -> ApiResult<Json<Value>> {
    let full = format!("system.{schema}.{name}");
    let mut az = Authorizer::new(&st, &p);
    az.require(Securable::Table, &full, "SELECT").await?;
    let mut rows = st.system_table_rows(&schema, &name).await?;
    let total = rows.len();
    rows.truncate(q.limit.unwrap_or(500).clamp(1, 10_000));
    Ok(Json(json!({ "full_name": full, "row_count": total, "rows": rows })))
}

/// Everything about one securable the Catalog UI needs in one call.
async fn securable_details(State(st): State<S>, Who(p): Who, Path((t, full)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    let sec = Securable::parse(&t)?;
    let kind = kind_of(sec)?;
    let (doc, is_virtual) = if sec == Securable::Table && system_tables::is_system_table(&full) {
        let (c, s, n) = split_name(&full);
        (system_tables::virtual_table_doc(&c, &s, &n, &st.config.admin_user).ok_or_else(|| ApiError::NotFound(format!("Table '{full}' not found")))?, true)
    } else if sec == Securable::Catalog && full == SYSTEM_CATALOG {
        (system_tables::system_catalog_doc(&st.config.admin_user), true)
    } else {
        (st.uc_get_visible(&p, kind, sec, &full, what_of(sec)).await?, false)
    };
    let mut az = Authorizer::new(&st, &p);
    let grants = if is_virtual { json!({ "privilege_assignments": [] }) } else { json!({ "privilege_assignments": crate::uc::privileges::assignments_json(&st.grants_for(sec, &full).await?) }) };
    let effective = az.effective(sec, &full, None).await?;
    let lineage = if sec == Securable::Table { st.table_lineage_for(&full).await? } else { Value::Null };
    let tags = if is_virtual { vec![] } else { st.uc_tag_assignments(&p, sec, &full).await? };
    let can_manage = az.can_manage(sec, &full).await?;
    Ok(Json(json!({ "info": doc, "grants": grants, "effective": effective, "lineage": lineage, "tags": tags, "can_manage": can_manage, "metastore": metastore_summary(&st) })))
}

/// Counts for the Catalog landing page / admin.
async fn metastore_stats(State(st): State<S>, Who(p): Who) -> ApiResult<Json<Value>> {
    if !p.is_admin {
        return Err(ApiError::PermissionDenied("admin only".into()));
    }
    let ws = st.ws();
    let mut out = Map::new();
    for (k, kind) in [("catalogs", KIND_CATALOG), ("schemas", KIND_SCHEMA), ("tables", KIND_TABLE), ("volumes", KIND_VOLUME), ("functions", KIND_FUNCTION), ("models", KIND_MODEL)] {
        out.insert(k.into(), json!(st.store.count(kind, ws, None).await?));
    }
    out.insert("audit_events".into(), json!(st.store.count(crate::uc::audit::KIND_AUDIT, ws, None).await?));
    out.insert("table_lineage_rows".into(), json!(st.store.count(crate::uc::lineage::KIND_TABLE_LINEAGE, ws, None).await?));
    out.insert("column_lineage_rows".into(), json!(st.store.count(crate::uc::lineage::KIND_COLUMN_LINEAGE, ws, None).await?));
    out.insert("metastore".into(), metastore_summary(&st));
    Ok(Json(Value::Object(out)))
}

pub fn router() -> Router<S> {
    Router::new()
        // tags (entity tag assignments API)
        .route("/api/2.1/unity-catalog/entity-tag-assignments", post(create_tag).get(list_tags))
        .route("/api/2.1/unity-catalog/entity-tag-assignments/{entity_type}/{entity_name}/tags/{tag_key}", axum::routing::patch(update_tag).delete(delete_tag))
        // constraints
        .route("/api/2.1/unity-catalog/constraints", post(create_constraint))
        .route("/api/2.1/unity-catalog/constraints/{full_name}", delete(delete_constraint))
        // workspace bindings
        .route("/api/2.1/unity-catalog/workspace-bindings/catalogs/{name}", get(get_catalog_bindings).patch(update_catalog_bindings))
        .route("/api/2.1/unity-catalog/bindings/{securable_type}/{securable_name}", get(get_bindings).patch(update_bindings))
        // system schemas
        .route("/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas", get(list_system_schemas))
        .route("/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}", put(enable_system_schema).delete(disable_system_schema))
        // temporary credentials
        .route("/api/2.0/unity-catalog/temporary-table-credentials", post(temp_table_credentials))
        // artifact allowlists
        .route("/api/2.1/unity-catalog/artifact-allowlists/{artifact_type}", get(get_allowlist).put(put_allowlist))
        // lineage tracking
        .route("/api/2.0/lineage-tracking/table-lineage", get(table_lineage).post(table_lineage_post))
        .route("/api/2.0/lineage-tracking/column-lineage", get(column_lineage).post(column_lineage_post))
        // row filters / column masks (Databricks sets these via ALTER TABLE; SQL form not yet parsed)
        .route("/api/2.0/lakeforge/unity-catalog/tables/{table}/row-filter", put(set_row_filter).delete(drop_row_filter))
        .route("/api/2.0/lakeforge/unity-catalog/tables/{table}/column-masks", put(set_column_mask))
        .route("/api/2.0/lakeforge/unity-catalog/tables/{table}/column-masks/{column}", delete(drop_column_mask))
        // Lakeforge extensions used by the UI
        .route("/api/2.0/lakeforge/audit", get(audit_log))
        .route("/api/2.0/lakeforge/system-tables", get(list_system_tables))
        .route("/api/2.0/lakeforge/system-tables/{schema}/{name}", get(system_table_rows))
        .route("/api/2.0/lakeforge/catalog/{securable_type}/{full_name}", get(securable_details))
        .route("/api/2.0/lakeforge/metastore/stats", get(metastore_stats))
}
