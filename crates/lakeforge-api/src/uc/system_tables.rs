//! The `system` catalog and `information_schema`: Databricks-shaped tables
//! built from control-plane state. They are exposed two ways:
//!
//! * as virtual UC securables (so `GET /tables?catalog_name=system...` and the
//!   catalog explorer show them), and
//! * as real Parquet tables registered on every running Forge cluster, so
//!   `SELECT * FROM system.access.audit` works from any SQL client. Tables are
//!   re-materialised right before a statement that references them runs.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use bytes::Bytes;
use serde_json::{json, Map, Value};

use crate::api::catalog::{self, KIND_CATALOG, KIND_CONNECTION, KIND_EXT_LOCATION, KIND_FUNCTION, KIND_SCHEMA, KIND_STORAGE_CRED, KIND_TABLE, KIND_VOLUME, METASTORE_ID, SYSTEM_CATALOG};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{Doc, Filter};
use crate::uc::privileges::{Securable, KIND_MODEL};
use crate::uc::models::KIND_MODEL_VERSION;

#[derive(Debug, Clone)]
pub enum Ty {
    Str,
    Long,
    Int,
    Double,
    Bool,
    Ts,
    Date,
    Struct(Vec<(&'static str, Ty)>),
}

impl Ty {
    pub fn type_text(&self) -> String {
        match self {
            Ty::Str => "string".into(),
            Ty::Long => "bigint".into(),
            Ty::Int => "int".into(),
            Ty::Double => "double".into(),
            Ty::Bool => "boolean".into(),
            Ty::Ts => "timestamp".into(),
            Ty::Date => "date".into(),
            Ty::Struct(f) => format!("struct<{}>", f.iter().map(|(n, t)| format!("{n}:{}", t.type_text())).collect::<Vec<_>>().join(",")),
        }
    }
    pub fn type_name(&self) -> &'static str {
        match self {
            Ty::Str => "STRING",
            Ty::Long => "LONG",
            Ty::Int => "INT",
            Ty::Double => "DOUBLE",
            Ty::Bool => "BOOLEAN",
            Ty::Ts => "TIMESTAMP",
            Ty::Date => "DATE",
            Ty::Struct(_) => "STRUCT",
        }
    }
    pub fn arrow(&self) -> DataType {
        match self {
            Ty::Str => DataType::Utf8,
            Ty::Long => DataType::Int64,
            Ty::Int => DataType::Int32,
            Ty::Double => DataType::Float64,
            Ty::Bool => DataType::Boolean,
            Ty::Ts => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Ty::Date => DataType::Date32,
            Ty::Struct(f) => DataType::Struct(Fields::from(f.iter().map(|(n, t)| Field::new(*n, t.arrow(), true)).collect::<Vec<_>>())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SysTable {
    pub schema: &'static str,
    pub name: &'static str,
    pub comment: &'static str,
    pub columns: Vec<(&'static str, Ty)>,
}

pub const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "access", "query", "compute", "lakeflow", "billing", "mlflow", "serving", "lakebase"];

pub fn system_schemas() -> &'static [&'static str] {
    SYSTEM_SCHEMAS
}

fn s(n: &'static str) -> (&'static str, Ty) {
    (n, Ty::Str)
}
fn l(n: &'static str) -> (&'static str, Ty) {
    (n, Ty::Long)
}
fn b(n: &'static str) -> (&'static str, Ty) {
    (n, Ty::Bool)
}
fn t(n: &'static str) -> (&'static str, Ty) {
    (n, Ty::Ts)
}
fn d(n: &'static str) -> (&'static str, Ty) {
    (n, Ty::Date)
}
fn f(n: &'static str) -> (&'static str, Ty) {
    (n, Ty::Double)
}

fn common_object_cols() -> Vec<(&'static str, Ty)> {
    vec![s("comment"), s("created"), t("created_at"), s("created_by"), t("last_altered"), s("last_altered_by")]
}

fn privileges_cols(object: &'static [&'static str]) -> Vec<(&'static str, Ty)> {
    let mut c = vec![s("grantor"), s("grantee")];
    c.extend(object.iter().map(|n| s(n)));
    c.extend([s("privilege_type"), s("is_grantable"), s("inherited_from")]);
    c
}

/// The full registry of system / information_schema tables.
pub fn tables() -> Vec<SysTable> {
    let mut v = vec![];
    let is = "information_schema";
    let mut add = |schema: &'static str, name: &'static str, comment: &'static str, columns: Vec<(&'static str, Ty)>| v.push(SysTable { schema, name, comment, columns });

    // ---- information_schema
    add(is, "information_schema_catalog_name", "Name of the catalog this information schema belongs to", vec![s("catalog_name")]);
    add(is, "metastores", "Metastores", vec![s("metastore_id"), s("metastore_name"), s("metastore_owner"), s("cloud"), s("region"), s("storage_root"), s("delta_sharing_scope"), s("privilege_model_version"), t("created"), s("created_by"), t("last_altered"), s("last_altered_by")]);
    add(is, "catalogs", "Catalogs in the metastore", [vec![s("catalog_name"), s("catalog_owner"), s("catalog_type"), s("securable_kind"), s("connection_name"), s("provider_name"), s("share_name")], common_object_cols()].concat());
    add(is, "schemata", "Schemas in the metastore", [vec![s("catalog_name"), s("schema_name"), s("schema_owner"), s("storage_root")], common_object_cols()].concat());
    add(is, "tables", "Tables and views", [vec![s("table_catalog"), s("table_schema"), s("table_name"), s("table_type"), s("is_insertable_into"), s("commit_action"), s("table_owner"), s("data_source_format"), s("storage_sub_directory"), s("storage_path"), s("view_definition"), s("row_filter_function")], common_object_cols()].concat());
    add(is, "views", "View definitions", vec![s("table_catalog"), s("table_schema"), s("table_name"), s("view_definition"), s("check_option"), s("is_updatable"), s("is_insertable_into"), s("sql_path"), s("is_materialized")]);
    add(is, "columns", "Table columns", vec![s("table_catalog"), s("table_schema"), s("table_name"), s("column_name"), l("ordinal_position"), s("column_default"), s("is_nullable"), s("full_data_type"), s("data_type"), s("comment"), s("is_identity"), s("is_generated"), s("mask_function"), l("partition_index")]);
    add(is, "volumes", "Volumes", [vec![s("volume_catalog"), s("volume_schema"), s("volume_name"), s("volume_type"), s("volume_owner"), s("storage_location")], common_object_cols()].concat());
    add(is, "routines", "SQL and Python functions", [vec![s("specific_catalog"), s("specific_schema"), s("specific_name"), s("routine_catalog"), s("routine_schema"), s("routine_name"), s("routine_type"), s("data_type"), s("full_data_type"), s("routine_body"), s("routine_definition"), s("external_language"), s("is_deterministic"), s("sql_data_access"), s("security_type"), s("routine_owner")], common_object_cols()].concat());
    add(is, "parameters", "Function parameters", vec![s("specific_catalog"), s("specific_schema"), s("specific_name"), l("ordinal_position"), s("parameter_mode"), s("parameter_name"), s("data_type"), s("full_data_type"), s("parameter_default"), s("comment")]);
    add(is, "models", "Registered models", [vec![s("model_catalog"), s("model_schema"), s("model_name"), s("model_owner"), s("storage_location")], common_object_cols()].concat());
    add(is, "model_versions", "Model versions", vec![s("model_catalog"), s("model_schema"), s("model_name"), l("model_version"), s("model_version_status"), s("source"), s("run_id"), s("storage_location"), s("comment"), s("created_by"), t("created_at"), s("last_altered_by"), t("last_altered")]);
    add(is, "model_version_aliases", "Model aliases", vec![s("model_catalog"), s("model_schema"), s("model_name"), s("alias_name"), l("model_version")]);
    add(is, "catalog_privileges", "Grants on catalogs", privileges_cols(&["catalog_name"]));
    add(is, "schema_privileges", "Grants on schemas", privileges_cols(&["catalog_name", "schema_name"]));
    add(is, "table_privileges", "Grants on tables", privileges_cols(&["table_catalog", "table_schema", "table_name"]));
    add(is, "volume_privileges", "Grants on volumes", privileges_cols(&["volume_catalog", "volume_schema", "volume_name"]));
    add(is, "routine_privileges", "Grants on functions", privileges_cols(&["specific_catalog", "specific_schema", "specific_name"]));
    add(is, "external_location_privileges", "Grants on external locations", privileges_cols(&["external_location_name"]));
    add(is, "storage_credential_privileges", "Grants on storage credentials", privileges_cols(&["storage_credential_name"]));
    add(is, "connection_privileges", "Grants on connections", privileges_cols(&["connection_name"]));
    add(is, "metastore_privileges", "Grants on the metastore", privileges_cols(&["metastore_id"]));
    add(is, "catalog_tags", "Catalog tags", vec![s("catalog_name"), s("tag_name"), s("tag_value")]);
    add(is, "schema_tags", "Schema tags", vec![s("catalog_name"), s("schema_name"), s("tag_name"), s("tag_value")]);
    add(is, "table_tags", "Table tags", vec![s("catalog_name"), s("schema_name"), s("table_name"), s("tag_name"), s("tag_value")]);
    add(is, "column_tags", "Column tags", vec![s("catalog_name"), s("schema_name"), s("table_name"), s("column_name"), s("tag_name"), s("tag_value")]);
    add(is, "volume_tags", "Volume tags", vec![s("catalog_name"), s("schema_name"), s("volume_name"), s("tag_name"), s("tag_value")]);
    add(is, "table_constraints", "Table constraints", vec![s("constraint_catalog"), s("constraint_schema"), s("constraint_name"), s("table_catalog"), s("table_schema"), s("table_name"), s("constraint_type"), s("is_deferrable"), s("initially_deferred"), s("enforced")]);
    add(is, "key_column_usage", "Columns in PK/FK constraints", vec![s("constraint_catalog"), s("constraint_schema"), s("constraint_name"), s("table_catalog"), s("table_schema"), s("table_name"), s("column_name"), l("ordinal_position"), l("position_in_unique_constraint")]);
    add(is, "referential_constraints", "Foreign keys", vec![s("constraint_catalog"), s("constraint_schema"), s("constraint_name"), s("unique_constraint_catalog"), s("unique_constraint_schema"), s("unique_constraint_name"), s("match_option"), s("update_rule"), s("delete_rule")]);
    add(is, "constraint_column_usage", "Columns referenced by constraints", vec![s("table_catalog"), s("table_schema"), s("table_name"), s("column_name"), s("constraint_catalog"), s("constraint_schema"), s("constraint_name")]);
    add(is, "check_constraints", "CHECK constraints", vec![s("constraint_catalog"), s("constraint_schema"), s("constraint_name"), s("sql_path")]);
    add(is, "row_filters", "Row filters", vec![s("table_catalog"), s("table_schema"), s("table_name"), s("filter_catalog"), s("filter_schema"), s("filter_name"), s("target_columns")]);
    add(is, "column_masks", "Column masks", vec![s("table_catalog"), s("table_schema"), s("table_name"), s("column_name"), s("mask_catalog"), s("mask_schema"), s("mask_name"), s("using_columns")]);
    add(is, "external_locations", "External locations", [vec![s("external_location_name"), s("url"), s("storage_credential_name"), s("external_location_owner"), b("read_only")], common_object_cols()].concat());
    add(is, "storage_credentials", "Storage credentials", [vec![s("storage_credential_name"), s("storage_credential_owner"), s("credential_type"), b("read_only")], common_object_cols()].concat());
    add(is, "connections", "Connections", [vec![s("connection_name"), s("connection_type"), s("connection_owner"), s("connection_url"), s("credential_type"), b("read_only")], common_object_cols()].concat());
    add(is, "shares", "Delta Sharing shares", vec![s("share_name"), s("share_owner"), s("comment"), t("created"), s("created_by")]);
    add(is, "recipients", "Delta Sharing recipients", vec![s("recipient_name"), s("recipient_owner"), s("authentication_type"), s("comment"), t("created"), s("created_by")]);
    add(is, "providers", "Delta Sharing providers", vec![s("provider_name"), s("provider_owner"), s("authentication_type"), s("comment"), t("created"), s("created_by")]);

    // ---- access
    add(
        "access",
        "audit",
        "Audit log of API and SQL activity",
        vec![
            s("version"),
            t("event_time"),
            d("event_date"),
            s("workspace_id"),
            s("source_ip_address"),
            s("user_agent"),
            s("session_id"),
            ("user_identity", Ty::Struct(vec![("email", Ty::Str), ("subject_name", Ty::Str)])),
            s("service_name"),
            s("action_name"),
            s("request_id"),
            s("request_params"),
            ("response", Ty::Struct(vec![("status_code", Ty::Long), ("error_message", Ty::Str), ("result", Ty::Str)])),
            s("audit_level"),
            s("account_id"),
            s("event_id"),
        ],
    );
    let lineage_common = vec![s("account_id"), s("metastore_id"), s("workspace_id"), s("entity_type"), s("entity_id"), s("entity_run_id"), s("statement_id"), s("source_table_full_name"), s("source_table_catalog"), s("source_table_schema"), s("source_table_name")];
    add("access", "table_lineage", "Table-level lineage from executed statements", [lineage_common.clone(), vec![s("source_path"), s("source_type"), s("target_table_full_name"), s("target_table_catalog"), s("target_table_schema"), s("target_table_name"), s("target_path"), s("target_type"), s("created_by"), t("event_time"), d("event_date"), s("event_id")]].concat());
    add("access", "column_lineage", "Column-level lineage from executed statements", [lineage_common, vec![s("source_column_name"), s("source_type"), s("target_table_full_name"), s("target_table_catalog"), s("target_table_schema"), s("target_table_name"), s("target_column_name"), s("target_type"), s("created_by"), t("event_time"), d("event_date"), s("event_id")]].concat());
    add("access", "workspaces_latest", "Workspaces", vec![s("account_id"), s("workspace_id"), s("workspace_name"), s("workspace_url"), t("create_time"), s("status")]);

    // ---- query
    add(
        "query",
        "history",
        "SQL statement history",
        vec![
            s("account_id"),
            s("workspace_id"),
            s("statement_id"),
            s("session_id"),
            s("execution_status"),
            ("compute", Ty::Struct(vec![("type", Ty::Str), ("cluster_id", Ty::Str), ("warehouse_id", Ty::Str)])),
            s("executed_by_user_id"),
            s("executed_by"),
            s("statement_text"),
            s("statement_type"),
            s("error_message"),
            s("client_application"),
            l("total_duration_ms"),
            l("execution_duration_ms"),
            l("compilation_duration_ms"),
            t("start_time"),
            t("end_time"),
            t("update_time"),
            l("read_rows"),
            l("produced_rows"),
            l("read_bytes"),
            l("written_bytes"),
            b("from_result_cache"),
            ("query_source", Ty::Struct(vec![("job_id", Ty::Str), ("notebook_id", Ty::Str), ("pipeline_id", Ty::Str), ("sql_query_id", Ty::Str)])),
        ],
    );

    // ---- compute
    add("compute", "clusters", "All-purpose and job clusters", vec![s("account_id"), s("workspace_id"), s("cluster_id"), s("cluster_name"), s("owned_by"), t("create_time"), t("delete_time"), s("driver_node_type"), s("worker_node_type"), l("worker_count"), l("min_autoscale_workers"), l("max_autoscale_workers"), l("auto_termination_minutes"), s("tags"), s("cluster_source"), s("dbr_version"), s("data_security_mode"), s("policy_id"), s("state"), t("change_time"), d("change_date")]);
    add("compute", "warehouses", "SQL warehouses", vec![s("warehouse_id"), s("workspace_id"), s("account_id"), s("warehouse_name"), s("warehouse_type"), s("warehouse_channel"), s("warehouse_size"), l("min_clusters"), l("max_clusters"), l("auto_stop_minutes"), s("tags"), s("cluster_id"), s("state"), t("change_time"), t("delete_time")]);
    add("compute", "node_types", "Available node types", vec![s("account_id"), s("node_type"), l("core_count"), l("memory_mb"), l("gpu_count")]);
    add("compute", "warehouse_events", "Warehouse lifecycle events", vec![s("account_id"), s("workspace_id"), s("warehouse_id"), s("event_type"), l("cluster_count"), t("event_time")]);

    // ---- lakeflow
    add("lakeflow", "jobs", "Jobs", vec![s("account_id"), s("workspace_id"), s("job_id"), s("name"), s("description"), s("creator_id"), s("run_as"), s("tags"), t("change_time"), t("delete_time")]);
    add("lakeflow", "job_tasks", "Job tasks", vec![s("account_id"), s("workspace_id"), s("job_id"), s("task_key"), s("depends_on_keys"), s("task_type"), t("change_time"), t("delete_time")]);
    add("lakeflow", "job_run_timeline", "Job runs", vec![s("account_id"), s("workspace_id"), s("job_id"), s("run_id"), s("run_name"), t("period_start_time"), t("period_end_time"), s("trigger_type"), s("run_type"), s("compute_ids"), s("result_state"), s("termination_code"), s("job_parameters")]);
    add("lakeflow", "job_task_run_timeline", "Job task runs", vec![s("account_id"), s("workspace_id"), s("job_id"), s("run_id"), s("job_run_id"), s("task_key"), t("period_start_time"), t("period_end_time"), s("compute_ids"), s("result_state"), s("termination_code")]);
    add("lakeflow", "pipelines", "Pipelines", vec![s("account_id"), s("workspace_id"), s("pipeline_id"), s("pipeline_type"), s("name"), s("created_by"), s("run_as"), s("tags"), s("settings"), s("configuration"), t("change_time"), t("delete_time")]);
    add("lakeflow", "pipeline_update_timeline", "Pipeline updates", vec![s("account_id"), s("workspace_id"), s("pipeline_id"), s("update_id"), s("cause"), s("state"), t("period_start_time"), t("period_end_time")]);

    // ---- billing
    add(
        "billing",
        "usage",
        "Estimated DBU usage per compute resource (derived from cluster/warehouse/job runtime)",
        vec![
            s("record_id"),
            s("account_id"),
            s("workspace_id"),
            s("sku_name"),
            s("cloud"),
            t("usage_start_time"),
            t("usage_end_time"),
            d("usage_date"),
            s("custom_tags"),
            s("usage_unit"),
            f("usage_quantity"),
            ("usage_metadata", Ty::Struct(vec![("cluster_id", Ty::Str), ("job_id", Ty::Str), ("job_run_id", Ty::Str), ("warehouse_id", Ty::Str), ("node_type", Ty::Str), ("endpoint_name", Ty::Str), ("database_instance_id", Ty::Str)])),
            ("identity_metadata", Ty::Struct(vec![("run_as", Ty::Str), ("owned_by", Ty::Str)])),
            s("record_type"),
            d("ingestion_date"),
            s("billing_origin_product"),
            s("usage_type"),
        ],
    );
    add("billing", "list_prices", "List prices per SKU", vec![t("price_start_time"), t("price_end_time"), s("account_id"), s("sku_name"), s("cloud"), s("currency_code"), s("usage_unit"), ("pricing", Ty::Struct(vec![("default", Ty::Double)]))]);

    // ---- mlflow / serving / lakebase
    add("mlflow", "experiments_latest", "MLflow experiments", vec![s("account_id"), s("workspace_id"), s("experiment_id"), s("name"), s("artifact_location"), s("lifecycle_stage"), t("create_time"), t("last_update_time")]);
    add("mlflow", "runs_latest", "MLflow runs", vec![s("account_id"), s("workspace_id"), s("experiment_id"), s("run_id"), s("run_name"), s("status"), s("user_id"), t("start_time"), t("end_time")]);
    add("serving", "endpoint_usage", "Model serving requests", vec![s("account_id"), s("workspace_id"), s("endpoint_name"), s("served_entity_name"), s("state"), l("request_count"), t("change_time")]);
    add("lakebase", "instances", "Lakebase database instances", vec![s("account_id"), s("workspace_id"), s("uid"), s("name"), s("state"), s("capacity"), s("pg_version"), s("creator"), s("backend"), s("read_write_dns"), l("node_count"), t("creation_time"), t("change_time")]);
    add("lakebase", "synced_tables", "Lakebase synced tables", vec![s("account_id"), s("workspace_id"), s("name"), s("database_instance_name"), s("logical_database_name"), s("source_table_full_name"), s("scheduling_policy"), s("state"), s("pipeline_id"), l("synced_rows"), t("last_sync_time")]);
    v
}

pub fn find(schema: &str, name: &str) -> Option<SysTable> {
    tables().into_iter().find(|t| t.schema == schema && t.name == name)
}

// ---------------------------------------------------------------- virtual UC docs

pub fn system_catalog_doc(owner: &str) -> Value {
    json!({
        "name": SYSTEM_CATALOG, "full_name": SYSTEM_CATALOG, "catalog_type": "SYSTEM_CATALOG", "securable_type": "CATALOG", "securable_kind": "CATALOG_SYSTEM",
        "owner": owner, "comment": "System tables: audit, lineage, query history, compute, jobs, billing and the metastore information schema.",
        "metastore_id": METASTORE_ID, "isolation_mode": "OPEN", "properties": {}, "created_at": 0, "created_by": owner, "updated_at": 0, "updated_by": owner, "browse_only": false,
    })
}

pub fn virtual_schema_doc(cat: &str, sch: &str, owner: &str) -> Value {
    json!({
        "name": sch, "catalog_name": cat, "full_name": format!("{cat}.{sch}"), "securable_type": "SCHEMA", "catalog_type": if cat == SYSTEM_CATALOG { "SYSTEM_CATALOG" } else { "MANAGED_CATALOG" },
        "owner": owner, "comment": if sch == "information_schema" { "Metadata about the metastore" } else { "System tables" }, "metastore_id": METASTORE_ID, "properties": {},
        "created_at": 0, "created_by": owner, "updated_at": 0, "updated_by": owner,
    })
}

fn table_doc(t: &SysTable, cat: &str, owner: &str) -> Value {
    let cols: Vec<Value> = t
        .columns
        .iter()
        .enumerate()
        .map(|(i, (n, ty))| json!({ "name": n, "type_text": ty.type_text(), "type_name": ty.type_name(), "position": i, "nullable": true, "type_json": json!({ "name": n, "type": ty.type_text(), "nullable": true }).to_string() }))
        .collect();
    json!({
        "name": t.name, "catalog_name": cat, "schema_name": t.schema, "full_name": format!("{cat}.{}.{}", t.schema, t.name), "table_type": "SYSTEM", "data_source_format": "PARQUET",
        "securable_type": "TABLE", "owner": owner, "comment": t.comment, "columns": cols, "properties": { "lakeforge.system_table": "true" }, "metastore_id": METASTORE_ID,
        "created_at": 0, "created_by": owner, "updated_at": 0, "updated_by": owner, "table_id": format!("system-{}-{}", t.schema, t.name), "browse_only": false,
    })
}

/// DataFusion's built-in `information_schema` (per catalog) — reported for
/// `<catalog>.information_schema` so the explorer shows what SQL will see.
fn datafusion_info_schema(cat: &str, owner: &str) -> Vec<Value> {
    let mk = |name: &'static str, cols: &[&'static str]| {
        json!({
            "name": name, "catalog_name": cat, "schema_name": "information_schema", "full_name": format!("{cat}.information_schema.{name}"), "table_type": "SYSTEM", "data_source_format": "VIEW",
            "securable_type": "TABLE", "owner": owner, "comment": "DataFusion information schema view", "metastore_id": METASTORE_ID, "properties": { "lakeforge.engine_view": "true" },
            "columns": cols.iter().enumerate().map(|(i, c)| json!({ "name": c, "type_text": "string", "type_name": "STRING", "position": i, "nullable": true })).collect::<Vec<_>>(),
            "created_at": 0, "created_by": owner, "updated_at": 0, "updated_by": owner, "table_id": format!("df-{cat}-{name}"),
        })
    };
    vec![
        mk("tables", &["table_catalog", "table_schema", "table_name", "table_type"]),
        mk("views", &["table_catalog", "table_schema", "table_name", "definition"]),
        mk("columns", &["table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "column_default", "is_nullable", "data_type"]),
        mk("schemata", &["catalog_name", "schema_name", "schema_owner"]),
        mk("routines", &["specific_catalog", "specific_schema", "specific_name", "routine_name", "routine_type", "data_type"]),
        mk("parameters", &["specific_catalog", "specific_schema", "specific_name", "ordinal_position", "parameter_name", "data_type"]),
        mk("df_settings", &["name", "value", "description"]),
    ]
}

pub fn virtual_tables_in(cat: &str, sch: &str, owner: &str) -> Vec<Value> {
    if cat == SYSTEM_CATALOG {
        tables().iter().filter(|t| t.schema == sch).map(|t| table_doc(t, cat, owner)).collect()
    } else if sch == "information_schema" {
        datafusion_info_schema(cat, owner)
    } else {
        vec![]
    }
}

pub fn virtual_table_doc(cat: &str, sch: &str, name: &str, owner: &str) -> Option<Value> {
    virtual_tables_in(cat, sch, owner).into_iter().find(|t| t["name"] == name)
}

// ---------------------------------------------------------------- row helpers

pub fn ts(ms: i64) -> Value {
    match chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms) {
        Some(dt) if ms > 0 => json!(dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
        _ => Value::Null,
    }
}

pub fn date(ms: i64) -> Value {
    match chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms) {
        Some(dt) if ms > 0 => json!(dt.format("%Y-%m-%d").to_string()),
        _ => Value::Null,
    }
}

fn ts_v(v: &Value) -> Value {
    v.as_i64().map(ts).unwrap_or(Value::Null)
}

fn sv(v: &Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::String(_) => v.clone(),
        other => json!(other.to_string()),
    }
}

fn strs(v: &Value) -> Value {
    if v.is_null() {
        Value::Null
    } else {
        json!(v.to_string())
    }
}

fn common_fields(row: &mut Map<String, Value>, o: &Value) {
    row.insert("comment".into(), sv(&o["comment"]));
    row.insert("created".into(), ts_v(&o["created_at"]));
    row.insert("created_at".into(), ts_v(&o["created_at"]));
    row.insert("created_by".into(), sv(&o["created_by"]));
    row.insert("last_altered".into(), ts_v(&o["updated_at"]));
    row.insert("last_altered_by".into(), sv(&o["updated_by"]));
}

fn parts3(full: &str) -> (Value, Value, Value) {
    let (c, s, n) = catalog::split_name(full);
    (json!(c), json!(s), json!(n))
}

fn tags_of(o: &Value) -> Vec<(String, String)> {
    o["tags"].as_array().map(|a| a.iter().filter_map(|t| Some((t["key"].as_str()?.to_string(), t["value"].as_str().unwrap_or("").to_string()))).collect()).unwrap_or_default()
}

impl AppState {
    async fn docs(&self, kind: &str) -> ApiResult<Vec<Value>> {
        let d: Vec<Doc<Value>> = self.store.list(kind, self.ws(), Filter::default()).await?;
        Ok(d.into_iter().map(|d| d.data).collect())
    }

    async fn privilege_rows(&self, sec: Securable, key_cols: &[&str]) -> ApiResult<Vec<Value>> {
        let mut out = vec![];
        for (key, assignments) in self.all_grants().await? {
            let Some((t, name)) = key.split_once(':') else { continue };
            if Securable::parse(t).ok() != Some(sec) {
                continue;
            }
            let owner = match sec.kind() {
                None => self.config.admin_user.clone(),
                Some(k) => self.store.get::<Value>(k, &format!("{k}:{name}")).await?.and_then(|d| d.data["owner"].as_str().map(str::to_string)).unwrap_or_default(),
            };
            let parts: Vec<&str> = name.split('.').collect();
            for a in assignments {
                for p in a.privileges {
                    let mut row = Map::new();
                    row.insert("grantor".into(), json!(owner));
                    row.insert("grantee".into(), json!(a.principal));
                    for (i, k) in key_cols.iter().enumerate() {
                        row.insert((*k).into(), json!(parts.get(i).copied().unwrap_or(name)));
                    }
                    row.insert("privilege_type".into(), json!(p));
                    row.insert("is_grantable".into(), json!("NO"));
                    row.insert("inherited_from".into(), json!("NONE"));
                    out.push(Value::Object(row));
                }
            }
        }
        Ok(out)
    }

    /// Rows for one system table.
    pub async fn system_table_rows(self: &Arc<Self>, schema: &str, name: &str) -> ApiResult<Vec<Value>> {
        let owner = self.config.admin_user.clone();
        let ws = self.ws().to_string();
        let acct = json!("lakeforge");
        Ok(match (schema, name) {
            ("information_schema", "information_schema_catalog_name") => vec![json!({ "catalog_name": SYSTEM_CATALOG })],
            ("information_schema", "metastores") => {
                let m = catalog::metastore_summary(self);
                vec![json!({ "metastore_id": m["metastore_id"], "metastore_name": m["name"], "metastore_owner": m["owner"], "cloud": m["cloud"], "region": m["region"], "storage_root": m["storage_root"], "delta_sharing_scope": m["delta_sharing_scope"], "privilege_model_version": m["privilege_model_version"], "created": ts_v(&m["created_at"]), "created_by": m["created_by"], "last_altered": ts_v(&m["created_at"]), "last_altered_by": m["created_by"] })]
            }
            ("information_schema", "catalogs") => {
                let mut docs = self.docs(KIND_CATALOG).await?;
                docs.push(system_catalog_doc(&owner));
                docs.iter()
                    .map(|c| {
                        let mut r = Map::new();
                        r.insert("catalog_name".into(), c["name"].clone());
                        r.insert("catalog_owner".into(), c["owner"].clone());
                        r.insert("catalog_type".into(), c["catalog_type"].clone());
                        r.insert("securable_kind".into(), c["securable_kind"].clone());
                        r.insert("connection_name".into(), c["connection_name"].clone());
                        r.insert("provider_name".into(), c["provider_name"].clone());
                        r.insert("share_name".into(), c["share_name"].clone());
                        common_fields(&mut r, c);
                        Value::Object(r)
                    })
                    .collect()
            }
            ("information_schema", "schemata") => {
                let mut docs = self.docs(KIND_SCHEMA).await?;
                for sch in SYSTEM_SCHEMAS {
                    docs.push(virtual_schema_doc(SYSTEM_CATALOG, sch, &owner));
                }
                docs.iter()
                    .map(|c| {
                        let mut r = Map::new();
                        r.insert("catalog_name".into(), c["catalog_name"].clone());
                        r.insert("schema_name".into(), c["name"].clone());
                        r.insert("schema_owner".into(), c["owner"].clone());
                        r.insert("storage_root".into(), c["storage_root"].clone());
                        common_fields(&mut r, c);
                        Value::Object(r)
                    })
                    .collect()
            }
            ("information_schema", "tables") => {
                let mut docs = self.docs(KIND_TABLE).await?;
                docs.extend(tables().iter().map(|t| table_doc(t, SYSTEM_CATALOG, &owner)));
                docs.iter()
                    .map(|t| {
                        let mut r = Map::new();
                        r.insert("table_catalog".into(), t["catalog_name"].clone());
                        r.insert("table_schema".into(), t["schema_name"].clone());
                        r.insert("table_name".into(), t["name"].clone());
                        r.insert("table_type".into(), t["table_type"].clone());
                        r.insert("is_insertable_into".into(), json!(if t["table_type"] == "VIEW" || t["table_type"] == "SYSTEM" { "NO" } else { "YES" }));
                        r.insert("commit_action".into(), json!("PRESERVE"));
                        r.insert("table_owner".into(), t["owner"].clone());
                        r.insert("data_source_format".into(), t["data_source_format"].clone());
                        r.insert("storage_sub_directory".into(), Value::Null);
                        r.insert("storage_path".into(), t["storage_location"].clone());
                        r.insert("view_definition".into(), t["view_definition"].clone());
                        r.insert("row_filter_function".into(), t["row_filter"]["function_name"].clone());
                        common_fields(&mut r, t);
                        Value::Object(r)
                    })
                    .collect()
            }
            ("information_schema", "views") => self
                .docs(KIND_TABLE)
                .await?
                .iter()
                .filter(|t| t["table_type"] == "VIEW" || t["table_type"] == "MATERIALIZED_VIEW")
                .map(|t| json!({ "table_catalog": t["catalog_name"], "table_schema": t["schema_name"], "table_name": t["name"], "view_definition": t["view_definition"], "check_option": "NONE", "is_updatable": "NO", "is_insertable_into": "NO", "sql_path": Value::Null, "is_materialized": if t["table_type"] == "MATERIALIZED_VIEW" { "YES" } else { "NO" } }))
                .collect(),
            ("information_schema", "columns") => {
                let mut docs = self.docs(KIND_TABLE).await?;
                docs.extend(tables().iter().map(|t| table_doc(t, SYSTEM_CATALOG, &owner)));
                let mut out = vec![];
                for t in &docs {
                    for (i, c) in t["columns"].as_array().into_iter().flatten().enumerate() {
                        out.push(json!({
                            "table_catalog": t["catalog_name"], "table_schema": t["schema_name"], "table_name": t["name"], "column_name": c["name"],
                            "ordinal_position": c["position"].as_i64().unwrap_or(i as i64), "column_default": Value::Null, "is_nullable": if c["nullable"].as_bool().unwrap_or(true) { "YES" } else { "NO" },
                            "full_data_type": c["type_text"], "data_type": c["type_name"], "comment": c["comment"], "is_identity": "NO", "is_generated": "NEVER",
                            "mask_function": c["mask"]["function_name"], "partition_index": c["partition_index"],
                        }));
                    }
                }
                out
            }
            ("information_schema", "volumes") => self
                .docs(KIND_VOLUME)
                .await?
                .iter()
                .map(|v| {
                    let mut r = Map::new();
                    r.insert("volume_catalog".into(), v["catalog_name"].clone());
                    r.insert("volume_schema".into(), v["schema_name"].clone());
                    r.insert("volume_name".into(), v["name"].clone());
                    r.insert("volume_type".into(), v["volume_type"].clone());
                    r.insert("volume_owner".into(), v["owner"].clone());
                    r.insert("storage_location".into(), v["storage_location"].clone());
                    common_fields(&mut r, v);
                    Value::Object(r)
                })
                .collect(),
            ("information_schema", "routines") => self
                .docs(KIND_FUNCTION)
                .await?
                .iter()
                .map(|fx| {
                    let mut r = Map::new();
                    for k in ["specific_catalog", "routine_catalog"] {
                        r.insert(k.into(), fx["catalog_name"].clone());
                    }
                    for k in ["specific_schema", "routine_schema"] {
                        r.insert(k.into(), fx["schema_name"].clone());
                    }
                    for k in ["specific_name", "routine_name"] {
                        r.insert(k.into(), fx["name"].clone());
                    }
                    r.insert("routine_type".into(), json!(if fx["is_table_function"] == true { "TABLE" } else { "FUNCTION" }));
                    r.insert("data_type".into(), fx["data_type"].clone());
                    r.insert("full_data_type".into(), fx["full_data_type"].clone());
                    r.insert("routine_body".into(), fx["routine_body"].clone());
                    r.insert("routine_definition".into(), fx["routine_definition"].clone());
                    r.insert("external_language".into(), fx["external_language"].clone());
                    r.insert("is_deterministic".into(), json!(if fx["is_deterministic"] == false { "NO" } else { "YES" }));
                    r.insert("sql_data_access".into(), fx["sql_data_access"].clone());
                    r.insert("security_type".into(), fx["security_type"].clone());
                    r.insert("routine_owner".into(), fx["owner"].clone());
                    common_fields(&mut r, fx);
                    Value::Object(r)
                })
                .collect(),
            ("information_schema", "parameters") => {
                let mut out = vec![];
                for fx in self.docs(KIND_FUNCTION).await? {
                    for (i, p) in fx["input_params"]["parameters"].as_array().into_iter().flatten().enumerate() {
                        out.push(json!({ "specific_catalog": fx["catalog_name"], "specific_schema": fx["schema_name"], "specific_name": fx["name"], "ordinal_position": p["position"].as_i64().unwrap_or(i as i64), "parameter_mode": p["parameter_mode"].as_str().unwrap_or("IN"), "parameter_name": p["name"], "data_type": p["type_name"], "full_data_type": p["type_text"], "parameter_default": p["parameter_default"], "comment": p["comment"] }));
                    }
                }
                out
            }
            ("information_schema", "models") => self
                .docs(KIND_MODEL)
                .await?
                .iter()
                .map(|m| {
                    let mut r = Map::new();
                    r.insert("model_catalog".into(), m["catalog_name"].clone());
                    r.insert("model_schema".into(), m["schema_name"].clone());
                    r.insert("model_name".into(), m["name"].clone());
                    r.insert("model_owner".into(), m["owner"].clone());
                    r.insert("storage_location".into(), m["storage_location"].clone());
                    common_fields(&mut r, m);
                    Value::Object(r)
                })
                .collect(),
            ("information_schema", "model_versions") => self
                .docs(KIND_MODEL_VERSION)
                .await?
                .iter()
                .map(|v| json!({ "model_catalog": v["catalog_name"], "model_schema": v["schema_name"], "model_name": v["model_name"], "model_version": v["version"], "model_version_status": v["status"], "source": v["source"], "run_id": v["run_id"], "storage_location": v["storage_location"], "comment": v["comment"], "created_by": v["created_by"], "created_at": ts_v(&v["created_at"]), "last_altered_by": v["updated_by"], "last_altered": ts_v(&v["updated_at"]) }))
                .collect(),
            ("information_schema", "model_version_aliases") => {
                let mut out = vec![];
                for m in self.docs(KIND_MODEL).await? {
                    for a in m["aliases"].as_array().into_iter().flatten() {
                        out.push(json!({ "model_catalog": m["catalog_name"], "model_schema": m["schema_name"], "model_name": m["name"], "alias_name": a["alias_name"], "model_version": a["version_num"] }));
                    }
                }
                out
            }
            ("information_schema", "catalog_privileges") => self.privilege_rows(Securable::Catalog, &["catalog_name"]).await?,
            ("information_schema", "schema_privileges") => self.privilege_rows(Securable::Schema, &["catalog_name", "schema_name"]).await?,
            ("information_schema", "table_privileges") => self.privilege_rows(Securable::Table, &["table_catalog", "table_schema", "table_name"]).await?,
            ("information_schema", "volume_privileges") => self.privilege_rows(Securable::Volume, &["volume_catalog", "volume_schema", "volume_name"]).await?,
            ("information_schema", "routine_privileges") => self.privilege_rows(Securable::Function, &["specific_catalog", "specific_schema", "specific_name"]).await?,
            ("information_schema", "external_location_privileges") => self.privilege_rows(Securable::ExternalLocation, &["external_location_name"]).await?,
            ("information_schema", "storage_credential_privileges") => self.privilege_rows(Securable::StorageCredential, &["storage_credential_name"]).await?,
            ("information_schema", "connection_privileges") => self.privilege_rows(Securable::Connection, &["connection_name"]).await?,
            ("information_schema", "metastore_privileges") => self.privilege_rows(Securable::Metastore, &["metastore_id"]).await?,
            ("information_schema", "catalog_tags") => self.docs(KIND_CATALOG).await?.iter().flat_map(|c| tags_of(c).into_iter().map(move |(k, v)| json!({ "catalog_name": c["name"], "tag_name": k, "tag_value": v }))).collect(),
            ("information_schema", "schema_tags") => self.docs(KIND_SCHEMA).await?.iter().flat_map(|c| tags_of(c).into_iter().map(move |(k, v)| json!({ "catalog_name": c["catalog_name"], "schema_name": c["name"], "tag_name": k, "tag_value": v }))).collect(),
            ("information_schema", "table_tags") => self.docs(KIND_TABLE).await?.iter().flat_map(|c| tags_of(c).into_iter().map(move |(k, v)| json!({ "catalog_name": c["catalog_name"], "schema_name": c["schema_name"], "table_name": c["name"], "tag_name": k, "tag_value": v }))).collect(),
            ("information_schema", "volume_tags") => self.docs(KIND_VOLUME).await?.iter().flat_map(|c| tags_of(c).into_iter().map(move |(k, v)| json!({ "catalog_name": c["catalog_name"], "schema_name": c["schema_name"], "volume_name": c["name"], "tag_name": k, "tag_value": v }))).collect(),
            ("information_schema", "column_tags") => {
                let mut out = vec![];
                for t in self.docs(KIND_TABLE).await? {
                    for c in t["columns"].as_array().into_iter().flatten() {
                        for (k, v) in tags_of(c) {
                            out.push(json!({ "catalog_name": t["catalog_name"], "schema_name": t["schema_name"], "table_name": t["name"], "column_name": c["name"], "tag_name": k, "tag_value": v }));
                        }
                    }
                }
                out
            }
            ("information_schema", "table_constraints" | "key_column_usage" | "referential_constraints" | "constraint_column_usage" | "check_constraints") => {
                let mut tc = vec![];
                let mut kcu = vec![];
                let mut rc = vec![];
                let mut ccu = vec![];
                let mut cc = vec![];
                for t in self.docs(KIND_TABLE).await? {
                    let (cat, sch) = (t["catalog_name"].clone(), t["schema_name"].clone());
                    for c in t["table_constraints"].as_array().into_iter().flatten() {
                        if let Some(pk) = c.get("primary_key_constraint") {
                            tc.push(json!({ "constraint_catalog": cat, "constraint_schema": sch, "constraint_name": pk["name"], "table_catalog": cat, "table_schema": sch, "table_name": t["name"], "constraint_type": "PRIMARY KEY", "is_deferrable": "NO", "initially_deferred": "NO", "enforced": "NO" }));
                            for (i, col) in pk["child_columns"].as_array().into_iter().flatten().enumerate() {
                                kcu.push(json!({ "constraint_catalog": cat, "constraint_schema": sch, "constraint_name": pk["name"], "table_catalog": cat, "table_schema": sch, "table_name": t["name"], "column_name": col, "ordinal_position": i as i64 + 1, "position_in_unique_constraint": Value::Null }));
                                ccu.push(json!({ "table_catalog": cat, "table_schema": sch, "table_name": t["name"], "column_name": col, "constraint_catalog": cat, "constraint_schema": sch, "constraint_name": pk["name"] }));
                            }
                        }
                        if let Some(fk) = c.get("foreign_key_constraint") {
                            tc.push(json!({ "constraint_catalog": cat, "constraint_schema": sch, "constraint_name": fk["name"], "table_catalog": cat, "table_schema": sch, "table_name": t["name"], "constraint_type": "FOREIGN KEY", "is_deferrable": "NO", "initially_deferred": "NO", "enforced": "NO" }));
                            let (pc, ps, pn) = fk["parent_table"].as_str().map(parts3).unwrap_or((Value::Null, Value::Null, Value::Null));
                            rc.push(json!({ "constraint_catalog": cat, "constraint_schema": sch, "constraint_name": fk["name"], "unique_constraint_catalog": pc, "unique_constraint_schema": ps, "unique_constraint_name": format!("{}_pk", pn.as_str().unwrap_or("")), "match_option": "NONE", "update_rule": "NO ACTION", "delete_rule": "NO ACTION" }));
                            for (i, col) in fk["child_columns"].as_array().into_iter().flatten().enumerate() {
                                kcu.push(json!({ "constraint_catalog": cat, "constraint_schema": sch, "constraint_name": fk["name"], "table_catalog": cat, "table_schema": sch, "table_name": t["name"], "column_name": col, "ordinal_position": i as i64 + 1, "position_in_unique_constraint": i as i64 + 1 }));
                            }
                            for col in fk["parent_columns"].as_array().into_iter().flatten() {
                                ccu.push(json!({ "table_catalog": pc, "table_schema": ps, "table_name": pn, "column_name": col, "constraint_catalog": cat, "constraint_schema": sch, "constraint_name": fk["name"] }));
                            }
                        }
                    }
                    if let Some(props) = t["properties"].as_object() {
                        for (k, v) in props {
                            if let Some(n) = k.strip_prefix("delta.constraints.") {
                                tc.push(json!({ "constraint_catalog": cat, "constraint_schema": sch, "constraint_name": n, "table_catalog": cat, "table_schema": sch, "table_name": t["name"], "constraint_type": "CHECK", "is_deferrable": "NO", "initially_deferred": "NO", "enforced": "YES" }));
                                cc.push(json!({ "constraint_catalog": cat, "constraint_schema": sch, "constraint_name": n, "sql_path": v }));
                            }
                        }
                    }
                }
                match name {
                    "table_constraints" => tc,
                    "key_column_usage" => kcu,
                    "referential_constraints" => rc,
                    "constraint_column_usage" => ccu,
                    _ => cc,
                }
            }
            ("information_schema", "row_filters") => self
                .docs(KIND_TABLE)
                .await?
                .iter()
                .filter_map(|t| {
                    let rf = t.get("row_filter")?.as_object()?;
                    let (fc, fs, fname) = parts3(rf.get("function_name")?.as_str()?);
                    Some(json!({ "table_catalog": t["catalog_name"], "table_schema": t["schema_name"], "table_name": t["name"], "filter_catalog": fc, "filter_schema": fs, "filter_name": fname, "target_columns": rf.get("input_column_names").map(|v| v.to_string()) }))
                })
                .collect(),
            ("information_schema", "column_masks") => {
                let mut out = vec![];
                for t in self.docs(KIND_TABLE).await? {
                    for c in t["columns"].as_array().into_iter().flatten() {
                        if let Some(fname) = c["mask"]["function_name"].as_str() {
                            let (mc, ms, mn) = parts3(fname);
                            out.push(json!({ "table_catalog": t["catalog_name"], "table_schema": t["schema_name"], "table_name": t["name"], "column_name": c["name"], "mask_catalog": mc, "mask_schema": ms, "mask_name": mn, "using_columns": c["mask"]["using_column_names"].as_array().map(|a| a.iter().filter_map(|x| x.as_str()).collect::<Vec<_>>().join(",")) }));
                        }
                    }
                }
                out
            }
            ("information_schema", "external_locations") => self
                .docs(KIND_EXT_LOCATION)
                .await?
                .iter()
                .map(|e| {
                    let mut r = Map::new();
                    r.insert("external_location_name".into(), e["name"].clone());
                    r.insert("url".into(), e["url"].clone());
                    r.insert("storage_credential_name".into(), e["credential_name"].clone());
                    r.insert("external_location_owner".into(), e["owner"].clone());
                    r.insert("read_only".into(), json!(e["read_only"].as_bool().unwrap_or(false)));
                    common_fields(&mut r, e);
                    Value::Object(r)
                })
                .collect(),
            ("information_schema", "storage_credentials") => self
                .docs(KIND_STORAGE_CRED)
                .await?
                .iter()
                .map(|e| {
                    let mut r = Map::new();
                    r.insert("storage_credential_name".into(), e["name"].clone());
                    r.insert("storage_credential_owner".into(), e["owner"].clone());
                    let ct = ["aws_iam_role", "azure_service_principal", "azure_managed_identity", "gcp_service_account_key", "databricks_gcp_service_account"].iter().find(|k| e.get(**k).is_some()).map(|k| k.to_ascii_uppercase());
                    r.insert("credential_type".into(), json!(ct));
                    r.insert("read_only".into(), json!(e["read_only"].as_bool().unwrap_or(false)));
                    common_fields(&mut r, e);
                    Value::Object(r)
                })
                .collect(),
            ("information_schema", "connections") => self
                .docs(KIND_CONNECTION)
                .await?
                .iter()
                .map(|e| {
                    let mut r = Map::new();
                    r.insert("connection_name".into(), e["name"].clone());
                    r.insert("connection_type".into(), e["connection_type"].clone());
                    r.insert("connection_owner".into(), e["owner"].clone());
                    r.insert("connection_url".into(), json!(format!("{}:{}", e["options"]["host"].as_str().unwrap_or(""), e["options"]["port"].as_str().unwrap_or("5432"))));
                    r.insert("credential_type".into(), e["credential_type"].clone());
                    r.insert("read_only".into(), json!(e["read_only"].as_bool().unwrap_or(false)));
                    common_fields(&mut r, e);
                    Value::Object(r)
                })
                .collect(),
            ("information_schema", "shares") => self.docs(crate::uc::privileges::KIND_SHARE).await?.iter().map(|e| json!({ "share_name": e["name"], "share_owner": e["owner"], "comment": e["comment"], "created": ts_v(&e["created_at"]), "created_by": e["created_by"] })).collect(),
            ("information_schema", "recipients") => self.docs(crate::uc::privileges::KIND_RECIPIENT).await?.iter().map(|e| json!({ "recipient_name": e["name"], "recipient_owner": e["owner"], "authentication_type": e["authentication_type"], "comment": e["comment"], "created": ts_v(&e["created_at"]), "created_by": e["created_by"] })).collect(),
            ("information_schema", "providers") => self.docs(crate::uc::privileges::KIND_PROVIDER).await?.iter().map(|e| json!({ "provider_name": e["name"], "provider_owner": e["owner"], "authentication_type": e["authentication_type"], "comment": e["comment"], "created": ts_v(&e["created_at"]), "created_by": e["created_by"] })).collect(),

            // ---- access
            ("access", "audit") => self
                .audit_events(50_000, None, None, None, None)
                .await?
                .into_iter()
                .map(|e| {
                    json!({
                        "version": "2.0", "event_time": ts(e.event_time), "event_date": date(e.event_time), "workspace_id": e.workspace_id, "source_ip_address": e.source_ip_address, "user_agent": e.user_agent, "session_id": e.session_id,
                        "user_identity": { "email": e.user_identity.email, "subject_name": e.user_identity.subject_name }, "service_name": e.service_name, "action_name": e.action_name, "request_id": e.request_id,
                        "request_params": Value::Object(e.request_params).to_string(), "response": { "status_code": e.response.status_code, "error_message": e.response.error_message, "result": e.response.result },
                        "audit_level": e.audit_level, "account_id": "lakeforge", "event_id": e.event_id,
                    })
                })
                .collect(),
            ("access", "table_lineage") => self.table_lineage_rows().await?.iter().map(|r| r.to_row(METASTORE_ID)).collect(),
            ("access", "column_lineage") => self.column_lineage_rows().await?.iter().map(|r| r.to_row(METASTORE_ID)).collect(),
            ("access", "workspaces_latest") => vec![json!({ "account_id": acct, "workspace_id": ws, "workspace_name": "lakeforge", "workspace_url": self.config.public_url, "create_time": ts(self.started_at.timestamp_millis()), "status": "RUNNING" })],

            // ---- query
            ("query", "history") => {
                let docs: Vec<Doc<Value>> = self.store.list(crate::api::sql::KIND_HISTORY, self.ws(), Filter { newest_first: true, limit: Some(50_000), ..Default::default() }).await?;
                docs.iter()
                    .map(|d| {
                        let h = &d.data;
                        let start = h["query_start_time_ms"].as_i64().unwrap_or(0);
                        let end = h["query_end_time_ms"].as_i64().unwrap_or(start);
                        json!({
                            "account_id": acct, "workspace_id": ws, "statement_id": h["query_id"], "session_id": h["session_id"], "execution_status": match h["status"].as_str() { Some("FINISHED") => "FINISHED", Some("FAILED") => "FAILED", Some("CANCELED") => "CANCELED", _ => "RUNNING" },
                            "compute": { "type": if h["warehouse_id"].is_null() { "CLUSTER" } else { "WAREHOUSE" }, "cluster_id": h["cluster_id"], "warehouse_id": h["warehouse_id"] },
                            "executed_by_user_id": h["user_id"], "executed_by": h["user_name"], "statement_text": h["query_text"], "statement_type": h["statement_type"], "error_message": h["error_message"],
                            "client_application": h["client_application"].as_str().unwrap_or("Lakeforge"), "total_duration_ms": end - start, "execution_duration_ms": h["metrics"]["execution_time_ms"].as_i64().unwrap_or(end - start), "compilation_duration_ms": 0,
                            "start_time": ts(start), "end_time": ts(end), "update_time": ts(end), "read_rows": h["metrics"]["read_rows"], "produced_rows": h["rows_produced"], "read_bytes": h["metrics"]["read_bytes"], "written_bytes": h["metrics"]["written_bytes"], "from_result_cache": false,
                            "query_source": { "job_id": h["query_source"]["job_id"], "notebook_id": h["query_source"]["notebook_id"], "pipeline_id": h["query_source"]["pipeline_id"], "sql_query_id": h["query_source"]["sql_query_id"] },
                        })
                    })
                    .collect()
            }

            // ---- compute
            ("compute", "clusters") => self
                .docs(crate::api::clusters::KIND)
                .await?
                .iter()
                .map(|c| {
                    let deleted = c["state"] == "TERMINATED" && c["terminated_time"].as_i64().unwrap_or(0) > 0;
                    json!({
                        "account_id": acct, "workspace_id": ws, "cluster_id": c["cluster_id"], "cluster_name": c["cluster_name"], "owned_by": c["creator_user_name"], "create_time": ts_v(&c["start_time"]), "delete_time": if deleted { ts_v(&c["terminated_time"]) } else { Value::Null },
                        "driver_node_type": c["driver_node_type_id"].as_str().or(c["node_type_id"].as_str()), "worker_node_type": c["node_type_id"], "worker_count": c["num_workers"], "min_autoscale_workers": c["autoscale"]["min_workers"], "max_autoscale_workers": c["autoscale"]["max_workers"],
                        "auto_termination_minutes": c["autotermination_minutes"], "tags": strs(&c["custom_tags"]), "cluster_source": c["cluster_source"], "dbr_version": c["spark_version"], "data_security_mode": c["data_security_mode"], "policy_id": c["policy_id"], "state": c["state"],
                        "change_time": ts_v(&c["last_activity_time"]), "change_date": c["last_activity_time"].as_i64().map(date).unwrap_or(Value::Null),
                    })
                })
                .collect(),
            ("compute", "warehouses") => self
                .docs(crate::api::sql::KIND_WAREHOUSE)
                .await?
                .iter()
                .map(|w| json!({ "warehouse_id": w["id"], "workspace_id": ws, "account_id": acct, "warehouse_name": w["name"], "warehouse_type": w["warehouse_type"], "warehouse_channel": w["channel"]["name"], "warehouse_size": w["cluster_size"], "min_clusters": w["min_num_clusters"], "max_clusters": w["max_num_clusters"], "auto_stop_minutes": w["auto_stop_mins"], "tags": strs(&w["tags"]), "cluster_id": w["cluster_id"], "state": w["state_hint"], "change_time": ts_v(&w["creation_time"]), "delete_time": if w["state_hint"] == "DELETED" { ts(chrono::Utc::now().timestamp_millis()) } else { Value::Null } }))
                .collect(),
            ("compute", "node_types") => crate::api::clusters::node_types().iter().map(|n| json!({ "account_id": acct, "node_type": n.node_type_id, "core_count": n.num_cores as i64, "memory_mb": n.memory_mb, "gpu_count": 0 })).collect(),
            ("compute", "warehouse_events") => self.docs(crate::api::sql::KIND_WAREHOUSE).await?.iter().map(|w| json!({ "account_id": acct, "workspace_id": ws, "warehouse_id": w["id"], "event_type": if w["state_hint"] == "RUNNING" { "RUNNING" } else { "STOPPED" }, "cluster_count": 1, "event_time": ts_v(&w["creation_time"]) })).collect(),

            // ---- lakeflow
            ("lakeflow", "jobs") => self.docs(crate::api::jobs::KIND_JOB).await?.iter().map(|j| json!({ "account_id": acct, "workspace_id": ws, "job_id": j["job_id"].to_string(), "name": j["settings"]["name"], "description": j["settings"]["description"], "creator_id": j["creator_user_name"], "run_as": j["run_as_user_name"], "tags": strs(&j["settings"]["tags"]), "change_time": ts_v(&j["created_time"]), "delete_time": Value::Null })).collect(),
            ("lakeflow", "job_tasks") => {
                let mut out = vec![];
                for j in self.docs(crate::api::jobs::KIND_JOB).await? {
                    for t in j["settings"]["tasks"].as_array().into_iter().flatten() {
                        let deps: Vec<&str> = t["depends_on"].as_array().map(|a| a.iter().filter_map(|d| d["task_key"].as_str()).collect()).unwrap_or_default();
                        let ty = ["notebook_task", "spark_python_task", "sql_task", "python_wheel_task", "pipeline_task", "run_job_task", "condition_task", "spark_jar_task", "dbt_task"].iter().find(|k| t.get(**k).is_some()).copied().unwrap_or("unknown");
                        out.push(json!({ "account_id": acct, "workspace_id": ws, "job_id": j["job_id"].to_string(), "task_key": t["task_key"], "depends_on_keys": json!(deps).to_string(), "task_type": ty, "change_time": ts_v(&j["created_time"]), "delete_time": Value::Null }));
                    }
                }
                out
            }
            ("lakeflow", "job_run_timeline" | "job_task_run_timeline") => {
                let runs: Vec<Doc<Value>> = self.store.list(crate::api::jobs::KIND_RUN, self.ws(), Filter { newest_first: true, limit: Some(20_000), ..Default::default() }).await?;
                let mut out = vec![];
                for d in runs {
                    let r = &d.data;
                    if name == "job_run_timeline" {
                        out.push(json!({ "account_id": acct, "workspace_id": ws, "job_id": r["job_id"].as_i64().map(|j| j.to_string()), "run_id": r["run_id"].to_string(), "run_name": r["run_name"], "period_start_time": ts_v(&r["start_time"]), "period_end_time": ts_v(&r["end_time"]), "trigger_type": r["trigger"], "run_type": r["run_type"], "compute_ids": json!(r["tasks"].as_array().map(|a| a.iter().filter_map(|t| t["cluster_instance"]["cluster_id"].as_str().or(t["existing_cluster_id"].as_str())).collect::<Vec<_>>()).unwrap_or_default()).to_string(), "result_state": r["state"]["result_state"], "termination_code": r["state"]["result_state"].as_str().map(|s| if s == "SUCCESS" { "SUCCESS" } else { "RUN_EXECUTION_ERROR" }), "job_parameters": strs(&r["job_parameters"]) }));
                    } else {
                        for t in r["tasks"].as_array().into_iter().flatten() {
                            out.push(json!({ "account_id": acct, "workspace_id": ws, "job_id": r["job_id"].as_i64().map(|j| j.to_string()), "run_id": t["run_id"].to_string(), "job_run_id": r["run_id"].to_string(), "task_key": t["task_key"], "period_start_time": ts_v(&t["start_time"]), "period_end_time": ts_v(&t["end_time"]), "compute_ids": json!([t["cluster_instance"]["cluster_id"].as_str().or(t["existing_cluster_id"].as_str())]).to_string(), "result_state": t["state"]["result_state"], "termination_code": t["state"]["result_state"] }));
                        }
                    }
                }
                out
            }
            ("lakeflow", "pipelines") => self.docs(crate::api::pipelines::KIND_PIPELINE).await?.iter().map(|p| json!({ "account_id": acct, "workspace_id": ws, "pipeline_id": p["pipeline_id"], "pipeline_type": if p["spec"]["continuous"] == true { "CONTINUOUS" } else { "TRIGGERED" }, "name": p["name"].as_str().or(p["spec"]["name"].as_str()), "created_by": p["creator_user_name"], "run_as": p["run_as_user_name"].as_str().or(p["creator_user_name"].as_str()), "tags": strs(&p["spec"]["tags"]), "settings": strs(&p["spec"]), "configuration": strs(&p["spec"]["configuration"]), "change_time": ts_v(&p["last_modified"]), "delete_time": Value::Null })).collect(),
            ("lakeflow", "pipeline_update_timeline") => {
                let ups: Vec<Doc<Value>> = self.store.list(crate::api::pipelines::KIND_UPDATE, self.ws(), Filter { newest_first: true, limit: Some(20_000), ..Default::default() }).await?;
                ups.iter().map(|d| json!({ "account_id": acct, "workspace_id": ws, "pipeline_id": d.data["pipeline_id"], "update_id": d.data["update_id"], "cause": d.data["cause"], "state": d.data["state"], "period_start_time": ts_v(&d.data["creation_time"]), "period_end_time": ts_v(&d.data["end_time"]) })).collect()
            }

            // ---- billing
            ("billing", "usage") => self.usage_rows().await?,
            ("billing", "list_prices") => list_prices(&self.config.cloud),

            // ---- mlflow / serving / lakebase
            ("mlflow", "experiments_latest") => self.docs(crate::api::mlflow::KIND_EXPERIMENT).await?.iter().map(|e| json!({ "account_id": acct, "workspace_id": ws, "experiment_id": e["experiment_id"], "name": e["name"], "artifact_location": e["artifact_location"], "lifecycle_stage": e["lifecycle_stage"], "create_time": ts_v(&e["creation_time"]), "last_update_time": ts_v(&e["last_update_time"]) })).collect(),
            ("mlflow", "runs_latest") => self.docs(crate::api::mlflow::KIND_RUN).await?.iter().map(|r| json!({ "account_id": acct, "workspace_id": ws, "experiment_id": r["info"]["experiment_id"], "run_id": r["info"]["run_id"], "run_name": r["info"]["run_name"], "status": r["info"]["status"], "user_id": r["info"]["user_id"], "start_time": ts_v(&r["info"]["start_time"]), "end_time": ts_v(&r["info"]["end_time"]) })).collect(),
            ("serving", "endpoint_usage") => self.docs(crate::api::serving::KIND_ENDPOINT).await?.iter().map(|e| json!({ "account_id": acct, "workspace_id": ws, "endpoint_name": e["name"], "served_entity_name": e["config"]["served_entities"][0]["name"], "state": e["state"]["ready"], "request_count": e["request_count"].as_i64().unwrap_or(0), "change_time": ts_v(&e["last_updated_timestamp"]) })).collect(),
            ("lakebase", "instances") => self.docs(crate::api::lakebase::KIND_INSTANCE).await?.iter().map(|i| json!({ "account_id": acct, "workspace_id": ws, "uid": i["uid"], "name": i["name"], "state": i["state"], "capacity": i["capacity"], "pg_version": i["pg_version"], "creator": i["creator"], "backend": i["backend"]["kind"], "read_write_dns": i["read_write_dns"], "node_count": i["effective_node_count"], "creation_time": i["creation_time"], "change_time": i["updated_time"] })).collect(),
            ("lakebase", "synced_tables") => self.docs(crate::api::lakebase::KIND_SYNCED_TABLE).await?.iter().map(|t| json!({ "account_id": acct, "workspace_id": ws, "name": t["name"], "database_instance_name": t["effective_database_instance_name"], "logical_database_name": t["effective_logical_database_name"], "source_table_full_name": t["spec"]["source_table_full_name"], "scheduling_policy": t["spec"]["scheduling_policy"], "state": t["data_synchronization_status"]["detailed_state"], "pipeline_id": t["data_synchronization_status"]["pipeline_id"], "synced_rows": t["data_synchronization_status"]["synced_rows"].as_i64().unwrap_or(0), "last_sync_time": t["data_synchronization_status"]["last_sync"]["timestamp"] })).collect(),
            _ => return Err(ApiError::NotFound(format!("System table system.{schema}.{name} does not exist."))),
        })
    }

    /// Estimated DBUs: 1 DBU per (worker + driver) hour on all-purpose
    /// clusters, 2 per warehouse-hour, plus 0.25 per Lakebase CU-hour.
    async fn usage_rows(&self) -> ApiResult<Vec<Value>> {
        let ws = self.ws().to_string();
        let now = chrono::Utc::now().timestamp_millis();
        let mut out = vec![];
        for c in self.docs(crate::api::clusters::KIND).await? {
            let start = c["start_time"].as_i64().unwrap_or(0);
            if start == 0 {
                continue;
            }
            let end = match c["state"].as_str() {
                Some("RUNNING") | Some("RESIZING") => now,
                _ => c["terminated_time"].as_i64().filter(|t| *t > start).unwrap_or(start),
            };
            let hours = (end - start).max(0) as f64 / 3_600_000.0;
            let nodes = c["num_workers"].as_f64().unwrap_or(0.0) + 1.0;
            let is_job = c["cluster_source"] == "JOB";
            out.push(json!({
                "record_id": format!("usage-{}", c["cluster_id"].as_str().unwrap_or("")), "account_id": "lakeforge", "workspace_id": ws, "sku_name": if is_job { "JOBS_COMPUTE" } else { "ALL_PURPOSE_COMPUTE" }, "cloud": self.config.cloud.to_ascii_uppercase(),
                "usage_start_time": ts(start), "usage_end_time": ts(end), "usage_date": date(start), "custom_tags": strs(&c["custom_tags"]), "usage_unit": "DBU", "usage_quantity": hours * nodes,
                "usage_metadata": { "cluster_id": c["cluster_id"], "job_id": c["custom_tags"]["JobId"], "job_run_id": c["custom_tags"]["RunId"], "warehouse_id": Value::Null, "node_type": c["node_type_id"], "endpoint_name": Value::Null, "database_instance_id": Value::Null },
                "identity_metadata": { "run_as": c["creator_user_name"], "owned_by": c["creator_user_name"] }, "record_type": "ORIGINAL", "ingestion_date": date(now), "billing_origin_product": if is_job { "JOBS" } else { "INTERACTIVE" }, "usage_type": "COMPUTE_TIME",
            }));
        }
        for i in self.docs(crate::api::lakebase::KIND_INSTANCE).await? {
            let Some(start) = i["creation_time"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|d| d.timestamp_millis()) else { continue };
            let cu = i["capacity"].as_str().and_then(|c| c.strip_prefix("CU_")).and_then(|c| c.parse::<f64>().ok()).unwrap_or(1.0);
            let hours = (now - start).max(0) as f64 / 3_600_000.0;
            out.push(json!({
                "record_id": format!("usage-{}", i["uid"].as_str().unwrap_or("")), "account_id": "lakeforge", "workspace_id": ws, "sku_name": "LAKEBASE_COMPUTE", "cloud": self.config.cloud.to_ascii_uppercase(),
                "usage_start_time": ts(start), "usage_end_time": ts(now), "usage_date": date(start), "custom_tags": strs(&i["custom_tags"]), "usage_unit": "DBU", "usage_quantity": hours * cu * 0.25,
                "usage_metadata": { "cluster_id": Value::Null, "job_id": Value::Null, "job_run_id": Value::Null, "warehouse_id": Value::Null, "node_type": Value::Null, "endpoint_name": Value::Null, "database_instance_id": i["uid"] },
                "identity_metadata": { "run_as": i["creator"], "owned_by": i["creator"] }, "record_type": "ORIGINAL", "ingestion_date": date(now), "billing_origin_product": "LAKEBASE", "usage_type": "COMPUTE_TIME",
            }));
        }
        Ok(out)
    }

    // ------------------------------------------------------------ materialisation

    /// Build the Parquet file for a system table and register it on every
    /// running cluster. Returns the storage URL.
    pub async fn materialize_system_table(self: &Arc<Self>, schema: &str, name: &str) -> ApiResult<String> {
        let t = find(schema, name).ok_or_else(|| ApiError::NotFound(format!("System table system.{schema}.{name} does not exist.")))?;
        let rows = self.system_table_rows(schema, name).await?;
        let bytes = to_parquet(&t, &rows)?;
        let path = format!("/system/{schema}/{name}/data.parquet");
        self.storage.put(&path, bytes).await?;
        let loc = self.storage.url_for(&path);
        let doc = json!({ "catalog_name": SYSTEM_CATALOG, "schema_name": schema, "name": name, "storage_location": loc, "data_source_format": "PARQUET", "table_type": "SYSTEM", "properties": {} });
        self.broadcast_table(&doc).await;
        Ok(loc)
    }

    /// Materialise every system table referenced by `tables`
    /// (`system.<schema>.<name>` full names), ignoring unknown ones.
    pub async fn refresh_system_tables_for(self: &Arc<Self>, tables: impl IntoIterator<Item = String>) {
        for full in tables {
            let parts: Vec<&str> = full.split('.').collect();
            if let [SYSTEM_CATALOG, sch, n] = parts.as_slice() {
                if let Err(e) = self.materialize_system_table(sch, n).await {
                    tracing::warn!(table = %full, error = %e, "system table refresh failed");
                }
            }
        }
    }

    /// Materialise all system tables (used after a cluster starts so `SHOW
    /// TABLES IN system.access` works before the first query).
    pub async fn refresh_all_system_tables(self: &Arc<Self>) {
        for t in tables() {
            if let Err(e) = self.materialize_system_table(t.schema, t.name).await {
                tracing::debug!(schema = t.schema, table = t.name, error = %e, "system table refresh failed");
            }
        }
    }
}

fn list_prices(cloud: &str) -> Vec<Value> {
    let cloud = cloud.to_ascii_uppercase();
    [("ALL_PURPOSE_COMPUTE", 0.55), ("JOBS_COMPUTE", 0.15), ("SQL_COMPUTE", 0.22), ("LAKEBASE_COMPUTE", 0.35), ("MODEL_SERVING", 0.07)]
        .iter()
        .map(|(sku, price)| json!({ "price_start_time": ts(1_700_000_000_000), "price_end_time": Value::Null, "account_id": "lakeforge", "sku_name": sku, "cloud": cloud, "currency_code": "USD", "usage_unit": "DBU", "pricing": { "default": price } }))
        .collect()
}

/// Encode JSON rows as a single Parquet file with the table's declared schema.
pub fn to_parquet(t: &SysTable, rows: &[Value]) -> ApiResult<Bytes> {
    let fields: Vec<Field> = t.columns.iter().map(|(n, ty)| Field::new(*n, ty.arrow(), true)).collect();
    let schema = Arc::new(Schema::new(fields));
    let batch: RecordBatch = if rows.is_empty() {
        RecordBatch::new_empty(Arc::clone(&schema))
    } else {
        let mut decoder = arrow::json::ReaderBuilder::new(Arc::clone(&schema)).with_coerce_primitive(true).build_decoder().map_err(|e| ApiError::Internal(format!("system table schema: {e}")))?;
        decoder.serialize(rows).map_err(|e| ApiError::Internal(format!("system table rows: {e}")))?;
        decoder.flush().map_err(|e| ApiError::Internal(format!("system table decode: {e}")))?.unwrap_or_else(|| RecordBatch::new_empty(Arc::clone(&schema)))
    };
    let mut buf = Vec::new();
    let mut w = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).map_err(|e| ApiError::Internal(format!("parquet writer: {e}")))?;
    w.write(&batch).map_err(|e| ApiError::Internal(format!("parquet write: {e}")))?;
    w.close().map_err(|e| ApiError::Internal(format!("parquet close: {e}")))?;
    Ok(Bytes::from(buf))
}

/// Full names of the tables in `refs` that live in the system catalog.
pub fn system_refs<'a>(refs: impl IntoIterator<Item = &'a String>) -> Vec<String> {
    refs.into_iter().filter(|r| r.starts_with("system.")).cloned().collect()
}

pub fn is_system_table(full: &str) -> bool {
    let parts: Vec<&str> = full.split('.').collect();
    matches!(parts.as_slice(), [SYSTEM_CATALOG, s, n] if find(s, n).is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_consistent() {
        let all = tables();
        assert!(all.len() > 50);
        for t in &all {
            assert!(SYSTEM_SCHEMAS.contains(&t.schema), "{} not a system schema", t.schema);
            let mut names: Vec<&str> = t.columns.iter().map(|(n, _)| *n).collect();
            names.sort();
            names.dedup();
            assert_eq!(names.len(), t.columns.len(), "duplicate column in {}.{}", t.schema, t.name);
        }
        assert!(is_system_table("system.access.audit"));
        assert!(!is_system_table("main.default.t"));
    }

    #[test]
    fn parquet_roundtrip_with_structs_and_timestamps() {
        let t = find("access", "audit").unwrap();
        let rows = vec![json!({ "version": "2.0", "event_time": ts(1_700_000_000_123), "event_date": date(1_700_000_000_123), "workspace_id": "w", "user_identity": { "email": "a@b", "subject_name": "u1" }, "service_name": "unityCatalog", "action_name": "createTable", "request_id": "r", "request_params": "{}", "response": { "status_code": 200, "error_message": null, "result": null }, "audit_level": "WORKSPACE_LEVEL", "account_id": "x", "event_id": "e" })];
        let bytes = to_parquet(&t, &rows).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap().build().unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let schema = batches[0].schema();
        assert!(matches!(schema.field_with_name("event_time").unwrap().data_type(), DataType::Timestamp(_, _)));
        assert!(matches!(schema.field_with_name("user_identity").unwrap().data_type(), DataType::Struct(_)));
        // empty tables still produce a typed file
        let empty = to_parquet(&t, &[]).unwrap();
        assert!(!empty.is_empty());
    }
}
