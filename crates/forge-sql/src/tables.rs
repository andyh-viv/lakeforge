//! Registration of external tables (Delta, Parquet, CSV, JSON) in a session.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions, SessionContext};
use datafusion::sql::TableReference;
use serde::{Deserialize, Serialize};

use crate::object_store::{ensure_object_store, parse_location};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableFormat {
    Delta,
    Parquet,
    Csv,
    Json,
}

impl std::str::FromStr for TableFormat {
    type Err = DataFusionError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "delta" => Ok(Self::Delta),
            "parquet" => Ok(Self::Parquet),
            "csv" => Ok(Self::Csv),
            "json" | "ndjson" => Ok(Self::Json),
            other => Err(DataFusionError::Plan(format!("unknown table format {other}"))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSpec {
    pub name: String,
    pub format: TableFormat,
    pub location: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// Register `spec` with `ctx`, replacing any table of the same name. Returns
/// the schema of the registered table.
pub async fn register_table(
    ctx: &SessionContext,
    spec: &TableSpec,
) -> DFResult<arrow::datatypes::SchemaRef> {
    let url = parse_location(&spec.location)?;
    ensure_object_store(&ctx.runtime_env(), &url)?;

    let table_ref = TableReference::parse_str(&spec.name);
    let _ = ctx.deregister_table(table_ref.clone());

    match spec.format {
        TableFormat::Delta => {
            let table = if spec.options.is_empty() {
                deltalake::open_table(url.clone()).await
            } else {
                deltalake::open_table_with_storage_options(url.clone(), spec.options.clone())
                    .await
            }
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let state = ctx.state();
            table
                .update_datafusion_session(&state)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let provider = table
                .table_provider()
                .with_session(Arc::new(state))
                .build()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            ctx.register_table(table_ref.clone(), Arc::new(provider))?;
        }
        TableFormat::Parquet => {
            ctx.register_parquet(table_ref.clone(), url.as_str(), ParquetReadOptions::default())
                .await?;
        }
        TableFormat::Csv => {
            let mut opts = CsvReadOptions::default();
            if let Some(v) = spec.options.get("header") {
                opts = opts.has_header(v.eq_ignore_ascii_case("true"));
            }
            if let Some(d) = spec.options.get("delimiter").and_then(|d| d.bytes().next()) {
                opts = opts.delimiter(d);
            }
            ctx.register_csv(table_ref.clone(), url.as_str(), opts).await?;
        }
        TableFormat::Json => {
            ctx.register_json(table_ref.clone(), url.as_str(), JsonReadOptions::default())
                .await?;
        }
    }

    let provider = ctx
        .table_provider(table_ref)
        .await?;
    Ok(provider.schema())
}
