//! Bridge from the control plane to Forge drivers: connection cache per
//! cluster and conversion of Arrow results into Databricks JSON shapes.

use std::collections::HashMap;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use dashmap::DashMap;
use forge_client::{ForgeClient, QueryEvent};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::{ApiError, ApiResult};

#[derive(Default)]
pub struct ForgeRegistry {
    clients: DashMap<String, ForgeClient>,
}

impl ForgeRegistry {
    pub async fn client(&self, driver_addr: &str) -> ApiResult<ForgeClient> {
        if let Some(c) = self.clients.get(driver_addr) {
            return Ok(c.clone());
        }
        let c = ForgeClient::connect(driver_addr)
            .await
            .map_err(|e| ApiError::Unavailable(format!("cluster driver at {driver_addr} unreachable: {e}")))?;
        self.clients.insert(driver_addr.to_string(), c.clone());
        Ok(c)
    }

    pub fn forget(&self, driver_addr: &str) {
        self.clients.remove(driver_addr);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_text: String,
    pub type_name: String,
    pub position: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SqlResult {
    pub job_id: String,
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<Option<String>>>,
    pub row_count: usize,
    pub truncated: bool,
    pub elapsed_ms: u64,
    pub stages: usize,
    #[serde(default)]
    pub explain: String,
}

impl SqlResult {
    /// `manifest` block of the Statement Execution API.
    pub fn manifest(&self) -> Value {
        json!({
            "format": "JSON_ARRAY",
            "schema": { "column_count": self.columns.len(), "columns": self.columns },
            "total_chunk_count": 1,
            "total_row_count": self.row_count,
            "total_byte_count": self.rows.iter().flatten().flatten().map(|s| s.len()).sum::<usize>(),
            "truncated": self.truncated,
            "chunks": [{ "chunk_index": 0, "row_offset": 0, "row_count": self.rows.len() }]
        })
    }

    /// `result` block of the Statement Execution API.
    pub fn result_chunk(&self) -> Value {
        json!({
            "chunk_index": 0,
            "row_offset": 0,
            "row_count": self.rows.len(),
            "data_array": self.rows
        })
    }

    /// Notebook-style tabular payload.
    pub fn table_json(&self) -> Value {
        json!({
            "columns": self.columns,
            "rows": self.rows,
            "row_count": self.row_count,
            "truncated": self.truncated,
            "elapsed_ms": self.elapsed_ms,
            "stages": self.stages,
            "job_id": self.job_id,
        })
    }
}

pub fn databricks_type(dt: &DataType) -> (&'static str, String) {
    match dt {
        DataType::Boolean => ("BOOLEAN", "BOOLEAN".into()),
        DataType::Int8 | DataType::UInt8 => ("BYTE", "TINYINT".into()),
        DataType::Int16 | DataType::UInt16 => ("SHORT", "SMALLINT".into()),
        DataType::Int32 | DataType::UInt32 => ("INT", "INT".into()),
        DataType::Int64 | DataType::UInt64 => ("LONG", "BIGINT".into()),
        DataType::Float16 | DataType::Float32 => ("FLOAT", "FLOAT".into()),
        DataType::Float64 => ("DOUBLE", "DOUBLE".into()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ("STRING", "STRING".into()),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => ("BINARY", "BINARY".into()),
        DataType::Date32 | DataType::Date64 => ("DATE", "DATE".into()),
        DataType::Timestamp(_, None) => ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ".into()),
        DataType::Timestamp(_, Some(_)) => ("TIMESTAMP", "TIMESTAMP".into()),
        DataType::Time32(_) | DataType::Time64(_) => ("STRING", "STRING".into()),
        DataType::Duration(TimeUnit::Second) | DataType::Duration(_) | DataType::Interval(_) => ("INTERVAL", "INTERVAL".into()),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => ("DECIMAL", format!("DECIMAL({p},{s})")),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => ("ARRAY", format!("ARRAY<{}>", databricks_type(f.data_type()).1)),
        DataType::Map(_, _) => ("MAP", "MAP".into()),
        DataType::Struct(fields) => (
            "STRUCT",
            format!("STRUCT<{}>", fields.iter().map(|f| format!("{}: {}", f.name(), databricks_type(f.data_type()).1)).collect::<Vec<_>>().join(", ")),
        ),
        DataType::Null => ("NULL", "VOID".into()),
        other => ("STRING", format!("{other}")),
    }
}

pub fn columns_of(schema: &Schema) -> Vec<ColumnInfo> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let (name, text) = databricks_type(f.data_type());
            ColumnInfo { name: f.name().clone(), type_text: text, type_name: name.into(), position: i }
        })
        .collect()
}

pub fn batch_to_rows(batch: &RecordBatch, out: &mut Vec<Vec<Option<String>>>, limit: usize) -> ApiResult<()> {
    let opts = FormatOptions::default().with_display_error(true).with_null("");
    let formatters: Vec<ArrayFormatter> = batch
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
        .collect::<Result<_, _>>()
        .map_err(ApiError::internal)?;
    for row in 0..batch.num_rows() {
        if out.len() >= limit {
            break;
        }
        let mut r = Vec::with_capacity(formatters.len());
        for (col, f) in formatters.iter().enumerate() {
            if batch.column(col).is_null(row) {
                r.push(None);
            } else {
                r.push(Some(f.value(row).to_string()));
            }
        }
        out.push(r);
    }
    Ok(())
}

/// Run `sql` on the driver at `driver_addr` and materialise up to `max_rows` rows.
pub async fn run_sql(
    registry: &ForgeRegistry,
    driver_addr: &str,
    session_id: &str,
    sql: &str,
    session_conf: HashMap<String, String>,
    max_rows: usize,
) -> ApiResult<SqlResult> {
    let client = registry.client(driver_addr).await?.with_session(session_id);
    let mut stream = match client.sql_stream_with(sql, session_conf, 0).await {
        Ok(s) => s,
        Err(e) => {
            registry.forget(driver_addr);
            return Err(ApiError::Unavailable(format!("cluster driver at {driver_addr} unreachable: {e}")));
        }
    };
    let mut res = SqlResult::default();
    let mut total = 0usize;
    while let Some(ev) = stream.next().await? {
        match ev {
            QueryEvent::Started(s) => {
                res.job_id = s.job_id;
                res.explain = s.explain;
            }
            QueryEvent::Schema(s) => res.columns = columns_of(&s),
            QueryEvent::Batch(b) => {
                total += b.num_rows();
                if res.columns.is_empty() {
                    res.columns = columns_of(&b.schema());
                }
                batch_to_rows(&b, &mut res.rows, max_rows)?;
            }
            QueryEvent::Progress(_) => {}
            QueryEvent::Finished(f) => {
                res.elapsed_ms = f.elapsed_ms;
                res.stages = f.stages.len();
                total = total.max(f.rows as usize);
            }
        }
    }
    res.row_count = total;
    res.truncated = res.rows.len() < total;
    Ok(res)
}
