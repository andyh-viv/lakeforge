//! Table provider for Delta tables that adds `DELETE`, `UPDATE` and
//! `TRUNCATE` on top of delta-rs' read/insert provider by delegating those
//! operations to the Delta transaction protocol (`DeltaOps`).

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Statistics;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use deltalake::DeltaTable;
use url::Url;

fn ext(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Delta table provider with full DML support.
#[derive(Debug)]
pub struct ManagedDeltaTable {
    inner: Arc<dyn TableProvider>,
    url: Url,
    storage_options: HashMap<String, String>,
}

impl ManagedDeltaTable {
    pub fn new(inner: Arc<dyn TableProvider>, url: Url, storage_options: HashMap<String, String>) -> Self {
        Self { inner, url, storage_options }
    }

    async fn open(&self) -> DFResult<DeltaTable> {
        if self.storage_options.is_empty() {
            deltalake::open_table(self.url.clone()).await
        } else {
            deltalake::open_table_with_storage_options(self.url.clone(), self.storage_options.clone()).await
        }
        .map_err(ext)
    }

    async fn count_plan(state: &dyn Session, n: u64) -> DFResult<Arc<dyn ExecutionPlan>> {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(vec![n]))])?;
        let mem = MemTable::try_new(schema, vec![vec![batch]])?;
        mem.scan(state, None, &[], None).await
    }
}

fn conjunction(filters: Vec<Expr>) -> Option<Expr> {
    filters.into_iter().reduce(|a, b| a.and(b))
}

#[async_trait::async_trait]
impl TableProvider for ManagedDeltaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.scan(session, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    async fn delete_from(&self, state: &dyn Session, filters: Vec<Expr>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.open().await?;
        let mut op = table.delete();
        if let Some(pred) = conjunction(filters) {
            op = op.with_predicate(pred);
        }
        let (_, metrics) = op.await.map_err(ext)?;
        Self::count_plan(state, metrics.num_deleted_rows.unwrap_or(0) as u64).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.open().await?;
        let mut op = table.update();
        if let Some(pred) = conjunction(filters) {
            op = op.with_predicate(pred);
        }
        for (col, value) in assignments {
            op = op.with_update(col, value);
        }
        let (_, metrics) = op.await.map_err(ext)?;
        Self::count_plan(state, metrics.num_updated_rows as u64).await
    }

    async fn truncate(&self, state: &dyn Session) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.open().await?;
        let (_, metrics) = table.delete().await.map_err(ext)?;
        Self::count_plan(state, metrics.num_deleted_rows.unwrap_or(0) as u64).await
    }
}
