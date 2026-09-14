//! `ShuffleReaderExec` — reads the output partitions of a completed stage.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use forge_proto::{FetchShuffleRequest, ShufflePartitionLocation};
use futures::{StreamExt, TryStreamExt};

use crate::client::ShuffleClient;
use crate::storage::ShuffleStorage;

#[derive(Debug)]
pub struct ShuffleReaderExec {
    job_id: String,
    stage_id: u32,
    schema: SchemaRef,
    /// `partitions[p]` lists every map-task output that belongs to reduce partition `p`.
    partitions: Vec<Vec<ShufflePartitionLocation>>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ShuffleReaderExec {
    pub fn new(
        job_id: impl Into<String>,
        stage_id: u32,
        schema: SchemaRef,
        partitions: Vec<Vec<ShufflePartitionLocation>>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            job_id: job_id.into(),
            stage_id,
            schema,
            partitions,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn job_id(&self) -> &str {
        &self.job_id
    }
    pub fn stage_id(&self) -> u32 {
        self.stage_id
    }
    pub fn partitions(&self) -> &[Vec<ShufflePartitionLocation>] {
        &self.partitions
    }

    /// Total bytes across all locations (used by adaptive planning).
    pub fn total_bytes(&self) -> u64 {
        self.partitions.iter().flatten().map(|l| l.num_bytes).sum()
    }
}

impl DisplayAs for ShuffleReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let locs: usize = self.partitions.iter().map(|p| p.len()).sum();
        write!(
            f,
            "ShuffleReaderExec: stage={}, partitions={}, locations={}",
            self.stage_id,
            self.partitions.len(),
            locs
        )
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn name(&self) -> &str {
        "ShuffleReaderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let locations = self
            .partitions
            .get(partition)
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "ShuffleReaderExec: invalid partition {partition}"
                ))
            })?;
        let schema = Arc::clone(&self.schema);
        let job_id = self.job_id.clone();
        let stage_id = self.stage_id;
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let fetch_time = MetricBuilder::new(&self.metrics).subset_time("fetch_time", partition);

        let local_addr = crate::local_executor_addr().map(|s| s.to_string());

        let streams = futures::stream::iter(locations.into_iter().filter(|l| l.num_rows > 0))
            .then(move |loc| {
                let schema = Arc::clone(&schema);
                let job_id = job_id.clone();
                let local_addr = local_addr.clone();
                let fetch_time = fetch_time.clone();
                async move {
                    let timer = fetch_time.timer();
                    let is_local = local_addr.as_deref() == Some(loc.executor_addr.as_str())
                        && std::path::Path::new(&loc.path).exists();
                    let r: DFResult<SendableRecordBatchStream> = if is_local {
                        read_local(&loc.path, schema)
                    } else {
                        ShuffleClient::global()
                            .fetch(
                                &loc.executor_addr,
                                FetchShuffleRequest {
                                    job_id,
                                    stage_id,
                                    map_partition_id: loc.map_partition_id,
                                    output_partition_id: loc.output_partition_id,
                                    path: loc.path.clone(),
                                },
                                schema,
                            )
                            .await
                    };
                    timer.done();
                    r
                }
            })
            .try_flatten()
            .inspect_ok(move |b| output_rows.add(b.num_rows()));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            streams,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn read_local(path: &str, schema: SchemaRef) -> DFResult<SendableRecordBatchStream> {
    let storage = ShuffleStorage::new("");
    let reader = storage
        .open(std::path::Path::new(path))
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let iter = reader.map(|r| r.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(iter),
    )))
}
