//! `ShuffleWriterExec` — the root operator of every stage task.
//!
//! Executes its child for one input partition and writes the output to local
//! disk, partitioned by the stage's output partitioning. Emits a single
//! statistics batch describing the written partitions.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use forge_proto::ShufflePartitionLocation;

use crate::storage::{PartitionWriter, ShuffleStorage};

#[derive(Debug)]
pub struct ShuffleWriterExec {
    job_id: String,
    stage_id: u32,
    input: Arc<dyn ExecutionPlan>,
    /// `None` means identity: each map task produces exactly one output partition.
    partitioning: Option<Partitioning>,
    work_dir: String,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ShuffleWriterExec {
    pub fn new(
        job_id: impl Into<String>,
        stage_id: u32,
        input: Arc<dyn ExecutionPlan>,
        partitioning: Option<Partitioning>,
        work_dir: impl Into<String>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(stats_schema()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            job_id: job_id.into(),
            stage_id,
            input,
            partitioning,
            work_dir: work_dir.into(),
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
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn shuffle_partitioning(&self) -> Option<&Partitioning> {
        self.partitioning.as_ref()
    }
    pub fn work_dir(&self) -> &str {
        &self.work_dir
    }

    /// Number of output partitions produced by every map task.
    pub fn output_partition_count(&self) -> usize {
        match &self.partitioning {
            Some(Partitioning::Hash(_, n)) | Some(Partitioning::RoundRobinBatch(n)) => *n,
            _ => 1,
        }
    }

    /// Schema of the statistics batch emitted by this operator.
    pub fn stats_schema() -> SchemaRef {
        stats_schema()
    }

    /// Decode the statistics batch produced by `execute` into partition locations.
    pub fn decode_stats(
        batch: &RecordBatch,
        map_partition_id: u32,
        executor_id: &str,
        executor_addr: &str,
    ) -> DFResult<Vec<ShufflePartitionLocation>> {
        let part = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| DataFusionError::Internal("bad stats col 0".into()))?;
        let path = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("bad stats col 1".into()))?;
        let rows = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Internal("bad stats col 2".into()))?;
        let bytes = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Internal("bad stats col 3".into()))?;
        let batches = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| DataFusionError::Internal("bad stats col 4".into()))?;
        Ok((0..batch.num_rows())
            .map(|i| ShufflePartitionLocation {
                map_partition_id,
                output_partition_id: part.value(i),
                executor_id: executor_id.to_string(),
                executor_addr: executor_addr.to_string(),
                path: path.value(i).to_string(),
                num_rows: rows.value(i),
                num_bytes: bytes.value(i),
                num_batches: batches.value(i),
            })
            .collect())
    }

    async fn write_partition(
        self: Arc<Self>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<RecordBatch> {
        let mut input = self.input.execute(partition, context)?;
        let schema = self.input.schema();
        let storage = ShuffleStorage::new(&self.work_dir);
        let n_out = self.output_partition_count();
        let num_input_partitions = self.input.output_partitioning().partition_count();

        // Identity mode: the single output file is labelled with the map
        // partition so consumers can group by `output_partition` uniformly.
        let identity = self.partitioning.is_none();
        let out_index = |o: usize| if identity { partition as u32 } else { o as u32 };
        let mut writers: Vec<PartitionWriter> = (0..n_out)
            .map(|o| {
                PartitionWriter::new(
                    storage.partition_path(&self.job_id, self.stage_id, partition as u32, out_index(o)),
                    Arc::clone(&schema),
                )
            })
            .collect();

        let repart_time = MetricBuilder::new(&self.metrics).subset_time("repartition_time", partition);
        let write_time = MetricBuilder::new(&self.metrics).subset_time("write_time", partition);
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);

        let mut partitioner = match &self.partitioning {
            Some(p @ Partitioning::Hash(_, _)) | Some(p @ Partitioning::RoundRobinBatch(_)) => Some(
                BatchPartitioner::try_new(p.clone(), repart_time, partition, num_input_partitions)?,
            ),
            _ => None,
        };

        while let Some(batch) = input.next().await {
            let batch = batch?;
            output_rows.add(batch.num_rows());
            match partitioner.as_mut() {
                Some(p) => {
                    let mut err: Option<DataFusionError> = None;
                    p.partition(batch, |idx, b| {
                        let timer = write_time.timer();
                        let r = writers[idx].write(&b).map_err(DataFusionError::from);
                        timer.done();
                        if let Err(e) = r {
                            err = Some(e);
                        }
                        Ok(())
                    })?;
                    if let Some(e) = err {
                        return Err(e);
                    }
                }
                None => {
                    let timer = write_time.timer();
                    writers[0].write(&batch)?;
                    timer.done();
                }
            }
        }

        let mut parts = Vec::with_capacity(n_out);
        let mut paths = Vec::with_capacity(n_out);
        let mut rows = Vec::with_capacity(n_out);
        let mut bytes = Vec::with_capacity(n_out);
        let mut batches = Vec::with_capacity(n_out);
        for (o, w) in writers.into_iter().enumerate() {
            let path = w.path().to_string_lossy().into_owned();
            let r = w.num_rows;
            let b = w.num_batches;
            let len = w.finish()?;
            parts.push(out_index(o));
            paths.push(if len == 0 { String::new() } else { path });
            rows.push(r);
            bytes.push(len);
            batches.push(b);
        }

        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from(parts)),
            Arc::new(StringArray::from(paths)),
            Arc::new(UInt64Array::from(rows)),
            Arc::new(UInt64Array::from(bytes)),
            Arc::new(UInt32Array::from(batches)),
        ];
        Ok(RecordBatch::try_new(stats_schema(), cols)?)
    }
}

fn stats_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("output_partition", DataType::UInt32, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("num_rows", DataType::UInt64, false),
        Field::new("num_bytes", DataType::UInt64, false),
        Field::new("num_batches", DataType::UInt32, false),
    ]))
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.partitioning {
            Some(p) => write!(
                f,
                "ShuffleWriterExec: stage={}, partitioning={p}",
                self.stage_id
            ),
            None => write!(f, "ShuffleWriterExec: stage={}, partitioning=identity", self.stage_id),
        }
    }
}

impl ExecutionPlan for ShuffleWriterExec {
    fn name(&self) -> &str {
        "ShuffleWriterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("ShuffleWriterExec needs one child".into()))?;
        Ok(Arc::new(ShuffleWriterExec::new(
            self.job_id.clone(),
            self.stage_id,
            input,
            self.partitioning.clone(),
            self.work_dir.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let this = Arc::new(ShuffleWriterExec {
            job_id: self.job_id.clone(),
            stage_id: self.stage_id,
            input: Arc::clone(&self.input),
            partitioning: self.partitioning.clone(),
            work_dir: self.work_dir.clone(),
            properties: Arc::clone(&self.properties),
            metrics: self.metrics.clone(),
        });
        let fut = this.write_partition(partition, context);
        let stream = futures::stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(stats_schema(), stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
