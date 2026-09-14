//! Placeholder for a shuffle whose producing stage has not yet completed.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

#[derive(Debug)]
pub struct UnresolvedShuffleExec {
    stage_id: u32,
    schema: SchemaRef,
    output_partition_count: usize,
    properties: Arc<PlanProperties>,
}

impl UnresolvedShuffleExec {
    pub fn new(stage_id: u32, schema: SchemaRef, output_partition_count: usize) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(output_partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { stage_id, schema, output_partition_count, properties }
    }

    pub fn stage_id(&self) -> u32 {
        self.stage_id
    }

    pub fn output_partition_count(&self) -> usize {
        self.output_partition_count
    }
}

impl DisplayAs for UnresolvedShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "UnresolvedShuffleExec: stage={}, partitions={}",
            self.stage_id, self.output_partition_count
        )
    }
}

impl ExecutionPlan for UnresolvedShuffleExec {
    fn name(&self) -> &str {
        "UnresolvedShuffleExec"
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        Err(DataFusionError::Internal(format!(
            "UnresolvedShuffleExec for stage {} cannot be executed; scheduler must resolve it first",
            self.stage_id
        )))
    }
}
