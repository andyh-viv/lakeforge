//! Splits a DataFusion physical plan into a DAG of shuffle stages.
//!
//! Every exchange point in the plan (`RepartitionExec`, `CoalescePartitionsExec`,
//! `SortPreservingMergeExec`) becomes a stage boundary: the producer side is
//! wrapped in a [`ShuffleWriterExec`] and the consumer sees an
//! [`UnresolvedShuffleExec`] that the scheduler swaps for a
//! [`ShuffleReaderExec`] once the producer stage has finished. The root of the
//! plan is itself written as a final identity-partitioned stage so that results
//! are fetched uniformly by the driver.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties, Partitioning};
use forge_proto::ShufflePartitionLocation;
use forge_shuffle::reader::ShuffleReaderExec;
use forge_shuffle::unresolved::UnresolvedShuffleExec;
use forge_shuffle::writer::ShuffleWriterExec;

/// One schedulable unit of a distributed query.
#[derive(Debug, Clone)]
pub struct QueryStage {
    pub stage_id: u32,
    /// Root is always a `ShuffleWriterExec`. Contains `UnresolvedShuffleExec`
    /// leaves until [`resolve_stage`] is applied.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Stage ids whose output this stage reads.
    pub depends_on: Vec<u32>,
    /// Number of map tasks (input partitions of the writer).
    pub num_tasks: usize,
    /// Number of output partitions each map task produces.
    pub output_partitions: usize,
}

impl QueryStage {
    pub fn writer(&self) -> &ShuffleWriterExec {
        self.plan
            .as_any()
            .downcast_ref::<ShuffleWriterExec>()
            .expect("stage root is ShuffleWriterExec")
    }

    pub fn display(&self) -> String {
        displayable(self.plan.as_ref()).indent(false).to_string()
    }
}

pub struct DistributedPlanner {
    job_id: String,
    work_dir: String,
    next_stage: u32,
    stages: Vec<QueryStage>,
}

impl DistributedPlanner {
    pub fn new(job_id: impl Into<String>, work_dir: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
            work_dir: work_dir.into(),
            next_stage: 1,
            stages: Vec::new(),
        }
    }

    /// Split `plan` into stages. The last stage in the returned vector is the
    /// final (result) stage; its output partitions are identity-partitioned.
    pub fn plan(mut self, plan: Arc<dyn ExecutionPlan>) -> DFResult<Vec<QueryStage>> {
        let (root, deps) = self.split(plan)?;
        self.push_stage(root, None, deps);
        Ok(self.stages)
    }

    fn push_stage(
        &mut self,
        input: Arc<dyn ExecutionPlan>,
        partitioning: Option<Partitioning>,
        depends_on: Vec<u32>,
    ) -> Arc<UnresolvedShuffleExec> {
        let stage_id = self.next_stage;
        self.next_stage += 1;
        let num_tasks = input.output_partitioning().partition_count();
        let schema = input.schema();
        let writer = ShuffleWriterExec::new(
            self.job_id.clone(),
            stage_id,
            input,
            partitioning,
            self.work_dir.clone(),
        );
        let output_partitions = writer.output_partition_count();
        // Identity partitioning yields one output partition per map task, so
        // the consumer sees `num_tasks` partitions.
        let consumer_partitions = if writer.shuffle_partitioning().is_some() {
            output_partitions
        } else {
            num_tasks.max(1)
        };
        self.stages.push(QueryStage {
            stage_id,
            plan: Arc::new(writer),
            depends_on,
            num_tasks: num_tasks.max(1),
            output_partitions,
        });
        Arc::new(UnresolvedShuffleExec::new(stage_id, schema, consumer_partitions))
    }

    /// Recursively rewrite `plan`, returning the rewritten node and the set of
    /// stages it (transitively, within its own stage) depends on.
    fn split(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DFResult<(Arc<dyn ExecutionPlan>, Vec<u32>)> {
        let mut new_children = Vec::with_capacity(plan.children().len());
        let mut deps = Vec::new();
        for child in plan.children() {
            let (c, d) = self.split(Arc::clone(child))?;
            new_children.push(c);
            deps.extend(d);
        }
        let plan = if new_children.is_empty() {
            plan
        } else {
            plan.with_new_children(new_children)?
        };

        let any = plan.as_any();
        if let Some(rep) = any.downcast_ref::<RepartitionExec>() {
            let input = Arc::clone(rep.input());
            let partitioning = rep.partitioning().clone();
            match partitioning {
                Partitioning::Hash(_, _) | Partitioning::RoundRobinBatch(_) => {
                    let unresolved = self.push_stage(input, Some(partitioning), deps);
                    let sid = unresolved.stage_id();
                    return Ok((unresolved, vec![sid]));
                }
                Partitioning::UnknownPartitioning(_) => return Ok((plan, deps)),
            }
        }
        if let Some(c) = any.downcast_ref::<CoalescePartitionsExec>() {
            if c.input().output_partitioning().partition_count() <= 1 {
                return Ok((plan, deps));
            }
            let unresolved = self.push_stage(Arc::clone(c.input()), None, deps);
            let sid = unresolved.stage_id();
            let rewritten = plan.with_new_children(vec![unresolved])?;
            return Ok((rewritten, vec![sid]));
        }
        if let Some(m) = any.downcast_ref::<SortPreservingMergeExec>() {
            if m.input().output_partitioning().partition_count() <= 1 {
                return Ok((plan, deps));
            }
            let unresolved = self.push_stage(Arc::clone(m.input()), None, deps);
            let sid = unresolved.stage_id();
            let rewritten = plan.with_new_children(vec![unresolved])?;
            return Ok((rewritten, vec![sid]));
        }
        Ok((plan, deps))
    }
}

/// Replace every `UnresolvedShuffleExec` in `plan` whose stage appears in
/// `outputs` with a concrete `ShuffleReaderExec`.
///
/// `outputs[stage_id]` lists all partition locations produced by that stage.
pub fn resolve_stage(
    job_id: &str,
    plan: Arc<dyn ExecutionPlan>,
    outputs: &dyn Fn(u32) -> Option<Vec<ShufflePartitionLocation>>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    if let Some(u) = plan.as_any().downcast_ref::<UnresolvedShuffleExec>() {
        let locs = outputs(u.stage_id()).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "stage {} not complete; cannot resolve shuffle",
                u.stage_id()
            ))
        })?;
        let n = u.output_partition_count();
        let mut parts: Vec<Vec<ShufflePartitionLocation>> = vec![Vec::new(); n];
        for loc in locs {
            let idx = loc.output_partition_id as usize;
            if idx < n {
                parts[idx].push(loc);
            } else {
                return Err(DataFusionError::Internal(format!(
                    "stage {} produced partition {idx} but consumer expects {n}",
                    u.stage_id()
                )));
            }
        }
        return Ok(Arc::new(ShuffleReaderExec::new(
            job_id,
            u.stage_id(),
            u.schema(),
            parts,
        )));
    }
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let mut new_children = Vec::with_capacity(children.len());
    for c in children {
        new_children.push(resolve_stage(job_id, Arc::clone(c), outputs)?);
    }
    plan.with_new_children(new_children)
}

/// Collect the stage ids of all unresolved shuffles below `plan`.
pub fn unresolved_stages(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<u32>) {
    if let Some(u) = plan.as_any().downcast_ref::<UnresolvedShuffleExec>() {
        out.push(u.stage_id());
    }
    for c in plan.children() {
        unresolved_stages(c, out);
    }
}
