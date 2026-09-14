//! In-memory state machine for a distributed job: stages, tasks and attempts.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use forge_common::{now_ms, ForgeError, Result};
use forge_proto::{
    JobInfo, QueryProgress, ShufflePartitionLocation, StageInfo, StageSummary, TaskId, TaskMetrics,
};
use forge_shuffle::codec::ForgeCodec;

use crate::planner::{resolve_stage, QueryStage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Running { executor_id: String },
    Completed { executor_id: String },
    Failed { error: String },
}

#[derive(Debug, Clone)]
pub struct TaskSlot {
    pub partition: u32,
    pub attempt: u32,
    pub state: TaskState,
    pub outputs: Vec<ShufflePartitionLocation>,
    pub metrics: TaskMetrics,
    pub started_ms: u64,
    pub finished_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageStatus {
    /// Waiting on upstream stages.
    Blocked,
    /// All inputs available; tasks may be launched.
    Runnable,
    Completed,
    Failed,
}

#[derive(Debug)]
pub struct StageState {
    pub stage: QueryStage,
    pub status: StageStatus,
    /// Plan with all shuffles resolved, encoded once for every task.
    pub encoded_plan: Option<Arc<Vec<u8>>>,
    pub resolved_plan: Option<Arc<dyn ExecutionPlan>>,
    pub tasks: Vec<TaskSlot>,
    pub started_ms: u64,
    pub finished_ms: u64,
}

impl StageState {
    fn new(stage: QueryStage) -> Self {
        let tasks = (0..stage.num_tasks as u32)
            .map(|p| TaskSlot {
                partition: p,
                attempt: 0,
                state: TaskState::Pending,
                outputs: Vec::new(),
                metrics: TaskMetrics::default(),
                started_ms: 0,
                finished_ms: 0,
            })
            .collect();
        let status = if stage.depends_on.is_empty() {
            StageStatus::Runnable
        } else {
            StageStatus::Blocked
        };
        Self {
            stage,
            status,
            encoded_plan: None,
            resolved_plan: None,
            tasks,
            started_ms: 0,
            finished_ms: 0,
        }
    }

    pub fn all_outputs(&self) -> Vec<ShufflePartitionLocation> {
        self.tasks.iter().flat_map(|t| t.outputs.iter().cloned()).collect()
    }

    pub fn is_complete(&self) -> bool {
        self.tasks
            .iter()
            .all(|t| matches!(t.state, TaskState::Completed { .. }))
    }

    pub fn counts(&self) -> (u32, u32, u32) {
        let mut completed = 0;
        let mut running = 0;
        let mut failed = 0;
        for t in &self.tasks {
            match t.state {
                TaskState::Completed { .. } => completed += 1,
                TaskState::Running { .. } => running += 1,
                TaskState::Failed { .. } => failed += 1,
                TaskState::Pending => {}
            }
        }
        (completed, running, failed)
    }

    pub fn info(&self) -> StageInfo {
        let (completed, running, failed) = self.counts();
        StageInfo {
            stage_id: self.stage.stage_id,
            state: format!("{:?}", self.status),
            num_partitions: self.stage.num_tasks as u32,
            completed,
            running,
            failed,
            plan: self.stage.display(),
            depends_on: self.stage.depends_on.clone(),
        }
    }

    pub fn summary(&self) -> StageSummary {
        let mut output_rows = 0;
        let mut output_bytes = 0;
        for t in &self.tasks {
            for o in &t.outputs {
                output_rows += o.num_rows;
                output_bytes += o.num_bytes;
            }
        }
        StageSummary {
            stage_id: self.stage.stage_id,
            num_tasks: self.stage.num_tasks as u32,
            output_rows,
            output_bytes,
            elapsed_ms: self.finished_ms.saturating_sub(self.started_ms),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

/// Result handed to the driver when the final stage completes.
#[derive(Debug, Clone)]
pub struct JobResult {
    pub job_id: String,
    pub schema: arrow::datatypes::SchemaRef,
    pub partitions: Vec<Vec<ShufflePartitionLocation>>,
    pub stages: Vec<StageSummary>,
    pub elapsed_ms: u64,
}

#[derive(Debug)]
pub struct JobState {
    pub job_id: String,
    pub sql: String,
    pub session_config: HashMap<String, String>,
    pub max_retries: u32,
    pub submitted_ms: u64,
    pub finished_ms: u64,
    pub status: JobStatus,
    pub stages: BTreeMap<u32, StageState>,
    pub final_stage: u32,
    pub result_schema: arrow::datatypes::SchemaRef,
    pub result_tx: Option<tokio::sync::oneshot::Sender<Result<JobResult>>>,
}

/// Outcome of applying a task status update.
#[derive(Debug, Default)]
pub struct Transition {
    pub stages_completed: Vec<u32>,
    pub job_finished: bool,
    /// Tasks that should be cancelled on executors (job failed / cancelled).
    pub cancel: Vec<(String, TaskId)>,
}

impl JobState {
    pub fn new(
        job_id: String,
        sql: String,
        session_config: HashMap<String, String>,
        max_retries: u32,
        stages: Vec<QueryStage>,
        result_schema: arrow::datatypes::SchemaRef,
        result_tx: tokio::sync::oneshot::Sender<Result<JobResult>>,
    ) -> Result<Self> {
        let final_stage = stages
            .last()
            .map(|s| s.stage_id)
            .ok_or_else(|| ForgeError::Scheduler("plan produced no stages".into()))?;
        let mut map = BTreeMap::new();
        for s in stages {
            map.insert(s.stage_id, StageState::new(s));
        }
        let mut job = Self {
            job_id,
            sql,
            session_config,
            max_retries,
            submitted_ms: now_ms(),
            finished_ms: 0,
            status: JobStatus::Running,
            stages: map,
            final_stage,
            result_schema,
            result_tx: Some(result_tx),
        };
        for sid in job.stages.keys().cloned().collect::<Vec<_>>() {
            if job.stages[&sid].status == StageStatus::Runnable {
                job.prepare_stage(sid)?;
            }
        }
        Ok(job)
    }

    /// Resolve shuffles and encode the plan of a runnable stage.
    fn prepare_stage(&mut self, stage_id: u32) -> Result<()> {
        let outputs: HashMap<u32, Vec<ShufflePartitionLocation>> = self
            .stages
            .values()
            .filter(|s| s.status == StageStatus::Completed)
            .map(|s| (s.stage.stage_id, s.all_outputs()))
            .collect();
        let st = self
            .stages
            .get_mut(&stage_id)
            .ok_or_else(|| ForgeError::Scheduler(format!("unknown stage {stage_id}")))?;
        let resolved = resolve_stage(&self.job_id, Arc::clone(&st.stage.plan), &|sid| {
            outputs.get(&sid).cloned()
        })?;
        let bytes = ForgeCodec::encode_plan(Arc::clone(&resolved))?;
        st.encoded_plan = Some(Arc::new(bytes));
        st.resolved_plan = Some(resolved);
        st.status = StageStatus::Runnable;
        st.started_ms = now_ms();
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.status == JobStatus::Running
    }

    /// Tasks ready to be launched, as (stage_id, partition, attempt, encoded plan).
    pub fn pending_tasks(&self) -> Vec<(u32, u32, u32, Arc<Vec<u8>>)> {
        if !self.is_active() {
            return Vec::new();
        }
        let mut out = Vec::new();
        for st in self.stages.values() {
            if st.status != StageStatus::Runnable {
                continue;
            }
            let Some(plan) = &st.encoded_plan else { continue };
            for t in &st.tasks {
                if t.state == TaskState::Pending {
                    out.push((st.stage.stage_id, t.partition, t.attempt, Arc::clone(plan)));
                }
            }
        }
        out
    }

    pub fn mark_running(&mut self, stage_id: u32, partition: u32, attempt: u32, executor_id: &str) {
        if let Some(t) = self.task_mut(stage_id, partition) {
            if t.attempt == attempt && t.state == TaskState::Pending {
                t.state = TaskState::Running {
                    executor_id: executor_id.to_string(),
                };
                t.started_ms = now_ms();
            }
        }
    }

    /// Return a launched-but-rejected task to the pending pool.
    pub fn mark_pending(&mut self, stage_id: u32, partition: u32, attempt: u32) {
        if let Some(t) = self.task_mut(stage_id, partition) {
            if t.attempt == attempt && matches!(t.state, TaskState::Running { .. }) {
                t.state = TaskState::Pending;
            }
        }
    }

    fn task_mut(&mut self, stage_id: u32, partition: u32) -> Option<&mut TaskSlot> {
        self.stages
            .get_mut(&stage_id)
            .and_then(|s| s.tasks.get_mut(partition as usize))
    }

    pub fn task_completed(
        &mut self,
        id: &TaskId,
        executor_id: &str,
        outputs: Vec<ShufflePartitionLocation>,
        metrics: Option<TaskMetrics>,
    ) -> Result<Transition> {
        let mut tr = Transition::default();
        if !self.is_active() {
            return Ok(tr);
        }
        let Some(t) = self.task_mut(id.stage_id, id.partition_id) else {
            return Ok(tr);
        };
        if t.attempt != id.attempt || matches!(t.state, TaskState::Completed { .. }) {
            return Ok(tr);
        }
        t.state = TaskState::Completed {
            executor_id: executor_id.to_string(),
        };
        t.outputs = outputs;
        t.metrics = metrics.unwrap_or_default();
        t.finished_ms = now_ms();

        let stage_done = self.stages[&id.stage_id].is_complete();
        if stage_done {
            let st = self.stages.get_mut(&id.stage_id).unwrap();
            st.status = StageStatus::Completed;
            st.finished_ms = now_ms();
            tr.stages_completed.push(id.stage_id);
            self.unblock_dependents(id.stage_id)?;
            if id.stage_id == self.final_stage {
                self.finish_ok();
                tr.job_finished = true;
            }
        }
        Ok(tr)
    }

    fn unblock_dependents(&mut self, completed: u32) -> Result<()> {
        let candidates: Vec<u32> = self
            .stages
            .values()
            .filter(|s| s.status == StageStatus::Blocked && s.stage.depends_on.contains(&completed))
            .filter(|s| {
                s.stage
                    .depends_on
                    .iter()
                    .all(|d| self.stages.get(d).map(|x| x.status == StageStatus::Completed).unwrap_or(false))
            })
            .map(|s| s.stage.stage_id)
            .collect();
        for sid in candidates {
            self.prepare_stage(sid)?;
        }
        Ok(())
    }

    pub fn task_failed(&mut self, id: &TaskId, error: String) -> Transition {
        let mut tr = Transition::default();
        if !self.is_active() {
            return tr;
        }
        let max_retries = self.max_retries;
        let Some(t) = self.task_mut(id.stage_id, id.partition_id) else {
            return tr;
        };
        if t.attempt != id.attempt || matches!(t.state, TaskState::Completed { .. }) {
            return tr;
        }
        if t.attempt < max_retries {
            tracing::warn!(task = %id, attempt = t.attempt, %error, "task failed; retrying");
            t.attempt += 1;
            t.state = TaskState::Pending;
            return tr;
        }
        t.state = TaskState::Failed {
            error: error.clone(),
        };
        if let Some(st) = self.stages.get_mut(&id.stage_id) {
            st.status = StageStatus::Failed;
            st.finished_ms = now_ms();
        }
        tr.cancel = self.running_tasks();
        self.finish_err(format!(
            "task {id} failed after {} attempts: {error}",
            id.attempt + 1
        ));
        tr.job_finished = true;
        tr
    }

    /// All running task ids with the executor they run on.
    pub fn running_tasks(&self) -> Vec<(String, TaskId)> {
        let mut out = Vec::new();
        for st in self.stages.values() {
            for t in &st.tasks {
                if let TaskState::Running { executor_id } = &t.state {
                    out.push((
                        executor_id.clone(),
                        TaskId {
                            job_id: self.job_id.clone(),
                            stage_id: st.stage.stage_id,
                            partition_id: t.partition,
                            attempt: t.attempt,
                        },
                    ));
                }
            }
        }
        out
    }

    /// Requeue tasks that were running on a lost executor. Returns `Err` if
    /// completed shuffle output required by an unfinished stage lived there.
    pub fn executor_lost(&mut self, executor_id: &str) -> Transition {
        let mut tr = Transition::default();
        if !self.is_active() {
            return tr;
        }
        let mut lost_outputs = false;
        for st in self.stages.values_mut() {
            for t in st.tasks.iter_mut() {
                match &t.state {
                    TaskState::Running { executor_id: e } if e == executor_id => {
                        t.attempt += 1;
                        t.state = TaskState::Pending;
                    }
                    TaskState::Completed { executor_id: e } if e == executor_id => {
                        lost_outputs = true;
                    }
                    _ => {}
                }
            }
        }
        if lost_outputs {
            tr.cancel = self.running_tasks();
            self.finish_err(format!(
                "executor {executor_id} lost with shuffle output still required"
            ));
            tr.job_finished = true;
        }
        tr
    }

    pub fn cancel(&mut self) -> Transition {
        let mut tr = Transition::default();
        if !self.is_active() {
            return tr;
        }
        tr.cancel = self.running_tasks();
        self.status = JobStatus::Cancelled;
        self.finished_ms = now_ms();
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(Err(ForgeError::Cancelled));
        }
        tr.job_finished = true;
        tr
    }

    fn finish_ok(&mut self) {
        self.status = JobStatus::Completed;
        self.finished_ms = now_ms();
        let final_stage = &self.stages[&self.final_stage];
        let n = if final_stage.stage.writer().shuffle_partitioning().is_some() {
            final_stage.stage.output_partitions
        } else {
            final_stage.stage.num_tasks
        }
        .max(1);
        let mut partitions: Vec<Vec<ShufflePartitionLocation>> = vec![Vec::new(); n];
        for loc in final_stage.all_outputs() {
            let idx = loc.output_partition_id as usize;
            if idx < n {
                partitions[idx].push(loc);
            }
        }
        let result = JobResult {
            job_id: self.job_id.clone(),
            schema: Arc::clone(&self.result_schema),
            partitions,
            stages: self.stages.values().map(|s| s.summary()).collect(),
            elapsed_ms: self.finished_ms.saturating_sub(self.submitted_ms),
        };
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(Ok(result));
        }
    }

    fn finish_err(&mut self, msg: String) {
        tracing::error!(job = %self.job_id, "{msg}");
        self.status = JobStatus::Failed(msg.clone());
        self.finished_ms = now_ms();
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(Err(ForgeError::Execution(msg)));
        }
    }

    pub fn progress(&self) -> QueryProgress {
        let mut p = QueryProgress {
            total_stages: self.stages.len() as u32,
            ..Default::default()
        };
        for st in self.stages.values() {
            if st.status == StageStatus::Completed {
                p.completed_stages += 1;
            }
            p.total_tasks += st.stage.num_tasks as u32;
            let (c, r, f) = st.counts();
            p.completed_tasks += c;
            p.running_tasks += r;
            p.failed_tasks += f;
        }
        p
    }

    pub fn info(&self) -> JobInfo {
        let (state, error) = match &self.status {
            JobStatus::Running => ("RUNNING".to_string(), String::new()),
            JobStatus::Completed => ("COMPLETED".to_string(), String::new()),
            JobStatus::Failed(e) => ("FAILED".to_string(), e.clone()),
            JobStatus::Cancelled => ("CANCELLED".to_string(), String::new()),
        };
        JobInfo {
            job_id: self.job_id.clone(),
            sql: self.sql.clone(),
            state,
            submitted_ms: self.submitted_ms,
            finished_ms: self.finished_ms,
            progress: Some(self.progress()),
            error,
        }
    }

    pub fn stage_infos(&self) -> Vec<StageInfo> {
        self.stages.values().map(|s| s.info()).collect()
    }
}
