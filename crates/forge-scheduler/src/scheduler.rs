//! The Forge scheduler: accepts physical plans, splits them into stages and
//! drives task execution across registered executors.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::physical_plan::ExecutionPlan;
use forge_common::config::SessionSettings;
use forge_common::{ForgeError, Result};
use forge_proto::{
    CancelTaskRequest, JobInfo, LaunchTaskRequest, StageInfo, TaskDefinition, TaskId, TaskState,
    TaskStatus,
};
use parking_lot::Mutex;
use tokio::sync::{oneshot, Notify};

use crate::executors::ExecutorRegistry;
use crate::job::{JobResult, JobState, Transition};
use crate::planner::{DistributedPlanner, QueryStage};

pub struct SchedulerConfig {
    pub driver_id: String,
    pub driver_addr: String,
    /// Placeholder work dir baked into plans; executors substitute their own.
    pub work_dir: String,
    pub executor_timeout: Duration,
    pub max_jobs_retained: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            driver_id: forge_common::new_id(),
            driver_addr: "http://127.0.0.1:50051".into(),
            work_dir: "/tmp/forge".into(),
            executor_timeout: Duration::from_secs(30),
            max_jobs_retained: 1000,
        }
    }
}

pub struct Scheduler {
    pub config: SchedulerConfig,
    pub executors: Arc<ExecutorRegistry>,
    jobs: Mutex<HashMap<String, JobState>>,
    job_order: Mutex<Vec<String>>,
    wake: Notify,
}

impl Scheduler {
    pub fn new(config: SchedulerConfig) -> Arc<Self> {
        let s = Arc::new(Self {
            config,
            executors: ExecutorRegistry::new(),
            jobs: Mutex::new(HashMap::new()),
            job_order: Mutex::new(Vec::new()),
            wake: Notify::new(),
        });
        let bg = Arc::clone(&s);
        tokio::spawn(async move { bg.run_loop().await });
        s
    }

    /// Split `plan` into stages without executing (for EXPLAIN).
    pub fn plan_stages(&self, job_id: &str, plan: Arc<dyn ExecutionPlan>) -> Result<Vec<QueryStage>> {
        Ok(DistributedPlanner::new(job_id, &self.config.work_dir).plan(plan)?)
    }

    /// Submit a physical plan. Resolves once the final stage has been written.
    pub fn submit(
        &self,
        job_id: String,
        sql: String,
        plan: Arc<dyn ExecutionPlan>,
        settings: &SessionSettings,
    ) -> Result<oneshot::Receiver<Result<JobResult>>> {
        let schema = plan.schema();
        let stages = self.plan_stages(&job_id, plan)?;
        let (tx, rx) = oneshot::channel();
        let job = JobState::new(
            job_id.clone(),
            sql,
            settings.to_map(),
            settings.task_max_retries,
            stages,
            schema,
            tx,
        )?;
        tracing::info!(job = %job_id, stages = job.stages.len(), "job submitted");
        self.jobs.lock().insert(job_id.clone(), job);
        {
            let mut order = self.job_order.lock();
            order.push(job_id);
            if order.len() > self.config.max_jobs_retained {
                let excess = order.len() - self.config.max_jobs_retained;
                let evict: Vec<String> = order.drain(..excess).collect();
                let mut jobs = self.jobs.lock();
                for id in evict {
                    if jobs.get(&id).map(|j| !j.is_active()).unwrap_or(false) {
                        jobs.remove(&id);
                    }
                }
            }
        }
        self.wake.notify_one();
        Ok(rx)
    }

    pub fn cancel(&self, job_id: &str) -> bool {
        let tr = {
            let mut jobs = self.jobs.lock();
            match jobs.get_mut(job_id) {
                Some(j) => j.cancel(),
                None => return false,
            }
        };
        self.apply_transition(job_id, tr);
        true
    }

    /// Mark a job's shuffle data as reclaimable once the client has consumed
    /// the results.
    pub fn release(&self, job_id: &str) {
        self.executors.schedule_purge(job_id);
    }

    pub fn job_info(&self, job_id: &str) -> Option<(JobInfo, Vec<StageInfo>)> {
        let jobs = self.jobs.lock();
        jobs.get(job_id).map(|j| (j.info(), j.stage_infos()))
    }

    pub fn list_jobs(&self, limit: usize) -> Vec<JobInfo> {
        let order = self.job_order.lock();
        let jobs = self.jobs.lock();
        order
            .iter()
            .rev()
            .filter_map(|id| jobs.get(id).map(|j| j.info()))
            .take(if limit == 0 { usize::MAX } else { limit })
            .collect()
    }

    /// Nudge the dispatch loop (e.g. after an executor registers).
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    pub fn running_jobs(&self) -> usize {
        self.jobs.lock().values().filter(|j| j.is_active()).count()
    }

    pub fn progress(&self, job_id: &str) -> Option<forge_proto::QueryProgress> {
        self.jobs.lock().get(job_id).map(|j| j.progress())
    }

    /// Handle task status reports from executors.
    pub fn update_task_status(&self, statuses: Vec<TaskStatus>) {
        for s in statuses {
            let Some(id) = s.id.clone() else { continue };
            let state = TaskState::try_from(s.state).unwrap_or(TaskState::TaskFailed);
            let tr = {
                let mut jobs = self.jobs.lock();
                let Some(job) = jobs.get_mut(&id.job_id) else {
                    // Unknown/evicted job: tell executor nothing, just drop.
                    self.executors.task_finished(&s.executor_id, &id, true);
                    continue;
                };
                match state {
                    TaskState::TaskCompleted => {
                        self.executors.task_finished(&s.executor_id, &id, true);
                        match job.task_completed(&id, &s.executor_id, s.outputs, s.metrics) {
                            Ok(tr) => tr,
                            Err(e) => job.task_failed(&id, format!("resolve failed: {e}")),
                        }
                    }
                    TaskState::TaskFailed | TaskState::TaskCancelled => {
                        self.executors.task_finished(&s.executor_id, &id, false);
                        job.task_failed(&id, s.error)
                    }
                    TaskState::TaskRunning | TaskState::TaskPending => Transition::default(),
                }
            };
            self.apply_transition(&id.job_id, tr);
        }
        self.wake.notify_one();
    }

    pub fn executor_lost(&self, executor_id: &str) {
        tracing::warn!(executor = %executor_id, "executor lost");
        self.executors.remove(executor_id);
        let transitions: Vec<(String, Transition)> = {
            let mut jobs = self.jobs.lock();
            jobs.iter_mut()
                .map(|(id, j)| (id.clone(), j.executor_lost(executor_id)))
                .collect()
        };
        for (id, tr) in transitions {
            self.apply_transition(&id, tr);
        }
        self.wake.notify_one();
    }

    fn apply_transition(&self, job_id: &str, tr: Transition) {
        if !tr.cancel.is_empty() {
            let mut by_exec: HashMap<String, Vec<TaskId>> = HashMap::new();
            for (e, t) in tr.cancel {
                by_exec.entry(e).or_default().push(t);
            }
            let execs = Arc::clone(&self.executors);
            tokio::spawn(async move {
                for (e, ids) in by_exec {
                    if let Ok(mut c) = execs.client(&e).await {
                        let _ = c.cancel_tasks(CancelTaskRequest { ids }).await;
                    }
                }
            });
        }
        if tr.job_finished {
            tracing::info!(job = %job_id, "job finished");
        }
        if !tr.stages_completed.is_empty() {
            self.wake.notify_one();
        }
    }

    async fn run_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tick.tick() => {}
            }
            for stale in self.executors.stale(self.config.executor_timeout) {
                self.executor_lost(&stale);
            }
            if let Err(e) = self.dispatch().await {
                tracing::error!("dispatch error: {e}");
            }
        }
    }

    /// Assign pending tasks to executors with free slots and launch them.
    async fn dispatch(&self) -> Result<()> {
        let mut execs = self.executors.list();
        if execs.is_empty() {
            return Ok(());
        }
        execs.sort_by_key(|e| std::cmp::Reverse(e.free_slots()));
        let mut free: Vec<(String, u32)> = execs
            .iter()
            .filter(|e| e.free_slots() > 0)
            .map(|e| (e.metadata.id.clone(), e.free_slots()))
            .collect();
        if free.is_empty() {
            return Ok(());
        }

        // Collect launch batches under the lock, then send without it.
        let mut launches: HashMap<String, Vec<TaskDefinition>> = HashMap::new();
        {
            let order = self.job_order.lock().clone();
            let mut jobs = self.jobs.lock();
            'outer: for job_id in order {
                let Some(job) = jobs.get_mut(&job_id) else { continue };
                if !job.is_active() {
                    continue;
                }
                for (stage_id, partition, attempt, plan) in job.pending_tasks() {
                    let Some(slot) = free.iter_mut().max_by_key(|(_, n)| *n) else {
                        break 'outer;
                    };
                    if slot.1 == 0 {
                        break 'outer;
                    }
                    slot.1 -= 1;
                    let executor_id = slot.0.clone();
                    let id = TaskId {
                        job_id: job_id.clone(),
                        stage_id,
                        partition_id: partition,
                        attempt,
                    };
                    job.mark_running(stage_id, partition, attempt, &executor_id);
                    self.executors.task_started(&executor_id, &id);
                    launches.entry(executor_id).or_default().push(TaskDefinition {
                        id: Some(id),
                        plan: plan.as_ref().clone(),
                        session_config: job.session_config.clone(),
                        work_dir: self.config.work_dir.clone(),
                    });
                }
            }
        }

        for (executor_id, tasks) in launches {
            let n = tasks.len();
            let ids: Vec<TaskId> = tasks.iter().filter_map(|t| t.id.clone()).collect();
            let resp = match self.executors.client(&executor_id).await {
                Ok(mut c) => c
                    .launch_tasks(LaunchTaskRequest {
                        tasks,
                        driver_addr: self.config.driver_addr.clone(),
                    })
                    .await
                    .map_err(|e| ForgeError::Transport(e.to_string())),
                Err(e) => Err(e),
            };
            match resp {
                Ok(r) => {
                    let rejected = r.into_inner().rejected;
                    tracing::debug!(executor = %executor_id, launched = n - rejected.len(), rejected = rejected.len(), "tasks launched");
                    self.requeue(&executor_id, &rejected);
                }
                Err(e) => {
                    tracing::warn!(executor = %executor_id, "launch failed: {e}");
                    self.requeue(&executor_id, &ids);
                    self.executor_lost(&executor_id);
                }
            }
        }
        Ok(())
    }

    fn requeue(&self, executor_id: &str, ids: &[TaskId]) {
        if ids.is_empty() {
            return;
        }
        let mut jobs = self.jobs.lock();
        for id in ids {
            self.executors.task_finished(executor_id, id, false);
            if let Some(j) = jobs.get_mut(&id.job_id) {
                j.mark_pending(id.stage_id, id.partition_id, id.attempt);
            }
        }
    }
}
