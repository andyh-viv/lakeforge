//! Executes task plans and reports their status to the driver.

use std::sync::Arc;

use dashmap::DashMap;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use forge_common::config::SessionSettings;
use forge_common::now_ms;
use forge_proto::{TaskDefinition, TaskId, TaskMetrics, TaskState, TaskStatus, TaskStatusUpdateRequest};
use forge_shuffle::codec::ForgeCodec;
use forge_shuffle::storage::ShuffleStorage;
use forge_shuffle::writer::ShuffleWriterExec;
use forge_sql::session::ensure_plan_object_stores;
use forge_sql::ForgeSessionBuilder;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::{connect_driver, ExecutorConfig};

struct RunningTask {
    cancel: CancellationToken,
    started_ms: u64,
}

pub struct TaskRunner {
    cfg: Arc<ExecutorConfig>,
    running: DashMap<String, RunningTask>,
    slots: Arc<tokio::sync::Semaphore>,
    storage: ShuffleStorage,
}

impl TaskRunner {
    pub fn new(cfg: Arc<ExecutorConfig>) -> Self {
        let slots = Arc::new(tokio::sync::Semaphore::new(cfg.task_slots));
        let storage = ShuffleStorage::new(&cfg.work_dir);
        Self {
            cfg,
            running: DashMap::new(),
            slots,
            storage,
        }
    }

    pub fn storage(&self) -> &ShuffleStorage {
        &self.storage
    }

    pub fn running_count(&self) -> usize {
        self.running.len()
    }

    pub fn running_statuses(&self) -> Vec<TaskStatus> {
        self.running
            .iter()
            .filter_map(|e| parse_task_key(e.key()).map(|id| (id, e.started_ms)))
            .map(|(id, started)| TaskStatus {
                id: Some(id),
                state: TaskState::TaskRunning as i32,
                executor_id: self.cfg.id.clone(),
                error: String::new(),
                outputs: vec![],
                metrics: Some(TaskMetrics {
                    start_ms: started,
                    ..Default::default()
                }),
            })
            .collect()
    }

    pub fn remove_job_data(&self, job_id: &str) -> u64 {
        match self.storage.remove_job(job_id) {
            Ok(b) => {
                tracing::debug!(job = %job_id, bytes = b, "purged shuffle data");
                b
            }
            Err(e) => {
                tracing::warn!(job = %job_id, "purge failed: {e}");
                0
            }
        }
    }

    /// Returns true if the task was accepted (a slot is available).
    pub fn try_launch(self: &Arc<Self>, task: TaskDefinition, driver_addr: String) -> bool {
        let Some(id) = task.id.clone() else { return false };
        let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() else {
            return false;
        };
        let key = id.to_string();
        let cancel = CancellationToken::new();
        self.running.insert(
            key.clone(),
            RunningTask {
                cancel: cancel.clone(),
                started_ms: now_ms(),
            },
        );
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let _permit = permit;
            let start = now_ms();
            let outcome = tokio::select! {
                r = this.execute(&task) => r,
                _ = cancel.cancelled() => Err(DataFusionError::Execution("cancelled".into())),
            };
            this.running.remove(&key);
            let status = match outcome {
                Ok((outputs, metrics)) => TaskStatus {
                    id: Some(id.clone()),
                    state: TaskState::TaskCompleted as i32,
                    executor_id: this.cfg.id.clone(),
                    error: String::new(),
                    outputs,
                    metrics: Some(TaskMetrics {
                        start_ms: start,
                        end_ms: now_ms(),
                        ..metrics
                    }),
                },
                Err(e) => {
                    let cancelled = cancel.is_cancelled();
                    tracing::warn!(task = %id, cancelled, "task failed: {e}");
                    TaskStatus {
                        id: Some(id.clone()),
                        state: if cancelled {
                            TaskState::TaskCancelled as i32
                        } else {
                            TaskState::TaskFailed as i32
                        },
                        executor_id: this.cfg.id.clone(),
                        error: e.to_string(),
                        outputs: vec![],
                        metrics: Some(TaskMetrics {
                            start_ms: start,
                            end_ms: now_ms(),
                            ..Default::default()
                        }),
                    }
                }
            };
            this.report(&driver_addr, status).await;
        });
        true
    }

    pub fn cancel(&self, id: &TaskId) -> bool {
        if let Some(t) = self.running.get(&id.to_string()) {
            t.cancel.cancel();
            true
        } else {
            false
        }
    }

    async fn report(&self, driver_addr: &str, status: TaskStatus) {
        for attempt in 0..5u32 {
            match connect_driver(driver_addr).await {
                Ok(mut c) => {
                    match c
                        .update_task_status(TaskStatusUpdateRequest {
                            statuses: vec![status.clone()],
                        })
                        .await
                    {
                        Ok(_) => return,
                        Err(e) => tracing::warn!("status report failed: {e}"),
                    }
                }
                Err(e) => tracing::warn!("status report connect failed: {e}"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(200 * (1 << attempt))).await;
        }
        tracing::error!(task = ?status.id, "giving up reporting task status");
    }

    async fn execute(
        &self,
        task: &TaskDefinition,
    ) -> DFResult<(Vec<forge_proto::ShufflePartitionLocation>, TaskMetrics)> {
        let id = task
            .id
            .clone()
            .ok_or_else(|| DataFusionError::Internal("task without id".into()))?;
        let settings = SessionSettings::from_map(
            task.session_config.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        );
        let ctx = ForgeSessionBuilder::new(settings).build();
        let task_ctx = ctx.task_ctx();
        let plan = ForgeCodec::decode_plan(&task.plan, &task_ctx)?;
        let plan = self.relocate_writer(plan)?;
        ensure_plan_object_stores(&ctx.runtime_env(), &plan)?;

        let partition = id.partition_id as usize;
        if partition >= plan.output_partitioning().partition_count() {
            return Err(DataFusionError::Internal(format!(
                "task {id}: partition {partition} out of range ({})",
                plan.output_partitioning().partition_count()
            )));
        }
        tracing::debug!(task = %id, "executing");
        let t0 = std::time::Instant::now();
        let mut stream = plan.execute(partition, task_ctx)?;
        let mut stats = Vec::new();
        while let Some(b) = stream.next().await {
            stats.push(b?);
        }
        let mut outputs = Vec::new();
        for b in &stats {
            outputs.extend(ShuffleWriterExec::decode_stats(
                b,
                id.partition_id,
                &self.cfg.id,
                &self.cfg.advertise_addr(),
            )?);
        }
        let metrics = TaskMetrics {
            output_rows: outputs.iter().map(|o| o.num_rows).sum(),
            output_bytes: outputs.iter().map(|o| o.num_bytes).sum(),
            elapsed_compute_ns: t0.elapsed().as_nanos() as u64,
            ..Default::default()
        };
        tracing::debug!(task = %id, rows = metrics.output_rows, ms = t0.elapsed().as_millis(), "task complete");
        Ok((outputs, metrics))
    }

    /// Point the root `ShuffleWriterExec` at this executor's work directory.
    fn relocate_writer(&self, plan: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let w = plan
            .as_any()
            .downcast_ref::<ShuffleWriterExec>()
            .ok_or_else(|| DataFusionError::Internal("task plan root must be ShuffleWriterExec".into()))?;
        Ok(Arc::new(ShuffleWriterExec::new(
            w.job_id(),
            w.stage_id(),
            Arc::clone(w.input()),
            w.shuffle_partitioning().cloned(),
            self.cfg.work_dir.clone(),
        )))
    }
}

fn parse_task_key(key: &str) -> Option<TaskId> {
    // format: {job}/s{stage}/p{partition}/a{attempt}
    let mut parts = key.rsplitn(4, '/');
    let attempt = parts.next()?.strip_prefix('a')?.parse().ok()?;
    let partition_id = parts.next()?.strip_prefix('p')?.parse().ok()?;
    let stage_id = parts.next()?.strip_prefix('s')?.parse().ok()?;
    let job_id = parts.next()?.to_string();
    Some(TaskId {
        job_id,
        stage_id,
        partition_id,
        attempt,
    })
}
