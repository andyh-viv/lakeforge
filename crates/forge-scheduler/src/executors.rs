//! Registry of live executors, their resources and gRPC clients.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use forge_common::{now_ms, ForgeError, Result};
use forge_proto::executor_service_client::ExecutorServiceClient;
use forge_proto::{ExecutorInfo, ExecutorMetadata, ExecutorResources, TaskId};
use tonic::transport::{Channel, Endpoint};

#[derive(Debug, Clone)]
pub struct ExecutorEntry {
    pub metadata: ExecutorMetadata,
    pub resources: ExecutorResources,
    pub last_heartbeat_ms: u64,
    pub running: HashSet<String>,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    /// Jobs whose shuffle data may be purged on the next heartbeat.
    pub purge_jobs: Vec<String>,
}

impl ExecutorEntry {
    pub fn addr(&self) -> String {
        format!("http://{}:{}", self.metadata.host, self.metadata.port)
    }

    pub fn free_slots(&self) -> u32 {
        self.metadata
            .task_slots
            .saturating_sub(self.running.len() as u32)
    }

    pub fn info(&self) -> ExecutorInfo {
        ExecutorInfo {
            metadata: Some(self.metadata.clone()),
            resources: Some(self.resources.clone()),
            last_heartbeat_ms: self.last_heartbeat_ms,
            running_tasks: self.running.len() as u32,
            completed_tasks: self.completed_tasks,
            failed_tasks: self.failed_tasks,
        }
    }
}

#[derive(Debug, Default)]
pub struct ExecutorRegistry {
    executors: DashMap<String, ExecutorEntry>,
    clients: DashMap<String, ExecutorServiceClient<Channel>>,
}

impl ExecutorRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self, metadata: ExecutorMetadata, resources: ExecutorResources) {
        let id = metadata.id.clone();
        tracing::info!(executor = %id, host = %metadata.host, port = metadata.port, slots = metadata.task_slots, "executor registered");
        self.executors.insert(
            id,
            ExecutorEntry {
                metadata,
                resources,
                last_heartbeat_ms: now_ms(),
                running: HashSet::new(),
                completed_tasks: 0,
                failed_tasks: 0,
                purge_jobs: Vec::new(),
            },
        );
    }

    /// Record a heartbeat; returns `false` when the executor is unknown and
    /// must re-register. Drains the pending purge list.
    pub fn heartbeat(&self, id: &str, resources: Option<ExecutorResources>) -> Option<Vec<String>> {
        let mut e = self.executors.get_mut(id)?;
        e.last_heartbeat_ms = now_ms();
        if let Some(r) = resources {
            e.resources = r;
        }
        Some(std::mem::take(&mut e.purge_jobs))
    }

    pub fn remove(&self, id: &str) -> Option<ExecutorEntry> {
        self.clients.remove(id);
        self.executors.remove(id).map(|(_, e)| e)
    }

    pub fn get(&self, id: &str) -> Option<ExecutorEntry> {
        self.executors.get(id).map(|e| e.clone())
    }

    pub fn len(&self) -> usize {
        self.executors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.executors.is_empty()
    }

    pub fn list(&self) -> Vec<ExecutorEntry> {
        self.executors.iter().map(|e| e.clone()).collect()
    }

    pub fn total_slots(&self) -> (u32, u32) {
        let mut total = 0;
        let mut free = 0;
        for e in self.executors.iter() {
            total += e.metadata.task_slots;
            free += e.free_slots();
        }
        (total, free)
    }

    pub fn task_started(&self, id: &str, task: &TaskId) {
        if let Some(mut e) = self.executors.get_mut(id) {
            e.running.insert(task.to_string());
        }
    }

    pub fn task_finished(&self, id: &str, task: &TaskId, ok: bool) {
        if let Some(mut e) = self.executors.get_mut(id) {
            e.running.remove(&task.to_string());
            if ok {
                e.completed_tasks += 1;
            } else {
                e.failed_tasks += 1;
            }
        }
    }

    pub fn schedule_purge(&self, job_id: &str) {
        for mut e in self.executors.iter_mut() {
            e.purge_jobs.push(job_id.to_string());
        }
    }

    /// Executors whose heartbeat is older than `timeout`.
    pub fn stale(&self, timeout: Duration) -> Vec<String> {
        let cutoff = now_ms().saturating_sub(timeout.as_millis() as u64);
        self.executors
            .iter()
            .filter(|e| e.last_heartbeat_ms < cutoff)
            .map(|e| e.metadata.id.clone())
            .collect()
    }

    pub async fn client(&self, id: &str) -> Result<ExecutorServiceClient<Channel>> {
        if let Some(c) = self.clients.get(id) {
            return Ok(c.clone());
        }
        let addr = self
            .get(id)
            .ok_or_else(|| ForgeError::NotFound(format!("executor {id}")))?
            .addr();
        let channel = Endpoint::from_shared(addr.clone())
            .map_err(|e| ForgeError::Transport(e.to_string()))?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .connect()
            .await
            .map_err(|e| ForgeError::Transport(format!("connect {addr}: {e}")))?;
        let client = ExecutorServiceClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        self.clients.insert(id.to_string(), client.clone());
        Ok(client)
    }
}
