//! gRPC surface of an executor.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use forge_proto::executor_service_server::ExecutorService;
use forge_proto::{
    CancelTaskRequest, CancelTaskResponse, FetchShuffleRequest, LaunchTaskRequest,
    LaunchTaskResponse, RemoveJobDataRequest, RemoveJobDataResponse, ShuffleChunk,
};
use futures::Stream;
use tokio::io::AsyncReadExt;
use tonic::{Request, Response, Status};

use crate::runner::TaskRunner;

const CHUNK_BYTES: usize = 4 * 1024 * 1024;

pub struct ExecutorServer {
    runner: Arc<TaskRunner>,
}

impl ExecutorServer {
    pub fn new(runner: Arc<TaskRunner>) -> Self {
        Self { runner }
    }
}

#[tonic::async_trait]
impl ExecutorService for ExecutorServer {
    async fn launch_tasks(
        &self,
        request: Request<LaunchTaskRequest>,
    ) -> Result<Response<LaunchTaskResponse>, Status> {
        let req = request.into_inner();
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        for task in req.tasks {
            let id = task.id.clone();
            if self.runner.try_launch(task, req.driver_addr.clone()) {
                accepted.extend(id);
            } else {
                rejected.extend(id);
            }
        }
        Ok(Response::new(LaunchTaskResponse { accepted, rejected }))
    }

    async fn cancel_tasks(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskResponse>, Status> {
        let n = request
            .into_inner()
            .ids
            .iter()
            .filter(|id| self.runner.cancel(id))
            .count();
        Ok(Response::new(CancelTaskResponse { cancelled: n as u32 }))
    }

    type FetchShuffleStream =
        Pin<Box<dyn Stream<Item = Result<ShuffleChunk, Status>> + Send + 'static>>;

    async fn fetch_shuffle(
        &self,
        request: Request<FetchShuffleRequest>,
    ) -> Result<Response<Self::FetchShuffleStream>, Status> {
        let req = request.into_inner();
        let storage = self.runner.storage();
        let expected = storage.partition_path(
            &req.job_id,
            req.stage_id,
            req.map_partition_id,
            req.output_partition_id,
        );
        let requested = Path::new(&req.path);
        // Only serve files inside our own shuffle directory.
        if requested != expected.as_path() && !requested.starts_with(storage.root()) {
            return Err(Status::permission_denied("path outside shuffle directory"));
        }
        let path = if requested.exists() { requested.to_path_buf() } else { expected };
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| Status::not_found(format!("{}: {e}", path.display())))?;
        let len = file
            .metadata()
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        let stream = async_stream(move |tx| async move {
            let mut sent = 0u64;
            loop {
                let mut buf = vec![0u8; CHUNK_BYTES];
                let n = match file.read(&mut buf).await {
                    Ok(n) => n,
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        return;
                    }
                };
                if n == 0 {
                    if sent == 0 {
                        let _ = tx.send(Ok(ShuffleChunk { ipc: vec![], last: true })).await;
                    }
                    return;
                }
                buf.truncate(n);
                sent += n as u64;
                let last = sent >= len;
                if tx.send(Ok(ShuffleChunk { ipc: buf, last })).await.is_err() {
                    return;
                }
                if last {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn remove_job_data(
        &self,
        request: Request<RemoveJobDataRequest>,
    ) -> Result<Response<RemoveJobDataResponse>, Status> {
        let bytes_freed = self.runner.remove_job_data(&request.into_inner().job_id);
        Ok(Response::new(RemoveJobDataResponse { bytes_freed }))
    }
}

/// Spawn `producer` feeding a bounded channel and expose it as a stream.
fn async_stream<T, F, Fut>(producer: F) -> tokio_stream::wrappers::ReceiverStream<T>
where
    T: Send + 'static,
    F: FnOnce(tokio::sync::mpsc::Sender<T>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(producer(tx));
    tokio_stream::wrappers::ReceiverStream::new(rx)
}
