//! gRPC client used by executors to fetch shuffle partitions from peers.

use std::sync::Arc;
use std::time::Duration;

use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamDecoder;
use dashmap::DashMap;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use forge_proto::executor_service_client::ExecutorServiceClient;
use forge_proto::FetchShuffleRequest;
use futures::StreamExt;
use tonic::transport::{Channel, Endpoint};

/// Connection cache keyed by executor address.
#[derive(Debug, Default, Clone)]
pub struct ShuffleClient {
    channels: Arc<DashMap<String, Channel>>,
}

impl ShuffleClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// Global shared instance so every `ShuffleReaderExec` in a process reuses
    /// connections.
    pub fn global() -> &'static ShuffleClient {
        static GLOBAL: std::sync::OnceLock<ShuffleClient> = std::sync::OnceLock::new();
        GLOBAL.get_or_init(ShuffleClient::new)
    }

    pub async fn channel(&self, addr: &str) -> DFResult<Channel> {
        if let Some(c) = self.channels.get(addr) {
            return Ok(c.clone());
        }
        let uri = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        };
        let ep = Endpoint::from_shared(uri)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .connect_timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(20));
        let channel = ep.connect_lazy();
        self.channels.insert(addr.to_string(), channel.clone());
        Ok(channel)
    }

    pub async fn executor(&self, addr: &str) -> DFResult<ExecutorServiceClient<Channel>> {
        let ch = self.channel(addr).await?;
        Ok(ExecutorServiceClient::new(ch)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX))
    }

    /// Stream a remote shuffle partition as record batches.
    pub async fn fetch(
        &self,
        addr: &str,
        req: FetchShuffleRequest,
        schema: SchemaRef,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut client = self.executor(addr).await?;
        let resp = client
            .fetch_shuffle(req)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let inbound = resp.into_inner();

        let decoder = StreamDecoder::new();
        let stream = futures::stream::unfold(
            (inbound, decoder, Vec::<RecordBatch>::new(), false),
            |(mut inbound, mut decoder, mut pending, done)| async move {
                loop {
                    if let Some(b) = pending.pop() {
                        return Some((Ok(b), (inbound, decoder, pending, done)));
                    }
                    if done {
                        return None;
                    }
                    match inbound.next().await {
                        None => return None,
                        Some(Err(e)) => {
                            return Some((
                                Err(DataFusionError::External(Box::new(e))),
                                (inbound, decoder, pending, true),
                            ))
                        }
                        Some(Ok(chunk)) => {
                            let mut buf = Buffer::from(chunk.ipc);
                            let mut batches = Vec::new();
                            loop {
                                match decoder.decode(&mut buf) {
                                    Ok(Some(b)) => batches.push(b),
                                    Ok(None) => break,
                                    Err(e) => {
                                        return Some((
                                            Err(DataFusionError::ArrowError(Box::new(e), None)),
                                            (inbound, decoder, pending, true),
                                        ))
                                    }
                                }
                            }
                            batches.reverse();
                            pending = batches;
                            if chunk.last {
                                if let Some(b) = pending.pop() {
                                    return Some((Ok(b), (inbound, decoder, pending, true)));
                                }
                                return None;
                            }
                        }
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
