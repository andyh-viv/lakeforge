//! Thin client for the Forge driver: runs SQL and decodes the Arrow IPC result
//! stream into record batches.

use std::collections::HashMap;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamDecoder;
use forge_common::{ForgeError, Result};
use forge_proto::driver_service_client::DriverServiceClient;
use forge_proto::result_chunk::Payload;
use forge_proto::*;
use futures::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tonic::Streaming;

#[derive(Clone)]
pub struct ForgeClient {
    inner: DriverServiceClient<Channel>,
    session_id: String,
}

/// One event from a running query.
#[derive(Debug)]
pub enum QueryEvent {
    Started(QueryStarted),
    Schema(SchemaRef),
    Batch(RecordBatch),
    Progress(QueryProgress),
    Finished(QueryFinished),
}

/// Fully materialised query result.
#[derive(Debug)]
pub struct QueryResult {
    pub job_id: String,
    pub schema: Option<SchemaRef>,
    pub batches: Vec<RecordBatch>,
    pub finished: Option<QueryFinished>,
}

impl QueryResult {
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

/// Streaming query handle.
pub struct QueryStream {
    inbound: Streaming<ResultChunk>,
    decoder: StreamDecoder,
    pending: Vec<RecordBatch>,
    schema: Option<SchemaRef>,
}

impl QueryStream {
    pub async fn next(&mut self) -> Result<Option<QueryEvent>> {
        loop {
            if let Some(b) = self.pending.pop() {
                return Ok(Some(QueryEvent::Batch(b)));
            }
            let Some(chunk) = self.inbound.next().await else {
                return Ok(None);
            };
            let chunk = chunk?;
            match chunk.payload {
                None => continue,
                Some(Payload::Started(s)) => return Ok(Some(QueryEvent::Started(s))),
                Some(Payload::Progress(p)) => return Ok(Some(QueryEvent::Progress(p))),
                Some(Payload::Finished(f)) => return Ok(Some(QueryEvent::Finished(f))),
                Some(Payload::Ipc(bytes)) => {
                    let mut buf = Buffer::from(bytes);
                    let mut batches = Vec::new();
                    while let Some(b) = self.decoder.decode(&mut buf)? {
                        batches.push(b);
                    }
                    batches.reverse();
                    self.pending = batches;
                    if self.schema.is_none() {
                        if let Some(s) = self.decoder.schema() {
                            self.schema = Some(s.clone());
                            return Ok(Some(QueryEvent::Schema(s)));
                        }
                    }
                }
            }
        }
    }

    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema.clone()
    }

    pub async fn collect(mut self) -> Result<QueryResult> {
        let mut out = QueryResult {
            job_id: String::new(),
            schema: None,
            batches: vec![],
            finished: None,
        };
        while let Some(ev) = self.next().await? {
            match ev {
                QueryEvent::Started(s) => out.job_id = s.job_id,
                QueryEvent::Schema(s) => out.schema = Some(s),
                QueryEvent::Batch(b) => out.batches.push(b),
                QueryEvent::Progress(_) => {}
                QueryEvent::Finished(f) => out.finished = Some(f),
            }
        }
        if out.schema.is_none() {
            out.schema = self.schema.take();
        }
        Ok(out)
    }
}

impl ForgeClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let addr = if addr.starts_with("http") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        };
        let channel = Endpoint::from_shared(addr.clone())
            .map_err(|e| ForgeError::Transport(e.to_string()))?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .connect()
            .await
            .map_err(|e| ForgeError::Transport(format!("connect {addr}: {e}")))?;
        Ok(Self {
            inner: DriverServiceClient::new(channel)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
            session_id: forge_common::new_id(),
        })
    }

    pub fn with_session(mut self, id: impl Into<String>) -> Self {
        self.session_id = id.into();
        self
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub async fn sql_stream(&self, sql: &str) -> Result<QueryStream> {
        self.sql_stream_with(sql, HashMap::new(), 0).await
    }

    pub async fn sql_stream_with(
        &self,
        sql: &str,
        session_config: HashMap<String, String>,
        max_rows: u64,
    ) -> Result<QueryStream> {
        let mut c = self.inner.clone();
        let resp = c
            .execute_sql(SqlRequest {
                sql: sql.to_string(),
                session_id: self.session_id.clone(),
                session_config,
                catalog: String::new(),
                schema: String::new(),
                max_rows,
            })
            .await?;
        Ok(QueryStream {
            inbound: resp.into_inner(),
            decoder: StreamDecoder::new(),
            pending: vec![],
            schema: None,
        })
    }

    pub async fn sql(&self, sql: &str) -> Result<QueryResult> {
        self.sql_stream(sql).await?.collect().await
    }

    pub async fn explain(&self, sql: &str, verbose: bool) -> Result<ExplainResponse> {
        let mut c = self.inner.clone();
        Ok(c.explain(ExplainRequest {
            sql: sql.to_string(),
            session_id: self.session_id.clone(),
            verbose,
        })
        .await?
        .into_inner())
    }

    pub async fn cancel(&self, job_id: &str) -> Result<bool> {
        let mut c = self.inner.clone();
        Ok(c.cancel_query(CancelQueryRequest { job_id: job_id.into() })
            .await?
            .into_inner()
            .cancelled)
    }

    pub async fn register_table(
        &self,
        name: &str,
        format: &str,
        location: &str,
        options: HashMap<String, String>,
    ) -> Result<String> {
        let mut c = self.inner.clone();
        Ok(c.register_table(RegisterTableRequest {
            name: name.into(),
            location: location.into(),
            format: format.into(),
            options,
            catalog: String::new(),
            schema: String::new(),
        })
        .await?
        .into_inner()
        .schema_json)
    }

    pub async fn list_executors(&self) -> Result<ListExecutorsResponse> {
        let mut c = self.inner.clone();
        Ok(c.list_executors(ListExecutorsRequest {}).await?.into_inner())
    }

    pub async fn list_jobs(&self, limit: u32) -> Result<Vec<JobInfo>> {
        let mut c = self.inner.clone();
        Ok(c.list_jobs(ListJobsRequest { limit }).await?.into_inner().jobs)
    }

    pub async fn get_job(&self, job_id: &str) -> Result<GetJobResponse> {
        let mut c = self.inner.clone();
        Ok(c.get_job(GetJobRequest { job_id: job_id.into() }).await?.into_inner())
    }

    pub async fn status(&self) -> Result<DriverStatusResponse> {
        let mut c = self.inner.clone();
        Ok(c.status(DriverStatusRequest {}).await?.into_inner())
    }
}
