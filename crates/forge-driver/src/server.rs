//! gRPC implementation of `DriverService`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{LogicalPlan, Statement};
use datafusion::physical_plan::{displayable, execute_stream_partitioned, ExecutionPlan};
use forge_common::ForgeError;
use forge_proto::driver_service_server::DriverService;
use forge_proto::result_chunk::Payload;
use forge_proto::*;
use forge_scheduler::Scheduler;
use forge_shuffle::codec::ForgeCodec;
use forge_shuffle::reader::ShuffleReaderExec;
use forge_sql::session::ensure_plan_object_stores;
use forge_sql::{register_table, TableFormat, TableSpec};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::session::{Session, SessionManager};
use crate::DriverConfig;

type ChunkTx = mpsc::Sender<Result<ResultChunk, Status>>;

pub struct DriverServer {
    config: DriverConfig,
    scheduler: Arc<Scheduler>,
    sessions: Arc<SessionManager>,
    started: Instant,
}

impl DriverServer {
    pub fn new(config: DriverConfig, scheduler: Arc<Scheduler>) -> Self {
        Self {
            config,
            scheduler,
            sessions: Arc::new(SessionManager::new(Default::default())),
            started: Instant::now(),
        }
    }

    pub fn config(&self) -> &DriverConfig {
        &self.config
    }

    pub fn scheduler(&self) -> &Arc<Scheduler> {
        &self.scheduler
    }

    pub fn sessions(&self) -> &Arc<SessionManager> {
        &self.sessions
    }

    /// Execute `sql` end-to-end, streaming chunks into `tx`.
    pub async fn run_sql(
        &self,
        session: Arc<Session>,
        sql: String,
        max_rows: u64,
        tx: ChunkTx,
    ) -> forge_common::Result<()> {
        let started = Instant::now();
        let ctx = self.sessions.context(&session);
        let logical = ctx.state().create_logical_plan(&sql).await?;

        let job_id = forge_common::new_id();
        let send = |p: Payload| {
            let tx = tx.clone();
            async move { tx.send(Ok(ResultChunk { payload: Some(p) })).await.is_ok() }
        };

        // SET statements mutate the session and return nothing.
        if let LogicalPlan::Statement(Statement::SetVariable(sv)) = &logical {
            self.sessions.set(&session, &sv.variable, &sv.value);
            send(Payload::Started(QueryStarted { job_id: job_id.clone(), explain: String::new() })).await;
            let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false), Field::new("value", DataType::Utf8, false)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![sv.variable.as_str()])),
                    Arc::new(StringArray::from(vec![sv.value.as_str()])),
                ],
            )?;
            let rows = stream_batches(&tx, schema, futures::stream::iter(vec![Ok(batch)]), max_rows).await?;
            send(Payload::Finished(QueryFinished { rows, elapsed_ms: started.elapsed().as_millis() as u64, stages: vec![] })).await;
            return Ok(());
        }

        let local_only = matches!(
            logical,
            LogicalPlan::Ddl(_)
                | LogicalPlan::Statement(_)
                | LogicalPlan::Copy(_)
                | LogicalPlan::Dml(_)
                | LogicalPlan::Explain(_)
                | LogicalPlan::Analyze(_)
                | LogicalPlan::DescribeTable(_)
        );

        if local_only {
            send(Payload::Started(QueryStarted { job_id: job_id.clone(), explain: String::new() })).await;
            let df = ctx.execute_logical_plan(logical).await?;
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
            let stream = df.execute_stream().await?;
            let rows = stream_batches(&tx, schema, stream, max_rows).await?;
            send(Payload::Finished(QueryFinished { rows, elapsed_ms: started.elapsed().as_millis() as u64, stages: vec![] })).await;
            return Ok(());
        }

        let physical = ctx.state().create_physical_plan(&logical).await?;
        ensure_plan_object_stores(&ctx.runtime_env(), &physical)?;
        let explain = displayable(physical.as_ref()).indent(false).to_string();
        send(Payload::Started(QueryStarted { job_id: job_id.clone(), explain })).await;

        // Plans that cannot be shipped to executors (information_schema,
        // in-memory tables, VALUES, ...) run on the driver.
        let shippable = ForgeCodec::encode_plan(Arc::clone(&physical)).is_ok();
        if self.scheduler.executors.is_empty() || !shippable {
            if !self.config.local_fallback && shippable {
                return Err(ForgeError::Scheduler("no executors registered".into()));
            }
            tracing::info!(job = %job_id, shippable, "executing on driver");
            let schema = physical.schema();
            let streams = execute_stream_partitioned(Arc::clone(&physical), ctx.task_ctx())?;
            let merged = futures::stream::iter(streams).flatten();
            let rows = stream_batches(&tx, schema, merged, max_rows).await?;
            send(Payload::Finished(QueryFinished { rows, elapsed_ms: started.elapsed().as_millis() as u64, stages: vec![] })).await;
            return Ok(());
        }

        let settings = self.sessions.settings(&session);
        let mut rx = self.scheduler.submit(job_id.clone(), sql.clone(), physical, &settings)?;
        let result = loop {
            tokio::select! {
                r = &mut rx => break r.map_err(|_| ForgeError::Internal("scheduler dropped job".into()))?,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                    if let Some(p) = self.scheduler.progress(&job_id) {
                        if !send(Payload::Progress(p)).await {
                            self.scheduler.cancel(&job_id);
                            return Err(ForgeError::Cancelled);
                        }
                    }
                }
            }
        };
        let result = match result {
            Ok(r) => r,
            Err(e) => {
                self.scheduler.release(&job_id);
                return Err(e);
            }
        };

        let reader: Arc<dyn ExecutionPlan> = Arc::new(ShuffleReaderExec::new(
            job_id.clone(),
            result.stages.last().map(|s| s.stage_id).unwrap_or(0),
            Arc::clone(&result.schema),
            result.partitions.clone(),
        ));
        let streams = execute_stream_partitioned(reader, ctx.task_ctx())?;
        let merged = futures::stream::iter(streams).flatten();
        let rows = stream_batches(&tx, Arc::clone(&result.schema), merged, max_rows).await;
        self.scheduler.release(&job_id);
        let rows = rows?;
        send(Payload::Finished(QueryFinished {
            rows,
            elapsed_ms: started.elapsed().as_millis() as u64,
            stages: result.stages,
        }))
        .await;
        Ok(())
    }

    async fn explain_sql(&self, session: Arc<Session>, sql: &str) -> forge_common::Result<ExplainResponse> {
        let ctx = self.sessions.context(&session);
        let logical = ctx.state().create_logical_plan(sql).await?;
        let optimized = ctx.state().optimize(&logical)?;
        let physical = ctx.state().create_physical_plan(&logical).await?;
        let stages = self.scheduler.plan_stages("explain", Arc::clone(&physical))?;
        let mut distributed = String::new();
        for s in &stages {
            distributed.push_str(&format!(
                "Stage {} (tasks={}, out_partitions={}, depends_on={:?})\n{}\n",
                s.stage_id,
                s.num_tasks,
                s.output_partitions,
                s.depends_on,
                displayable(s.plan.as_ref()).indent(true)
            ));
        }
        let logical_plan = optimized.display_indent().to_string();
        let physical_plan = displayable(physical.as_ref()).indent(true).to_string();
        Ok(ExplainResponse { logical_plan, physical_plan, distributed_plan: distributed })
    }
}

/// Encode `stream` as Arrow IPC chunks into `tx`. Returns the row count.
async fn stream_batches<S>(tx: &ChunkTx, schema: SchemaRef, mut stream: S, max_rows: u64) -> forge_common::Result<u64>
where
    S: Stream<Item = DFResult<RecordBatch>> + Unpin,
{
    let mut writer = StreamWriter::try_new(Vec::new(), &schema)?;
    let mut rows = 0u64;
    let flush = |writer: &mut StreamWriter<Vec<u8>>| -> Vec<u8> { std::mem::take(writer.get_mut()) };
    let head = flush(&mut writer);
    if tx.send(Ok(ResultChunk { payload: Some(Payload::Ipc(head)) })).await.is_err() {
        return Err(ForgeError::Cancelled);
    }
    while let Some(batch) = stream.next().await {
        let mut batch = batch?;
        if max_rows > 0 && rows + batch.num_rows() as u64 > max_rows {
            let keep = (max_rows - rows) as usize;
            batch = batch.slice(0, keep);
        }
        if batch.num_rows() == 0 {
            if max_rows > 0 && rows >= max_rows {
                break;
            }
            continue;
        }
        rows += batch.num_rows() as u64;
        writer.write(&batch)?;
        let bytes = flush(&mut writer);
        if tx.send(Ok(ResultChunk { payload: Some(Payload::Ipc(bytes)) })).await.is_err() {
            return Err(ForgeError::Cancelled);
        }
        if max_rows > 0 && rows >= max_rows {
            break;
        }
    }
    writer.finish()?;
    let tail = flush(&mut writer);
    if !tail.is_empty() {
        let _ = tx.send(Ok(ResultChunk { payload: Some(Payload::Ipc(tail)) })).await;
    }
    Ok(rows)
}

fn to_status(e: ForgeError) -> Status {
    match e {
        ForgeError::NotFound(m) => Status::not_found(m),
        ForgeError::InvalidArgument(m) => Status::invalid_argument(m),
        ForgeError::Cancelled => Status::cancelled("cancelled"),
        ForgeError::Planning(m) => Status::invalid_argument(m),
        ForgeError::DataFusion(DataFusionError::Plan(m)) | ForgeError::DataFusion(DataFusionError::SQL(_, Some(m))) => {
            Status::invalid_argument(m)
        }
        ForgeError::DataFusion(DataFusionError::SchemaError(e, _)) => Status::invalid_argument(e.to_string()),
        other => Status::internal(other.to_string()),
    }
}

#[tonic::async_trait]
impl DriverService for DriverServer {
    async fn register_executor(
        &self,
        request: Request<RegisterExecutorRequest>,
    ) -> Result<Response<RegisterExecutorResponse>, Status> {
        let req = request.into_inner();
        let Some(md) = req.metadata else {
            return Err(Status::invalid_argument("metadata required"));
        };
        tracing::info!(executor = %md.id, host = %md.host, port = md.port, slots = md.task_slots, "executor registered");
        self.scheduler.executors.register(md, req.resources.unwrap_or_default());
        self.scheduler.wake();
        Ok(Response::new(RegisterExecutorResponse {
            accepted: true,
            driver_id: self.config.id.clone(),
        }))
    }

    async fn heartbeat(&self, request: Request<HeartbeatRequest>) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        match self.scheduler.executors.heartbeat(&req.executor_id, req.resources) {
            Some(completed_jobs) => {
                self.scheduler.wake();
                Ok(Response::new(HeartbeatResponse { known: true, completed_jobs }))
            }
            None => Ok(Response::new(HeartbeatResponse { known: false, completed_jobs: vec![] })),
        }
    }

    async fn update_task_status(
        &self,
        request: Request<TaskStatusUpdateRequest>,
    ) -> Result<Response<TaskStatusUpdateResponse>, Status> {
        self.scheduler.update_task_status(request.into_inner().statuses);
        Ok(Response::new(TaskStatusUpdateResponse {}))
    }

    type ExecuteSqlStream = Pin<Box<dyn Stream<Item = Result<ResultChunk, Status>> + Send + 'static>>;

    async fn execute_sql(&self, request: Request<SqlRequest>) -> Result<Response<Self::ExecuteSqlStream>, Status> {
        let req = request.into_inner();
        let session = self.sessions.session(&req.session_id, &req.session_config);
        let (tx, rx) = mpsc::channel(16);
        let this = DriverHandle {
            config: self.config.clone(),
            scheduler: Arc::clone(&self.scheduler),
            sessions: Arc::clone(&self.sessions),
            started: self.started,
        };
        tokio::spawn(async move {
            let server = this.into_server();
            if let Err(e) = server.run_sql(session, req.sql, req.max_rows, tx.clone()).await {
                let _ = tx.send(Err(to_status(e))).await;
            }
        });
        Ok(Response::new(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))))
    }

    async fn explain(&self, request: Request<ExplainRequest>) -> Result<Response<ExplainResponse>, Status> {
        let req = request.into_inner();
        let session = self.sessions.session(&req.session_id, &HashMap::new());
        self.explain_sql(session, &req.sql).await.map(Response::new).map_err(to_status)
    }

    async fn cancel_query(&self, request: Request<CancelQueryRequest>) -> Result<Response<CancelQueryResponse>, Status> {
        let cancelled = self.scheduler.cancel(&request.into_inner().job_id);
        Ok(Response::new(CancelQueryResponse { cancelled }))
    }

    async fn register_table(&self, request: Request<RegisterTableRequest>) -> Result<Response<RegisterTableResponse>, Status> {
        let req = request.into_inner();
        let format: TableFormat = req.format.parse().map_err(|e: DataFusionError| Status::invalid_argument(e.to_string()))?;
        let name = if req.schema.is_empty() {
            req.name.clone()
        } else if req.catalog.is_empty() {
            format!("{}.{}", req.schema, req.name)
        } else {
            format!("{}.{}.{}", req.catalog, req.schema, req.name)
        };
        let spec = TableSpec { name, format, location: req.location, options: req.options };
        let session = self.sessions.session("", &HashMap::new());
        let ctx = self.sessions.context(&session);
        let schema = register_table(&ctx, &spec).await.map_err(|e| to_status(e.into()))?;
        let fields: Vec<serde_json::Value> = schema
            .fields()
            .iter()
            .map(|f| serde_json::json!({"name": f.name(), "type": f.data_type().to_string(), "nullable": f.is_nullable()}))
            .collect();
        Ok(Response::new(RegisterTableResponse { schema_json: serde_json::Value::Array(fields).to_string() }))
    }

    async fn list_executors(&self, _: Request<ListExecutorsRequest>) -> Result<Response<ListExecutorsResponse>, Status> {
        let executors = self.scheduler.executors.list().iter().map(|e| e.info()).collect();
        Ok(Response::new(ListExecutorsResponse { executors, driver_id: self.config.id.clone() }))
    }

    async fn list_jobs(&self, request: Request<ListJobsRequest>) -> Result<Response<ListJobsResponse>, Status> {
        let limit = request.into_inner().limit;
        let limit = if limit == 0 { 100 } else { limit as usize };
        Ok(Response::new(ListJobsResponse { jobs: self.scheduler.list_jobs(limit) }))
    }

    async fn get_job(&self, request: Request<GetJobRequest>) -> Result<Response<GetJobResponse>, Status> {
        let id = request.into_inner().job_id;
        match self.scheduler.job_info(&id) {
            Some((job, stages)) => Ok(Response::new(GetJobResponse { job: Some(job), stages })),
            None => Err(Status::not_found(format!("job {id}"))),
        }
    }

    async fn status(&self, _: Request<DriverStatusRequest>) -> Result<Response<DriverStatusResponse>, Status> {
        let (total_slots, free_slots) = self.scheduler.executors.total_slots();
        Ok(Response::new(DriverStatusResponse {
            driver_id: self.config.id.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            uptime_ms: self.started.elapsed().as_millis() as u64,
            executors: self.scheduler.executors.len() as u32,
            total_slots,
            free_slots,
            running_jobs: self.scheduler.running_jobs() as u32,
        }))
    }
}

/// Cheap clone of the server's shared handles so query execution can be
/// spawned onto its own task.
struct DriverHandle {
    config: DriverConfig,
    scheduler: Arc<Scheduler>,
    sessions: Arc<SessionManager>,
    started: Instant,
}

impl DriverHandle {
    fn into_server(self) -> DriverServer {
        DriverServer {
            config: self.config,
            scheduler: self.scheduler,
            sessions: self.sessions,
            started: self.started,
        }
    }
}
