# Lakeforge architecture

Lakeforge mirrors the Databricks split between a **control plane** (the
workspace: API, UI, metadata, scheduling) and a **compute plane** (clusters
that run user code and SQL). Both are open source here, both run inside your
cloud account, and both deploy from one Helm chart.

```
                 ┌──────────────────────────────────────────────────────────┐
  browser / SDK  │  lakeforge-api  (control plane, 1 container)             │
  databricks-cli ├───────────────────────────────────────────────────────────┤
 ───────────────►│  axum REST  ─ /api/2.0|2.1/**   (Databricks-compatible)  │
                 │  static SPA ─ web/dist          (workspace UI)           │
                 │  auth: argon2 passwords, JWT sessions, dapi PATs, SCIM   │
                 │  store: sqlx::Any  → SQLite (single node) | PostgreSQL   │
                 │  storage: object_store → file | s3 | gs | az             │
                 │  workers: job scheduler, pipeline runner, cluster GC     │
                 │  kernel:  Python notebook kernels (one per context)      │
                 │  cluster-manager: local-process | kubernetes backend     │
                 └───────────────┬──────────────────────────────────────────┘
                                 │ gRPC (forge.proto)
        ┌────────────────────────┼─────────────────────────────┐
        ▼                        ▼                             ▼
 ┌─────────────┐  cluster A  ┌──────────────┐    ┌──────────────┐
 │ forge driver│◄───────────►│forge executor│◄──►│forge executor│  … per cluster
 │  SQL, catalog│ tasks/heart-│ DataFusion   │shuffle fetch     │
 │  scheduler   │ beats       │ tasks, shuffle│   │              │
 └─────────────┘             └──────────────┘    └──────────────┘
        │  reads/writes Delta + Parquet/CSV/JSON via object_store
        ▼
   lakehouse storage  (S3 / GCS / ADLS / local)  <warehouse>/<catalog>/<schema>/<table>
```

## Forge — the compute engine

Forge is a from-scratch distributed SQL engine in Rust. It uses Apache Arrow as
its memory format, DataFusion for parsing / planning / vectorised operators and
delta-rs for the Delta Lake transaction protocol. Everything above that —
distribution, scheduling, shuffle, cluster lifecycle — is Forge.

| Crate | Role |
| --- | --- |
| `forge-proto` | The gRPC contract (`proto/forge.proto`, via `tonic`/`prost`): `DriverService` (ExecuteSql, Explain, CancelQuery, RegisterTable, ListExecutors/Jobs, Status, RegisterExecutor, Heartbeat, UpdateTaskStatus) and `ExecutorService` (LaunchTasks, CancelTasks, FetchShuffle, RemoveJobData). Physical plans travel as DataFusion protobuf. |
| `forge-common` | Shared config (`FORGE_*` env), ids, error types. |
| `forge-sql` | Session construction shared by driver and executors: object-store wiring for `file://`, `s3://`, `gs://`, `az://`; table registration for Delta, Parquet, CSV, JSON; a Delta table provider with `INSERT`, `DELETE`, `UPDATE`, `TRUNCATE`; **managed tables** that turn `CREATE TABLE [AS]` into Delta tables under `<warehouse>/<catalog>/<schema>/<table>` so every cluster in the workspace sees them. |
| `forge-shuffle` | `ShuffleWriterExec` hash/round-robin partitions a stage's output into Arrow IPC files on executor-local disk; `ShuffleReaderExec` streams them back, locally or over gRPC; `UnresolvedShuffleExec` is the planner's placeholder. |
| `forge-scheduler` | The planner cuts a DataFusion physical plan at every exchange (`RepartitionExec`, `CoalescePartitionsExec`, `SortPreservingMergeExec`) into a DAG of **stages**; the scheduler runs stages bottom-up as sets of tasks (one per partition), tracks executor slots and heartbeats, retries failed tasks on other executors, and resolves shuffle locations as producers finish. |
| `forge-executor` | Registers with the driver, heartbeats, executes tasks on its slot pool, serves shuffle partitions, cleans up job data. Memory-limited via `FORGE_MEMORY_LIMIT_MB` (DataFusion memory pool); all `forge.*` / Spark-alias settings arrive as `FORGE_CONF_*`. |
| `forge-driver` | The cluster's front door: owns the catalog and runtime, vends per-client sessions, plans and schedules queries, streams results back as Arrow IPC. Falls back to single-process execution for plans that cannot be distributed unless `--no-local-fallback`. |
| `forge-client` | Async Rust client used by the CLI and control plane. |
| `forge-cli` | `forge driver | executor | local | sql | register | status | executors | jobs`. `forge local` boots a driver plus N executors for development and CI. |

A query's life: SQL → DataFusion logical plan → physical plan → stage DAG →
tasks launched on executors → shuffle files → final stage → Arrow IPC batches
streamed to the caller. `forge sql --explain` prints the physical plan and the
stage DAG.

### Why "superior"?

Compared to a JVM Spark cluster, Forge is a single static binary per role with
no JVM warm-up or GC pauses, Arrow-native end to end (no row/column
conversion, no Java↔native boundary), starts in milliseconds so clusters
launch in seconds rather than minutes, and its scheduler/shuffle are a few
thousand lines that can be read in an afternoon. It does **not** yet match
Spark's breadth (no DataFrame API in Python/Scala, no streaming, no adaptive
query execution, no Photon-class vectorised joins); see `docs/parity.md`.

## Control plane — `lakeforge-api`

A single axum binary that serves the REST API, the workspace UI and the
notebook kernels.

- **Store** (`store.rs`): a document store over `sqlx::Any`, so the same code
  runs on the embedded SQLite (`sqlite://.lakeforge/lakeforge.db`) for
  single-node installs and on managed PostgreSQL in the cloud. Every entity
  (clusters, jobs, runs, notebooks, catalog objects, secrets, MLflow
  runs/models, …) is a versioned JSON document with indexed keys.
- **Storage** (`storage.rs`): one `object_store` root (`LAKEFORGE_STORAGE_ROOT`)
  holding DBFS, workspace files, MLflow artifacts, the Unity Catalog warehouse
  and volumes. `file://` for local, `s3://` / `gs://` / `az://` in the cloud,
  authenticated via IRSA / Workload Identity / federated identity — never with
  long-lived keys.
- **Auth** (`auth.rs`): argon2 password hashes, JWT browser sessions, `dapi…`
  personal access tokens, HTTP Basic, service principals with client secrets.
  `Principal { user_id, user_name, is_admin, groups }` flows into every handler;
  object ACLs live in `api/permissions.rs` and Unity Catalog grants in
  `api/catalog.rs`.
- **API** (`api/`): one module per Databricks service. Handlers are thin over
  the store; the interesting ones drive Forge (`api/sql.rs` statement
  execution, `api/catalog.rs` table/volume metadata + `SHOW`/`DESCRIBE`
  materialisation, `api/commands.rs` 1.2 command execution), or run workloads
  (`api/jobs.rs` multi-task DAG runs with `depends_on` / `run_if` / repair /
  retries / timeouts / schedules; `api/pipelines.rs` DLT-style
  `CREATE [STREAMING] LIVE TABLE` graphs executed in dependency order;
  `api/serving.rs` model endpoints backed by the model registry).
- **Kernel** (`kernel.rs`): each execution context is a Python subprocess
  running `python/lakeforge_kernel.py`, with `spark.sql()` (SQL pushed to the
  cluster's Forge driver), `dbutils` and `display()` pre-wired, and the
  `LAKEFORGE_*` / `DATABRICKS_*` environment set so `WorkspaceClient()` works
  without configuration. Results (stdout, rich outputs, errors) stream to the
  UI over SSE.
- **Workers** (`workers.rs`): background loops for cluster health and
  auto-termination, the job scheduler tick (Quartz cron schedules, paused
  schedules honoured) and reaping execution contexts whose cluster is gone.
- **Cluster manager** (`lakeforge-cluster-manager`): `LaunchSpec` → running
  cluster. The local backend spawns `forge driver` / `forge executor`
  processes; the Kubernetes backend creates a driver Deployment + Service and
  an executor Deployment in `LAKEFORGE_K8S_NAMESPACE` using the
  `lakeforge-forge` image, with optional service account, pod labels, node
  selector and extra env (`LAKEFORGE_FORGE_*`) so cloud workload identity
  reaches the compute pods.

## Workspace UI — `web/`

React 19 + Vite + TypeScript, served from `web/dist` by the API with SPA
fallback. Pages map one-to-one to Databricks' left nav: Workspace, Notebook,
SQL Editor, Query History, Warehouses, Alerts, Dashboards, Catalog, Compute,
Workflows, Run detail, Pipelines, Experiments, Models, Serving, Repos, DBFS,
Secrets, Settings, Admin. It talks only to the public REST API (`src/api.ts`)
plus the SSE stream for notebook output.

## Python — `python/`

- `lakeforge-sdk`: `WorkspaceClient` with one attribute per service
  (`clusters`, `jobs`, `workspace`, `statement_execution`, `catalogs`,
  `tables`, `secrets`, `tokens`, `users`, `experiments`, `model_registry`,
  `serving_endpoints`, `pipelines`, `repos`, `dbfs`, `files`, …), typed
  `ApiError` subclasses, retries, pagination, multipart upload, and
  Databricks-compatible config resolution (`DATABRICKS_HOST`/`TOKEN`,
  `~/.databrickscfg` profiles).
- `dbutils`: `fs`, `secrets`, `widgets`, `notebook` (`run`/`exit`),
  `jobs.taskValues`, `library`.
- `lakeforge` CLI: `auth api clusters workspace fs sql jobs pipelines catalog
  secrets tokens users groups service-principals repos experiments models
  serving`, `-o json` for scripting.
- `lakeforge_kernel.py`: the notebook kernel described above.

## Deployment shape

One Helm chart (`deploy/helm/lakeforge`) deploys the control plane
Deployment + Service (+ Ingress), its Secret (admin password, JWT secret,
database URL), RBAC that lets it manage Forge Deployments/Services in the
compute namespace, an optional embedded PostgreSQL for dev, and a PVC or
object-store root for storage. Terraform roots for AWS / GCP / Azure provision
the network, Kubernetes cluster (with a control node pool and an autoscaling
compute pool), managed PostgreSQL, an object-storage bucket and workload
identities, then install the chart. `deploy/deploy.sh` wraps all of it. Details
in [deploy.md](deploy.md).
