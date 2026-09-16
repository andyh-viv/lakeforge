# Databricks parity matrix

Status legend:

- **Full** — behaviour and API match Databricks closely enough that the
  official SDK/CLI calls succeed and do what a user expects.
- **Partial** — the API exists and the core path works, but noted options are
  missing, simplified or approximated.
- **API only** — endpoints accept and persist Databricks payloads but there is
  no real behaviour behind them (returns sensible defaults).
- **Missing** — not implemented.

Lakeforge is **not** 100% feature-equivalent to Databricks. This file is the
authoritative list of what is and is not there; anything not listed should be
assumed missing.

## Compute

| Area | Databricks | Lakeforge | Status |
| --- | --- | --- | --- |
| Engine | Spark + Photon (JVM/C++) | Forge (Rust, Arrow, DataFusion, delta-rs) | Full (own engine) |
| Distributed SQL | Yes | Stage DAG, hash/round-robin shuffle, N executors, task retry | Full |
| SQL dialect | Spark SQL | DataFusion SQL + Spark-compatible functions where DataFusion has them; `spark.*` conf aliases | Partial |
| Delta Lake | Full | Read, `INSERT`, `DELETE`, `UPDATE`, `TRUNCATE`, `CREATE TABLE [AS]`, `DROP TABLE` via delta-rs | Partial — no `MERGE INTO`, time travel (`VERSION AS OF`), `OPTIMIZE`, `VACUUM`, `ZORDER`, liquid clustering, CDF, deletion vectors |
| File formats | Delta, Parquet, CSV, JSON, Avro, ORC, text, XML | Delta, Parquet, CSV, JSON | Partial |
| Cloud storage | S3, GCS, ADLS | S3, GCS, ADLS (`object_store`), workload-identity auth | Full |
| Adaptive Query Execution | Yes | Settings accepted, not applied | Missing |
| Spark DataFrame API (PySpark) | Full | `spark.sql`, `spark.table`, `spark.read.{parquet,csv,json,delta,format().load}`, `spark.range`, `createDataFrame`, `DataFrame.{select,filter,orderBy,groupBy.agg/count/sum/avg,limit,collect,count,first,take,show,toPandas,createOrReplaceTempView,display}` — implemented as SQL pushdown | Partial |
| Structured Streaming | Yes | — | Missing |
| Scala / R / Java notebooks | Yes | — | Missing (Python, SQL, Markdown, shell only) |
| Cluster lifecycle (`clusters/*` 2.0 & 2.1) | Yes | create/edit/start/restart/resize/delete/permanent-delete/pin/unpin/events/list-node-types/list-zones/spark-versions | Full |
| Autoscaling | Yes | `autoscale.{min,max}_workers` persisted; cluster starts at `min_workers`; no load-based scaling | Partial |
| Auto-termination | Yes | Idle timeout enforced by background worker | Full |
| Instance pools, cluster policies, policy families | Yes | CRUD persisted; policies not enforced on create | API only |
| Libraries API | Yes | install/uninstall/cluster-status persisted; pip libraries installed into the kernel via `%pip`, jars/maven ignored | Partial |
| Global init scripts | Yes | CRUD persisted, not executed | API only |
| Photon | Yes | Forge is vectorised natively | n/a |
| Serverless compute | Yes | — | Missing (every workload needs a cluster or warehouse) |

## Workspace & notebooks

| Area | Status | Notes |
| --- | --- | --- |
| Workspace API (`list/get-status/mkdirs/import/export/delete`) | Partial | import: SOURCE, JUPYTER, RAW, AUTO; export: SOURCE, JUPYTER, HTML. No DBC or R_MARKDOWN. `workspace-files/{path}` for arbitrary files |
| Notebooks (cells, run, run-all, outputs, `%sql %md %sh %pip %fs %run`) | Full | Outputs persisted server-side, streamed over SSE |
| `display()` / rich HTML / matplotlib | Partial | tables and HTML; images via base64 PNG; no interactive charts |
| Widgets (`dbutils.widgets`) | Partial | text/dropdown/combobox/multiselect values, editable widget panel in the notebook UI; no widget type-specific controls |
| Command Execution API 1.2 | Full | contexts + commands, used by the UI and by `databricks-connect`-style tooling |
| Repos / Git folders | Partial | clone, pull, checkout branch, commit/push, status via `git` on the control plane; git-credentials CRUD; no sparse checkout, no GitHub App auth |
| Workspace search | Partial | name/path search over workspace objects |
| Files API (`/api/2.0/fs`) & Volumes | Full | backed by the object-store root |
| DBFS API | Full | put/read/add-block/close/list/mkdirs/move/copy/delete/get-status |

## SQL

| Area | Status | Notes |
| --- | --- | --- |
| Statement Execution API (`sql/statements`) | Full | INLINE + JSON_ARRAY, chunks, cancel, wait_timeout, parameters, catalog/schema |
| SQL warehouses (`sql/warehouses`, legacy `sql/endpoints`) | Partial | A warehouse is a Forge cluster with `warehouse` semantics; sizes map to worker counts; no serverless, no multi-cluster load balancing |
| Queries, alerts, data sources (`sql/queries`, `sql/alerts`, legacy `preview/sql/*`) | Full | alerts evaluated on demand (`lakeforge/sql/alerts/{id}/evaluate`) and from a job `sql_task.alert` |
| Query history | Full | every statement recorded with timings |
| Lakeview dashboards | API only | CRUD + publish persisted; UI renders a basic grid of query results |
| Visualizations | Missing | |

## Data governance (Unity Catalog)

| Area | Status | Notes |
| --- | --- | --- |
| Metastore / metastore summary / assignment | Full | one metastore per deployment |
| Catalogs, schemas, tables, table summaries, volumes, functions | Full | managed Delta tables materialised by Forge under the warehouse root; external tables by location |
| External locations, storage credentials, connections | Partial | persisted; `READ_FILES`/`WRITE_FILES` on the matching external location is required for path-based SQL (`delta.\`s3://…\``, `COPY INTO`, `LOCATION`); no cloud-IAM credential vending |
| Grants / effective permissions | Partial | full privilege set with ownership, inheritance and `ALL_PRIVILEGES`; **enforced** on every UC REST call and on every SQL statement (notebooks, jobs, SQL editor, Statement API all go through `execute_sql → prepare_sql`); `effective-permissions` reports `inherited_from`. Missing: `DENY`, `ON ALL TABLES IN SCHEMA`, nested groups, column-level privileges. Verified by `tests/smoke/uc-lakebase-smoke.sh` |
| SQL `GRANT` / `REVOKE` / `SHOW GRANTS` / `ALTER … OWNER TO` | Partial | Databricks grammar subset (multi-word privileges, explicit securable keywords, inferred type from name arity); `SHOW GRANTS TO principal` across the metastore not supported |
| Row filters and column masks | Partial | attached via Lakeforge REST routes and applied by query rewrite to every reader (owners/admins included); `ALTER TABLE … SET ROW FILTER` / `ALTER COLUMN … SET MASK` SQL syntax not parsed; statements `sqlparser` cannot parse are privilege-checked by text fallback but **not rewritten**, so policies are skipped for them (LF-003) |
| SQL UDFs, session functions | Partial | `CREATE [OR REPLACE] FUNCTION … RETURN <expr>` persisted in UC and inlined into statements; `current_user()`, `current_catalog()`, `current_schema()`, `is_account_group_member()`; no Python/table UDFs |
| Audit log | Partial | every mutating REST call + SQL statements (incl. denials) in `system.access.audit` and `/api/2.0/lakeforge/audit`; not the full Databricks event schema |
| Lineage (table, column) | Partial | captured from executed SQL; `lineage-tracking` API; column lineage limited to direct projections; notebook/job attribution not wired |
| System tables + `information_schema` | Partial | `system.{access,query,compute,lakeflow,billing,mlflow,serving,lakebase}` and `information_schema.*` queryable from SQL (materialised on demand as Delta); `billing.usage` synthesised; no streaming refresh |
| Tags, constraints, workspace bindings, artifact allowlists, temp table credentials | Partial | persisted with Databricks shapes; constraints not enforced by the engine; temp credentials return a Lakeforge Files-API token, not a cloud credential |
| Models in Unity Catalog | Missing | workspace MLflow registry only |
| Delta Sharing | Missing | securable types/grants exist, no shares API or protocol server |
| Lakehouse Federation | Missing | connections stored only; Forge has no PostgreSQL table provider |

Detailed per-capability status: [uc-lakebase-status.md](uc-lakebase-status.md).

## Lakebase (managed PostgreSQL)

| Area | Status | Notes |
| --- | --- | --- |
| Database instances API (`/api/2.0/database/instances`, roles, `findByUid`) | API only | lifecycle `STARTING→AVAILABLE`, capacities, parent refs are **metadata emulation**; no PostgreSQL server is started |
| Credentials (`/api/2.0/database/credentials`) | API only | short-lived Lakeforge token, **not a PostgreSQL password**; response carries `backend: emulated\|external` |
| Database catalogs in UC | API only | UC catalog of type `DATABASE_CATALOG` registered; not queryable from Forge |
| Synced tables | API only | validation + simulated `PROVISIONING→ONLINE_*` states; no Delta→PostgreSQL data movement |
| External PostgreSQL (`LAKEFORGE_LAKEBASE_POSTGRES_URL`) | Partial | reported in metadata; roles/databases not provisioned |
| Lakebase UI, SDK service, Helm/Terraform PostgreSQL | Missing | |

## Workflows

| Area | Status | Notes |
| --- | --- | --- |
| Jobs API 2.0 / 2.1 | Full | create/get/list/update/reset/delete, run-now, submit, runs list/get/get-output/cancel/cancel-all/delete/repair/export |
| Task types | Partial | `notebook_task`, `spark_python_task`, `sql_task` (query/file/alert/dashboard), `pipeline_task`, `run_job_task`, `condition_task`, `for_each_task`. **Not** `python_wheel_task`, `spark_jar_task`, `spark_submit_task`, `dbt_task` |
| DAG semantics (`depends_on`, `run_if`, retries, timeouts, max_concurrent_runs) | Full | |
| Schedules (Quartz cron, pause) | Full | |
| File-arrival / continuous triggers | Missing | |
| Job parameters, task values, notebook widgets | Full | `dbutils.jobs.taskValues` backed by the API |
| Job clusters | Partial | `job_clusters` + `new_cluster` create an ephemeral Forge cluster per run |
| Notifications / webhooks | Partial | notification-destinations CRUD; generic webhook fired on run terminal state; no email/Slack/PagerDuty |
| Queueing | Missing | |

## Delta Live Tables / Lakeflow pipelines

| Area | Status | Notes |
| --- | --- | --- |
| Pipelines API | Full | CRUD, updates, events, stop, reset, permissions |
| SQL `CREATE [OR REFRESH] [STREAMING] LIVE TABLE / MATERIALIZED VIEW / LIVE VIEW` | Partial | dependency graph resolved from `LIVE.x` references, executed in topological order as full refresh into the target schema |
| Python DLT (`@dlt.table`) | Missing | |
| Streaming / incremental processing, expectations, CDC (`APPLY CHANGES`) | Missing | streaming tables are treated as batch |
| Development vs production mode, continuous | API only | flags stored |

## Machine learning

| Area | Status | Notes |
| --- | --- | --- |
| MLflow Tracking (`/api/2.0/mlflow`, `/ajax-api/2.0/mlflow`) | Full | experiments, runs, metrics/params/tags, log-batch, log-inputs, log-model, metric history, search. The upstream `mlflow` client works with `MLFLOW_TRACKING_URI=databricks` + `DATABRICKS_HOST/TOKEN` |
| MLflow artifacts | Full | `mlflow-artifacts` REST proxy over the object-store root |
| Model Registry (workspace) | Full | registered models, versions, stages, aliases, tags, download URI |
| Models in Unity Catalog | Missing | workspace registry only |
| Feature Store | Missing | |
| Model Serving | Partial | endpoints, served entities/models, config updates, traffic split, tags, rate limits (stored), invocations served from a Python worker loading `mlflow.pyfunc` or pickled models; no GPU, no scale-to-zero, no external/foundation models, `ai-gateway` config stored only |
| AutoML, Mosaic AI, Vector Search, Genie | Missing | |

## Security & administration

| Area | Status | Notes |
| --- | --- | --- |
| Users / groups / service principals (SCIM 2.0, workspace + account paths) | Full | filter/attributes/PATCH ops |
| Personal access tokens (`token`, `token-management`, on-behalf-of) | Full | |
| Service-principal OAuth secrets | Partial | secret create; `/oidc/v1/token` client-credentials flow not implemented — use the secret as a bearer token |
| Permissions API (`permissions/{type}/{id}`, permission levels) | Partial | ACLs for clusters, jobs, notebooks/directories, pipelines, warehouses, repos, serving endpoints, tokens, experiments, models, policies, pools, dashboards persisted and returned; only ACL management itself is authorised (owner/admin). **Object operations are not gated by ACL** — every authenticated user can act on every object. |
| Secrets API (scopes, secrets, ACLs) | Full | values AES-GCM encrypted at rest with a key derived from the JWT secret; scope ACLs (MANAGE/WRITE/READ) enforced; no `[REDACTED]` masking in notebook output |
| Workspace conf, IP access lists, settings | API only | persisted; IP lists not enforced |
| SSO / SAML / OIDC login | Missing | local accounts only |
| Account console, multi-workspace, Unity Catalog cross-workspace | Missing | one workspace per deployment |
| Audit logs | Partial | mutating REST calls + SQL in `system.access.audit` (see Unity Catalog) |
| Compliance (HIPAA, PCI), customer-managed keys, private link | Missing | |

## Platform & deployment

| Area | Status | Notes |
| --- | --- | --- |
| Control plane + compute plane in your cloud | Full | one Helm chart, Terraform for AWS/GCP/Azure |
| One-click deploy | Full | `deploy/deploy.sh {local,kubernetes,aws,gcp,azure}` |
| Marketplace / partner connect | Missing | |
| Databricks CLI / SDK compatibility | Partial | Lakeforge ships its own SDK/CLI; the official clients work on the **Full** rows above but are not part of CI |

## Summary

Every Databricks *product area* has a home in Lakeforge and the daily
developer loop is real. The deepest gaps are: Structured Streaming, the full
PySpark DataFrame API, Delta `MERGE`/`OPTIMIZE`/`VACUUM`, serverless compute,
Python DLT with expectations/CDC, Models-in-UC, Delta Sharing and
federation, a real Lakebase data plane (PostgreSQL provisioning and synced
tables), Feature Store / Vector Search / Mosaic AI, SSO, account-level
multi-workspace administration, and **workspace object ACLs** (stored but not
enforced on cluster/job/notebook routes). Unity Catalog grants *are* enforced
on UC REST calls and on all SQL; see [uc-lakebase-status.md](uc-lakebase-status.md).
