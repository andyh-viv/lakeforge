# REST API surface

Every route served by `lakeforge-api`, grouped by the module that owns it
(`crates/lakeforge-api/src/api/<module>.rs`). Paths are Databricks-compatible
unless prefixed with `/api/2.0/lakeforge/`, which marks Lakeforge-only
extensions (no Databricks equivalent, or a simplified shape used by the UI).

Notation: `{x}` path parameter, `{*path}` wildcard. Where a Databricks API
has both `2.0` and `2.1` versions both are mounted. All routes except
`/health`, `/api/2.0/lakeforge/login` and static UI files require
`Authorization: Bearer <token>` (PAT or session JWT). Mutating calls are
recorded by the audit middleware (`uc/audit.rs`).

For the *behaviour* of the Unity Catalog and Lakebase routes see
[uc-lakebase-status.md](uc-lakebase-status.md); for parity gaps see
[parity.md](parity.md).

## Authentication & identity — `tokens.rs`, `scim.rs`

| Method | Path | Notes |
| --- | --- | --- |
| POST | `/api/2.0/lakeforge/login` | `{username,password}` → session JWT (Lakeforge) |
| GET | `/api/2.0/lakeforge/me` | current principal incl. groups, admin flag |
| POST | `/api/2.0/lakeforge/password` | change own password |
| GET | `/api/2.0/preview/scim/v2/Me` | SCIM Me |
| POST/GET/DELETE | `/api/2.0/token/{create,list,delete}` | personal access tokens |
| GET/POST/DELETE | `/api/2.0/token-management/tokens[/{id}]`, `/on-behalf-of/tokens` | admin token management |
| DELETE | `/api/2.0/lakeforge/tokens/{id}` | |
| GET/POST/PUT/PATCH/DELETE | `/api/2.0/preview/scim/v2/{Users,Groups,ServicePrincipals}[/{id}]` | SCIM 2.0 (also under `/api/2.0/account/scim/v2`) |
| POST | `/api/2.0/accounts/servicePrincipals/{id}/credentials/secrets` | SP OAuth secret (returns client id/secret usable as PAT) |

## Compute — `clusters.rs`, `misc.rs`

| Method | Path | Notes |
| --- | --- | --- |
| POST | `/api/2.{0,1}/clusters/{create,edit,start,restart,resize,delete,permanent-delete,pin,unpin}` | backed by `lakeforge-cluster-manager` (local process or Kubernetes) |
| GET | `/api/2.{0,1}/clusters/{get,list,events,list-node-types,list-zones,spark-versions}` | |
| GET | `/api/2.0/lakeforge/clusters/forge-status` | Forge driver/executor health for a cluster |
| POST/GET | `/api/2.0/instance-pools/{create,edit,delete,get,list}` | metadata only |
| POST/GET | `/api/2.0/policies/clusters/{create,edit,delete,get,list}`, `/api/2.0/policy-families[/{id}]` | metadata only |
| POST/GET | `/api/2.0/libraries/{install,uninstall,cluster-status,all-cluster-statuses}` | PyPI installs into the cluster venv |
| GET/POST/PATCH/DELETE | `/api/2.0/global-init-scripts[/{id}]` | stored, run by local backend |

## Command execution & notebooks — `commands.rs`, `notebooks.rs`

| Method | Path | Notes |
| --- | --- | --- |
| POST/GET | `/api/1.2/contexts/{create,status,destroy}`, `/api/1.2/commands/{execute,status,cancel}` | Databricks Command Execution 1.2 |
| GET/DELETE | `/api/2.0/lakeforge/contexts`, `/api/2.0/lakeforge/commands/{id}[/events]` | UI streaming of cell output |
| GET/PUT | `/api/2.0/lakeforge/task-values` | `dbutils.jobs.taskValues` |
| GET/PUT | `/api/2.0/lakeforge/notebooks`, `/notebooks/outputs` | notebook cells + cached outputs |
| POST | `/api/2.0/lakeforge/notebooks/run` | run all cells on a cluster (used by jobs) |

## Workspace, files, repos — `workspace.rs`, `dbfs.rs`, `repos.rs`

| Method | Path | Notes |
| --- | --- | --- |
| GET/POST | `/api/2.0/workspace/{list,get-status,mkdirs,import,export,delete}` | |
| GET/PUT/DELETE | `/api/2.0/workspace-files/{*path}` | raw file content |
| POST/GET | `/api/2.0/lakeforge/workspace/{move,search}` | |
| POST/GET | `/api/2.0/dbfs/{put,read,list,get-status,mkdirs,delete,move,copy,create,add-block,close}` | |
| GET/PUT/DELETE/HEAD | `/api/2.0/fs/files/{*path}`, `/api/2.0/fs/directories/{*path}` | Files API (volumes, temp-credential downloads) |
| GET/POST/PATCH/DELETE | `/api/2.0/repos[/{id}]`, `/repos/{id}/permissions` | git clone/pull via `git` binary |
| GET/POST | `/api/2.0/lakeforge/repos/by-path`, `/repos/{id}/{branches,status,commit}` | |
| GET/POST/PATCH/DELETE | `/api/2.0/git-credentials[/{id}]` | |

## Jobs — `jobs.rs`

| Method | Path | Notes |
| --- | --- | --- |
| POST | `/api/2.{0,1}/jobs/{create,reset,update,delete,run-now}` | notebook, SQL, Python-wheel-less `spark_python_task` (file), pipeline tasks; `depends_on` DAG; cron schedules |
| GET | `/api/2.{0,1}/jobs/{get,list}` | |
| POST/GET | `/api/2.{0,1}/jobs/runs/{submit,get,list,cancel,cancel-all,delete,repair,export,get-output}` | |

## SQL — `sql.rs`

| Method | Path | Notes |
| --- | --- | --- |
| POST/GET | `/api/2.0/sql/statements[/]`, `/statements/{id}`, `/statements/{id}/cancel`, `/statements/{id}/result/chunks/{n}` | Statement Execution API; one statement per request; all statements pass through UC authorization |
| GET/POST/PATCH/DELETE | `/api/2.0/sql/warehouses[/{id}]`, `/warehouses/{id}/{edit,start,stop}`, `/api/2.0/sql/endpoints…` (legacy alias) | warehouse = Forge cluster with a SQL profile |
| GET/PUT | `/api/2.0/sql/config/{warehouses,endpoints}` | |
| GET | `/api/2.0/sql/history/queries` | query history (also `system.query.history`) |
| GET/POST/PATCH/DELETE | `/api/2.0/sql/queries[/{id}]`, `/api/2.0/preview/sql/queries…` | saved queries |
| GET/POST/PATCH/DELETE | `/api/2.0/sql/alerts[/{id}]`, `/api/2.0/preview/sql/alerts…` | alerts; `POST /api/2.0/lakeforge/sql/alerts/{id}/evaluate` |
| GET | `/api/2.0/preview/sql/data_sources` | |
| GET/POST/PATCH/DELETE | `/api/2.0/lakeview/dashboards[/{id}]`, `/dashboards/{id}/published` | Lakeview dashboards (serialized_dashboard JSON) |

## Unity Catalog — `catalog.rs` (mounted under `/api/2.1/unity-catalog` and `/api/2.0/unity-catalog`)

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/metastores`, `/metastores/{id}`, `/metastore_summary`, `/current-metastore-assignment`, `/metastores/{id}/workspaces`, `/workspaces/{ws}/metastore` | single metastore `lakeforge-metastore` |
| GET/POST/PATCH/DELETE | `/catalogs[/{name}]` | `CREATE_CATALOG` on metastore or admin |
| GET/POST/PATCH/DELETE | `/schemas[/{full_name}]` | |
| GET/POST/PATCH/DELETE | `/tables[/{full_name}]`, `/tables/{full_name}/exists`, `/table-summaries` | tables mirror engine DDL (`observe_ddl`) |
| GET/POST/PATCH/DELETE | `/volumes[/{full_name}]` | managed + external volumes |
| GET/POST/PATCH/DELETE | `/functions[/{full_name}]` | SQL UDFs (also created via SQL) |
| GET/POST/PATCH/DELETE | `/external-locations[/{name}]`, `/storage-credentials[/{name}]`, `/connections[/{name}]` | |
| GET/PATCH | `/permissions/{securable_type}/{full_name}` | grants; PATCH `{changes:[{principal,add,remove}]}` |
| GET | `/effective-permissions/{securable_type}/{full_name}[?principal=]` | includes `inherited_from_type/name` |
| GET | `/api/2.0/lakeforge/catalog/browse`, `/api/2.0/lakeforge/catalog/grants/{type}/{full_name}` | UI helpers |

## Unity Catalog extensions — `catalog_ext.rs`

| Method | Path | Notes |
| --- | --- | --- |
| GET/PUT/DELETE | `/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas[/{schema_name}]` | enable/disable system schemas |
| GET/POST/PUT/DELETE | `/api/2.1/unity-catalog/entity-tag-assignments[/{entity_type}/{entity_name}/tags/{tag_key}]` | tags on securables and columns |
| GET/POST/DELETE | `/api/2.1/unity-catalog/constraints[/{full_name}]` | PK/FK/CHECK metadata |
| GET/PATCH | `/api/2.1/unity-catalog/workspace-bindings/catalogs/{name}`, `/bindings/{securable_type}/{securable_name}` | |
| GET/PUT | `/api/2.1/unity-catalog/artifact-allowlists/{artifact_type}` | |
| POST | `/api/2.0/unity-catalog/temporary-table-credentials` | `{table_id, operation}` → storage URL + Lakeforge bearer token (no cloud STS) |
| GET/POST | `/api/2.0/lineage-tracking/table-lineage`, `/column-lineage` | |
| GET | `/api/2.0/lakeforge/audit` | `?limit&service&action&user&since` |
| GET | `/api/2.0/lakeforge/system-tables[/{schema}/{name}]` | schema + rows of a system table without SQL |
| PUT/DELETE | `/api/2.0/lakeforge/unity-catalog/tables/{table}/row-filter` | `{function_name,input_columns}` |
| PUT/DELETE | `/api/2.0/lakeforge/unity-catalog/tables/{table}/column-masks[/{column}]` | `{column,function_name,using_columns}` |
| GET | `/api/2.0/lakeforge/catalog/{securable_type}/{full_name}`, `/api/2.0/lakeforge/metastore/stats` | UI detail panes |

**Not present (Databricks has them):** `/models`, `/model-versions`,
`/shares`, `/recipients`, `/providers`, `/online-tables`, `/quality-monitors`,
`/credentials` (service credentials), `/resource-quotas`, `/lakehouse-monitors`.

## Lakebase — `lakebase.rs`

| Method | Path | Notes |
| --- | --- | --- |
| GET/POST | `/api/2.0/database/instances` | `{name, capacity, stopped, parent_instance_ref, …}` |
| GET/PATCH/DELETE | `/api/2.0/database/instances/{name}` | `?force=true`, `?purge=true` on delete |
| GET | `/api/2.0/database/instances:findByUid?uid=` | |
| GET/POST | `/api/2.0/database/instances/{name}/roles`; GET/DELETE `/roles/{role}` | |
| POST | `/api/2.0/database/credentials` | `{instance_names:[…], request_id}` → `{token, expiration_time, backend}` — **not a PostgreSQL password in `emulated` mode** |
| GET/POST/DELETE | `/api/2.0/database/catalogs[/{name}]` | creates a UC catalog of type `DATABASE_CATALOG` |
| GET/POST/DELETE | `/api/2.0/database/synced_tables[/{name}]` | `{spec:{source_table_full_name, primary_key_columns, scheduling_policy, …}}` |
| POST/GET/DELETE | `/api/2.0/database/tables[/{name}]` | |
| GET | `/api/2.0/lakeforge/lakebase/backend` | `{backend: "emulated" \| "external", postgres_host?}` |

## Secrets — `secrets.rs`

`/api/2.0/secrets/scopes/{create,list,delete}`, `/secrets/{put,get,list,delete}`,
`/secrets/acls/{put,get,list,delete}`. Values encrypted at rest with the JWT secret-derived key.

## Permissions (workspace objects) — `permissions.rs`

`GET/PUT/PATCH /api/2.0/permissions/{type}/{id}`, `/permissionLevels`,
`/api/2.0/permissions/{a}/{b}/{id}` (e.g. `sql/warehouses`), `/api/2.0/preview/permissions/{type}/{id}`.
Stored ACLs for clusters, jobs, notebooks, directories, warehouses, pipelines, serving endpoints, repos,
with Databricks level names per object type. **Enforcement gap:** `check_permission` (admin/owner pass;
empty ACL = open; otherwise level rank) exists but is only used by the permissions routes themselves —
cluster/job/notebook/warehouse handlers do **not** call it yet (see docs/issues.md, cross-cutting).

## MLflow — `mlflow.rs` (mounted at `/api/2.0/mlflow`, `/api/2.0/preview/mlflow`, `/ajax-api/2.0/mlflow`)

`experiments/{create,get,get-by-name,search,list,update,delete,restore,set-experiment-tag}`,
`runs/{create,get,search,update,delete,restore,log-metric,log-parameter,log-batch,log-inputs,log-model,set-tag,delete-tag}`,
`metrics/get-history`, `artifacts/list`,
`registered-models/{create,get,list,search,update,rename,delete,get-latest-versions,set-tag,delete-tag,alias}`,
`model-versions/{create,get,search,update,delete,transition-stage,set-tag,delete-tag,get-download-uri}`,
`databricks/registered-models/get`, `databricks/model-versions/get-download-uri`;
artifact proxy `GET/PUT /api/2.0/mlflow-artifacts/artifacts[/{*path}]`.
Registry is **workspace-scoped**; Models in UC are not implemented.

## Pipelines (Lakeflow/DLT) — `pipelines.rs`

`GET/POST/PUT/DELETE /api/2.0/pipelines[/{id}]`, `/pipelines/{id}/{updates,updates/{update_id},events,reset,stop,permissions}`.
Pipelines execute a notebook's SQL `CREATE [LIVE|STREAMING] TABLE` statements in dependency order as batch
materialisations; no streaming.

## Model Serving — `serving.rs`

`GET/POST/PUT/PATCH/DELETE /api/2.0/serving-endpoints[/{name}]`, `/{name}/{config,tags,rate-limits,ai-gateway,permissions,openapi,metrics}`,
`/served-entities/state`, `/served-{entities,models}/{entity}/{logs,build-logs}`,
`POST /api/2.0/serving-endpoints/{name}/invocations` and `/serving-endpoints/{name}/invocations`.
Serving runs MLflow pyfunc models in a Python subprocess per endpoint; no autoscaling, no GPU.

## Settings & misc — `misc.rs`

`/api/2.0/workspace-conf`, `/api/2.0/settings/types/{name}/names/default`, `/api/2.0/lakeforge/settings/{name}`,
`/api/2.0/ip-access-lists[/{id}]` (stored, not enforced), `/api/2.0/notification-destinations[/{id}]`,
`/api/2.0/lakeforge/info`, `/api/2.0/lakeforge/workspace-status`, `/health`.

## Conventions

- Errors: `{ "error_code": "...", "message": "..." }` with Databricks codes
  (`RESOURCE_DOES_NOT_EXIST`, `PERMISSION_DENIED`, `INVALID_PARAMETER_VALUE`,
  `RESOURCE_ALREADY_EXISTS`, `RESOURCE_CONFLICT`, `INTERNAL_ERROR`). See
  `ApiError` in `crates/lakeforge-api/src/error.rs`.
- Lists: Databricks list envelopes (`{catalogs:[…]}`, `{jobs:[…], has_more}`,
  `{Resources:[…]}` for SCIM). Pagination tokens are accepted but most lists return everything.
- Timestamps: epoch milliseconds (`created_at`, `updated_at`).
- Principal identifiers: user emails, group display names, service-principal
  application ids; `Principal.groups` includes `users`/`account users` and
  `admins` when applicable.
