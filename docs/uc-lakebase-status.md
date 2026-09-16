# Unity Catalog & Lakebase — implementation status

Authoritative status of the governance and Lakebase work on branch
`devin/1789466312-unity-catalog-lakebase`. Every row is one of:

- **Implemented** — code, wired into the request path, covered by
  `tests/smoke/uc-lakebase-smoke.sh` (50 checks, all passing) and/or unit tests.
- **Partial** — works for the documented path; named gaps remain.
- **Placeholder** — a module or constant exists, no behaviour.
- **Unimplemented** — nothing in the tree.

Evidence for "Implemented" is a smoke-check name (`smoke: …`) or a unit test
(`test: …`). Anything not listed here should be assumed unimplemented.

## 1. Privilege model (`uc/privileges.rs`)

| Capability | Status | Notes / evidence |
| --- | --- | --- |
| Securables: metastore, catalog, schema, table, volume, function, external location, storage credential, connection, share, recipient, provider | Implemented | `Securable`, `Securable::parse` accepts Databricks type strings (`table`, `materialized_view`, `registered_model`→function, …) |
| Privilege set (`ALL_PRIVILEGES`, `SELECT`, `MODIFY`, `USE_CATALOG`, `USE_SCHEMA`, `CREATE_*`, `READ_VOLUME`, `WRITE_VOLUME`, `EXECUTE`, `MANAGE`, `READ_FILES`, `WRITE_FILES`, `CREATE_EXTERNAL_*`, `BROWSE`, …) | Implemented | `normalize_privilege`, `is_known_privilege` |
| Ownership (owner has everything on the object and below) | Implemented | `Authorizer::require_owner`, `principal_matches_owner` (user, group membership, admin) |
| Inheritance metastore → catalog → schema → object | Implemented | `lineage_of`; `effective()` reports `inherited_from_type/name` |
| `ALL_PRIVILEGES` expansion | Implemented | treated as a wildcard in `has()` |
| Grants persisted per securable (`uc_grants` docs) | Implemented | `grants_doc_id(sec, full)`; `change_grants` add/remove |
| Grant/revoke requires ownership (metastore: admin only); principal must exist | Implemented | `uc_update_grants`; smoke: `grant`, `sql grant` |
| Effective permissions REST | Implemented | `GET /effective-permissions/{type}/{name}[?principal=]` |
| Group principals via SCIM groups; `account users` / `users` built-ins | Partial | membership resolved from `Principal.groups`; no nested groups |
| Deny rules, privilege on columns, `BROWSE` semantics beyond listing | Unimplemented | |

## 2. REST enforcement (`api/catalog.rs`, `api/catalog_ext.rs`)

| Capability | Status | Notes / evidence |
| --- | --- | --- |
| List/get filtered to what the principal can browse (`can_browse`) | Implemented | catalogs, schemas, tables, volumes, functions |
| Create requires `CREATE_*` (+ `USE_*` on parents) | Implemented | `uc_create_in_schema`, catalog create requires `CREATE_CATALOG` on metastore or admin |
| Update/delete/rename/owner-change require ownership or `MANAGE` | Implemented | `uc_patch`, `uc_delete_in_schema`; smoke: `sql alter owner` |
| System catalog objects read-only | Implemented | writes to `system.*` rejected in `uc_patch` and `authorize_sql`; smoke: `bob write system denied` |
| Tags (`entity-tag-assignments`) on securables and columns | Implemented | create/list/update/delete; requires ownership/`MANAGE` via `uc_mutate` (no separate `APPLY_TAG` check); smoke: `tag` |
| Constraints (`constraints`) | Implemented (metadata) | PK/FK/CHECK stored on the table doc, **not enforced by the engine**; smoke: `constraint` |
| Workspace bindings (`workspace-bindings`, `bindings`) | Implemented (metadata) | single workspace, so bindings are stored and returned, never filter |
| System schemas enable/disable | Implemented | `metastores/{id}/systemschemas[/{schema}]`; enabling makes `system.<schema>` visible; smoke: `enable system schema` |
| Temporary table credentials | Partial | checks `SELECT`/`MODIFY` + external-location path access, returns the table's storage URL plus a short-lived Lakeforge bearer token for the Files API (`lakeforge_credentials`); **no cloud STS/SAS credential vending**; smoke: `temp table credential` |
| Artifact allowlists | Implemented (metadata) | stored/returned; not consulted by the kernel |
| Row filter / column mask REST (`/api/2.0/lakeforge/unity-catalog/tables/{t}/row-filter`, `/column-masks[/{col}]`) | Implemented | Lakeforge-only routes (Databricks sets these via SQL `ALTER TABLE … SET ROW FILTER`, which is **not parsed** — see §4) |
| Audit log REST (`/api/2.0/lakeforge/audit`) | Implemented | filters: limit, service, action, user, since |
| Lineage REST (`/api/2.0/lineage-tracking/{table,column}-lineage`) | Implemented | GET and POST forms |
| Securable details / metastore stats (`/api/2.0/lakeforge/catalog/…`, `/metastore/stats`) | Implemented | used by the Catalog UI |
| External locations / storage credentials / connections as real access control | Unimplemented | persisted; `READ_FILES`/`WRITE_FILES` checked for path access in SQL (§3) but no cloud IAM materialisation |
| Models in UC REST (`/api/2.1/unity-catalog/models`, `model-versions`, aliases) | Placeholder | `uc/models.rs` holds one constant; no routes |
| Delta Sharing (shares/recipients/providers, open protocol server) | Placeholder | securable types and grant plumbing exist; no routes, no server |
| Lakehouse Federation (foreign catalogs backed by connections) | Unimplemented | connections are stored only; Forge has no PostgreSQL/JDBC table provider |

## 3. SQL enforcement (`uc/sqlguard.rs`, `uc/sqlauth.rs`, `api/sql.rs`)

Every statement from the Statement Execution API, notebooks (`spark.sql`,
`%sql`), jobs and the SQL editor goes through `AppState::execute_sql` →
`prepare_sql` before it reaches Forge.

| Capability | Status | Notes / evidence |
| --- | --- | --- |
| Statement analysis (reads, writes, creates, drops, paths, column edges) with `sqlparser` + text fallback | Implemented | `sqlguard::analyze`; test: `uc::sqlguard::tests::*` |
| Default catalog/schema resolution (`forge.sql.defaultCatalog/Schema` conf, `USE`) | Implemented | `sqlauth::defaults` |
| Reads require `SELECT` (+ `USE_CATALOG`/`USE_SCHEMA`) | Implemented | smoke: `bob select denied (no SELECT)`, `bob select granted table` |
| Writes require `MODIFY` | Implemented | smoke: `bob insert denied` |
| `CREATE TABLE/VIEW` → `CREATE_TABLE`; `CREATE SCHEMA` → `CREATE_SCHEMA`; `CREATE FUNCTION` → `CREATE_FUNCTION` | Implemented | `authorize_sql` |
| `DROP`/`ALTER` require ownership | Implemented | `owned` set in analysis |
| Path access (`SELECT * FROM delta.\`s3://…\``, `COPY INTO`, `LOCATION`) → `READ_FILES`/`WRITE_FILES` on the matching external location | Partial | longest-prefix match on external-location URLs; admins bypass; no path-level grants |
| System table reads require the schema enabled + `SELECT`; system writes always denied | Implemented | smoke: `system.access.audit`, `bob write system denied` |
| Objects unknown to the metastore (temp views, engine-only names) checked at catalog/schema level only | Implemented | lets Forge report its own "not found" errors |
| `GRANT … ON <securable> TO principal` | Implemented | `grant_sql.rs` (Databricks grammar: multi-word privileges, securable keywords, inferred type from name arity); smoke: `sql grant`, `sql grant visible via REST`; test: `grant_sql::tests::*` |
| `REVOKE … FROM principal` | Implemented | smoke: `sql revoke`, `sql revoke applied` |
| `SHOW GRANTS [principal] ON <securable>` → rows `Principal, ActionType, ObjectType, ObjectKey` incl. inherited | Implemented | smoke: `sql show grants`, `sql show grants inherited` |
| `ALTER <securable> [SET] OWNER TO principal` | Implemented | routes to `uc_patch(owner)`; smoke: `sql alter owner` |
| `GRANT … ON ALL TABLES IN SCHEMA`, `SHOW GRANTS TO principal` (all objects), `DENY` | Unimplemented | parser rejects with `PARSE_SYNTAX_ERROR` |
| Denied statements audited (`unityCatalog.sqlStatementDenied`, 403) | Implemented | `api/sql.rs` |
| Statements with a `MetastoreOp` (UDF DDL, grants) never reach Forge | Implemented | `apply_metastore_op` |

## 4. Policies: row filters, column masks, SQL UDFs, session functions

| Capability | Status | Notes / evidence |
| --- | --- | --- |
| SQL UDF create/drop (`CREATE [OR REPLACE] FUNCTION … RETURNS t RETURN expr`, `DROP FUNCTION [IF EXISTS]`) persisted as UC functions | Implemented | `sqlauth::metastore_op`; smoke: `create mask fn` (idempotent via OR REPLACE) |
| SQL UDF invocation by **inlining** the body at prepare time | Implemented | `sqlguard::inline_functions`; scalar SQL bodies only |
| Python UDFs, table-valued UDFs, UDF `EXECUTE` privilege checks | Unimplemented / Partial | `EXECUTE` is checked when the function is a policy function; inlined UDFs are not privilege-checked separately |
| Session functions `current_user()`, `current_catalog()`, `current_schema()`, `is_account_group_member(g)` | Implemented | rewritten to literals per principal; smoke: `session fn` |
| `is_member(g)`, `current_metastore()`, `session_user()` | Unimplemented | |
| Row filter attached via REST → `WHERE <fn(cols)>` injected on every read of the table | Implemented | `TablePolicy.row_filter`; policy function must be a SQL UDF the caller can `EXECUTE`; **no smoke check yet** (issue #3) |
| Column mask via REST → column expression replaced by `<fn(col, using…)>` | Implemented | smoke: `set column mask`, `masked select (admin sees clear)`, `bob select masked` |
| Policies apply to everyone including owners and admins (Databricks semantics); exemptions are expressed inside the policy function (e.g. `is_account_group_member('admins')`) | Implemented | smoke: `masked select (admin sees clear)` relies on that branch in the mask UDF |
| `ALTER TABLE … SET ROW FILTER / ALTER COLUMN … SET MASK` SQL syntax | Unimplemented | REST routes only |
| Policies on views, `INSERT … SELECT` through masked columns, `MERGE` | Partial / Unimplemented | masks apply to any read; `MERGE` is not supported by Forge at all |

## 5. Audit, query history, lineage

| Capability | Status | Notes / evidence |
| --- | --- | --- |
| Audit middleware on every mutating REST call (`service`, `action`, `params`, `status`, user, IP, request id) | Implemented | `uc/audit.rs::audit_middleware`; GETs not audited except denials |
| Explicit audit events from SQL path (`sqlStatement`, `sqlStatementDenied`) | Implemented | smoke: `audit events` |
| Audit retention cap (`MAX_EVENTS = 200_000`, oldest trimmed) | Implemented | |
| Audit exposed as `system.access.audit` | Implemented | smoke: `system.access.audit` |
| Query history (`sql/history/queries`) with user, warehouse, duration, status, rows | Implemented | pre-existing; now also records `statement_type`, read/written tables — **basic**, no per-stage metrics |
| Table lineage captured per statement (`reads × writes`, incl. `CREATE TABLE AS`, `INSERT … SELECT`) | Implemented | `uc/lineage.rs::record_lineage`; smoke: `table lineage` |
| Column lineage from projection/column edges | Partial | direct `SELECT a AS b` / `INSERT (cols) SELECT cols` edges only; expressions, joins through aliases and CTEs partially resolved |
| Lineage attributed to notebooks/jobs (`entity_type`, `entity_id`, `entity_run_id`) | Partial | `LineageContext::from_conf` reads `lakeforge.entity_*` statement conf keys, but the notebook kernel and job runner do **not** set them yet (issue #7) |
| Lineage retention cap and dedupe | Implemented | `MAX_ROWS = 200_000` |
| Lineage UI tab in Catalog | Unimplemented | REST only (issue #20) |

## 6. System tables & `information_schema` (`uc/system_tables.rs`)

| Capability | Status | Notes / evidence |
| --- | --- | --- |
| `system` catalog with schemas `information_schema, access, query, compute, lakeflow, billing, mlflow, serving, lakebase` | Implemented | virtual docs; `SHOW SCHEMAS IN system` |
| Tables: `access.{audit,table_lineage,column_lineage,workspaces_latest}`, `query.history`, `compute.{clusters,warehouses,node_types,warehouse_events}`, `lakeflow.{jobs,job_tasks,job_run_timeline,job_task_run_timeline,pipelines,pipeline_update_timeline}`, `billing.{usage,list_prices}`, `mlflow.{experiments_latest,runs_latest}`, `serving.endpoint_usage`, `lakebase.{instances,synced_tables}` | Implemented (metadata + rows) | rows produced from control-plane documents |
| `information_schema.*` (catalogs, schemata, tables, views, columns, volumes, routines, parameters, *_privileges, *_tags, table_constraints, row_filters, column_masks, connections, external_locations, storage_credentials, metastores, models, model_versions, model_version_aliases, shares, recipients, providers) | Partial | all tables exist with Databricks column names; rows for models/shares/recipients/providers are empty because those features are placeholders |
| Queryable from SQL: on-demand materialisation as Delta tables under the warehouse before a statement that references `system.*` | Implemented | `refresh_system_tables_for`; smoke: `information_schema`, `system.query.history`, `system.lakebase.instances` |
| Full refresh after cluster start | Implemented | `refresh_all_system_tables` |
| Incremental / streaming refresh, `billing.usage` with real DBU-like metering | Unimplemented | `billing.usage` rows are synthesised from cluster uptime |
| REST view of system tables (`/api/2.0/lakeforge/system-tables[/{schema}/{name}]`) | Implemented | |

## 7. Models in Unity Catalog

| Capability | Status |
| --- | --- |
| Registered models with 3-level names, versions, aliases, tags, comments, `CREATE_MODEL` privilege, `information_schema.models` rows | **Placeholder** — `uc/models.rs` defines `KIND_MODEL_VERSION` only; MLflow registry remains workspace-scoped (`api/mlflow.rs`) |

## 8. Lakebase (`api/lakebase.rs`)

Databricks-shaped `/api/2.0/database/*` control-plane APIs. **Default backend
is `emulated`: everything is metadata in the control plane; there is no
PostgreSQL server, no DNS endpoint that accepts connections, and credentials
are not PostgreSQL passwords.** Setting `LAKEFORGE_LAKEBASE_POSTGRES_URL`
switches the reported backend to `external` and exposes that host in instance
metadata, but the code still does **not** create roles/databases there or move
data. The backend is reported by `GET /api/2.0/lakeforge/lakebase/backend` and
in every credential response so clients cannot mistake emulation for a real
database.

| Capability | Status | Notes / evidence |
| --- | --- | --- |
| Instances CRUD (`instances`, `instances/{name}`, `instances:findByUid`), capacities `CU_1..CU_8`, `PG_VERSION_16`, `read_write_dns`, `uid`, `state` | Implemented (metadata) | smoke: `lb create instance`, `lb bad capacity`, `lb findByUid`, `lb patch` |
| Lifecycle `STARTING → AVAILABLE` (after `STARTING_SECS`), `UPDATING → AVAILABLE`, `stopped → STOPPED`, `DELETING` | Implemented (metadata) | background settle on read; smoke: `lb available` |
| Delete blocked while dependents (catalogs, synced tables) exist unless `force`; `purge` removes immediately | Implemented | smoke: `lb delete blocked`, `lb delete force`, `lb gone` |
| Child/branch instances (`parent_instance_ref`, `effective_*`) | Partial | fields stored and echoed; no data branching |
| Roles (`instances/{name}/roles`) with `membership_role`, `identity_type`, attributes | Implemented (metadata) | smoke: `lb role` |
| Credentials (`POST database/credentials`) — short-lived token, `expiration_time`, `request_id`, `backend` | Implemented (metadata) | token is a Lakeforge JWT scoped to the instance; **not usable as a PostgreSQL password**; smoke: `lb credential` |
| Database catalogs (`database/catalogs`) registered as UC catalogs of type `DATABASE_CATALOG` (`database_instance_name`, `database_name`) | Implemented (metadata) | smoke: `lb catalog`, `lb catalog in UC` |
| Synced tables (`database/synced_tables`): validation (3-level name, source table exists + `SELECT`, PK columns exist, timeseries key, scheduling policy, `existing_pipeline_id` xor `new_pipeline_spec`), state `PROVISIONING → ONLINE_*` by policy | Implemented (metadata) | smoke: `lb synced table`, `lb synced bad pk`, `lb synced online` |
| Synced-table data movement (Delta → PostgreSQL), pipeline linkage, continuous mode | Unimplemented | states are simulated |
| Database tables (`database/tables`) registration | Implemented (metadata) | |
| Permissions: admins or instance owner manage; `USE_CATALOG` etc. on database catalogs via UC | Implemented | |
| `system.lakebase.{instances,synced_tables}` | Implemented | smoke: `system.lakebase.instances` |
| Real PostgreSQL provisioning (embedded server locally, StatefulSet on Kubernetes, managed service in cloud) | Unimplemented | design in issue #17 |
| Querying a database catalog from Forge SQL (federation) | Unimplemented | issue #18 |
| Lakebase UI page, SDK `database` service | Unimplemented | issues #19, #21 |

## 9. Cross-cutting

| Capability | Status |
| --- | --- |
| Python SDK coverage of new endpoints (`grants` exists; no `lineage`, `audit`, `system_tables`, `database`, `row_filters`, `tags`, `constraints`) | Partial |
| Catalog UI: shows grants (read-only), details; no grant editor, lineage, tags, policies tabs | Partial |
| Deployment: Helm/Terraform know nothing about Lakebase PostgreSQL | Unimplemented |
| Docs/parity updated for the above | Implemented (this branch) |

## What "parity" means here

Databricks Unity Catalog and Lakebase are managed cloud services. Lakeforge
implements the **control-plane contract and the SQL-side enforcement
semantics** so that the same API calls, SQL statements and governance model
work; it does **not** yet implement the data-plane pieces (cloud IAM
credential vending, managed PostgreSQL, synced-table pipelines, Delta Sharing
server, federation connectors). Do not describe the current state as feature
parity; describe it as "UC governance semantics enforced; Lakebase control
plane emulated".
