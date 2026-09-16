# Unity Catalog Audit, Query History and Lineage Specification

## Purpose

Databricks records every API call in `system.access.audit`, every SQL
statement in `system.query.history`, and data flow between tables/columns in
`system.access.table_lineage` / `column_lineage` with a REST lineage API.
Lakeforge produces the same records from two capture points: an HTTP
middleware (audit) and the SQL choke point (history, lineage, DDL
observation).

## Scope

In scope: audit event capture, classification and redaction; query history
records; table and column lineage capture, storage and REST; retention.
Out of scope: exposing these as SQL tables (`unity-catalog-system-tables`),
UI (LF-021).

## Data Model

`AuditEvent` (`kind = uc_audit`, capped at `MAX_EVENTS = 200_000`, oldest
evicted):

```
event_id, event_time (ms), workspace_id, service_name, action_name,
request_id, request_params: map, user_identity { email, subject_name },
source_ip_address?, user_agent?, session_id?,
response { status_code, error_message?, result? }, audit_level = "WORKSPACE_LEVEL"
```

`service_name`/`action_name` are derived from method + path by
`uc::audit::classify` (e.g. `PATCH /api/2.1/unity-catalog/permissions/…` →
`("unityCatalog", "updatePermissions")`, `POST /api/2.0/sql/statements` →
`("sql", "executeStatement")`, `POST /api/2.0/database/instances` →
`("database", "createDatabaseInstance")`). Explicit calls to
`AppState::audit(...)` from handlers add domain events (e.g. SQL metastore
ops) with their own params.

Query history record (`kind = sql_history`): `statement_id`, `status`,
`statement_text`, `executed_by`, `warehouse_id`/`cluster_id`, `start_time`,
`end_time`, `duration_ms`, `rows_produced`, `error_message`, `statement_type`,
`entity_type`/`entity_id`/`entity_run_id` (from `LineageContext`).

`TableLineage` (`kind = uc_table_lineage`) and `ColumnLineage`
(`kind = uc_column_lineage`), capped at `MAX_ROWS = 200_000`, with the
Databricks column set (`source_table_full_name`, `source_table_catalog/
schema/name`, `source_path`, `source_type`, `target_*`, `entity_type`,
`entity_id`, `entity_run_id`, `statement_id`, `created_by`, `event_time`,
`event_date`, `event_id`; column lineage adds `source_column_name`,
`target_column_name`).

`LineageContext` is read from statement conf keys `lakeforge.entity_type`
(`NOTEBOOK` | `JOB` | `PIPELINE` | `DASHBOARD` | `SQL_EDITOR`),
`lakeforge.entity_id`, `lakeforge.entity_run_id`.

## API

| Route | Behaviour |
| --- | --- |
| `GET /api/2.0/lakeforge/audit?limit&service_name&action_name&user_name&since` | newest-first audit events (admins: all; others: own) |
| `GET /api/2.0/sql/history/queries` | Databricks-shaped history list with filters |
| `GET/POST /api/2.0/lineage-tracking/table-lineage` | `{ table_name, include_entity_lineage? }` → `{ upstreams: [{tableInfo|fileInfo, queryInfos}], downstreams: [{tableInfo|notebookInfos|jobInfos, queryInfos}] }` |
| `GET/POST /api/2.0/lineage-tracking/column-lineage` | `{ table_name, column_name }` → `{ upstream_cols, downstream_cols }` |

## Requirements

### Requirement: Every mutating API call is audited
The audit middleware SHALL record one event for each non-`GET`/`HEAD`/`OPTIONS`
request that `classify` maps to a service (and for any request answered
`403`), after the response is produced, with the final status code, the
caller identity (or `anonymous`), and request params containing `path`,
`method`, `query` (if any) and the redacted top-level fields of a JSON body
(bodies up to 4 MiB). Statement execution, command execution, notebook cell
execution, DBFS block uploads and heartbeats SHALL be skipped by the
middleware because the SQL/commands layers record richer events themselves.

#### Scenario: Grant is audited
- **WHEN** an admin `PATCH`es `/api/2.1/unity-catalog/permissions/table/main.s.t`
- **THEN** an event with `service_name = unityCatalog`,
  `action_name = updatePermissions`, `request_params.securable_type = table`,
  `response.status_code = 200` exists within one request.

#### Scenario: Failed call is audited
- **WHEN** a non-admin calls `DELETE /api/2.1/unity-catalog/catalogs/main`
- **THEN** an event with `status_code = 403` and `error_message` is recorded.

### Requirement: Secrets never reach the audit log
Request params SHALL be redacted: any key equal (case-insensitively) to one
of `SECRET_KEYS` (`password`, `new_password`, `string_value`, `bytes_value`,
`token`, `token_value`, `secret`, `client_secret`, `private_key`,
`personal_access_token`, `aws_secret_access_key`) SHALL be stored as
`"***"`, recursively through nested objects.

#### Scenario: Secret put
- **WHEN** `POST /api/2.0/secrets/put` with `string_value = "s3cr3t"`
- **THEN** the event's `request_params.string_value == "***"`.

### Requirement: Audit reads are scoped and bounded
`GET /api/2.0/lakeforge/audit` SHALL return at most `limit` (default 200,
max 5000) events newest-first; admins see all events, other principals see
only events whose `user_identity.email` is their own. `system.access.audit`
SHALL be readable by admins or principals with `SELECT` on `system.access`.

#### Scenario: Non-admin sees only own events
- **WHEN** `bob` calls `GET /api/2.0/lakeforge/audit?user_name=alice`
- **THEN** the response contains only events performed by `bob`.

### Requirement: Query history is complete
Every statement passing `execute_sql` SHALL create a history record before
execution (`status = RUNNING`) and update it on completion (`FINISHED` |
`FAILED` | `CANCELED`) with duration, rows, error, `statement_type` from the
analysis, and entity attribution when present in conf.

#### Scenario: Denied statement is still recorded
- **WHEN** `bob` runs a statement that fails authorization
- **THEN** a history record with `status = FAILED` and
  `error_message` containing `PERMISSION_DENIED` exists; nothing reached
  Forge.

### Requirement: Table lineage is captured from writes
For each successful statement whose analysis has writes (`INSERT`, `CREATE
TABLE AS SELECT`, `CREATE VIEW`, `MERGE`, `UPDATE … FROM`, `COPY INTO`), the
system SHALL record one `TableLineage` row per (source, target) pair where
source is each read table or path and target is each written table or
path, with `source_type`/`target_type` from the table types
(`TABLE`/`VIEW`/`PATH`/`STREAMING_TABLE`).

#### Scenario: CTAS lineage
- **WHEN** `CREATE TABLE main.s.summary AS SELECT region, count(*) FROM
  main.s.users GROUP BY region`
- **THEN** `GET /lineage-tracking/table-lineage?table_name=main.s.summary`
  lists `main.s.users` in `upstreams[].tableInfo` and
  `…?table_name=main.s.users` lists `main.s.summary` in `downstreams`.

#### Scenario: Read-only statements do not create lineage
- **WHEN** `SELECT * FROM main.s.users`
- **THEN** no lineage rows are added (history only).

### Requirement: Column lineage for direct projections
For `CTAS`/`INSERT … SELECT`/`CREATE VIEW` whose select list is column
references or aliases of column references, the system SHALL record
`ColumnLineage` rows mapping each source column to the target column by
position (or alias name).

#### Scenario: Aliased projection
- **WHEN** `CREATE TABLE main.s.t2 AS SELECT id AS user_id, email FROM
  main.s.users`
- **THEN** `GET /lineage-tracking/column-lineage?table_name=main.s.t2&column_name=user_id`
  returns `upstream_cols = [{ name: id, table_name: users, … }]`.

### Requirement: Entity attribution
When conf carries `lakeforge.entity_type/entity_id/entity_run_id`, lineage
and history rows SHALL include them, and the lineage REST response SHALL
surface `notebookInfos`/`jobInfos` for `NOTEBOOK`/`JOB` entities.

#### Scenario: Job-attributed lineage
- **GIVEN** a job task submits SQL with `lakeforge.entity_type = JOB`,
  `entity_id = 42`, `entity_run_id = 7`
- **WHEN** the statement writes `main.s.out`
- **THEN** the table-lineage response contains `jobInfos: [{ job_id: "42",
  run_id: "7" }]`.

### Requirement: Retention
Audit and lineage stores SHALL evict the oldest rows when exceeding their
caps so that the store never grows unbounded.

#### Scenario: Cap enforced
- **GIVEN** 200 000 audit events exist
- **WHEN** one more is recorded
- **THEN** the count stays at 200 000 and the oldest event is gone.

## Negative Cases

- Lineage request for a table the caller cannot see → 404.
- Lineage request without `table_name` → 400.
- Column lineage for a column with no recorded rows → empty arrays, 200.
- Audit middleware never fails a request: storage errors are logged and the
  original response is returned unchanged.
- History `GET` filters by `user_ids`/`statuses`/`warehouse_ids`; unknown
  filter values return empty results, not errors.

## Tests

- Unit: `uc::audit::tests::{classifies_uc_calls, redacts_secrets}`.
- Smoke: `uc-lakebase-smoke.sh` — `table lineage`, `audit events`,
  `system.access.audit` readable, `system.query.history` readable.
- Planned (LF-006..LF-009): audit event per Databricks service; failing
  statement in history; entity attribution from notebook and job runner;
  expression lineage.

## Parity Boundaries

- Column lineage only through direct/aliased column references; expressions,
  joins with ambiguous columns, CTEs and `UNION` are not resolved (LF-009).
- Lineage through views is recorded to the view, not expanded to base tables
  (LF-008).
- Notebook kernel and job runner do not yet populate `lakeforge.entity_*`
  conf consistently, so most rows have empty `entity_type` (LF-007).
- Audit `request_params` are top-level body fields, not Databricks'
  per-action parameter names (LF-006); `audit_level` is always
  `WORKSPACE_LEVEL` (no account-level events).
- No `system.access.outbound_network`, no `clean_room_events`.
- History lacks `query_source`, `compilation_time`, metrics breakdown
  (LF-007).
