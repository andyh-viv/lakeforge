# Issue inventory

Coherent, self-contained work items for continuing Lakeforge. Each issue is
written so an agent can pick it up cold: it names the files, the evidence for
the current state, the acceptance criteria and the tests to add. Issues are
numbered `LF-###`; the OpenSpec change that governs each one is listed so the
spec deltas and task lists in `openspec/` stay in sync.

Ordering and dependencies are summarised in
[continuation-plan.md](continuation-plan.md). Status vocabulary matches
[uc-lakebase-status.md](uc-lakebase-status.md).

Conventions for every issue:

- **Verify first**: run `tests/smoke/uc-lakebase-smoke.sh` against a fresh
  `.lakeforge/` before and after; it must stay at `failed=0`.
- **Gates**: `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test -p lakeforge-api`, and the web/python gates when those trees
  change (see [development.md](development.md)).
- **Docs**: update `docs/uc-lakebase-status.md` and `docs/parity.md` rows
  touched by the change in the same PR; never upgrade a row to "Full" without
  a smoke or unit test proving it.

---

## A. Unity Catalog — authorization

### LF-001 Broaden SQL authorization coverage and negative tests

- **Problem**: `prepare_sql` authorises the statement shapes exercised by the
  smoke test (SELECT/INSERT/CREATE/DROP/ALTER on tables, system tables, paths)
  but statement kinds Databricks users hit daily are unverified: `MERGE`
  (unsupported by Forge but should fail with a clear error, not bypass auth),
  `UPDATE`/`DELETE`, `CREATE VIEW … AS SELECT` across catalogs, `INSERT
  OVERWRITE`, `CREATE TABLE … AS SELECT` reading masked tables, CTEs, `UNION`,
  subqueries in `WHERE`, `DESCRIBE`/`SHOW` variants, `USE CATALOG/SCHEMA`.
- **Evidence**: `crates/lakeforge-api/src/uc/sqlguard.rs` (`analyze`,
  `StmtKind`), `crates/lakeforge-api/src/uc/sqlauth.rs::authorize_sql`; unit
  tests in `sqlguard::tests` cover ~10 shapes; smoke checks `bob select denied`,
  `bob insert denied`, `bob write system denied`.
- **Scope**: table-level and schema-level privilege checks for all statement
  kinds Forge accepts; explicit `PERMISSION_DENIED` for kinds we cannot analyse
  (fail closed) unless the caller is admin.
- **Proposed implementation**: extend `StmtKind` (`Merge`, `Update`, `Delete`,
  `Describe`, `Show`, `Use`, `Explain`); make `analyze` fall back to *deny* for
  parseable-but-unclassified statements instead of `Other` pass-through;
  resolve view definitions when a read hits a UC view so the underlying
  tables are checked (Databricks: view owner's privileges — implement the
  simpler "caller needs SELECT on the view only" first and document);
  add a `--dry-run` style debug endpoint `POST /api/2.0/lakeforge/sql/analyze`
  returning the `Analysis` for a statement (admin only) to make tests cheap.
- **Dependencies**: none.
- **Acceptance criteria**: a table-driven unit test with ≥40 statements
  asserting `reads/writes/creates/drops/owned/paths`; smoke checks for `UPDATE`,
  `DELETE`, `CTAS from masked table`, `CREATE VIEW` by a non-owner without
  `CREATE_TABLE` (denied), `USE CATALOG` without `USE_CATALOG` (denied);
  fail-closed behaviour covered by a test with an exotic statement.
- **Focused tests**: `cargo test -p lakeforge-api sqlguard`, `sqlauth`;
  new smoke section "sql-auth-matrix" in `tests/smoke/uc-lakebase-smoke.sh`.
- **Docs/parity**: update §3 of `uc-lakebase-status.md`; parity row "Grants /
  effective permissions".
- **OpenSpec**: `openspec/specs/unity-catalog-authorization/spec.md`
  (requirements *SQL statement authorization*, *Fail closed*).

### LF-002 Complete Databricks privilege SQL grammar

- **Problem**: `grant_sql.rs` parses `GRANT/REVOKE … ON <securable> …`,
  `SHOW GRANTS [principal] ON …`, `ALTER … [SET] OWNER TO`. Databricks also
  supports `SHOW GRANTS TO principal` / `SHOW GRANTS principal` (all objects),
  `SHOW GRANTS ON METASTORE`, `GRANT … ON ALL TABLES IN SCHEMA s` (legacy but
  used), `GRANT … ON ANY FILE`, `DENY`, `REVOKE GRANT OPTION FOR`, privilege
  aliases (`READ_METADATA`, `USAGE`, `SELECT ON VIEW`), and `SHOW GRANT` on
  `MATERIALIZED VIEW`/`REGISTERED MODEL`/`SHARE`.
- **Evidence**: `crates/lakeforge-api/src/uc/grant_sql.rs` (`parse`,
  `GrantStmt`), tests `grant_sql::tests::*`; smoke `sql grant`, `sql revoke`,
  `sql show grants`, `sql invalid privilege`.
- **Scope**: grammar completeness + error messages matching Databricks
  (`[PARSE_SYNTAX_ERROR]`, `[INVALID_PRIVILEGE]`); no `DENY` semantics beyond
  parsing (return `UNSUPPORTED_OPERATION` for `DENY`).
- **Proposed implementation**: add `GrantStmt::ShowGrantsTo { principal }`
  (aggregates `uc_grants` docs across the metastore, filtered by visibility),
  `Securable::Metastore` in `SHOW GRANTS ON METASTORE`, `ON ALL TABLES IN
  SCHEMA` expansion to per-table grants (documented as non-atomic), alias
  normalisation in `privileges::normalize_privilege`; return
  `[UNSUPPORTED_FEATURE] DENY` for `DENY`.
- **Dependencies**: none.
- **Acceptance criteria**: every example in the Databricks `GRANT`, `REVOKE`,
  `SHOW GRANTS` reference pages parses (encode them verbatim as a fixture);
  `SHOW GRANTS TO bob` returns rows across catalogs the caller can see; `ON
  ALL TABLES IN SCHEMA` grants appear on every table's `permissions`.
- **Focused tests**: `cargo test -p lakeforge-api grant_sql`; smoke additions.
- **Docs/parity**: parity row "SQL GRANT / REVOKE / SHOW GRANTS".
- **OpenSpec**: `unity-catalog-authorization` (*Privilege statements*).

### LF-003 Row filters and column masks — semantics and SQL syntax

- **Problem**: policies are attached via Lakeforge-only REST routes and applied
  by text rewrite. Missing: `ALTER TABLE t SET ROW FILTER fn ON (cols)` /
  `DROP ROW FILTER`, `ALTER TABLE t ALTER COLUMN c SET MASK fn [USING COLUMNS
  (…)]` / `DROP MASK`, `CREATE TABLE … WITH ROW FILTER`; policy functions with
  extra literal arguments; masks whose return type differs from the column
  (Databricks requires same type); row-filter smoke coverage for non-admins;
  interaction with `INSERT … SELECT`, `CREATE TABLE AS`, views over masked
  tables; error when the policy function is dropped. **Security gap**: when
  `sqlparser` cannot parse a statement, `sqlguard::analyze_fallback` still
  checks table privileges on extracted names but performs no rewrite, so
  policies are silently skipped for that statement (`Analysis.parsed ==
  false`).
- **Evidence**: `sqlauth::table_policies`, `sqlguard::apply_policies`,
  `catalog_ext.rs::{uc_set_row_filter, uc_set_column_mask,
  require_policy_function}`; smoke `set column mask`, `bob select masked`,
  `masked select (admin sees clear)` (relies on the UDF's own
  `is_account_group_member('admins')` branch); no row-filter smoke check.
- **Scope**: fail closed — reject (`[UNSUPPORTED_FEATURE] statement could not
  be analysed`) any unparsed statement that references a table with a row
  filter or mask, unless the caller is the owner/admin; SQL syntax as
  `MetastoreOp`s; type check at attach time (compare
  UDF `return_type` to column `type_name`); block dropping a function that is
  referenced by a policy (`RESOURCE_CONFLICT`) unless `FORCE`; `information_schema.row_filters`
  / `column_masks` rows; row filter smoke with a non-admin principal.
- **Proposed implementation**: extend `grant_sql.rs` (or a sibling
  `policy_sql.rs`) with `ALTER TABLE … {SET|DROP} ROW FILTER` and `ALTER COLUMN
  … {SET|DROP} MASK`; route to existing `uc_set_*`; in `uc_delete_in_schema`
  for functions scan `uc_table` docs for references; add `policy_args` (extra
  literals) to `TablePolicy`.
- **Dependencies**: LF-001 (analysis of `ALTER TABLE` kinds).
- **Acceptance criteria**: the Databricks docs example (row filter by region
  with `is_account_group_member`, mask on SSN) works verbatim via SQL; a
  non-admin sees only their rows (smoke); dropping the mask function fails
  with `RESOURCE_CONFLICT`; `CTAS` from a masked table stores masked values
  for the non-admin who runs it; an unparsable statement against a masked
  table is rejected for a non-owner (test with engine-specific syntax such
  as `SELECT * FROM t VERSION AS OF 0`).
- **Focused tests**: `cargo test -p lakeforge-api sqlguard::tests::polic`;
  smoke section "policies".
- **Docs/parity**: parity row "Row filters and column masks"; status §4.
- **OpenSpec**: `openspec/specs/unity-catalog-policies/spec.md`.

### LF-004 SQL UDF lifecycle, `EXECUTE` checks and invocation coverage

- **Problem**: `CREATE FUNCTION` persists a UC function and calls are inlined
  at prepare time. Gaps: `EXECUTE` is only checked for policy functions, not
  for ordinary invocations; no parameter default values; `DESCRIBE FUNCTION`
  / `SHOW FUNCTIONS` do not list UC functions (Forge does not know them);
  `CREATE FUNCTION` with `LANGUAGE PYTHON` should return
  `UNSUPPORTED_FEATURE` not a parse error; functions calling functions
  (recursive inlining) and name shadowing of built-ins are untested.
- **Evidence**: `sqlauth::metastore_op` (`ParsedFunction`),
  `sqlguard::inline_functions`, smoke `create mask fn`, `masked select`.
- **Scope**: `EXECUTE` enforcement on inlined calls; `SHOW FUNCTIONS [IN
  schema]` and `DESCRIBE FUNCTION [EXTENDED]` as `MetastoreOp`s producing rows;
  nested inlining with a depth cap; Python UDF rejection with a clear error;
  `information_schema.routines`/`parameters` rows verified.
- **Proposed implementation**: in `inline_functions` collect the set of
  inlined function names and `az.require(Function, name, "EXECUTE")` for each
  (owners/admins pass); implement `ShowFunctions`/`DescribeFunction` in
  `MetastoreOp` returning Databricks-shaped rows (`function`, then `Function:`,
  `Type:`, `Input:`, `Returns:` lines for DESCRIBE).
- **Dependencies**: none.
- **Acceptance criteria**: non-admin without `EXECUTE` gets `PERMISSION_DENIED`
  when calling `main.s.f(x)`; `SHOW FUNCTIONS IN main.s` lists it; nested
  functions inline to a depth of 8 and error beyond; smoke covers all three.
- **Focused tests**: `cargo test -p lakeforge-api sqlguard::tests::inline`;
  smoke section "functions".
- **Docs/parity**: parity row "SQL UDFs, session functions".
- **OpenSpec**: `unity-catalog-policies` (*SQL UDFs*).

### LF-005 Session functions and current-context parity

- **Problem**: `current_user()`, `current_catalog()`, `current_schema()`,
  `is_account_group_member()` are rewritten. Databricks also has
  `session_user()`, `current_metastore()`, `is_member()`,
  `current_database()` (alias), `current_version()`; and `USE CATALOG`/`USE
  SCHEMA` should persist per SQL-editor session / notebook context rather than
  requiring `forge.sql.defaultCatalog` conf.
- **Evidence**: `sqlauth::defaults`, `sqlguard::rewrite_session_functions`;
  smoke `session fn`.
- **Scope**: add the missing functions; `USE` statements update the execution
  context's defaults (`api/commands.rs` context doc; SQL editor uses
  statement-level `catalog`/`schema` fields already).
- **Proposed implementation**: extend the rewrite table; treat `USE CATALOG x`
  / `USE SCHEMA y` as `MetastoreOp::Use` that mutates the context document and
  returns an empty result; `kernel.rs` passes the context id in conf.
- **Dependencies**: LF-001.
- **Acceptance criteria**: notebook cell `USE CATALOG main; USE SCHEMA sales`
  followed by `SELECT * FROM orders` resolves to `main.sales.orders`
  (smoke via Command Execution 1.2); all listed functions return correct
  literals for two different principals.
- **Focused tests**: unit tests in `sqlguard`; smoke section "session".
- **Docs/parity**: status §4.
- **OpenSpec**: `unity-catalog-policies` (*Session functions*).

## B. Unity Catalog — audit, history, lineage

### LF-006 Audit event completeness and Databricks event schema

- **Problem**: `audit_middleware` records `service`, `action`, method, path,
  status, user, IP, request id and a `params` map derived from the path.
  Databricks events carry `serviceName`, `actionName`, `requestParams` (the
  request body for most UC actions), `response.statusCode`/`errorMessage`,
  `userIdentity.email`, `sourceIPAddress`, `userAgent`, `sessionId`,
  `auditLevel`, `workspaceId`, `eventTime`. Request bodies are not captured;
  GET denials are captured only from the SQL path; `classify` covers the main
  services but not all.
- **Evidence**: `crates/lakeforge-api/src/uc/audit.rs::{audit_middleware,
  classify}`; `system_tables.rs` `access.audit` columns; smoke `audit events`.
- **Scope**: capture a redacted request body (drop `password`, `token`,
  `secret`, `value` keys) for mutating calls; record `PERMISSION_DENIED`
  responses for GETs; fill `userAgent`, `sessionId` (token id), `auditLevel`
  (`WORKSPACE_LEVEL`/`ACCOUNT_LEVEL`); exhaustive `classify` table generated
  from the router; retention configurable (`LAKEFORGE_AUDIT_MAX_EVENTS`).
- **Proposed implementation**: buffer the request body in the middleware (limit
  64 KiB), redact via a key allowlist/denylist, store as `request_params`;
  map `system.access.audit` columns 1:1 to Databricks names; a unit test that
  walks `api::router()` routes and asserts every mutating route has a
  `classify` mapping.
- **Dependencies**: none.
- **Acceptance criteria**: audit row for `POST /api/2.1/unity-catalog/tables`
  includes the table name from the body; a denied `GET /catalogs/secret`
  produces an event with `status=403`; no secret value ever appears in the
  audit table (test uses `secrets/put`).
- **Focused tests**: `cargo test -p lakeforge-api audit`; smoke section
  "audit".
- **Docs/parity**: parity rows "Audit log", "Audit logs" (Security).
- **OpenSpec**: `openspec/specs/unity-catalog-audit-lineage/spec.md`
  (*Audit events*).

### LF-007 Query history enrichment and notebook/job attribution

- **Problem**: `sql/history/queries` rows carry user, warehouse, text, status,
  timings and (new) `statement_type` + tables. Databricks adds `query_source`
  (`job_info`, `notebook_id`, `dashboard_id`, `alert_id`),
  `metrics` (rows produced/read, bytes, compilation/execution time, spill),
  `executed_as_user`, `plans_state`, `client_application`. Lineage context
  keys (`lakeforge.entity_type`, `entity_id`, `entity_run_id`) are read from
  statement conf but never set by the notebook kernel or job runner.
- **Evidence**: `api/sql.rs` (history doc), `uc/lineage.rs::LineageContext::from_conf`,
  `system_tables.rs` `query.history` columns; `kernel.rs` and `api/jobs.rs` do
  not populate the conf keys.
- **Scope**: set entity context from every caller (notebook context → `NOTEBOOK`
  + path; job run → `JOB` + job id/run id; dashboard/alert evaluation;
  pipeline update); add `query_source` and `metrics` (from Forge
  `QueryResult` stats: rows, batches, elapsed per stage when available) to
  history docs and `system.query.history`.
- **Proposed implementation**: introduce `ExecContext { entity_type,
  entity_id, entity_run_id, client_application }` passed into `execute_sql`
  (replacing ad-hoc conf keys); `kernel.rs` command execution sets it;
  `jobs.rs` sets it for `sql_task`/`notebook_task`; Forge driver already
  returns `rows`/`elapsed_ms` — extend `forge.proto` `QueryResult` with
  `bytes_read`, `tasks`, `stages` if cheap.
- **Dependencies**: none (proto change optional).
- **Acceptance criteria**: a job run's SQL appears in `system.query.history`
  with `query_source.job_info.job_id`; a notebook cell's lineage row carries
  `entity_type=NOTEBOOK`; `metrics.rows_produced_count` is set for `SELECT`.
- **Focused tests**: smoke section "history" (run a job with `sql_task` and
  query `system.query.history`); unit test for `ExecContext` propagation.
- **Docs/parity**: parity row "Query history"; status §5.
- **OpenSpec**: `unity-catalog-audit-lineage` (*Query history*).

### LF-008 Table lineage robustness

- **Problem**: `record_lineage` writes `reads × writes` edges per statement.
  Not covered: views (a `SELECT` from a view should attribute to the view's
  base tables as Databricks does), `INSERT OVERWRITE`, `MERGE` (once
  supported), `COPY INTO` from paths (`source_path`), lineage of
  `CREATE OR REPLACE TABLE`, dedupe window (currently exact-row dedupe), and
  the Databricks `entity_metadata`/`source_type` (`TABLE`, `PATH`, `STREAMING_TABLE`).
- **Evidence**: `uc/lineage.rs::{record_lineage, MAX_ROWS}`; smoke `table lineage`.
- **Scope**: view expansion (one level, using stored `view_definition`
  analysed by `sqlguard`); path sources (`source_path`); `source_type`;
  `lineage-tracking` API `include_entity_lineage` flag; retention policy
  (keep last N days, configurable).
- **Proposed implementation**: in `record_lineage`, for each read that is a
  `uc_table` with `table_type=VIEW`, analyse its `view_definition` and add
  edges from its base tables (mark `via_view`); capture `analysis.paths` as
  `source_path` rows; add `source_type` column.
- **Dependencies**: LF-001 (analysis of new kinds).
- **Acceptance criteria**: `CREATE VIEW v AS SELECT … FROM t; CREATE TABLE u AS
  SELECT * FROM v` produces edges `t→u` (via view) and `v→u`; `COPY INTO t
  FROM 's3://…'` produces a path-source row; smoke asserts both.
- **Focused tests**: `cargo test -p lakeforge-api lineage`; smoke additions.
- **Docs/parity**: parity row "Lineage".
- **OpenSpec**: `unity-catalog-audit-lineage` (*Table lineage*).

### LF-009 Column lineage through expressions, joins and CTEs

- **Problem**: column edges come from direct projections
  (`SELECT a AS b`, `INSERT (cols) SELECT cols`). Expressions (`a + b AS c`),
  functions, `CASE`, aggregates, joins with aliases, CTEs, `UNION`, `SELECT *`
  expansion via UC column lists and nested subqueries produce incomplete or
  missing edges.
- **Evidence**: `sqlguard::column_edges`; smoke does not assert column lineage
  rows.
- **Scope**: an expression walker over `sqlparser::ast::Expr` collecting
  referenced columns per output column; alias/CTE resolution table; `*`
  expansion using `uc_table.columns`; `UNION` merges per position.
- **Proposed implementation**: new `uc/column_lineage.rs` with `fn edges(stmt,
  resolver) -> Vec<(src_table, src_col, dst_table, dst_col)>`, unit-tested
  against a fixture of 30 statements; `record_lineage` uses it; keep the old
  simple path as fallback when parsing fails.
- **Dependencies**: LF-008.
- **Acceptance criteria**: fixture passes; smoke `CREATE TABLE u AS SELECT
  o.id, UPPER(c.name) AS cname FROM o JOIN c ON … ` yields edges
  `o.id→u.id`, `c.name→u.cname`; `system.access.column_lineage` shows them.
- **Focused tests**: `cargo test -p lakeforge-api column_lineage`; smoke.
- **Docs/parity**: parity row "Lineage".
- **OpenSpec**: `unity-catalog-audit-lineage` (*Column lineage*).

## C. Unity Catalog — system tables, information_schema, models

### LF-010 System-table materialisation: freshness, cost, `billing.usage`

- **Problem**: any statement referencing `system.*` triggers
  `refresh_system_tables_for`, rewriting the affected Delta tables in full
  before execution; `refresh_all_system_tables` runs after cluster start. This
  is O(documents) per query and racy under concurrency; `billing.usage` is
  synthesised from cluster uptime with fixed DBU rates; there is no
  `system.compute.node_timeline`, `system.access.outbound_network`,
  `system.storage.predictive_optimization_operations_history`.
- **Evidence**: `uc/system_tables.rs::{tables, refresh_system_tables_for,
  refresh_all_system_tables, rows_for}`; smoke `information_schema`,
  `system.query.history`.
- **Scope**: incremental refresh keyed on `Store` `updated_at` watermark per
  table; a per-table mutex to serialise refreshes; `billing.usage` from actual
  cluster/warehouse runtime events with a configurable price sheet
  (`system.billing.list_prices` already exists); `compute.node_timeline` from
  executor heartbeats.
- **Proposed implementation**: store `{table → last_refreshed_at,
  source_watermark}` in a `system_table_state` doc; `rows_for` accepts a
  `since` parameter and appends (Delta append) rather than overwrite for
  append-only tables (audit, history, lineage, timelines); overwrite only
  for snapshot tables (`*_latest`, `clusters`, `information_schema.*`).
- **Dependencies**: none.
- **Acceptance criteria**: 1,000 audit rows → a `SELECT count(*) FROM
  system.access.audit` refreshes in <1 s incremental after the first run
  (assert in a Rust integration test with a temp store); concurrent queries
  do not produce Delta conflicts; `billing.usage` rows match cluster
  lifecycle events within the smoke run.
- **Focused tests**: `cargo test -p lakeforge-api system_tables`; smoke.
- **Docs/parity**: parity row "System tables".
- **OpenSpec**: `openspec/specs/unity-catalog-system-tables/spec.md`.

### LF-011 `information_schema` completeness and semantics

- **Problem**: all Databricks `information_schema` tables exist with the right
  column names, but several are empty or approximate: `column_privileges`
  (no column privileges), `table_privileges`/`schema_privileges`/… derive
  from `uc_grants` docs without inherited rows, `views` lacks `view_definition`
  for engine-created views, `columns.ordinal_position`/`is_nullable`/`data_type`
  are best effort from `observe_ddl`, `routines.routine_definition` is set only
  for SQL UDFs, `information_schema` is visible per catalog in Databricks
  (`main.information_schema.tables` scoped to `main`) whereas Lakeforge has
  only `system.information_schema`.
- **Evidence**: `system_tables.rs` (`information_schema_*` builders).
- **Scope**: per-catalog `information_schema` virtual schemas resolving to a
  filtered view of the global ones; `*_privileges` include `inherited_from`;
  `columns` typed from Forge `DESCRIBE` at mirror time; `views.view_definition`
  captured by `observe_ddl`.
- **Proposed implementation**: in `sqlauth::prepare_sql`, rewrite
  `<catalog>.information_schema.<t>` to `system.information_schema.<t>` with
  a `WHERE table_catalog = '<catalog>'` predicate injected (same mechanism as
  row filters); enrich `observe_ddl` to run `DESCRIBE TABLE` on new tables to
  capture types/nullability.
- **Dependencies**: LF-010.
- **Acceptance criteria**: `SELECT * FROM main.information_schema.tables`
  returns only `main` tables; `columns.data_type` matches `DESCRIBE`; smoke
  asserts both; every `information_schema` table has ≥1 row after the smoke
  run except those tied to unimplemented features (models, shares).
- **Focused tests**: unit tests per builder; smoke.
- **Docs/parity**: parity row "System tables + information_schema".
- **OpenSpec**: `unity-catalog-system-tables` (*information_schema*).

### LF-012 Models in Unity Catalog

- **Problem**: `uc/models.rs` is a placeholder; MLflow's registry is
  workspace-scoped. Databricks exposes `/api/2.1/unity-catalog/models`,
  `/models/{full_name}`, `/models/{full_name}/versions[/{version}]`,
  `/models/{full_name}/aliases/{alias}`, `CREATE_MODEL` privilege, `EXECUTE`
  on models for serving, `information_schema.models/model_versions/
  model_version_aliases`, and the MLflow client uses
  `registry_uri=databricks-uc` with three-level names.
- **Evidence**: `uc/models.rs` (`KIND_MODEL_VERSION` only), `api/mlflow.rs`
  registry handlers, `privileges.rs` maps `registered_model` to
  `Securable::Function`.
- **Scope**: UC model securable (`Securable::Model`, kind `uc_model`,
  `uc_model_version`), REST routes above, MLflow registry routes accepting
  `catalog.schema.model` names and delegating to UC when three-level,
  aliases, `information_schema` rows, serving endpoints referencing UC models
  (`entity_name=catalog.schema.model`, `entity_version`).
- **Proposed implementation**: add `Securable::Model` with privileges
  `EXECUTE`, `APPLY_TAG`, `MANAGE`, `ALL_PRIVILEGES`; `CREATE_MODEL` on the
  schema; routes in a new `api/uc_models.rs`; in `mlflow.rs`, detect dotted
  names in `registered-models/*` and `model-versions/*` and call the UC
  layer; store artifacts under the schema's storage root; `system_tables.rs`
  builders populate the three model tables.
- **Dependencies**: none (LF-011 for information_schema polish).
- **Acceptance criteria**: `mlflow.set_registry_uri("databricks-uc");
  mlflow.register_model(uri, "main.ml.churn")` succeeds via the SDK;
  `GET /api/2.1/unity-catalog/models/main.ml.churn` returns versions; a
  non-admin without `EXECUTE` cannot create a serving endpoint on it; smoke
  covers create/version/alias/privilege-denied.
- **Focused tests**: `cargo test -p lakeforge-api uc_models`; smoke section
  "models"; python SDK test for three-level names.
- **Docs/parity**: parity row "Models in Unity Catalog" → Partial.
- **OpenSpec**: `openspec/specs/unity-catalog-models/spec.md`.

## D. Lakebase

### LF-013 Lakebase instance CRUD and lifecycle hardening

- **Problem**: instances are metadata; lifecycle settles on read after
  `STARTING_SECS`; `stopped` toggling, `capacity` change, `parent_instance_ref`
  branching and `retention_window_in_days`, `enable_readable_secondaries`,
  `node_count` are stored but only partially validated; `read_only_dns` is
  not set; deletion of a parent with children is not blocked; `list` lacks
  pagination.
- **Evidence**: `api/lakebase.rs::{create_instance, patch_instance,
  delete_instance, settle}`; smoke `lb create instance` … `lb gone`.
- **Scope**: full validation table (name regex, capacity ∈ `CAPACITIES`,
  `node_count` 1..=N, `retention_window_in_days` 2..=35); state machine
  `STARTING→AVAILABLE`, `AVAILABLE→STOPPED→AVAILABLE` (via `stopped`),
  `UPDATING`, `DELETING`, `FAILED`; child instances require parent `AVAILABLE`,
  copy `effective_*` fields; delete parent blocked with children unless
  `force`; `page_token`/`page_size`; `update_mask` semantics for `PATCH`.
- **Proposed implementation**: extract `lakebase::state::transition(doc,
  event)` pure function + unit tests; a `workers.rs` tick that advances states
  (instead of settle-on-read) so `system.lakebase.instances` is consistent
  without reads.
- **Dependencies**: none.
- **Acceptance criteria**: state-machine unit tests cover every transition and
  illegal transition (`RESOURCE_CONFLICT`); smoke: stop→`STOPPED`,
  start→`AVAILABLE`, child create, parent delete blocked, `update_mask`
  respected.
- **Focused tests**: `cargo test -p lakeforge-api lakebase`; smoke section
  "lakebase-lifecycle".
- **Docs/parity**: Lakebase parity rows.
- **OpenSpec**: `openspec/specs/lakebase-instances/spec.md`.

### LF-014 Lakebase credentials, redaction and token scoping

- **Problem**: `POST /database/credentials` mints a Lakeforge JWT with
  `instance_names` in claims, TTL 1 h, and reports `backend`. Gaps: the token
  is a general Lakeforge bearer token (usable on any REST route); credential
  requests are not audited with instance names; there is no rotation/revoke;
  responses/logs must never echo the token beyond the response body;
  `expiration_time` format should match Databricks (RFC 3339).
- **Evidence**: `api/lakebase.rs::generate_credential`, `auth.rs::create_token`.
- **Scope**: scoped token type (`kind: "database_credential"`) rejected by
  `auth_middleware` for non-Lakebase routes; audit event
  `database.generateDatabaseCredential` with instance names only; revoke on
  instance delete; RFC 3339 `expiration_time`; redaction in audit/history.
- **Proposed implementation**: add `TokenKind` to token docs; `auth.rs`
  principal carries `token_kind`; a route allowlist for
  `database_credential`; `delete_instance` revokes tokens with matching
  claims.
- **Dependencies**: LF-013.
- **Acceptance criteria**: a credential token gets 403 on
  `/api/2.0/clusters/list`; audit row lacks the token; token invalid after
  instance deletion; smoke covers the three.
- **Focused tests**: `cargo test -p lakeforge-api lakebase::cred`; smoke.
- **Docs/parity**: Lakebase "Credentials" row.
- **OpenSpec**: `openspec/specs/lakebase-credentials/spec.md`.

### LF-015 Lakebase roles and permissions model

- **Problem**: roles are stored per instance (`membership_role`,
  `identity_type`, `attributes`). Databricks semantics: `DATABRICKS_SUPERUSER`
  creator role, `PG_ROLE` attributes (`LOGIN`, `CREATEDB`, `CREATEROLE`,
  `BYPASSRLS`), role deletion with `reassign_owned_to`, `allow_missing`; only
  instance owner/admins can manage roles; roles must map to Lakeforge
  principals (users, groups, service principals).
- **Evidence**: `api/lakebase.rs::{create_role, list_roles, delete_role}`;
  smoke `lb role`.
- **Scope**: validation of `identity_type ∈ {USER, GROUP, SERVICE_PRINCIPAL}`
  against SCIM; attribute set; creator auto-role; `reassign_owned_to`;
  audit; in `external` backend, translate to `CREATE ROLE`/`GRANT` statements
  (see LF-017).
- **Proposed implementation**: `lakebase::roles` module with a
  `RoleSpec` type and `to_pg_sql()` for the external backend; SCIM lookup via `st.principal_exists()` (as `uc_update_grants` does).
- **Dependencies**: LF-013.
- **Acceptance criteria**: unknown principal → `RESOURCE_DOES_NOT_EXIST`;
  non-owner → `PERMISSION_DENIED`; creator role present after instance create;
  smoke asserts each.
- **Focused tests**: unit + smoke.
- **Docs/parity**: status §8 roles row.
- **OpenSpec**: `lakebase-instances` (*Roles*).

### LF-016 Lakebase catalogs and UC registration

- **Problem**: `database/catalogs` creates a UC catalog of type
  `DATABASE_CATALOG` with `database_instance_name`/`database_name` and
  `create_database_if_not_exists`. Gaps: schemas under a database catalog
  are not enumerated (Databricks lists PostgreSQL schemas); catalog deletion
  does not check dependents; UC grants on the catalog (`USE_CATALOG`,
  `SELECT` on tables) are not mapped to PostgreSQL privileges; `information_schema`
  rows for database catalogs missing `catalog_type`.
- **Evidence**: `api/lakebase.rs::{create_catalog, delete_catalog}`,
  `catalog.rs` list filters; smoke `lb catalog`, `lb catalog in UC`.
- **Scope**: `information_schema.catalogs.catalog_type=DATABASE_CATALOG`; in
  `emulated` mode, a `public` schema and any registered `database/tables`
  appear under the catalog; delete requires no synced tables/tables;
  privileges recorded in UC only (documented).
- **Proposed implementation**: virtual schema docs (like system schemas) for
  database catalogs; `uc_list` merges them; delete guard.
- **Dependencies**: LF-013.
- **Acceptance criteria**: `SHOW SCHEMAS IN <dbcatalog>` returns `public`;
  `GET /schemas?catalog_name=<dbcatalog>` returns it; delete with a synced
  table → `RESOURCE_CONFLICT`; smoke asserts.
- **Focused tests**: unit + smoke.
- **Docs/parity**: Lakebase "Database catalogs" row.
- **OpenSpec**: `openspec/specs/lakebase-catalogs/spec.md`.

### LF-017 Lakebase external PostgreSQL backend (real data plane)

- **Problem**: `LAKEFORGE_LAKEBASE_POSTGRES_URL` only changes reported
  metadata. A real backend must create a database per instance, roles per
  Lakebase role with passwords derived from credential tokens (or a `pgbouncer`
  auth query hook), enforce `stopped`, and provide `read_write_dns` that
  actually accepts connections.
- **Evidence**: `api/lakebase.rs::backend()`, `BACKEND_EXTERNAL`; sqlx already
  has the `postgres` feature (`Cargo.toml`).
- **Scope**: `LakebaseBackend` trait `{ create_instance, drop_instance,
  create_role, drop_role, set_password, create_database, list_schemas }` with
  `Emulated` and `ExternalPostgres` (sqlx) implementations; credentials in
  external mode: generate a random password, `ALTER ROLE … PASSWORD … VALID
  UNTIL`, return it as `token`; `read_write_dns` = external host;
  Helm value `lakebase.postgres.url` and Terraform output wiring; a local
  `docker-compose` profile with a PostgreSQL container for dev.
- **Proposed implementation**: new `crates/lakeforge-api/src/lakebase/`
  module tree (`mod.rs`, `backend.rs`, `emulated.rs`, `postgres.rs`); keep REST
  handlers backend-agnostic; integration test gated on
  `LAKEFORGE_TEST_POSTGRES_URL` (CI service container `postgres:16`).
- **Dependencies**: LF-013, LF-014, LF-015.
- **Acceptance criteria**: with the compose profile, `POST
  /database/credentials` returns a password with which `psql
  "host=… user=<role> password=<token> dbname=<instance>"` connects;
  `stopped=true` revokes `LOGIN`; instance delete drops the database
  (`purge`) — all asserted by a CI integration test.
- **Focused tests**: `cargo test -p lakeforge-api --features pg-tests lakebase::postgres`
  (new feature flag); CI job with service container.
- **Docs/parity**: Lakebase rows → Partial/Full for external backend; keep
  emulated caveat.
- **OpenSpec**: `openspec/specs/lakebase-emulation/spec.md` (*Backends*).

### LF-018 Lakebase synced tables — pipeline execution

- **Problem**: synced tables validate and simulate states. Databricks moves
  data from a Delta source to PostgreSQL according to `scheduling_policy`
  (`SNAPSHOT` once, `TRIGGERED` on demand/schedule, `CONTINUOUS`), tracks
  `data_synchronization_status` (`pipeline_id`, `last_sync`, `provisioning_phase`,
  `sync_state`, `failed_status`), and exposes the table under the database
  catalog.
- **Evidence**: `api/lakebase.rs::{create_synced_table, settle_synced}`;
  smoke `lb synced table`, `lb synced online`.
- **Scope** (external backend): `SNAPSHOT`/`TRIGGERED` implemented as a job
  (`jobs.rs` `run_job_task`-style internal job) that runs `SELECT * FROM
  source` on a warehouse through `execute_sql` (so UC privileges apply) and
  upserts into PostgreSQL via `COPY`/`INSERT … ON CONFLICT (pk)`; `CONTINUOUS`
  implemented as `TRIGGERED` on a short interval (documented); status fields
  updated from the job run; `emulated` backend keeps simulation but also
  materialises a Delta copy under the database catalog so `SELECT` from the
  synced table works in Forge.
- **Proposed implementation**: `lakebase/sync.rs` with `run_sync(table)`
  executed from `workers.rs`; reuse `forge-client` result streaming to page
  batches; PK conflict semantics per Databricks (last write wins by
  `timeseries_key` when present).
- **Dependencies**: LF-017, LF-016.
- **Acceptance criteria**: `SNAPSHOT` sync of a 10k-row Delta table lands in
  PostgreSQL with matching count; `TRIGGERED` re-run applies updates;
  status shows `ONLINE_TRIGGERED_UPDATE` with `last_sync.delta_table_version`;
  a source table the creator lacks `SELECT` on fails with
  `PERMISSION_DENIED` (already validated at create; re-checked at run).
- **Focused tests**: pg-gated integration test; smoke in emulated mode for
  the Delta copy.
- **Docs/parity**: Lakebase "Synced tables" row.
- **OpenSpec**: `openspec/specs/lakebase-synced-tables/spec.md`.

### LF-019 Forge federation: PostgreSQL table provider (Lakehouse Federation + Lakebase reads)

- **Problem**: UC connections of type `POSTGRESQL` and Lakebase database
  catalogs cannot be queried from Forge; Databricks lets you `SELECT` from a
  foreign catalog. Forge has DataFusion + Delta providers only.
- **Evidence**: `crates/forge-sql`, `crates/forge-driver/src/session.rs`
  (catalog registration); `api/catalog.rs` connections stored only.
- **Scope**: a DataFusion `TableProvider` for PostgreSQL (via `sqlx` or
  `tokio-postgres` + Arrow conversion) with projection and simple filter
  pushdown; catalog resolution: when `prepare_sql` sees a read on a table in a
  catalog of type `FOREIGN` (backed by a connection) or `DATABASE_CATALOG`,
  it passes a `TableSpec { kind: Postgres, url, schema, table }` to the driver
  (`forge.proto` already has `TableSpec` for external tables — extend with a
  `postgres` variant); credentials come from the connection's UC storage
  credential or the Lakebase backend, never from the SQL text.
- **Proposed implementation**: new crate `crates/forge-federation` (feature
  `postgres`) registered by `ForgeSessionBuilder`; driver resolves specs per
  query; read-only in v1.
- **Dependencies**: LF-017 (for Lakebase reads), none for UC connections.
- **Acceptance criteria**: `CREATE CONNECTION pg TYPE POSTGRESQL OPTIONS (host
  …)` + `CREATE FOREIGN CATALOG fc USING CONNECTION pg OPTIONS (database …)`
  (SQL or REST) then `SELECT count(*) FROM fc.public.t` returns the PostgreSQL
  count; UC `SELECT` on `fc.public.t` enforced; pg-gated integration test.
- **Focused tests**: `cargo test -p forge-federation`; pg-gated API test.
- **Docs/parity**: parity rows "Lakehouse Federation", Lakebase "Database
  catalogs".
- **OpenSpec**: `lakebase-catalogs` (*Queryability*), new
  `openspec/specs/lakehouse-federation/spec.md` (to be written by the
  implementer from the proposal in `openspec/changes/`).

## E. Clients, UI, deployment

### LF-020 Python SDK services for UC extensions and Lakebase

- **Problem**: `WorkspaceClient` has `grants`, `catalogs`, … but no
  `database` (Lakebase), `lineage`, `audit`, `system_tables`, `tags`
  (`entity_tag_assignments`), `constraints`, `row_filters`/`column_masks`,
  `system_schemas`, `temporary_table_credentials`, `workspace_bindings`,
  `artifact_allowlists`; CLI lacks `lakeforge database …` and `lakeforge
  catalog {tags,lineage,grants-sql}`.
- **Evidence**: `python/lakeforge-sdk/lakeforge/services.py` (class list),
  `python/lakeforge-sdk/lakeforge/cli.py`.
- **Scope**: services above with Databricks SDK method names
  (`w.database.create_database_instance`, `w.database.generate_database_credential`,
  `w.database.create_synced_database_table`, `w.grants.get_effective`,
  `w.tables.set_row_filter` (Lakeforge extension), …); dataclasses for
  responses; CLI subcommands; tests against a `responses`-style fake.
- **Proposed implementation**: follow `_UcCollection` pattern; add
  `Database(_Service)`; extend `cli.py` command table; docs in the SDK README.
- **Dependencies**: none (LF-013 for final shapes).
- **Acceptance criteria**: `python -m pytest` covers every new method with a
  recorded request/response; a scripted example
  (`python/examples/lakebase_quickstart.py`) runs against the local API in
  the smoke test.
- **Focused tests**: `python -m pytest -q python/lakeforge-sdk/tests`.
- **Docs/parity**: parity row "Databricks CLI / SDK compatibility".
- **OpenSpec**: tasks in `openspec/changes/implement-lakebase-control-plane/tasks.md`.

### LF-021 Catalog UI: permissions editor, lineage, tags, policies, audit

- **Problem**: `web/src/pages/Catalog.tsx` shows a read-only grants list and
  object details. Databricks Catalog Explorer has Permissions (grant/revoke
  dialog with privilege checkboxes per securable type), Lineage (upstream/
  downstream tables + columns graph), Tags, Details (row filter / masks,
  constraints, properties), Sample data, History; Admin has an Audit log
  view; there is no System Tables browser.
- **Evidence**: `web/src/pages/Catalog.tsx`, `web/src/api.ts`.
- **Scope**: Permissions tab with grant/revoke against
  `/permissions/{type}/{name}` and effective-permissions display; Lineage tab
  from `/lineage-tracking/*`; Tags tab; Policies panel using the Lakeforge
  row-filter/column-mask routes; Audit page (`/api/2.0/lakeforge/audit`) in
  Admin; System tables browser (`/api/2.0/lakeforge/system-tables`).
- **Proposed implementation**: components `GrantsEditor`, `LineageGraph`
  (simple two-column list first, graph later), `TagEditor`, `PolicyPanel`;
  API wrappers in `api.ts`; keep `react-query` keys consistent.
- **Dependencies**: none.
- **Acceptance criteria**: browser test (extend
  `.agents/skills/testing-workspace`) grants `SELECT` to a user via UI and
  verifies via REST; lineage tab shows the smoke-created edge; lint/build
  pass.
- **Focused tests**: `npm run lint && npm run build`; browser golden path.
- **Docs/parity**: parity "Workspace UI" notes.
- **OpenSpec**: tasks in `openspec/changes/integrate-uc-sql-enforcement/tasks.md`.

### LF-022 Lakebase UI page

- **Problem**: no UI for Lakebase. Databricks has Compute → Database
  instances (list, create with capacity, status, connection details, roles,
  credentials button) and Catalog → database catalogs / synced tables
  creation.
- **Evidence**: `web/src/pages/` has no Lakebase page; `web/src/App.tsx` nav.
- **Scope**: `Lakebase.tsx` page: instances table with state badges, create
  dialog (name, capacity, stopped, parent), detail drawer (DNS, roles,
  "Get credentials" showing token once with the `backend` warning banner when
  `emulated`), database catalogs list/create, synced tables list/create with
  scheduling policy and PK columns; nav entry.
- **Proposed implementation**: follow `Warehouses.tsx` patterns; API wrappers.
- **Dependencies**: LF-013 (state names).
- **Acceptance criteria**: browser test creates an instance, waits for
  `AVAILABLE`, creates a catalog and a synced table, sees the emulation banner.
- **Focused tests**: lint/build; browser golden path.
- **Docs/parity**: Lakebase "UI" row.
- **OpenSpec**: `implement-lakebase-control-plane/tasks.md`.

### LF-023 Deployment: PostgreSQL-backed Lakebase and control-plane DB in Helm/Terraform/Compose

- **Problem**: Helm has `database.url`/`embeddedPostgres` for the control
  plane store only; Terraform provisions managed PostgreSQL for the store;
  nothing wires `LAKEFORGE_LAKEBASE_POSTGRES_URL`; compose has no PostgreSQL
  service; `deploy/deploy.sh` has no Lakebase flags.
- **Evidence**: `deploy/helm/lakeforge/values.yaml`, `deploy/terraform/*`,
  `deploy/docker/docker-compose.yml`, `deploy/deploy.sh`.
- **Scope**: Helm `lakebase.{enabled, url, embeddedPostgres}` → env +
  optional StatefulSet (separate from the store DB); Terraform variable
  `lakebase_postgres = { enabled, sku }` creating a second managed instance
  (RDS/Cloud SQL/Flexible Server) and passing its URL as a Kubernetes Secret;
  compose profile `lakebase`; `deploy.sh --with-lakebase`; `helm lint`,
  `terraform validate`, `compose config` in CI already — extend.
- **Proposed implementation**: mirror the existing `database` block; secrets
  never in values files (use `existingSecret`).
- **Dependencies**: LF-017.
- **Acceptance criteria**: `helm template --set lakebase.enabled=true` renders
  env + StatefulSet; `terraform validate` passes with the variable for all
  three clouds; compose profile brings up PostgreSQL and the API reports
  `backend=external`.
- **Focused tests**: CI "Deploy manifests" job.
- **Docs/parity**: `docs/deploy.md` Lakebase section; parity row.
- **OpenSpec**: `lakebase-emulation` (*Deployment*).

### LF-024 Delta Sharing (provider side) and shares/recipients/providers API

- **Problem**: `Securable::{Share, Recipient, Provider}` and grant plumbing
  exist; there are no `/api/2.1/unity-catalog/shares`, `/recipients`,
  `/providers` routes, no `CREATE SHARE`/`ALTER SHARE ADD TABLE` SQL, and no
  Delta Sharing protocol server (`/delta-sharing/shares/…`, bearer-token
  recipients, `queryTable` with pre-signed URLs).
- **Evidence**: `privileges.rs`, `grant_sql.rs` accept `SHARE`/`RECIPIENT`/
  `PROVIDER` keywords; `information_schema.shares/recipients/providers`
  builders return no rows.
- **Scope**: shares CRUD + `ALTER SHARE {ADD|REMOVE} TABLE` (REST `PATCH
  /shares/{name}` `updates[]`), recipients with `authentication_type=TOKEN`
  and activation link (token shown once), `SHARE` privilege → recipient
  access, protocol server implementing `GET /shares`, `/shares/{s}/schemas`,
  `/tables`, `/tables/{t}/metadata`, `POST /tables/{t}/query` returning Delta
  log-style responses with file URLs (local `file://`/Files-API URLs first,
  pre-signed cloud URLs later).
- **Proposed implementation**: `api/sharing.rs` for UC routes;
  `api/delta_sharing_server.rs` for the protocol; reuse `deltalake` crate to
  read the log; recipient tokens as a distinct `TokenKind` (see LF-014).
- **Dependencies**: LF-014 (token kinds).
- **Acceptance criteria**: the open-source `delta-sharing` Python client with
  a profile file lists shares and loads a shared table into pandas from the
  local server; `information_schema.shares` populated; smoke covers CRUD +
  a protocol `GET /shares` with a recipient token.
- **Focused tests**: `cargo test -p lakeforge-api sharing`; smoke; python
  client test in CI (pip `delta-sharing`).
- **Docs/parity**: parity row "Delta Sharing".
- **OpenSpec**: new `openspec/specs/delta-sharing/spec.md` (write from the
  proposal skeleton in `openspec/changes/README.md`).

## F. Quality and maintenance

### LF-025 Test pyramid: Rust integration tests for the API, smoke in CI

- **Problem**: coverage is `cargo test` units (31 in `lakeforge-api`),
  Python SDK tests, web lint/build, and two bash smoke scripts run manually.
  CI's control-plane smoke only checks `/Me` and `/catalogs`. There are no
  Rust integration tests spinning up `AppState` with a temp SQLite store and
  calling handlers, so authorization regressions are caught only by hand.
- **Evidence**: `.github/workflows/ci.yml`, `tests/smoke/*.sh`,
  `crates/lakeforge-api/src/**/tests`.
- **Scope**: `crates/lakeforge-api/tests/` integration harness (`AppState::new`
  with `sqlite::memory:`, `axum::Router` via `tower::ServiceExt::oneshot`)
  covering UC REST authorization matrix, grant SQL round-trips through
  `prepare_sql` + `apply_metastore_op` (no Forge needed), Lakebase CRUD/state
  machine, audit middleware; run `tests/smoke/uc-lakebase-smoke.sh` in the CI
  control-plane job (needs a Forge cluster — reuse the "Distributed SQL
  smoke" step's binary; budget ≤6 min); nightly workflow for the full
  platform smoke.
- **Proposed implementation**: `tests/common/mod.rs` builder; `tests/uc_auth.rs`,
  `tests/grant_sql_roundtrip.rs`, `tests/lakebase.rs`, `tests/audit.rs`; CI
  step after the API boots: `LF_URL=http://127.0.0.1:8080 bash
  tests/smoke/uc-lakebase-smoke.sh`.
- **Dependencies**: none.
- **Acceptance criteria**: ≥60 integration assertions; CI runs the UC smoke
  and fails on `failed>0`; flake-free over 3 consecutive runs.
- **Focused tests**: `cargo test -p lakeforge-api --tests`.
- **Docs/parity**: `docs/development.md` test table; `tests/smoke/README.md`.
- **OpenSpec**: `integrate-uc-sql-enforcement/tasks.md` (Testing section).

### LF-026 Documentation and parity maintenance

- **Problem**: parity/status docs drift as features land; several docs
  (`architecture.md`, `api-surface.md`, `uc-lakebase-status.md`, `parity.md`,
  OpenSpec specs) describe overlapping facts.
- **Evidence**: this branch had to correct "UC grants not enforced" in
  `parity.md` after enforcement shipped.
- **Scope**: a PR checklist (`.github/pull_request_template.md`) requiring the
  status/parity rows touched; a `scripts/check-docs.sh` that (a) extracts
  routes from `api/*.rs` and diffs against `docs/api-surface.md`, (b) checks
  every `LF-###` in `docs/issues.md` is referenced by an OpenSpec task or
  marked done; run it in CI (warn-only first).
- **Proposed implementation**: dependency-free bash + coreutils (awk, sed,
  grep, sort, comm); no `rg`, no GNU-only flags, portable across bash 3.2/5.
- **Dependencies**: none.
- **Acceptance criteria**: script passes on this branch; CI shows the job.
- **Focused tests**: the script itself.
- **Docs/parity**: `docs/development.md` conventions.
- **OpenSpec**: `openspec/project.md` conventions.

### LF-027 Enforce workspace object ACLs on object routes (cross-cutting)

- **Problem**: `api/permissions.rs::check_permission` implements Databricks
  level semantics but no cluster/job/notebook/warehouse/pipeline/repo/serving
  handler calls it; every authenticated user can start any cluster or run
  any job. This predates the UC work but blocks any multi-user use.
- **Evidence**: `rg check_permission crates/lakeforge-api/src` shows uses
  only inside `permissions.rs`; parity row "Permissions API".
- **Scope**: call `check_permission` in each handler with the Databricks
  required level (clusters: `CAN_ATTACH_TO` to attach/execute, `CAN_RESTART`
  to start/restart, `CAN_MANAGE` to edit/delete; jobs: `CAN_VIEW` get,
  `CAN_MANAGE_RUN` run-now/cancel, `IS_OWNER`/`CAN_MANAGE` edit/delete;
  notebooks/directories: `CAN_READ/RUN/EDIT/MANAGE`; warehouses: `CAN_USE`
  to execute; pipelines/repos/serving likewise); directory ACL inheritance for
  workspace objects; UI hides actions the user lacks.
- **Proposed implementation**: helper `st.require_level(p, type, id, level,
  owner)`; a table in `permissions.rs` mapping (route → level) used by a unit
  test that asserts every mutating route in those modules is covered.
- **Dependencies**: none.
- **Acceptance criteria**: non-admin `bob` without ACL entries on a cluster
  with a non-empty ACL cannot start it (403) but can once granted
  `CAN_RESTART`; jobs equivalent; smoke section "object-acls".
- **Focused tests**: integration tests (LF-025 harness) + smoke.
- **Docs/parity**: parity row "Permissions API" → Partial (enforced).
- **OpenSpec**: new `openspec/specs/workspace-object-acls/spec.md`.

### LF-028 Smoke-test idempotency and fixtures

- **Problem**: `tests/smoke/uc-lakebase-smoke.sh` drops/recreates most
  objects but still assumes a fresh `.lakeforge/` for some checks (users,
  groups, external location names); `platform-smoke.sh` has the same
  property; both are bash with ad-hoc JSON parsing.
- **Evidence**: `tests/smoke/README.md` caveat; `lib.sh`.
- **Scope**: every section creates a unique namespace (`uc_smoke_<epoch>`) and
  cleans up at the end (or `--keep`); `lib.sh` helpers `expect_json`,
  `expect_status`, `wait_for`; optional `python` runner replacing bash if the
  team prefers (`tests/smoke/run.py`) — keep bash working in CI.
- **Proposed implementation**: namespace variable threaded through; a
  `cleanup()` trap.
- **Dependencies**: none (do before LF-025 puts it in CI).
- **Acceptance criteria**: running the script twice in a row against the same
  workspace passes both times with `failed=0`.
- **Focused tests**: the script.
- **Docs/parity**: `tests/smoke/README.md`.
- **OpenSpec**: n/a.

---

## Index by dependency order

| Wave | Issues | Why |
| --- | --- | --- |
| 0 | LF-028, LF-025, LF-026 | make the safety net reliable before touching semantics |
| 1 | LF-001, LF-002, LF-006, LF-013 | close the authorization/audit/Lakebase-lifecycle gaps that everything else builds on |
| 2 | LF-003, LF-004, LF-005, LF-007, LF-008, LF-014, LF-015, LF-016, LF-027 | policy semantics, attribution, Lakebase model, object ACLs |
| 3 | LF-009, LF-010, LF-011, LF-012, LF-020, LF-021, LF-022 | depth: column lineage, system-table cost, models, clients, UI |
| 4 | LF-017, LF-018, LF-019, LF-023, LF-024 | real data planes: PostgreSQL backend, sync, federation, deploy, sharing |
