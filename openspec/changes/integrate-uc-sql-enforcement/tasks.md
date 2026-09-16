# Tasks: integrate-uc-sql-enforcement

Tick items as they land. `[x]` = on branch and covered by
`tests/smoke/uc-lakebase-smoke.sh` or `cargo test -p lakeforge-api`.
Each open item names its `docs/issues.md` entry.

## 1. Authorization core

- [x] 1.1 Privilege vocabulary, `Securable`, hierarchy, ownership,
      `ALL_PRIVILEGES`, inheritance (`uc/privileges.rs`)
- [x] 1.2 `Authorizer` with `require`, `require_on_object`, `require_owner`,
      `require_use_path`, admin bypass
- [x] 1.3 `effective-permissions` with `inherited_from_type/name`
- [x] 1.4 Principal existence check on grant (`principal_exists`)
- [ ] 1.5 Per-request privilege cache in `Authorizer` (LF-001)
- [ ] 1.6 Authorization matrix test: securable × privilege × {REST, SQL}
      table-driven (`uc/tests/matrix.rs`) (LF-001)
- [ ] 1.7 `SHOW GRANTS TO <principal>` metastore-wide; `ON ALL TABLES IN
      SCHEMA`; `DENY` decision recorded in spec (LF-002)

## 2. REST enforcement

- [x] 2.1 All `/api/2.1/unity-catalog/*` handlers call `Authorizer`
- [x] 2.2 List endpoints filter to visible objects
- [x] 2.3 Owner change requires owner/admin; owner must exist
- [ ] 2.4 `securable_type = MODEL` routes (LF-012)
- [ ] 2.5 Workspace object ACL enforcement on notebooks/jobs/clusters/…
      (`api/permissions.rs::require_object_permission`) (LF-027)

## 3. SQL choke point

- [x] 3.1 `AppState::execute_sql → prepare_sql`; all SQL surfaces routed
- [x] 3.2 `sqlguard::analyze` with `sqlparser` + text fallback
- [x] 3.3 Reads/writes/DDL/system-table/external-path authorization
- [x] 3.4 `observe_ddl` mirrors CREATE/DROP into UC docs
- [x] 3.5 `grant_sql.rs`: GRANT/REVOKE/SHOW GRANTS/ALTER OWNER →
      `MetastoreOp::Grant`
- [ ] 3.6 Fail closed when `Analysis.parsed == false` and a referenced table
      has a row filter or mask (non-owner) (LF-003)
- [ ] 3.7 Extend grammar: `USE CATALOG/SCHEMA` statements update session
      conf; `DESCRIBE FUNCTION`; `SHOW FUNCTIONS` filtered by privilege
      (LF-002, LF-004)
- [ ] 3.8 `EXECUTE` check on every inlined UDF call (LF-004)

## 4. Policies and functions

- [x] 4.1 Row filter / column mask storage + REST routes
- [x] 4.2 Query rewrite for filters, masks, UDF inlining, session functions
- [x] 4.3 `CREATE [OR REPLACE] FUNCTION … RETURN expr` / `DROP FUNCTION` as
      metastore ops
- [ ] 4.4 `ALTER TABLE … SET/DROP ROW FILTER`, `ALTER COLUMN … SET/DROP
      MASK` SQL (LF-003)
- [ ] 4.5 Mask return-type check; block dropping referenced function
      (LF-003)
- [ ] 4.6 Row-filter smoke check with a non-admin principal (LF-003)
- [ ] 4.7 `is_member`, `session_user`, `current_metastore` (LF-005)

## 5. Audit, history, lineage

- [x] 5.1 Audit middleware (mutating + denied), redaction, retention cap
- [x] 5.2 SQL `sqlStatement`/`sqlStatementDenied` events
- [x] 5.3 Query history documents; `system.query.history`
- [x] 5.4 Table + column lineage from `Analysis`; lineage-tracking API
- [ ] 5.5 Audit events for SQL/command/notebook routes carry
      `response.result` and Databricks `audit_level` (LF-006)
- [ ] 5.6 History: `statement_type`, `executed_as`, `client_application`,
      notebook/job ids from `LineageContext` (LF-007)
- [ ] 5.7 Notebook kernel and job runner populate
      `lakeforge.entity_type/id/run_id` conf (LF-007, LF-008)
- [ ] 5.8 Lineage for CTAS-with-CTE, `INSERT … SELECT` across catalogs,
      views; column lineage through expressions/joins (LF-008, LF-009)

## 6. System tables and information_schema

- [x] 6.1 Registry, Parquet materialization, Forge registration, refresh
      routes
- [x] 6.2 `information_schema.*` tables with Databricks columns
- [ ] 6.3 Incremental refresh for `access.audit`, `query.history`,
      lineage (append-only) (LF-010)
- [ ] 6.4 `billing.usage` from cluster/warehouse runtime (LF-010)
- [ ] 6.5 `information_schema` privilege rows include inherited grants;
      `views.view_definition`; `parameters` (LF-011)
- [ ] 6.6 `system.access.*` visibility: non-admins see own rows only
      (LF-010)

## 7. Extended UC objects

- [x] 7.1 Tags, constraints, workspace bindings, temp table credentials,
      artifact allowlists, system-schema enable/disable
- [ ] 7.2 Models in UC: routes, versions, aliases, MLflow bridge, serving
      (LF-012)

## 8. Clients, UI, CI

- [ ] 8.1 Python SDK services: `grants`, `row_filters`, `column_masks`,
      `lineage`, `audit`, `system_tables`, `tags`, `constraints` (LF-020)
- [ ] 8.2 Catalog UI: Permissions tab with grant editor, Lineage tab, Tags,
      Policies, Audit page (LF-021)
- [ ] 8.3 CI: run `uc-lakebase-smoke.sh` in `.github/workflows/ci.yml`
      (LF-025); unique namespaces per run (LF-028)
- [ ] 8.4 Rust integration tests spinning `lakeforge-api` in-process
      (LF-025)

## 9. Docs

- [x] 9.1 `docs/uc-lakebase-status.md`, `docs/parity.md`,
      `docs/api-surface.md`, `docs/architecture.md`
- [ ] 9.2 Update status/parity rows and this file as items close (LF-026)
- [ ] 9.3 On completion: apply deltas in `specs/` here to
      `openspec/specs/`, archive this change
