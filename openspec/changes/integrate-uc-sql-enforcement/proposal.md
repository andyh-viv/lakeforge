# Change: integrate-uc-sql-enforcement

Status: **partially implemented** on branch `devin/1789466312-unity-catalog-lakebase`
(see "Done so far"). Remaining tasks are in `tasks.md`; each maps to an
issue in `docs/issues.md`.

## Why

Before this change Unity Catalog grants in Lakeforge were stored and returned
by `permissions`/`effective-permissions` but **enforced nowhere**: any
authenticated user could read or write any table from a notebook, job, SQL
editor or the statement API, and could call any UC REST route. Databricks'
core governance promise — one privilege model applied uniformly across the
REST API and every SQL surface, with row/column policies, audit, lineage and
system tables — was absent. Closing that gap is the prerequisite for the
platform being usable by more than one trusted user and for every later
governance feature (models in UC, federation, Delta Sharing).

## What Changes

1. **Authorization core** (`uc/privileges.rs`): full privilege vocabulary,
   securable hierarchy, ownership, `ALL_PRIVILEGES`, inheritance,
   `Authorizer` with `require`/`require_on_object`/`require_owner`/
   `require_use_path`, effective permissions with `inherited_from_type/name`.
2. **REST enforcement**: every `/api/2.1/unity-catalog/*` handler consults
   `Authorizer`; list endpoints filter to visible objects.
3. **Single SQL choke point** (`AppState::execute_sql → prepare_sql` in
   `uc/sqlauth.rs`): parse with `forge-sql`, resolve default catalog/schema
   from session conf, classify reads/writes/DDL/system-table/external-path
   access, authorize, then rewrite for policies and functions.
4. **SQL grammar for privileges** (`uc/grant_sql.rs`): `GRANT`, `REVOKE`,
   `SHOW GRANTS`, `ALTER … OWNER TO`, executed as metastore operations
   without touching Forge.
5. **Row filters / column masks** stored on the table, applied by query
   rewrite; **SQL UDFs** (`CREATE FUNCTION … RETURN …`) stored in UC and
   inlined; **session functions** (`current_user()` etc.) substituted.
6. **Audit** middleware + explicit `audit()` calls; **query history**;
   **table and column lineage** derived from the analysis; **system
   tables** and **information_schema** materialized to Parquet and
   registered in Forge.
7. **Extended UC objects**: tags, constraints, workspace bindings,
   temporary table credentials, artifact allowlists, system-schema
   enable/disable.

## Impact

- **Behaviour**: non-admin users are now denied SQL and REST access they
  were previously granted implicitly. Admins (`is_admin`) bypass all UC
  checks. Existing deployments must grant `USE CATALOG`/`USE SCHEMA`/`SELECT`
  (or make principals owners) before non-admins can query.
- **Code**: `crates/lakeforge-api/src/uc/*` (new), `api/catalog.rs`,
  `api/catalog_ext.rs`, `api/sql.rs`, `api/commands.rs`, `api/jobs.rs`
  (all SQL routes through `execute_sql`), `main.rs` (audit layer, routers),
  `forge-sql` (analysis structs, `sqlparser` upgrade).
- **Specs**: `unity-catalog-authorization`, `unity-catalog-policies`,
  `unity-catalog-audit-lineage`, `unity-catalog-system-tables`,
  `unity-catalog-models` (deltas in `specs/` here list what is still
  proposed vs. current).
- **Docs**: `docs/uc-lakebase-status.md`, `docs/parity.md`,
  `docs/api-surface.md`.
- **Tests**: `tests/smoke/uc-lakebase-smoke.sh` (50 checks); Rust unit tests
  in `grant_sql.rs`, `privileges.rs`, `sqlauth.rs`.

## Done so far (on branch, verified by the smoke suite)

- Items 1–7 above exist and pass `cargo test -p lakeforge-api`, `cargo
  clippy --workspace --all-targets -D warnings`, and the 50-check smoke
  suite (`passed=50 failed=0`) against a clean SQLite state.
- Non-admin denial, row-filter application, write denial and system-table
  write denial are exercised with a second user in the smoke suite.

## Not done (tracked)

| Gap | Issue |
| --- | --- |
| Authorization matrix tests (every securable × privilege × surface); `DENY`, `ON ALL TABLES IN SCHEMA`, grant option | LF-001, LF-002 |
| `ALTER TABLE … SET ROW FILTER / SET MASK` SQL syntax; policy functions with multiple args; mask on nested types | LF-003 |
| UDF `EXECUTE` checks on invocation, `DESCRIBE FUNCTION`, Python UDFs | LF-004 |
| `is_account_group_member` nested groups; `current_metastore()` | LF-005 |
| Audit of SQL/command/notebook routes via `audit()` completeness; Databricks `audit_level`, `response.result` | LF-006 |
| Query history: `statement_type`, `executed_as`, notebook/job attribution, `read_files/written_bytes` | LF-007 |
| Lineage: MERGE, CTAS with CTE, `INSERT … SELECT` across catalogs, view expansion; column lineage through expressions/joins | LF-008, LF-009 |
| System tables: incremental refresh, `billing.usage`, retention | LF-010 |
| `information_schema` semantics (views, routines params, privilege rows for inherited grants) | LF-011 |
| Models in UC (all routes) | LF-012 |
| Workspace object ACL enforcement (non-UC) | LF-027 |
| SDK, UI, CI integration | LF-020, LF-021, LF-025, LF-028 |

## Risks

- Every SQL statement now incurs a parse + authorization round trip against
  the document store; large notebooks with thousands of tiny statements may
  notice latency (mitigation: per-request privilege cache — LF-001).
- Query rewrite for masks/filters relies on `sqlparser` round-tripping. SQL
  that `sqlparser` cannot parse falls back to keyword/regex analysis
  (`sqlguard::analyze_fallback`): table privileges are still checked on the
  extracted names, but **no row filter / column mask rewrite is applied**
  (`Analysis.parsed == false`). LF-003 must decide between failing closed
  (deny unparsable statements that touch policy-protected tables) and
  extending the parser; until then treat policies as best-effort for
  engine-specific syntax.
- `sqlparser` upgrade changed AST shapes; keep `forge-sql` tests green when
  bumping again.
