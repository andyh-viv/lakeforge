# Continuation plan

How to take Lakeforge from its current state (see
[uc-lakebase-status.md](uc-lakebase-status.md) and [parity.md](parity.md)) to
Unity Catalog governance parity and a real Lakebase data plane, then onward.
Work items are the `LF-###` issues in [issues.md](issues.md); the
specifications they must satisfy are in [`openspec/`](../openspec/).

## Where the project is

- **Forge** (Rust engine): distributed DataFusion + Delta execution with
  driver/scheduler/executors/shuffle, gRPC control, local and Kubernetes
  cluster backends. Stable; not the focus of this plan (its gaps —
  streaming, `MERGE`, full PySpark — are listed in `parity.md`).
- **Control plane** (`lakeforge-api`): Databricks-compatible REST for the
  whole product surface, workspace UI, Python SDK/CLI, notebook kernels,
  jobs, pipelines, MLflow, serving, deploy assets. Merged in PR #1.
- **Unity Catalog governance** (this branch): privilege model with
  inheritance/ownership, enforced on UC REST and on *all* SQL through one
  choke point; SQL `GRANT/REVOKE/SHOW GRANTS/OWNER TO`; row filters, column
  masks, SQL UDFs, session functions by query rewrite; audit middleware;
  table/column lineage; system tables + `information_schema` queryable from
  SQL; tags, constraints, bindings, temp credentials, artifact allowlists.
  Validated by `tests/smoke/uc-lakebase-smoke.sh` (50/50).
- **Lakebase** (this branch): Databricks-shaped `/api/2.0/database/*`
  control plane with **metadata emulation** — no PostgreSQL yet.

## Target state

1. **UC governance parity** — every statement kind and securable authorised
   with Databricks semantics; policies attachable via SQL; complete privilege
   grammar; audit/lineage/history with Databricks schemas; system tables
   incremental and cheap; Models in UC.
2. **Lakebase data plane** — real PostgreSQL behind the same API (external
   managed server or in-cluster StatefulSet), credentials that are PostgreSQL
   passwords, synced tables that move data, database catalogs queryable from
   Forge via federation.
3. **Clients and UI** — SDK/CLI and Catalog/Lakebase UI expose all of the
   above; browser golden paths cover them.
4. **Trust boundaries** — workspace object ACLs enforced (LF-027) so the
   platform is usable by more than one trusted user.
5. **Sharing/federation** — Delta Sharing provider and PostgreSQL
   federation.

## Waves

Each wave is roughly one focused agent session (a few hours of build/test
loops). Waves are ordered by dependency; items inside a wave are independent
and can be parallelised across agents (one PR each, rebased on `main`).

### Wave 0 — safety net (do first)

| Issue | Outcome |
| --- | --- |
| LF-028 | smoke scripts idempotent (run twice → pass twice) |
| LF-025 | Rust integration harness for `AppState` + UC smoke in CI |
| LF-026 | docs drift checker; PR template |

Exit criteria: CI runs the UC/Lakebase smoke and the new integration tests;
`main` is green.

### Wave 1 — foundations

| Issue | Outcome |
| --- | --- |
| LF-001 | authorization matrix for all statement kinds, fail-closed |
| LF-002 | full `GRANT/REVOKE/SHOW GRANTS` grammar |
| LF-006 | audit events with request params, Databricks column names |
| LF-013 | Lakebase instance state machine as a pure function, worker-driven |

Exit criteria: ≥40-statement authorization fixture; Databricks doc examples
for privilege SQL parse; audit table schema matches Databricks; state
machine unit tests.

### Wave 2 — semantics

| Issue | Outcome |
| --- | --- |
| LF-003 | row filter / column mask SQL syntax, type checks, drop protection |
| LF-004 | UDF `EXECUTE`, `SHOW/DESCRIBE FUNCTION`, nested inlining |
| LF-005 | remaining session functions; `USE CATALOG/SCHEMA` per context |
| LF-007 | history enrichment + notebook/job attribution (`ExecContext`) |
| LF-008 | lineage through views and paths |
| LF-014 | scoped Lakebase credential tokens |
| LF-015 | Lakebase roles model |
| LF-016 | database catalogs: virtual schemas, delete guards |
| LF-027 | workspace object ACL enforcement |

Exit criteria: Databricks row-filter/mask tutorial works verbatim; job SQL is
attributed in `system.query.history`; non-admin cannot start an ACL'd
cluster.

### Wave 3 — depth

| Issue | Outcome |
| --- | --- |
| LF-009 | expression-level column lineage |
| LF-010 | incremental system-table refresh, real `billing.usage` |
| LF-011 | per-catalog `information_schema`, typed columns |
| LF-012 | Models in UC + MLflow `databricks-uc` registry URI |
| LF-020 | SDK/CLI services for UC extensions and Lakebase |
| LF-021 | Catalog UI: permissions editor, lineage, tags, policies, audit |
| LF-022 | Lakebase UI |

Exit criteria: browser golden paths for Catalog governance and Lakebase;
`mlflow.register_model("main.ml.x")` works.

### Wave 4 — data planes

| Issue | Outcome |
| --- | --- |
| LF-017 | `LakebaseBackend` trait; external PostgreSQL provisions DBs/roles; credentials are PG passwords |
| LF-018 | synced tables actually sync (`SNAPSHOT`/`TRIGGERED`; `CONTINUOUS` as interval) |
| LF-019 | Forge PostgreSQL table provider → foreign catalogs and Lakebase reads |
| LF-023 | Helm/Terraform/Compose wiring for Lakebase PostgreSQL |
| LF-024 | Delta Sharing provider + protocol server |

Exit criteria: `psql` connects with a Lakebase credential; a synced table's
row count matches in PostgreSQL; `SELECT` from a foreign catalog returns
PostgreSQL rows; `delta-sharing` client reads a share.

## Working agreement for agents

1. Start from `main` (after this branch's PR merges) on a
   `devin/<epoch>-<slug>` branch; one issue per PR unless issues are
   trivially coupled.
2. Read `docs/development.md`, the issue, and the OpenSpec spec(s) it cites.
   If the spec lacks a scenario you need, add it in
   `openspec/changes/<change>/specs/<capability>/spec.md` as an
   `## ADDED|MODIFIED Requirements` delta **before** coding.
3. Keep the choke points: never bypass `execute_sql`/`prepare_sql` for SQL,
   never mutate UC docs without the `Authorizer`.
4. Before opening the PR: clippy clean, unit tests, the smoke scripts against
   a fresh `.lakeforge/`, and the doc rows updated. Paste the smoke summary
   (`passed=N failed=0`) in the PR description.
5. Mark the issue in `docs/issues.md` as done (strike-through title + PR
   link) and tick tasks in `openspec/changes/*/tasks.md`.
6. Never claim "Databricks parity" for a row without a test that demonstrates
   the Databricks-documented behaviour.

## Explicit non-goals for these waves

- Structured Streaming, Delta `MERGE`/`OPTIMIZE`/`VACUUM`, PySpark DataFrame
  API (engine work tracked in `parity.md`, not here).
- Serverless compute, SSO/SAML, account-level multi-workspace.
- Vector Search, Feature Store, Mosaic AI, Genie.
- Cloud IAM credential vending for temporary table credentials (requires
  per-cloud STS/SAS integration; design after LF-017 settles the backend
  abstraction pattern).

## Estimates

Per wave, one agent session with a normal build/test loop; Wave 4 items
involving PostgreSQL integration tests and Terraform changes may take two.
External waits: none for Waves 0–3; Wave 4 needs a PostgreSQL container in CI
(service container — no external approval) and, for cloud validation, real
AWS/GCP/Azure credentials.
