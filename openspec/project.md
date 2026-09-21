# Project Context

## Purpose

Lakeforge is an open, self-hostable Databricks-compatible lakehouse platform:
a Rust distributed compute engine (**Forge**, DataFusion + Delta Lake) and a
control plane (**lakeforge-api**) that serves Databricks-shaped REST APIs, a
workspace UI, notebook kernels, jobs, SQL warehouses, Unity Catalog
governance, MLflow, serving, pipelines and Lakebase. The goal is behavioural
compatibility with Databricks (same API calls, same SQL, same governance
model) so existing clients, SDKs and skills work unchanged, deployable with
one command to AWS, GCP, Azure or Kubernetes.

Parity is tracked honestly in `docs/parity.md` and
`docs/uc-lakebase-status.md`; specs in this directory describe what the
system **does today** (`specs/`) and what changes are proposed (`changes/`).

## Tech Stack

- Rust 1.85+ workspace: `axum` 0.8 (HTTP), `sqlx` (SQLite/PostgreSQL via
  `Any`), `tokio`, `tonic`/`prost` (gRPC), DataFusion 53, Arrow 58,
  `deltalake` 0.32, `sqlparser`, `object_store`, `serde_json`.
- Web: React 19, Vite, TypeScript, TanStack Query.
- Python 3.10+: `lakeforge-sdk` (`WorkspaceClient`, CLI, `dbutils`), notebook
  kernel (`python/lakeforge_kernel.py`).
- Deploy: Docker, docker-compose, Helm, Terraform (AWS/GCP/Azure), GitHub
  Actions.

## Project Conventions

### Code Style

- Rust: clippy clean with `-D warnings`; long lines tolerated (no
  `rustfmt.toml` yet); no `unwrap()` on request data; errors are `ApiError`
  variants mapping to Databricks `error_code`s.
- Handlers are thin; behaviour lives in `impl AppState` methods
  (`crates/lakeforge-api/src/api/*.rs`, `uc/*.rs`).
- Every persisted entity is a JSON document (`Doc<T>` in `store.rs`) with a
  `kind` constant (`KIND_*`), a workspace id, optional parent id and name.
- Databricks route shapes and field names are used verbatim; Lakeforge-only
  extensions live under `/api/2.0/lakeforge/…`.
- TypeScript: ESLint config in `web/`; components per page under
  `web/src/pages`.
- Python: `ruff`-clean, dataclasses for responses, positional-only path
  parameters in SDK endpoint helpers.

### Architecture Patterns

- **Single SQL choke point**: all SQL goes through
  `AppState::execute_sql → prepare_sql` (authorize → rewrite → metastore op
  or Forge). Never call the Forge driver from a handler.
- **Authorizer**: `uc::privileges::Authorizer` evaluates ownership, direct and
  inherited grants and `ALL_PRIVILEGES`; UC handlers call `require`.
- **Virtual documents**: system catalog/schemas, `information_schema` and
  database-catalog schemas are synthesised, not stored.
- **Backends behind traits/enums**: cluster manager (`local` | `kubernetes`),
  Lakebase (`emulated` | `external`), store dialect (`sqlite` | `postgres`).
- **Observability from the choke point**: query history, audit, lineage and
  UC DDL mirroring are side effects of `execute_sql`, not of handlers.

### Testing Strategy

- Unit tests co-located (`#[cfg(test)]`) for parsers, analysis and pure
  logic (`uc::sqlguard`, `uc::grant_sql`, `uc::privileges`).
- End-to-end bash smoke scripts in `tests/smoke/` run against a live API and
  print `passed=N failed=M`; `uc-lakebase-smoke.sh` is the acceptance gate
  for governance and Lakebase work.
- Python SDK tests with `pytest`; web `npm run lint && npm run build`.
- Browser golden paths via `.agents/skills/testing-workspace/SKILL.md`.
- CI: `.github/workflows/ci.yml` (Rust build/test/clippy, distributed SQL
  smoke, control-plane smoke, Python, web, Helm/Terraform/compose validation).

### Git Workflow

- Branch `devin/<epoch>-<slug>` from `main`; one issue (`docs/issues.md`
  `LF-###`) per PR; PR description includes the smoke summary.
- Never amend or force-push shared branches; never skip hooks.
- Update `docs/uc-lakebase-status.md` / `docs/parity.md` in the same PR as
  behaviour changes; archive OpenSpec changes after merge.

## Domain Context

- **Securable hierarchy**: metastore → catalog → schema → {table, view,
  volume, function, model}; plus metastore-level external locations, storage
  credentials, connections, shares, recipients, providers.
- **Privileges** follow Databricks names (`USE_CATALOG`, `USE_SCHEMA`,
  `SELECT`, `MODIFY`, `CREATE_TABLE`, `EXECUTE`, `READ_VOLUME`, `MANAGE`,
  `ALL_PRIVILEGES`, …); owners hold all privileges; grants inherit downward.
- **Principals**: users (email), groups (display name; built-ins `users`,
  `account users`, `admins`), service principals (application id).
- **Lakebase**: Databricks' managed PostgreSQL. Objects: database instance
  (capacity `CU_1..CU_8`, PG 16), instance roles, credentials (short-lived
  tokens used as PostgreSQL passwords), database catalogs (UC catalog of type
  `DATABASE_CATALOG`), synced tables (Delta → PostgreSQL by
  `SNAPSHOT`/`TRIGGERED`/`CONTINUOUS`), database tables.
- **System tables**: `system.<schema>.<table>` (access, query, compute,
  lakeflow, billing, mlflow, serving, lakebase) and `information_schema`.

## Important Constraints

- Do not claim Databricks parity for behaviour without a test demonstrating
  the documented Databricks behaviour.
- Lakebase `emulated` backend must never be presented as a reachable
  PostgreSQL; every credential response carries `backend`.
- Secrets never appear in audit events, history, logs or docs.
- SQLite and PostgreSQL must both work for the control-plane store.
- Constraints (PK/FK/CHECK) are metadata only until the engine enforces
  them; docs must say so.

## External Dependencies

- Forge driver gRPC (`crates/forge-proto/proto/forge.proto`) — internal.
- Object storage via `object_store` (`file://`, `s3://`, `gs://`, `az://`).
- Optional external PostgreSQL for the store (`LAKEFORGE_DATABASE_URL`) and
  for Lakebase (`LAKEFORGE_LAKEBASE_POSTGRES_URL`).
- Kubernetes API (cluster manager `kubernetes` backend).
- Python runtime for kernels and serving workers.
