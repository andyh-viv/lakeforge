# Developer guide

How to build, run, test and change Lakeforge locally. Read
[architecture.md](architecture.md) first for the shape of the system and
[handoff.md](handoff.md) if you are picking the project up cold.

## Prerequisites

| Tool | Version | Notes |
| --- | --- | --- |
| Rust | stable (1.85+) | `rustup`; `clippy` and `rustfmt` components |
| `protoc` | 3.x+ | `apt-get install protobuf-compiler` — needed by `forge-proto` |
| Node | 24.x | `nvm use 24` — the web UI (`web/`) |
| Python | 3.9 – 3.12 | SDK, kernel, tests (`pip install -e "python/lakeforge-sdk[dev]"`) |
| Docker / Helm / Terraform | optional | only for `deploy/` work |

The first `cargo build` compiles DataFusion, Arrow and delta-rs and takes
10–20 minutes on a laptop; subsequent builds are incremental. `target/` is
large (>10 GB) — keep it between sessions.

## Repository layout

```
Cargo.toml                 workspace: crates/* (edition 2021, resolver 2)
crates/
  forge-proto/             gRPC contract (proto/forge.proto → tonic/prost)
  forge-common/            FORGE_* config, ids, errors
  forge-sql/               DataFusion session + object_store + Delta provider
  forge-shuffle/           ShuffleWriterExec / ShuffleReaderExec (Arrow IPC)
  forge-scheduler/         stage planner + task scheduler
  forge-executor/          executor daemon
  forge-driver/            driver daemon (sessions, catalog, results)
  forge-client/            async Rust client
  forge-cli/               `forge` binary
  lakeforge-cluster-manager/  local-process + Kubernetes cluster backends
  lakeforge-api/           control plane (axum) — see below
web/                       React 19 + Vite + TypeScript workspace UI
python/
  lakeforge-sdk/           WorkspaceClient, dbutils, `lakeforge` CLI, tests
  lakeforge_kernel.py      notebook kernel launched by the API
deploy/
  docker/                  Dockerfile.api, Dockerfile.forge, docker-compose.yml
  helm/lakeforge/          Helm chart
  terraform/               aws/ gcp/ azure/ roots + modules/lakeforge
  deploy.sh                one-click wrapper
tests/smoke/               shell/API smoke tests (need a running API)
docs/                      this documentation
openspec/                  OpenSpec specs + change proposals for future agents
.github/workflows/         ci.yml (build/test/lint/smoke), images.yml (GHCR)
```

### `crates/lakeforge-api/src`

```
main.rs        clap → Config → AppState → axum serve
config.rs      LAKEFORGE_* flags (bind, database_url, storage_root, admin creds,
               jwt secret, forge bin, k8s, lakebase_postgres_url, …)
state.rs       AppState { store, storage, config, forge client, cluster manager, kernels, … }
store.rs       Store: JSON document store over sqlx::Any (SQLite | PostgreSQL)
storage.rs     object_store root (file:// | s3:// | gs:// | az://)
auth.rs        Principal, passwords, JWT, PATs, auth_middleware
error.rs       ApiError → Databricks-style { error_code, message }
forge.rs       run_sql(): SQL → Forge driver gRPC → SqlResult { columns, rows }
kernel.rs      Python notebook kernel processes + SSE output
workers.rs     background loops (cluster health, job scheduler, GC)
api/           one module per Databricks service (see api-surface.md)
uc/            Unity Catalog governance:
  privileges.rs    Securable, privilege set, Authorizer (owner/inherit/ALL)
  sqlguard.rs      SQL analysis (reads/writes/creates/paths/column edges),
                   row-filter/column-mask/UDF/session-function rewriting
  sqlauth.rs       prepare_sql(): analyse → authorise → rewrite; metastore ops
  grant_sql.rs     GRANT / REVOKE / SHOW GRANTS / ALTER … OWNER TO parser
  audit.rs         audit_middleware + audit event store
  lineage.rs       table/column lineage capture + query
  system_tables.rs system.* + information_schema virtual/materialised tables
  models.rs        Models-in-UC — placeholder (constant only)
```

## Build

```bash
cargo build -p lakeforge-api -p forge-cli          # dev profile, what the smoke tests use
cargo build --release -p lakeforge-api -p forge-cli
(cd web && npm ci && npm run build)                # → web/dist, served by the API
pip install -e "python/lakeforge-sdk[dev]"
```

## Run locally

```bash
# from the repo root; state lives under ./.lakeforge (SQLite DB, storage, cluster work dirs)
LAKEFORGE_FORGE_BIN=target/debug/forge \
LAKEFORGE_UI_DIR=web/dist \
target/debug/lakeforge-api
# → http://localhost:8080   admin@lakeforge.local / admin
```

Reset state with `rm -rf .lakeforge`. Every option is a flag or an env var
(`target/debug/lakeforge-api --help`); the important ones:

| Env | Default | Purpose |
| --- | --- | --- |
| `LAKEFORGE_BIND` | `0.0.0.0:8080` | listen address |
| `LAKEFORGE_DATABASE_URL` | `sqlite://.lakeforge/lakeforge.db?mode=rwc` | `postgres://…` in production |
| `LAKEFORGE_STORAGE_ROOT` | `.lakeforge/storage` | `s3://bucket/prefix`, `gs://…`, `az://…` |
| `LAKEFORGE_ADMIN_USER` / `LAKEFORGE_ADMIN_PASSWORD` | `admin@lakeforge.local` / `admin` | first-boot admin (dev only) |
| `LAKEFORGE_JWT_SECRET` | random per boot | **set in production** — also derives the secrets-encryption key |
| `LAKEFORGE_FORGE_BIN` | `forge` on PATH | engine binary for the local cluster backend |
| `LAKEFORGE_CLUSTER_BACKEND` | `local` | `kubernetes` in Helm deployments |
| `LAKEFORGE_UI_DIR` | unset (API only) | static SPA directory, e.g. `web/dist` |
| `LAKEFORGE_LAKEBASE_POSTGRES_URL` | unset | marks the Lakebase backend as `external` (metadata only; see uc-lakebase-status.md) |

Useful API calls once it is up:

```bash
TOK=$(curl -s -X POST localhost:8080/api/2.0/lakeforge/login -H 'content-type: application/json' \
      -d '{"username":"admin@lakeforge.local","password":"admin"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
curl -s localhost:8080/api/2.0/clusters/list -H "Authorization: Bearer $TOK"
curl -s -X POST localhost:8080/api/2.0/sql/statements -H "Authorization: Bearer $TOK" -H 'content-type: application/json' \
     -d '{"warehouse_id":"<id>","statement":"SELECT 1","wait_timeout":"30s"}'
```

## Test

| What | Command | Expect |
| --- | --- | --- |
| Rust unit tests | `cargo test --workspace` | all green (`lakeforge-api` has 31 incl. SQL guard + grant parser) |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | clean (a `proc-macro-error2` future-incompat *note* from a dependency is expected) |
| Python | `python -m pytest -q python/lakeforge-sdk/tests` and `python -m compileall -q python/lakeforge-sdk/lakeforge` | 9 passed |
| Web | `cd web && npm run lint && npm run build` | build OK; `oxlint` reports pre-existing warnings only (only-export-components, set-state-in-effect, jsx-key, `Date.now` purity) and no errors |
| Helm / Terraform | `helm lint deploy/helm/lakeforge`; `terraform init -backend=false && terraform validate` in each root | clean |
| Platform smoke | `bash tests/smoke/platform-smoke.sh` (API running, fresh state) | `passed=N failed=0` |
| UC + Lakebase smoke | `bash tests/smoke/uc-lakebase-smoke.sh` (API running) | `passed=50 failed=0` |
| Browser golden path | `.agents/skills/testing-workspace/SKILL.md` | manual / agent-driven |
| Docs drift | `bash scripts/check-docs.sh` + `bash tests/check-docs.sh` (fixtures) | warn-only; `--strict` exits 1 on findings |

Smoke tests are the only end-to-end coverage of SQL authorisation, policies,
lineage, system tables and Lakebase today; unit coverage of those paths is an
open issue (see [issues.md](issues.md) #24).

## Conventions

- **Documents, not tables.** Control-plane entities are `Doc<T>` JSON rows
  (`kind`, `workspace_id`, `parent_id`, `name`, `data`) in one table; add a new
  resource by choosing a `KIND_*` constant and using `Store::{insert,get,list,
  update,delete,count}` with `Filter`s. Do not add SQL migrations for new kinds.
- **Databricks shapes first.** Route paths, request/response JSON and error
  codes follow the public Databricks REST reference so the official SDK/CLI
  can be pointed at Lakeforge. Lakeforge-only extensions live under
  `/api/2.0/lakeforge/**`.
- **Authorisation.** REST handlers take `Who(p): Who` (a `Principal`) and call
  `Authorizer` (`uc/privileges.rs`) for UC objects or `permissions.rs` for
  workspace ACLs. Every SQL statement — notebooks, jobs, SQL editor, statement
  API — goes through `AppState::execute_sql` → `prepare_sql`; never call the
  Forge driver directly from a handler.
- **Errors.** Return `ApiError::{invalid, NotFound, PermissionDenied, …}`;
  UC denials use the Databricks `[INSUFFICIENT_PERMISSIONS] …` message shape.
- **Style.** Long lines are common (the code base was written without a
  `rustfmt.toml`; a future agent may add one and reformat in a dedicated
  commit). Clippy clean with `-D warnings`; no `unwrap()` on request data.
- **Comments.** Terse, describe the code not the change; module-level `//!`
  docs on every file.
- **Tests.** Unit tests live next to the code (`#[cfg(test)] mod tests`);
  end-to-end behaviour goes in `tests/smoke/*.sh` as `check "name" "$(cmd)"
  'expected-substring'` lines, kept idempotent.

### Docs drift checker

`scripts/check-docs.sh` keeps the two most drift-prone documents honest:

- **Routes**: every path registered with `.route("…")` under
  `crates/lakeforge-api/src/api/*.rs` (plus `lib.rs`/`api/mod.rs`) is compared with
  the backticked paths in `docs/api-surface.md`. Documented shorthand is expanded
  before comparison — `{create,list,delete}` alternation and `[/{id}]` optionals —
  so a family entry covers its real routes. A documented entry that is a strict
  prefix of a registered route is reported separately as namespace/family
  notation rather than as drift. Path parameters are compared as `{name}` and
  `:name` equivalently, and trailing slashes are ignored.
- **Issue references**: every `LF-###` defined in `docs/issues.md` must either be
  referenced from an OpenSpec change under `openspec/changes/` or be marked done
  (strikethrough title in the issue list).

The checker reports what it could **not** parse (currently: nested shorthand
groups) instead of silently skipping it, and CI runs it **warn-only**
(`.github/workflows/ci.yml`, job `docs-drift`, `continue-on-error: true`). Run it
with `--strict` to make it fail, and `--root DIR` to run it against a fixture
tree (that is what `tests/check-docs.sh` does).

Adding a route without documenting it, or deleting a documented one, is drift the
checker will report — update `docs/api-surface.md` in the same PR rather than
leaving the finding.

## Adding things — recipes

**A new REST service.** Create `api/<svc>.rs` with a `router() -> Router<S>`,
mount it in `api/mod.rs`, add SDK methods in `python/lakeforge-sdk/lakeforge/
services.py`, a page or panel in `web/src/pages`, a parity row in
`docs/parity.md`, and a smoke check.

**A new UC securable or privilege.** Extend `Securable` / the privilege list in
`uc/privileges.rs` (`lineage_of` decides inheritance), the `KIND_*` mapping in
`api/catalog.rs`, the securable keywords in `uc/grant_sql.rs`, and the
`information_schema.*_privileges` table in `uc/system_tables.rs`.

**A new system table.** Add it to `tables()` in `uc/system_tables.rs`
(schema, name, columns) and a row producer in `system_table_rows`; it becomes
visible under `system.<schema>` once the schema is enabled and is materialised
on demand before queries that reference it.

**A new SQL statement the engine cannot run.** Detect it in
`sqlauth::prepare_sql` (or `grant_sql.rs`), return a `MetastoreOp`, and
implement it in `apply_metastore_op` returning a `MetastoreOutput` (rows are
returned to the caller as a normal result set).

## Debugging

- API log: stdout of `lakeforge-api` (`RUST_LOG=lakeforge_api=debug,forge=info`).
- Forge cluster logs: `.lakeforge/work/<cluster-id>/{driver,executor-*}.log`.
- SQL denials are recorded as `unityCatalog.sqlStatementDenied` audit events:
  `GET /api/2.0/lakeforge/audit?service=unityCatalog`.
- `forge sql --explain -e '…'` against a running driver prints the physical
  plan and stage DAG.
