# Smoke tests

Shell/API smoke tests that run against a **local `lakeforge-api`** on
`http://localhost:8080` with the development admin credentials
(`admin@lakeforge.local` / `admin`). They are not unit tests: they need a
running control plane and a local cluster backend (they create a cluster on
first run and wait for it to be `RUNNING`).

```bash
# 1. build + start the API from a clean state (see docs/development.md)
cargo build -p lakeforge-api -p forge-cli
rm -rf .lakeforge && LAKEFORGE_UI_DIR=web/dist target/debug/lakeforge-api &

# 2. run
bash tests/smoke/platform-smoke.sh      # clusters, SQL/Delta, notebooks, jobs, pipelines, UC, MLflow, repos, serving
bash tests/smoke/uc-lakebase-smoke.sh   # UC grants/SQL enforcement, policies, lineage, audit, system tables, Lakebase
```

Both print `passed=N failed=M` and exit non-zero on failure. Run them against a
**fresh** `.lakeforge/` state: several checks create objects and are not
idempotent (a second run fails on "already exists" for those steps only).

Env overrides: `LF_URL` (default `http://localhost:8080`), `LF_TOKEN_FILE`.
