# Lakeforge

Lakeforge is an open, self-hostable data & AI platform that speaks the
Databricks REST API and ships its own distributed query engine, **Forge**,
written in Rust on Apache Arrow / DataFusion / Delta Lake.

It deploys as a single control-plane image plus a compute image, onto a laptop
(Docker Compose), any Kubernetes cluster (Helm), or a fully provisioned
**AWS / GCP / Azure** stack with one command.

```
deploy/deploy.sh local                      # docker compose, http://localhost:8080
deploy/deploy.sh aws   --region us-east-1   # VPC + EKS + RDS + S3 + Helm
deploy/deploy.sh gcp   --project my-proj    # VPC + GKE + Cloud SQL + GCS + Helm
deploy/deploy.sh azure --subscription_id … # RG + AKS + Flexible Server + ADLS + Helm
```

Default login: `admin@lakeforge.local` / `admin` (local only — cloud targets
generate a password and print it as a Terraform output).

## What is in the box

| Layer | Component | Where |
| --- | --- | --- |
| Compute engine | **Forge**: driver, stage scheduler, executors, Arrow IPC shuffle, Delta Lake tables, gRPC protocol, `forge` CLI | `crates/forge-*` |
| Control plane | `lakeforge-api`: Databricks-compatible REST API (axum, SQLite or PostgreSQL) — clusters, jobs, workspace, notebooks, SQL warehouses & statements, Unity Catalog, secrets, tokens, DBFS/Files, MLflow tracking & registry, repos, DLT pipelines, model serving, SCIM, permissions | `crates/lakeforge-api` |
| Cluster manager | Launches Forge clusters as local processes or Kubernetes Deployments | `crates/lakeforge-cluster-manager` |
| Workspace UI | React/Vite single-page app served by the API: notebooks, SQL editor, catalog explorer, compute, workflows, pipelines, experiments, models, serving, admin | `web/` |
| Python | `lakeforge-sdk` (`WorkspaceClient`, `dbutils`, `lakeforge` CLI) and the notebook kernel | `python/` |
| Deploy | Dockerfiles, Compose, Helm chart, Terraform for AWS/GCP/Azure, `deploy.sh`, GitHub Actions | `deploy/`, `.github/` |

See [docs/architecture.md](docs/architecture.md) for how the pieces fit,
[docs/deploy.md](docs/deploy.md) for every deployment path and
[docs/parity.md](docs/parity.md) for an honest Databricks feature-parity matrix.

## Quick start (from source)

Prerequisites: Rust (stable), `protoc`, Node 24, Python 3.9+.

```bash
# 1. build the engine and control plane
cargo build --release -p forge-cli -p lakeforge-api

# 2. build the web UI (served by the API from web/dist)
(cd web && npm ci && npm run build)

# 3. run the control plane (SQLite + local Forge clusters under .lakeforge/)
LAKEFORGE_FORGE_BIN=target/release/forge target/release/lakeforge-api
# → http://localhost:8080  (admin@lakeforge.local / admin)
```

Then from Python:

```bash
pip install -e python/lakeforge-sdk
export LAKEFORGE_HOST=http://localhost:8080 LAKEFORGE_USER=admin@lakeforge.local LAKEFORGE_PASSWORD=admin

python - <<'EOF'
from lakeforge import WorkspaceClient
w = WorkspaceClient()                       # or WorkspaceClient(host=..., token="dapi...")
print(w.current_user.me())
print(w.statement_execution.rows("SELECT 40 + 2 AS answer"))   # [{'answer': 42}]
EOF

lakeforge clusters list                      # CLI; add `-o json` for machine output
lakeforge tokens create --comment ci         # mint a PAT (dapi...) for DATABRICKS_TOKEN
```

The REST surface follows Databricks' paths and payloads, so the official
`databricks-sdk` / `databricks` CLI can be pointed at Lakeforge via
`DATABRICKS_HOST` / `DATABRICKS_TOKEN` for the endpoints listed in the parity
matrix (compatibility is tested with Lakeforge's own SDK; the official clients
are not part of CI).

## Running Forge standalone

```bash
forge local --executors 2 --slots 2                 # driver + 2 executors in one process tree
forge sql -e "SELECT 40 + 2 AS answer" --json       # distributed SQL against the driver
forge register trips --format delta --location s3://lake/trips   # register a table in the driver catalog
forge sql -e "SELECT count(*) FROM trips" --explain # show the physical + distributed stage plan
forge executors && forge jobs                       # inspect the cluster
```

## Repository layout

```
crates/            Rust workspace (Forge engine + control plane)
web/               Workspace UI (React 19, Vite, TypeScript)
python/            lakeforge-sdk, dbutils, CLI, notebook kernel
deploy/docker/     Dockerfile.api, Dockerfile.forge, docker-compose.yml
deploy/helm/       lakeforge Helm chart
deploy/terraform/  aws/, gcp/, azure/ roots + shared modules/lakeforge
deploy/deploy.sh   one-click deploy / destroy for every target
docs/              architecture, deployment, parity matrix
```

## Development

```bash
cargo test --workspace                         # engine + control plane tests
cargo clippy --workspace --all-targets -- -D warnings
(cd python/lakeforge-sdk && python -m pytest)  # SDK tests
(cd web && npm run lint && npm run build)      # UI
helm lint deploy/helm/lakeforge
for d in deploy/terraform/{modules/lakeforge,aws,gcp,azure}; do (cd $d && terraform init -backend=false && terraform validate); done
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs all of the
above plus a distributed-SQL smoke test and a control-plane API smoke test on
every push; [`images.yml`](.github/workflows/images.yml) publishes
`ghcr.io/<owner>/lakeforge-api` and `ghcr.io/<owner>/lakeforge-forge` on `main`
and on `v*` tags.

## Status

Lakeforge is a working platform, not a finished Databricks replacement. The
core loop — log in, create a cluster, run notebooks and SQL against Delta
tables in Unity Catalog, schedule multi-task jobs, track MLflow experiments,
register and serve models, deploy to a cloud — works end to end. Many
Databricks features are implemented at API level only, approximated, or
missing; [docs/parity.md](docs/parity.md) lists each one with its status.

## License

Apache-2.0.
