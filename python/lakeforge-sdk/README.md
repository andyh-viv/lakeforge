# lakeforge-sdk

Python SDK, `dbutils` and command-line client for [Lakeforge](../../README.md) — a
Databricks-compatible lakehouse platform with the Forge (Rust) compute engine.

The package has **no runtime dependencies** beyond the Python standard library
(Python 3.9+). `pandas` is optional.

```bash
pip install -e python/lakeforge-sdk          # from the repo
pip install -e 'python/lakeforge-sdk[dev]'   # + pytest
```

## Authentication

Credentials are resolved in this order:

1. explicit `WorkspaceClient(host=..., token=... | username=..., password=...)`
2. `LAKEFORGE_HOST` / `LAKEFORGE_TOKEN` / `LAKEFORGE_USER` / `LAKEFORGE_PASSWORD`
3. `DATABRICKS_HOST` / `DATABRICKS_TOKEN` / `DATABRICKS_USERNAME` / `DATABRICKS_PASSWORD`
4. `~/.lakeforgecfg` or `~/.databrickscfg` (`--profile`, default `DEFAULT`)

Inside a Lakeforge notebook the host and a session token are pre-set, so
`WorkspaceClient()` works with no configuration.

```bash
lakeforge auth login --host http://localhost:8080 -u admin@lakeforge.local --pat
```

## SDK

```python
from lakeforge import WorkspaceClient

w = WorkspaceClient()                       # or WorkspaceClient(host=..., token=...)

w.sql("SELECT 1 AS one")                    # -> [{"one": 1}]  (default SQL warehouse)
w.clusters.create_and_wait("etl", num_workers=2)
w.jobs.run_now_and_wait(job_id, notebook_params={"date": "2026-01-01"})
w.workspace.import_source("/Users/me/nb", "print('hi')", language="PYTHON")
w.dbfs.put("/tmp/a.csv", b"a,b\n1,2\n", overwrite=True)
w.tables.list("main", "default")
w.secrets.put_secret("scope", "key", "value")

exp = w.experiments.get_or_create("/exp")
run = w.runs.create(exp)["info"]["run_id"]
w.runs.log_metric(run, "rmse", 0.42)
w.runs.log_artifact(run, "model/MLmodel", "flavors: {}")
w.model_registry.create_version("m", source=f"runs:/{run}/model", run_id=run)
w.serving_endpoints.create_for_model("m-endpoint", "m", "1")
```

Every Databricks REST family the control plane serves has a service group
(`clusters`, `jobs`, `pipelines`, `workspace`, `repos`, `warehouses`,
`statement_execution`, `queries`, `alerts`, `dashboards`, `catalogs`, `schemas`,
`tables`, `volumes`, `functions`, `grants`, `dbfs`, `files`, `secrets`, `tokens`,
`users`, `groups`, `service_principals`, `permissions`, `experiments`, `runs`,
`model_registry`, `serving_endpoints`, …). Anything not wrapped can be called
through `w.api_client.get/post/put/patch/delete(path, body)`.

Errors are raised as `LakeforgeError` subclasses (`NotFound`, `PermissionDenied`,
`Unauthenticated`, `AlreadyExists`, `InvalidParameterValue`) carrying the
Databricks-style `error_code` and `message`.

## dbutils outside notebooks

```python
dbutils = w.dbutils
dbutils.fs.ls("/")
dbutils.fs.put("/tmp/x.txt", "hello", overwrite=True)
dbutils.secrets.get("scope", "key")
dbutils.notebook.run("/Users/me/child", 600, {"a": "1"})
dbutils.jobs.taskValues.get("upstream", "count", default=0)
```

## CLI

```bash
lakeforge clusters list
lakeforge clusters create etl --workers 2 --wait
lakeforge sql exec "SELECT count(*) FROM main.default.t"
lakeforge workspace import ./nb.py /Users/me/nb --overwrite
lakeforge workspace run /Users/me/nb --arg date=2026-01-01
lakeforge fs cp ./data.csv dbfs:/tmp/data.csv
lakeforge jobs run-now --job-id 42 --wait
lakeforge catalog tables main default
lakeforge secrets put scope key --value v
lakeforge api post /api/2.1/jobs/create --json @job.json
lakeforge experiments list -o json
```

`-o json` / `--host` / `--token` / `--profile` may appear anywhere on the
command line.

## Tests

```bash
cd python/lakeforge-sdk && python -m pytest
```
