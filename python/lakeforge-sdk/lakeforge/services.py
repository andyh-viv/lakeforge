"""Service groups exposed on :class:`lakeforge.WorkspaceClient`.

Each method maps 1:1 onto a Lakeforge REST endpoint (which is
Databricks-compatible), returns plain ``dict``/``list`` payloads and raises
:class:`lakeforge.LakeforgeError` subclasses on failure.
"""

from __future__ import annotations

import base64
import os
import time
from typing import Any, Iterator, Optional

from .client import ApiClient, LakeforgeError

UC = "/api/2.1/unity-catalog"
SCIM = "/api/2.0/preview/scim/v2"
SCIM_PATCH = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
MLFLOW = "/api/2.0/mlflow"


class _Service:
    def __init__(self, api: ApiClient):
        self._api = api


# ------------------------------------------------------------------ identity --
class CurrentUser(_Service):
    def me(self) -> dict:
        return self._api.get("/api/2.0/lakeforge/me")

    def scim_me(self) -> dict:
        return self._api.get(f"{SCIM}/Me")

    def change_password(self, old_password: str, new_password: str) -> dict:
        return self._api.post("/api/2.0/lakeforge/password", {"old_password": old_password, "new_password": new_password})


class Tokens(_Service):
    def create(self, comment: Optional[str] = None, lifetime_seconds: Optional[int] = None) -> dict:
        return self._api.post("/api/2.0/token/create", {"comment": comment, "lifetime_seconds": lifetime_seconds})

    def list(self) -> list:
        return self._api.get("/api/2.0/token/list").get("token_infos", [])

    def delete(self, token_id: str) -> dict:
        return self._api.post("/api/2.0/token/delete", {"token_id": token_id})


class TokenManagement(_Service):
    def list(self, created_by_id: Optional[str] = None) -> list:
        return self._api.get("/api/2.0/token-management/tokens", created_by_id=created_by_id).get("token_infos", [])

    def get(self, token_id: str) -> dict:
        return self._api.get(f"/api/2.0/token-management/tokens/{token_id}")

    def delete(self, token_id: str) -> dict:
        return self._api.delete(f"/api/2.0/token-management/tokens/{token_id}")


# ----------------------------------------------------------------- workspace --
class Workspace(_Service):
    def list(self, path: str, recursive: bool = False) -> list:
        objs = self._api.get("/api/2.0/workspace/list", path=path).get("objects", [])
        if not recursive:
            return objs
        out = []
        for o in objs:
            out.append(o)
            if o.get("object_type") == "DIRECTORY":
                out.extend(self.list(o["path"], recursive=True))
        return out

    def get_status(self, path: str) -> dict:
        return self._api.get("/api/2.0/workspace/get-status", path=path)

    def mkdirs(self, path: str) -> dict:
        return self._api.post("/api/2.0/workspace/mkdirs", {"path": path})

    def delete(self, path: str, recursive: bool = False) -> dict:
        return self._api.post("/api/2.0/workspace/delete", {"path": path, "recursive": recursive})

    def import_(self, path: str, content: bytes | str, format: str = "SOURCE", language: Optional[str] = None, overwrite: bool = False) -> dict:
        data = content.encode() if isinstance(content, str) else content
        return self._api.post(
            "/api/2.0/workspace/import",
            {"path": path, "content": base64.b64encode(data).decode(), "format": format, "language": language, "overwrite": overwrite},
        )

    upload = import_

    def import_source(self, path: str, source: str, language: str = "PYTHON", overwrite: bool = True) -> dict:
        return self.import_(path, source, format="SOURCE", language=language, overwrite=overwrite)

    def export(self, path: str, format: str = "SOURCE") -> bytes:
        res = self._api.get("/api/2.0/workspace/export", path=path, format=format)
        return base64.b64decode(res.get("content", ""))

    download = export

    def move(self, source_path: str, destination_path: str) -> dict:
        return self._api.post("/api/2.0/lakeforge/workspace/move", {"source_path": source_path, "destination_path": destination_path})

    def search(self, query: str, path: str = "/") -> list:
        return self._api.get("/api/2.0/lakeforge/workspace/search", q=query, path=path).get("objects", [])


class Notebooks(_Service):
    def run(self, path: str, cluster_id: Optional[str] = None, arguments: Optional[dict] = None, timeout_seconds: int = 0) -> dict:
        """Run a notebook to completion. Without ``cluster_id`` the current notebook's cluster
        (``LAKEFORGE_CLUSTER_ID``) is used, then the first running cluster, then any cluster."""
        cluster_id = cluster_id or os.environ.get("LAKEFORGE_CLUSTER_ID") or self._default_cluster()
        return self._api.post("/api/2.0/lakeforge/notebooks/run", {"path": path, "cluster_id": cluster_id, "arguments": arguments or {}, "timeout_seconds": timeout_seconds})

    def _default_cluster(self) -> str:
        clusters = self._api.get("/api/2.0/clusters/list").get("clusters", [])
        if not clusters:
            raise LakeforgeError(400, "INVALID_PARAMETER_VALUE", "cluster_id is required and the workspace has no clusters")
        running = [c for c in clusters if c.get("state") == "RUNNING"]
        return (running or clusters)[0]["cluster_id"]

    def outputs(self, path: str) -> dict:
        return self._api.get("/api/2.0/lakeforge/notebooks/outputs", path=path)


class Repos(_Service):
    def list(self, path_prefix: Optional[str] = None) -> list:
        return list(self._api.paginate("GET", "/api/2.0/repos", "repos", {"path_prefix": path_prefix}))

    def create(self, url: str, provider: Optional[str] = None, path: Optional[str] = None, branch: Optional[str] = None, sparse_checkout: Optional[dict] = None) -> dict:
        return self._api.post("/api/2.0/repos", {"url": url, "provider": provider, "path": path, "branch": branch, "sparse_checkout": sparse_checkout})

    def get(self, repo_id: int) -> dict:
        return self._api.get(f"/api/2.0/repos/{repo_id}")

    def update(self, repo_id: int, branch: Optional[str] = None, tag: Optional[str] = None) -> dict:
        return self._api.patch(f"/api/2.0/repos/{repo_id}", {"branch": branch, "tag": tag})

    def delete(self, repo_id: int) -> dict:
        return self._api.delete(f"/api/2.0/repos/{repo_id}")

    def status(self, repo_id: int) -> dict:
        return self._api.get(f"/api/2.0/lakeforge/repos/{repo_id}/status")

    def commit(self, repo_id: int, message: str, push: bool = True, files: Optional[list] = None) -> dict:
        return self._api.post(f"/api/2.0/lakeforge/repos/{repo_id}/commit", {"message": message, "push": push, "files": files or []})

    def create_branch(self, repo_id: int, name: str) -> dict:
        return self._api.post(f"/api/2.0/lakeforge/repos/{repo_id}/branches", {"name": name})


class GitCredentials(_Service):
    def list(self) -> list:
        return self._api.get("/api/2.0/git-credentials").get("credentials", [])

    def create(self, git_provider: str, git_username: Optional[str] = None, personal_access_token: Optional[str] = None) -> dict:
        return self._api.post("/api/2.0/git-credentials", {"git_provider": git_provider, "git_username": git_username, "personal_access_token": personal_access_token})

    def delete(self, credential_id: int) -> dict:
        return self._api.delete(f"/api/2.0/git-credentials/{credential_id}")


# ------------------------------------------------------------------- compute --
class Clusters(_Service):
    def list(self) -> list:
        return self._api.get("/api/2.0/clusters/list").get("clusters", [])

    def get(self, cluster_id: str) -> dict:
        return self._api.get("/api/2.0/clusters/get", cluster_id=cluster_id)

    def create(self, cluster_name: str, num_workers: int = 1, spark_version: str = "forge-1.0", node_type_id: str = "local-small", autotermination_minutes: int = 60, **kw) -> dict:
        body = {"cluster_name": cluster_name, "num_workers": num_workers, "spark_version": spark_version, "node_type_id": node_type_id, "autotermination_minutes": autotermination_minutes, **kw}
        return self._api.post("/api/2.0/clusters/create", body)

    def edit(self, cluster_id: str, **spec) -> dict:
        return self._api.post("/api/2.0/clusters/edit", {"cluster_id": cluster_id, **spec})

    def start(self, cluster_id: str) -> dict:
        return self._api.post("/api/2.0/clusters/start", {"cluster_id": cluster_id})

    def restart(self, cluster_id: str) -> dict:
        return self._api.post("/api/2.0/clusters/restart", {"cluster_id": cluster_id})

    def delete(self, cluster_id: str) -> dict:
        return self._api.post("/api/2.0/clusters/delete", {"cluster_id": cluster_id})

    terminate = delete

    def permanent_delete(self, cluster_id: str) -> dict:
        return self._api.post("/api/2.0/clusters/permanent-delete", {"cluster_id": cluster_id})

    def resize(self, cluster_id: str, num_workers: int) -> dict:
        return self._api.post("/api/2.0/clusters/resize", {"cluster_id": cluster_id, "num_workers": num_workers})

    def events(self, cluster_id: str, limit: int = 50) -> list:
        return self._api.post("/api/2.0/clusters/events", {"cluster_id": cluster_id, "limit": limit}).get("events", [])

    def list_node_types(self) -> list:
        return self._api.get("/api/2.0/clusters/list-node-types").get("node_types", [])

    def spark_versions(self) -> list:
        return self._api.get("/api/2.0/clusters/spark-versions").get("versions", [])

    def forge_status(self, cluster_id: str) -> dict:
        return self._api.get("/api/2.0/lakeforge/clusters/forge-status", cluster_id=cluster_id)

    def wait_running(self, cluster_id: str, timeout: float = 600) -> dict:
        return _wait(lambda: self.get(cluster_id), lambda c: c.get("state") in ("RUNNING", "ERROR", "TERMINATED"), timeout, "cluster")

    def create_and_wait(self, cluster_name: str, timeout: float = 600, **spec) -> dict:
        return self.wait_running(self.create(cluster_name, **spec)["cluster_id"], timeout)

    def ensure_running(self, cluster_id: str, timeout: float = 600) -> dict:
        c = self.get(cluster_id)
        if c.get("state") == "TERMINATED":
            self.start(cluster_id)
        return self.wait_running(cluster_id, timeout)


class InstancePools(_Service):
    def list(self) -> list:
        return self._api.get("/api/2.0/instance-pools/list").get("instance_pools", [])

    def create(self, instance_pool_name: str, node_type_id: str = "local-small", **kw) -> dict:
        return self._api.post("/api/2.0/instance-pools/create", {"instance_pool_name": instance_pool_name, "node_type_id": node_type_id, **kw})

    def get(self, instance_pool_id: str) -> dict:
        return self._api.get("/api/2.0/instance-pools/get", instance_pool_id=instance_pool_id)

    def delete(self, instance_pool_id: str) -> dict:
        return self._api.post("/api/2.0/instance-pools/delete", {"instance_pool_id": instance_pool_id})


class ClusterPolicies(_Service):
    def list(self) -> list:
        return self._api.get("/api/2.0/policies/clusters/list").get("policies", [])

    def create(self, name: str, definition: str, **kw) -> dict:
        return self._api.post("/api/2.0/policies/clusters/create", {"name": name, "definition": definition, **kw})

    def get(self, policy_id: str) -> dict:
        return self._api.get("/api/2.0/policies/clusters/get", policy_id=policy_id)

    def delete(self, policy_id: str) -> dict:
        return self._api.post("/api/2.0/policies/clusters/delete", {"policy_id": policy_id})


class Libraries(_Service):
    def install(self, cluster_id: str, libraries: list) -> dict:
        return self._api.post("/api/2.0/libraries/install", {"cluster_id": cluster_id, "libraries": libraries})

    def uninstall(self, cluster_id: str, libraries: list) -> dict:
        return self._api.post("/api/2.0/libraries/uninstall", {"cluster_id": cluster_id, "libraries": libraries})

    def cluster_status(self, cluster_id: str) -> list:
        return self._api.get("/api/2.0/libraries/cluster-status", cluster_id=cluster_id).get("library_statuses", [])


class CommandExecution(_Service):
    """Execution-context API (``/api/1.2``) — the same path notebooks use."""

    def create_context(self, cluster_id: str, language: str = "python") -> str:
        return self._api.post("/api/1.2/contexts/create", {"clusterId": cluster_id, "language": language})["id"]

    def context_status(self, cluster_id: str, context_id: str) -> dict:
        return self._api.get("/api/1.2/contexts/status", clusterId=cluster_id, contextId=context_id)

    def destroy_context(self, cluster_id: str, context_id: str) -> dict:
        return self._api.post("/api/1.2/contexts/destroy", {"clusterId": cluster_id, "contextId": context_id})

    def execute(self, cluster_id: str, context_id: str, command: str, language: str = "python") -> str:
        return self._api.post("/api/1.2/commands/execute", {"clusterId": cluster_id, "contextId": context_id, "language": language, "command": command})["id"]

    def status(self, cluster_id: str, context_id: str, command_id: str) -> dict:
        return self._api.get("/api/1.2/commands/status", clusterId=cluster_id, contextId=context_id, commandId=command_id)

    def cancel(self, cluster_id: str, context_id: str, command_id: str) -> dict:
        return self._api.post("/api/1.2/commands/cancel", {"clusterId": cluster_id, "contextId": context_id, "commandId": command_id})

    def run(self, cluster_id: str, command: str, language: str = "python", context_id: Optional[str] = None, timeout: float = 600) -> dict:
        """Execute a command and block until it finishes; returns the command status."""
        own = context_id is None
        ctx = context_id or self.create_context(cluster_id, language)
        try:
            cid = self.execute(cluster_id, ctx, command, language)
            return _wait(lambda: self.status(cluster_id, ctx, cid), lambda s: s.get("status") in ("Finished", "Error", "Cancelled"), timeout, "command")
        finally:
            if own:
                try:
                    self.destroy_context(cluster_id, ctx)
                except LakeforgeError:
                    pass


# ---------------------------------------------------------------------- jobs --
class Jobs(_Service):
    V = "/api/2.1/jobs"

    def list(self, name: Optional[str] = None, expand_tasks: bool = False) -> list:
        return list(self._api.paginate("GET", f"{self.V}/list", "jobs", {"name": name, "expand_tasks": expand_tasks, "limit": 100}))

    def get(self, job_id: int) -> dict:
        return self._api.get(f"{self.V}/get", job_id=job_id)

    def create(self, name: str, tasks: list, **settings) -> dict:
        return self._api.post(f"{self.V}/create", {"name": name, "tasks": tasks, **settings})

    def reset(self, job_id: int, new_settings: dict) -> dict:
        return self._api.post(f"{self.V}/reset", {"job_id": job_id, "new_settings": new_settings})

    def update(self, job_id: int, new_settings: Optional[dict] = None, fields_to_remove: Optional[list] = None) -> dict:
        return self._api.post(f"{self.V}/update", {"job_id": job_id, "new_settings": new_settings or {}, "fields_to_remove": fields_to_remove or []})

    def delete(self, job_id: int) -> dict:
        return self._api.post(f"{self.V}/delete", {"job_id": job_id})

    def run_now(self, job_id: int, **params) -> dict:
        return self._api.post(f"{self.V}/run-now", {"job_id": job_id, **params})

    def submit(self, tasks: list, run_name: Optional[str] = None, **kw) -> dict:
        return self._api.post(f"{self.V}/runs/submit", {"tasks": tasks, "run_name": run_name, **kw})

    def list_runs(self, job_id: Optional[int] = None, active_only: bool = False, completed_only: bool = False, limit: int = 25) -> list:
        return list(self._api.paginate("GET", f"{self.V}/runs/list", "runs", {"job_id": job_id, "active_only": active_only, "completed_only": completed_only, "limit": limit}))

    def get_run(self, run_id: int) -> dict:
        return self._api.get(f"{self.V}/runs/get", run_id=run_id)

    def get_run_output(self, run_id: int) -> dict:
        return self._api.get(f"{self.V}/runs/get-output", run_id=run_id)

    def cancel_run(self, run_id: int) -> dict:
        return self._api.post(f"{self.V}/runs/cancel", {"run_id": run_id})

    def cancel_all_runs(self, job_id: int) -> dict:
        return self._api.post(f"{self.V}/runs/cancel-all", {"job_id": job_id})

    def delete_run(self, run_id: int) -> dict:
        return self._api.post(f"{self.V}/runs/delete", {"run_id": run_id})

    def repair_run(self, run_id: int, rerun_tasks: Optional[list] = None, rerun_all_failed_tasks: bool = False) -> dict:
        return self._api.post(f"{self.V}/runs/repair", {"run_id": run_id, "rerun_tasks": rerun_tasks, "rerun_all_failed_tasks": rerun_all_failed_tasks})

    def export_run(self, run_id: int) -> dict:
        return self._api.get(f"{self.V}/runs/export", run_id=run_id)

    def wait_run(self, run_id: int, timeout: float = 3600) -> dict:
        return _wait(lambda: self.get_run(run_id), lambda r: r.get("state", {}).get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"), timeout, "run")

    def run_now_and_wait(self, job_id: int, timeout: float = 3600, **params) -> dict:
        return self.wait_run(self.run_now(job_id, **params)["run_id"], timeout)


class Pipelines(_Service):
    def list(self) -> list:
        return list(self._api.paginate("GET", "/api/2.0/pipelines", "statuses"))

    def create(self, name: str, libraries: list, target: Optional[str] = None, catalog: Optional[str] = None, continuous: bool = False, **kw) -> dict:
        return self._api.post("/api/2.0/pipelines", {"name": name, "libraries": libraries, "target": target, "catalog": catalog, "continuous": continuous, **kw})

    def get(self, pipeline_id: str) -> dict:
        return self._api.get(f"/api/2.0/pipelines/{pipeline_id}")

    def update(self, pipeline_id: str, **spec) -> dict:
        return self._api.put(f"/api/2.0/pipelines/{pipeline_id}", spec)

    def delete(self, pipeline_id: str) -> dict:
        return self._api.delete(f"/api/2.0/pipelines/{pipeline_id}")

    def start_update(self, pipeline_id: str, full_refresh: bool = False, refresh_selection: Optional[list] = None) -> dict:
        return self._api.post(f"/api/2.0/pipelines/{pipeline_id}/updates", {"full_refresh": full_refresh, "refresh_selection": refresh_selection})

    def get_update(self, pipeline_id: str, update_id: str) -> dict:
        return self._api.get(f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")

    def list_updates(self, pipeline_id: str) -> list:
        return self._api.get(f"/api/2.0/pipelines/{pipeline_id}/updates").get("updates", [])

    def events(self, pipeline_id: str) -> list:
        return self._api.get(f"/api/2.0/pipelines/{pipeline_id}/events").get("events", [])

    def stop(self, pipeline_id: str) -> dict:
        return self._api.post(f"/api/2.0/pipelines/{pipeline_id}/stop")

    def reset(self, pipeline_id: str) -> dict:
        return self._api.post(f"/api/2.0/pipelines/{pipeline_id}/reset")

    def wait_update(self, pipeline_id: str, update_id: str, timeout: float = 3600) -> dict:
        return _wait(lambda: self.get_update(pipeline_id, update_id).get("update", {}), lambda u: u.get("state") in ("COMPLETED", "FAILED", "CANCELED"), timeout, "pipeline update")


# ----------------------------------------------------------------------- sql --
class Warehouses(_Service):
    def list(self) -> list:
        return self._api.get("/api/2.0/sql/warehouses").get("warehouses", [])

    def get(self, warehouse_id: str) -> dict:
        return self._api.get(f"/api/2.0/sql/warehouses/{warehouse_id}")

    def create(self, name: str, cluster_size: str = "2X-Small", auto_stop_mins: int = 10, **kw) -> dict:
        return self._api.post("/api/2.0/sql/warehouses", {"name": name, "cluster_size": cluster_size, "auto_stop_mins": auto_stop_mins, **kw})

    def edit(self, warehouse_id: str, **spec) -> dict:
        return self._api.post(f"/api/2.0/sql/warehouses/{warehouse_id}/edit", spec)

    def delete(self, warehouse_id: str) -> dict:
        return self._api.delete(f"/api/2.0/sql/warehouses/{warehouse_id}")

    def start(self, warehouse_id: str) -> dict:
        return self._api.post(f"/api/2.0/sql/warehouses/{warehouse_id}/start")

    def stop(self, warehouse_id: str) -> dict:
        return self._api.post(f"/api/2.0/sql/warehouses/{warehouse_id}/stop")

    def wait_running(self, warehouse_id: str, timeout: float = 600) -> dict:
        return _wait(lambda: self.get(warehouse_id), lambda w: w.get("state") in ("RUNNING", "STOPPED", "DELETED"), timeout, "warehouse")

    def default(self) -> dict:
        ws = self.list()
        if not ws:
            raise LakeforgeError(404, "RESOURCE_DOES_NOT_EXIST", "no SQL warehouses exist in this workspace")
        for w in ws:
            if w.get("state") == "RUNNING":
                return w
        return ws[0]


class StatementExecution(_Service):
    def execute(self, statement: str, warehouse_id: Optional[str] = None, catalog: Optional[str] = None, schema: Optional[str] = None, wait_timeout: str = "30s", parameters: Optional[list] = None, row_limit: Optional[int] = None, byte_limit: Optional[int] = None, disposition: str = "INLINE", format: str = "JSON_ARRAY") -> dict:
        if warehouse_id is None:
            warehouse_id = Warehouses(self._api).default()["id"]
        body = {"statement": statement, "warehouse_id": warehouse_id, "catalog": catalog, "schema": schema, "wait_timeout": wait_timeout, "parameters": parameters, "row_limit": row_limit, "byte_limit": byte_limit, "disposition": disposition, "format": format}
        return self._api.post("/api/2.0/sql/statements", {k: v for k, v in body.items() if v is not None})

    def get(self, statement_id: str) -> dict:
        return self._api.get(f"/api/2.0/sql/statements/{statement_id}")

    def cancel(self, statement_id: str) -> dict:
        return self._api.post(f"/api/2.0/sql/statements/{statement_id}/cancel")

    def get_chunk(self, statement_id: str, chunk_index: int) -> dict:
        return self._api.get(f"/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}")

    def execute_and_wait(self, statement: str, timeout: float = 600, **kw) -> dict:
        res = self.execute(statement, **kw)
        sid = res.get("statement_id")
        state = res.get("status", {}).get("state")
        if state in ("PENDING", "RUNNING") and sid:
            res = _wait(lambda: self.get(sid), lambda s: s.get("status", {}).get("state") not in ("PENDING", "RUNNING"), timeout, "statement")
        st = res.get("status", {})
        if st.get("state") == "FAILED":
            err = st.get("error", {})
            raise LakeforgeError(400, err.get("error_code", "STATEMENT_FAILED"), err.get("message", "statement failed"), "POST", "/api/2.0/sql/statements")
        return res

    def rows(self, statement: str, **kw) -> list[dict]:
        """Execute ``statement`` and return rows as dicts (all chunks), with values coerced
        from the ``JSON_ARRAY`` string encoding to Python types using the result manifest."""
        res = self.execute_and_wait(statement, **kw)
        columns = res.get("manifest", {}).get("schema", {}).get("columns", [])
        cols = [c["name"] for c in columns]
        types = [c.get("type_name", "STRING") for c in columns]
        data = list(res.get("result", {}).get("data_array") or [])
        nxt = res.get("result", {}).get("next_chunk_index")
        while nxt is not None:
            chunk = self.get_chunk(res["statement_id"], nxt)
            data.extend(chunk.get("data_array") or [])
            nxt = chunk.get("next_chunk_index")
        return [dict(zip(cols, (_coerce(v, t) for v, t in zip(r, types)))) for r in data]


_INT_TYPES = {"TINYINT", "SMALLINT", "INT", "BIGINT", "LONG", "SHORT", "BYTE"}
_FLOAT_TYPES = {"FLOAT", "DOUBLE", "DECIMAL"}


def _coerce(v: Any, type_name: str) -> Any:
    if v is None or not isinstance(v, str):
        return v
    t = type_name.upper().split("(")[0]
    try:
        if t in _INT_TYPES:
            return int(v)
        if t in _FLOAT_TYPES:
            return float(v)
        if t == "BOOLEAN":
            return v.lower() == "true"
    except ValueError:
        return v
    return v


class Queries(_Service):
    def list(self) -> list:
        return list(self._api.paginate("GET", "/api/2.0/sql/queries", "results"))

    def create(self, display_name: str, query_text: str, warehouse_id: Optional[str] = None, **kw) -> dict:
        return self._api.post("/api/2.0/sql/queries", {"query": {"display_name": display_name, "query_text": query_text, "warehouse_id": warehouse_id, **kw}})

    def get(self, query_id: str) -> dict:
        return self._api.get(f"/api/2.0/sql/queries/{query_id}")

    def update(self, query_id: str, update_mask: str, **fields) -> dict:
        return self._api.patch(f"/api/2.0/sql/queries/{query_id}", {"update_mask": update_mask, "query": fields})

    def delete(self, query_id: str) -> dict:
        return self._api.delete(f"/api/2.0/sql/queries/{query_id}")


class QueryHistory(_Service):
    def list(self, max_results: int = 100, **filter_by) -> list:
        return self._api.get("/api/2.0/sql/history/queries", max_results=max_results, **filter_by).get("res", [])


class Alerts(_Service):
    def list(self) -> list:
        return list(self._api.paginate("GET", "/api/2.0/sql/alerts", "results"))

    def create(self, display_name: str, query_id: str, condition: dict, **kw) -> dict:
        return self._api.post("/api/2.0/sql/alerts", {"alert": {"display_name": display_name, "query_id": query_id, "condition": condition, **kw}})

    def get(self, alert_id: str) -> dict:
        return self._api.get(f"/api/2.0/sql/alerts/{alert_id}")

    def delete(self, alert_id: str) -> dict:
        return self._api.delete(f"/api/2.0/sql/alerts/{alert_id}")

    def evaluate(self, alert_id: str) -> dict:
        return self._api.post(f"/api/2.0/lakeforge/sql/alerts/{alert_id}/evaluate")


class Dashboards(_Service):
    def list(self) -> list:
        return list(self._api.paginate("GET", "/api/2.0/lakeview/dashboards", "dashboards"))

    def create(self, display_name: str, serialized_dashboard: Optional[str] = None, warehouse_id: Optional[str] = None, **kw) -> dict:
        return self._api.post("/api/2.0/lakeview/dashboards", {"display_name": display_name, "serialized_dashboard": serialized_dashboard, "warehouse_id": warehouse_id, **kw})

    def get(self, dashboard_id: str) -> dict:
        return self._api.get(f"/api/2.0/lakeview/dashboards/{dashboard_id}")

    def update(self, dashboard_id: str, **fields) -> dict:
        return self._api.patch(f"/api/2.0/lakeview/dashboards/{dashboard_id}", fields)

    def delete(self, dashboard_id: str) -> dict:
        return self._api.delete(f"/api/2.0/lakeview/dashboards/{dashboard_id}")

    def publish(self, dashboard_id: str, warehouse_id: Optional[str] = None, embed_credentials: bool = True) -> dict:
        return self._api.post(f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", {"warehouse_id": warehouse_id, "embed_credentials": embed_credentials})


# --------------------------------------------------------------- unity catalog --
class _UcCollection(_Service):
    kind: str = ""
    key: str = ""
    name_field: str = "name"

    def list(self, **query) -> list:
        return list(self._api.paginate("GET", f"{UC}/{self.kind}", self.key, query))

    def create(self, **spec) -> dict:
        return self._api.post(f"{UC}/{self.kind}", spec)

    def get(self, name: str) -> dict:
        return self._api.get(f"{UC}/{self.kind}/{name}")

    def update(self, name: str, **fields) -> dict:
        return self._api.patch(f"{UC}/{self.kind}/{name}", fields)

    def delete(self, name: str, force: bool = False) -> dict:
        return self._api.delete(f"{UC}/{self.kind}/{name}", force=force)


class Catalogs(_UcCollection):
    kind, key = "catalogs", "catalogs"

    def create(self, name: str, comment: Optional[str] = None, **kw) -> dict:  # type: ignore[override]
        return super().create(name=name, comment=comment, **kw)


class Schemas(_UcCollection):
    kind, key = "schemas", "schemas"

    def list(self, catalog_name: str) -> list:  # type: ignore[override]
        return super().list(catalog_name=catalog_name)

    def create(self, name: str, catalog_name: str, comment: Optional[str] = None, **kw) -> dict:  # type: ignore[override]
        return super().create(name=name, catalog_name=catalog_name, comment=comment, **kw)


class Tables(_UcCollection):
    kind, key = "tables", "tables"

    def list(self, catalog_name: str, schema_name: str) -> list:  # type: ignore[override]
        return super().list(catalog_name=catalog_name, schema_name=schema_name)

    def exists(self, full_name: str) -> bool:
        return bool(self._api.get(f"{UC}/tables/{full_name}/exists").get("table_exists"))

    def summaries(self, catalog_name: str, **kw) -> list:
        return self._api.get(f"{UC}/table-summaries", catalog_name=catalog_name, **kw).get("tables", [])


class Volumes(_UcCollection):
    kind, key = "volumes", "volumes"

    def list(self, catalog_name: str, schema_name: str) -> list:  # type: ignore[override]
        return super().list(catalog_name=catalog_name, schema_name=schema_name)

    def create(self, name: str, catalog_name: str, schema_name: str, volume_type: str = "MANAGED", storage_location: Optional[str] = None, comment: Optional[str] = None) -> dict:  # type: ignore[override]
        return super().create(name=name, catalog_name=catalog_name, schema_name=schema_name, volume_type=volume_type, storage_location=storage_location, comment=comment)


class Functions(_UcCollection):
    kind, key = "functions", "functions"

    def list(self, catalog_name: str, schema_name: str) -> list:  # type: ignore[override]
        return super().list(catalog_name=catalog_name, schema_name=schema_name)


class ExternalLocations(_UcCollection):
    kind, key = "external-locations", "external_locations"


class StorageCredentials(_UcCollection):
    kind, key = "storage-credentials", "storage_credentials"


class Connections(_UcCollection):
    kind, key = "connections", "connections"


class Grants(_Service):
    def get(self, securable_type: str, full_name: str, principal: Optional[str] = None) -> list:
        return self._api.get(f"{UC}/permissions/{securable_type}/{full_name}", principal=principal).get("privilege_assignments", [])

    def get_effective(self, securable_type: str, full_name: str, principal: Optional[str] = None) -> list:
        return self._api.get(f"{UC}/effective-permissions/{securable_type}/{full_name}", principal=principal).get("privilege_assignments", [])

    def update(self, securable_type: str, full_name: str, changes: list) -> dict:
        return self._api.patch(f"{UC}/permissions/{securable_type}/{full_name}", {"changes": changes})

    def grant(self, securable_type: str, full_name: str, principal: str, privileges: list) -> dict:
        return self.update(securable_type, full_name, [{"principal": principal, "add": privileges}])

    def revoke(self, securable_type: str, full_name: str, principal: str, privileges: list) -> dict:
        return self.update(securable_type, full_name, [{"principal": principal, "remove": privileges}])


class Metastores(_Service):
    def summary(self) -> dict:
        return self._api.get(f"{UC}/metastore_summary")

    def current(self) -> dict:
        return self._api.get(f"{UC}/current-metastore-assignment")


# ---------------------------------------------------------------------- files --
class Dbfs(_Service):
    def list(self, path: str) -> list:
        return self._api.get("/api/2.0/dbfs/list", path=path).get("files", [])

    def get_status(self, path: str) -> dict:
        return self._api.get("/api/2.0/dbfs/get-status", path=path)

    def exists(self, path: str) -> bool:
        try:
            self.get_status(path)
            return True
        except LakeforgeError as e:
            if e.status == 404:
                return False
            raise

    def mkdirs(self, path: str) -> dict:
        return self._api.post("/api/2.0/dbfs/mkdirs", {"path": path})

    def delete(self, path: str, recursive: bool = False) -> dict:
        return self._api.post("/api/2.0/dbfs/delete", {"path": path, "recursive": recursive})

    def move(self, source_path: str, destination_path: str) -> dict:
        return self._api.post("/api/2.0/dbfs/move", {"source_path": source_path, "destination_path": destination_path})

    def copy(self, source_path: str, destination_path: str, recursive: bool = False) -> dict:
        return self._api.post("/api/2.0/dbfs/copy", {"source_path": source_path, "destination_path": destination_path, "recursive": recursive})

    def put(self, path: str, contents: bytes | str, overwrite: bool = False) -> dict:
        data = contents.encode() if isinstance(contents, str) else contents
        if len(data) <= 1024 * 1024:
            return self._api.post("/api/2.0/dbfs/put", {"path": path, "contents": base64.b64encode(data).decode(), "overwrite": overwrite})
        handle = self._api.post("/api/2.0/dbfs/create", {"path": path, "overwrite": overwrite})["handle"]
        for i in range(0, len(data), 1024 * 1024):
            self._api.post("/api/2.0/dbfs/add-block", {"handle": handle, "data": base64.b64encode(data[i : i + 1024 * 1024]).decode()})
        return self._api.post("/api/2.0/dbfs/close", {"handle": handle})

    def upload(self, path: str, local_path: str, overwrite: bool = False) -> dict:
        with open(local_path, "rb") as f:
            return self.put(path, f.read(), overwrite)

    def read(self, path: str, offset: int = 0, length: Optional[int] = None) -> bytes:
        res = self._api.get("/api/2.0/dbfs/read", path=path, offset=offset, length=length or 1024 * 1024)
        return base64.b64decode(res.get("data", ""))

    def read_all(self, path: str) -> bytes:
        size = self.get_status(path).get("file_size", 0)
        out = bytearray()
        while len(out) < size:
            chunk = self.read(path, offset=len(out), length=1024 * 1024)
            if not chunk:
                break
            out.extend(chunk)
        return bytes(out)

    def download(self, path: str, local_path: str) -> None:
        with open(local_path, "wb") as f:
            f.write(self.read_all(path))


class Files(_Service):
    """Files API for Unity Catalog volumes (``/Volumes/<catalog>/<schema>/<volume>/...``)."""

    def upload(self, file_path: str, contents: bytes | str, overwrite: bool = True) -> Any:
        data = contents.encode() if isinstance(contents, str) else contents
        return self._api.do("PUT", f"/api/2.0/fs/files{file_path}", query={"overwrite": overwrite}, data=data, content_type="application/octet-stream")

    def download(self, file_path: str) -> bytes:
        return self._api.do("GET", f"/api/2.0/fs/files{file_path}", raw=True)

    def delete(self, file_path: str) -> Any:
        return self._api.delete(f"/api/2.0/fs/files{file_path}")

    def get_metadata(self, file_path: str) -> Any:
        return self._api.do("HEAD", f"/api/2.0/fs/files{file_path}")

    def list_directory_contents(self, directory_path: str) -> list:
        return self._api.get(f"/api/2.0/fs/directories{directory_path}").get("contents", [])

    def create_directory(self, directory_path: str) -> Any:
        return self._api.put(f"/api/2.0/fs/directories{directory_path}")

    def delete_directory(self, directory_path: str) -> Any:
        return self._api.delete(f"/api/2.0/fs/directories{directory_path}")


# -------------------------------------------------------------------- secrets --
class Secrets(_Service):
    def list_scopes(self) -> list:
        return self._api.get("/api/2.0/secrets/scopes/list").get("scopes", [])

    def create_scope(self, scope: str, initial_manage_principal: Optional[str] = None) -> dict:
        return self._api.post("/api/2.0/secrets/scopes/create", {"scope": scope, "initial_manage_principal": initial_manage_principal})

    def delete_scope(self, scope: str) -> dict:
        return self._api.post("/api/2.0/secrets/scopes/delete", {"scope": scope})

    def list_secrets(self, scope: str) -> list:
        return self._api.get("/api/2.0/secrets/list", scope=scope).get("secrets", [])

    def put_secret(self, scope: str, key: str, string_value: Optional[str] = None, bytes_value: Optional[bytes] = None) -> dict:
        return self._api.post("/api/2.0/secrets/put", {"scope": scope, "key": key, "string_value": string_value, "bytes_value": base64.b64encode(bytes_value).decode() if bytes_value else None})

    def get_secret(self, scope: str, key: str) -> str:
        return base64.b64decode(self._api.get("/api/2.0/secrets/get", scope=scope, key=key)["value"]).decode()

    def delete_secret(self, scope: str, key: str) -> dict:
        return self._api.post("/api/2.0/secrets/delete", {"scope": scope, "key": key})

    def list_acls(self, scope: str) -> list:
        return self._api.get("/api/2.0/secrets/acls/list", scope=scope).get("items", [])

    def put_acl(self, scope: str, principal: str, permission: str) -> dict:
        return self._api.post("/api/2.0/secrets/acls/put", {"scope": scope, "principal": principal, "permission": permission})

    def delete_acl(self, scope: str, principal: str) -> dict:
        return self._api.post("/api/2.0/secrets/acls/delete", {"scope": scope, "principal": principal})


# ---------------------------------------------------------------- scim / admin --
class _ScimCollection(_Service):
    kind: str = ""

    def list(self, filter: Optional[str] = None, count: int = 100) -> list:
        out, start = [], 1
        while True:
            res = self._api.get(f"{SCIM}/{self.kind}", filter=filter, startIndex=start, count=count)
            items = res.get("Resources", [])
            out.extend(items)
            if not items or start + len(items) > res.get("totalResults", 0):
                return out
            start += len(items)

    def get(self, id: str) -> dict:
        return self._api.get(f"{SCIM}/{self.kind}/{id}")

    def create(self, **body) -> dict:
        return self._api.post(f"{SCIM}/{self.kind}", body)

    def patch(self, id: str, operations: list) -> dict:
        return self._api.patch(f"{SCIM}/{self.kind}/{id}", {"schemas": [SCIM_PATCH], "Operations": operations})

    def update(self, id: str, **body) -> dict:
        return self._api.put(f"{SCIM}/{self.kind}/{id}", body)

    def delete(self, id: str) -> dict:
        return self._api.delete(f"{SCIM}/{self.kind}/{id}")


class Users(_ScimCollection):
    kind = "Users"

    def create(self, user_name: str, display_name: Optional[str] = None, password: Optional[str] = None, active: bool = True, entitlements: Optional[list] = None, groups: Optional[list] = None) -> dict:  # type: ignore[override]
        return super().create(userName=user_name, displayName=display_name or user_name, password=password, active=active, entitlements=[{"value": e} for e in entitlements or []], groups=[{"value": g} for g in groups or []])

    def by_name(self, user_name: str) -> Optional[dict]:
        r = self.list(filter=f'userName eq "{user_name}"')
        return r[0] if r else None

    def set_active(self, id: str, active: bool) -> dict:
        return self.patch(id, [{"op": "replace", "path": "active", "value": active}])


class Groups(_ScimCollection):
    kind = "Groups"

    def create(self, display_name: str, members: Optional[list] = None, entitlements: Optional[list] = None) -> dict:  # type: ignore[override]
        return super().create(displayName=display_name, members=[{"value": m} for m in members or []], entitlements=[{"value": e} for e in entitlements or []])

    def add_member(self, group_id: str, member_id: str) -> dict:
        return self.patch(group_id, [{"op": "add", "path": "members", "value": [{"value": member_id}]}])

    def remove_member(self, group_id: str, member_id: str) -> dict:
        return self.patch(group_id, [{"op": "remove", "path": f'members[value eq "{member_id}"]'}])


class ServicePrincipals(_ScimCollection):
    kind = "ServicePrincipals"

    def create(self, display_name: str, application_id: Optional[str] = None, active: bool = True, entitlements: Optional[list] = None) -> dict:  # type: ignore[override]
        return super().create(displayName=display_name, applicationId=application_id, active=active, entitlements=[{"value": e} for e in entitlements or []])

    def create_secret(self, id: str) -> dict:
        return self._api.post(f"/api/2.0/accounts/servicePrincipals/{id}/credentials/secrets")

    def list_secrets(self, id: str) -> list:
        return self._api.get(f"/api/2.0/accounts/servicePrincipals/{id}/credentials/secrets").get("secrets", [])


class Permissions(_Service):
    def get(self, object_type: str, object_id: str) -> dict:
        return self._api.get(f"/api/2.0/permissions/{object_type}/{object_id}")

    def levels(self, object_type: str, object_id: str) -> list:
        return self._api.get(f"/api/2.0/permissions/{object_type}/{object_id}/permissionLevels").get("permission_levels", [])

    def set(self, object_type: str, object_id: str, access_control_list: list) -> dict:
        return self._api.put(f"/api/2.0/permissions/{object_type}/{object_id}", {"access_control_list": access_control_list})

    def update(self, object_type: str, object_id: str, access_control_list: list) -> dict:
        return self._api.patch(f"/api/2.0/permissions/{object_type}/{object_id}", {"access_control_list": access_control_list})


class WorkspaceConf(_Service):
    def get(self, *keys: str) -> dict:
        return self._api.get("/api/2.0/workspace-conf", keys=",".join(keys))

    def set(self, **values) -> dict:
        return self._api.patch("/api/2.0/workspace-conf", {k: (str(v).lower() if isinstance(v, bool) else str(v)) for k, v in values.items()})


class IpAccessLists(_Service):
    def list(self) -> list:
        return self._api.get("/api/2.0/ip-access-lists").get("ip_access_lists", [])

    def create(self, label: str, list_type: str, ip_addresses: list) -> dict:
        return self._api.post("/api/2.0/ip-access-lists", {"label": label, "list_type": list_type, "ip_addresses": ip_addresses})

    def delete(self, list_id: str) -> dict:
        return self._api.delete(f"/api/2.0/ip-access-lists/{list_id}")


class GlobalInitScripts(_Service):
    def list(self) -> list:
        return self._api.get("/api/2.0/global-init-scripts").get("scripts", [])

    def create(self, name: str, script: str, enabled: bool = True, position: Optional[int] = None) -> dict:
        return self._api.post("/api/2.0/global-init-scripts", {"name": name, "script": base64.b64encode(script.encode()).decode(), "enabled": enabled, "position": position})

    def get(self, script_id: str) -> dict:
        return self._api.get(f"/api/2.0/global-init-scripts/{script_id}")

    def delete(self, script_id: str) -> dict:
        return self._api.delete(f"/api/2.0/global-init-scripts/{script_id}")


class Settings(_Service):
    def info(self) -> dict:
        return self._api.get("/api/2.0/lakeforge/info")

    def workspace_status(self) -> dict:
        return self._api.get("/api/2.0/lakeforge/workspace-status")


# --------------------------------------------------------------------- mlflow --
class Experiments(_Service):
    def create(self, name: str, artifact_location: Optional[str] = None, tags: Optional[dict] = None) -> str:
        return self._api.post(f"{MLFLOW}/experiments/create", {"name": name, "artifact_location": artifact_location, "tags": [{"key": k, "value": v} for k, v in (tags or {}).items()]})["experiment_id"]

    def get(self, experiment_id: str) -> dict:
        return self._api.get(f"{MLFLOW}/experiments/get", experiment_id=experiment_id)["experiment"]

    def get_by_name(self, name: str) -> Optional[dict]:
        try:
            return self._api.get(f"{MLFLOW}/experiments/get-by-name", experiment_name=name)["experiment"]
        except LakeforgeError as e:
            if e.status == 404:
                return None
            raise

    def get_or_create(self, name: str) -> str:
        e = self.get_by_name(name)
        return e["experiment_id"] if e else self.create(name)

    def search(self, filter: Optional[str] = None, max_results: int = 100, view_type: str = "ACTIVE_ONLY") -> list:
        return list(self._api.paginate("POST", f"{MLFLOW}/experiments/search", "experiments", body={"filter": filter, "max_results": max_results, "view_type": view_type}))

    list = search

    def update(self, experiment_id: str, new_name: str) -> dict:
        return self._api.post(f"{MLFLOW}/experiments/update", {"experiment_id": experiment_id, "new_name": new_name})

    def delete(self, experiment_id: str) -> dict:
        return self._api.post(f"{MLFLOW}/experiments/delete", {"experiment_id": experiment_id})

    def restore(self, experiment_id: str) -> dict:
        return self._api.post(f"{MLFLOW}/experiments/restore", {"experiment_id": experiment_id})

    def set_tag(self, experiment_id: str, key: str, value: str) -> dict:
        return self._api.post(f"{MLFLOW}/experiments/set-experiment-tag", {"experiment_id": experiment_id, "key": key, "value": value})


class Runs(_Service):
    def create(self, experiment_id: str, run_name: Optional[str] = None, tags: Optional[dict] = None, start_time: Optional[int] = None) -> dict:
        return self._api.post(f"{MLFLOW}/runs/create", {"experiment_id": experiment_id, "run_name": run_name, "start_time": start_time or int(time.time() * 1000), "tags": [{"key": k, "value": v} for k, v in (tags or {}).items()]})["run"]

    def get(self, run_id: str) -> dict:
        return self._api.get(f"{MLFLOW}/runs/get", run_id=run_id)["run"]

    def update(self, run_id: str, status: Optional[str] = None, end_time: Optional[int] = None, run_name: Optional[str] = None) -> dict:
        return self._api.post(f"{MLFLOW}/runs/update", {"run_id": run_id, "status": status, "end_time": end_time, "run_name": run_name})

    def finish(self, run_id: str, status: str = "FINISHED") -> dict:
        return self.update(run_id, status=status, end_time=int(time.time() * 1000))

    def delete(self, run_id: str) -> dict:
        return self._api.post(f"{MLFLOW}/runs/delete", {"run_id": run_id})

    def restore(self, run_id: str) -> dict:
        return self._api.post(f"{MLFLOW}/runs/restore", {"run_id": run_id})

    def search(self, experiment_ids: list, filter: Optional[str] = None, max_results: int = 1000, order_by: Optional[list] = None, run_view_type: str = "ACTIVE_ONLY") -> list:
        return list(self._api.paginate("POST", f"{MLFLOW}/runs/search", "runs", body={"experiment_ids": experiment_ids, "filter": filter, "max_results": max_results, "order_by": order_by, "run_view_type": run_view_type}))

    def log_metric(self, run_id: str, key: str, value: float, step: int = 0, timestamp: Optional[int] = None) -> dict:
        return self._api.post(f"{MLFLOW}/runs/log-metric", {"run_id": run_id, "key": key, "value": value, "step": step, "timestamp": timestamp or int(time.time() * 1000)})

    def log_param(self, run_id: str, key: str, value: Any) -> dict:
        return self._api.post(f"{MLFLOW}/runs/log-parameter", {"run_id": run_id, "key": key, "value": str(value)})

    def set_tag(self, run_id: str, key: str, value: str) -> dict:
        return self._api.post(f"{MLFLOW}/runs/set-tag", {"run_id": run_id, "key": key, "value": value})

    def delete_tag(self, run_id: str, key: str) -> dict:
        return self._api.post(f"{MLFLOW}/runs/delete-tag", {"run_id": run_id, "key": key})

    def log_batch(self, run_id: str, metrics: Optional[list] = None, params: Optional[list] = None, tags: Optional[list] = None) -> dict:
        return self._api.post(f"{MLFLOW}/runs/log-batch", {"run_id": run_id, "metrics": metrics or [], "params": params or [], "tags": tags or []})

    def metric_history(self, run_id: str, metric_key: str) -> list:
        return self._api.get(f"{MLFLOW}/metrics/get-history", run_id=run_id, metric_key=metric_key).get("metrics", [])

    def list_artifacts(self, run_id: str, path: Optional[str] = None) -> dict:
        return self._api.get(f"{MLFLOW}/artifacts/list", run_id=run_id, path=path)

    def _artifact_url(self, run_id: str, artifact_path: str) -> str:
        """Map a run-relative artifact path onto the ``mlflow-artifacts`` proxy (rooted at the ``mlflow/`` storage prefix)."""
        uri = self.get(run_id)["info"]["artifact_uri"].rstrip("/")
        _, sep, rel = uri.partition("/mlflow/")
        root = rel if sep else uri.split("://", 1)[-1]
        return f"/api/2.0/mlflow-artifacts/artifacts/{root}/{artifact_path.lstrip('/')}"

    def log_artifact(self, run_id: str, artifact_path: str, contents: bytes | str) -> Any:
        data = contents.encode() if isinstance(contents, str) else contents
        return self._api.do("PUT", self._artifact_url(run_id, artifact_path), data=data, content_type="application/octet-stream")

    def download_artifact(self, run_id: str, artifact_path: str) -> bytes:
        return self._api.do("GET", self._artifact_url(run_id, artifact_path), raw=True)


class ModelRegistry(_Service):
    def create_model(self, name: str, description: Optional[str] = None, tags: Optional[dict] = None) -> dict:
        return self._api.post(f"{MLFLOW}/registered-models/create", {"name": name, "description": description, "tags": [{"key": k, "value": v} for k, v in (tags or {}).items()]})["registered_model"]

    def get_model(self, name: str) -> dict:
        return self._api.get(f"{MLFLOW}/registered-models/get", name=name)["registered_model"]

    def search_models(self, filter: Optional[str] = None, max_results: int = 100) -> list:
        return list(self._api.paginate("GET", f"{MLFLOW}/registered-models/search", "registered_models", {"filter": filter, "max_results": max_results}))

    list_models = search_models

    def rename_model(self, name: str, new_name: str) -> dict:
        return self._api.post(f"{MLFLOW}/registered-models/rename", {"name": name, "new_name": new_name})

    def update_model(self, name: str, description: str) -> dict:
        return self._api.patch(f"{MLFLOW}/registered-models/update", {"name": name, "description": description})

    def delete_model(self, name: str) -> dict:
        return self._api.delete(f"{MLFLOW}/registered-models/delete", {"name": name})

    def set_model_tag(self, name: str, key: str, value: str) -> dict:
        return self._api.post(f"{MLFLOW}/registered-models/set-tag", {"name": name, "key": key, "value": value})

    def latest_versions(self, name: str, stages: Optional[list] = None) -> list:
        return self._api.post(f"{MLFLOW}/registered-models/get-latest-versions", {"name": name, "stages": stages}).get("model_versions", [])

    def create_version(self, name: str, source: str, run_id: Optional[str] = None, description: Optional[str] = None, tags: Optional[dict] = None) -> dict:
        return self._api.post(f"{MLFLOW}/model-versions/create", {"name": name, "source": source, "run_id": run_id, "description": description, "tags": [{"key": k, "value": v} for k, v in (tags or {}).items()]})["model_version"]

    def get_version(self, name: str, version: str | int) -> dict:
        return self._api.get(f"{MLFLOW}/model-versions/get", name=name, version=str(version))["model_version"]

    def search_versions(self, filter: Optional[str] = None, max_results: int = 100) -> list:
        return list(self._api.paginate("GET", f"{MLFLOW}/model-versions/search", "model_versions", {"filter": filter, "max_results": max_results}))

    def update_version(self, name: str, version: str | int, description: str) -> dict:
        return self._api.patch(f"{MLFLOW}/model-versions/update", {"name": name, "version": str(version), "description": description})

    def delete_version(self, name: str, version: str | int) -> dict:
        return self._api.delete(f"{MLFLOW}/model-versions/delete", {"name": name, "version": str(version)})

    def transition_stage(self, name: str, version: str | int, stage: str, archive_existing_versions: bool = False) -> dict:
        return self._api.post(f"{MLFLOW}/model-versions/transition-stage", {"name": name, "version": str(version), "stage": stage, "archive_existing_versions": archive_existing_versions})

    def set_version_tag(self, name: str, version: str | int, key: str, value: str) -> dict:
        return self._api.post(f"{MLFLOW}/model-versions/set-tag", {"name": name, "version": str(version), "key": key, "value": value})

    def set_alias(self, name: str, alias: str, version: str | int) -> dict:
        return self._api.post(f"{MLFLOW}/registered-models/alias", {"name": name, "alias": alias, "version": str(version)})

    def delete_alias(self, name: str, alias: str) -> dict:
        return self._api.delete(f"{MLFLOW}/registered-models/alias", {"name": name, "alias": alias})

    def get_version_by_alias(self, name: str, alias: str) -> dict:
        return self._api.get(f"{MLFLOW}/registered-models/alias", name=name, alias=alias)["model_version"]

    def download_uri(self, name: str, version: str | int) -> str:
        return self._api.get(f"{MLFLOW}/model-versions/get-download-uri", name=name, version=str(version))["artifact_uri"]


class ServingEndpoints(_Service):
    def list(self) -> list:
        return self._api.get("/api/2.0/serving-endpoints").get("endpoints", [])

    def create(self, name: str, served_entities: list, traffic_config: Optional[dict] = None, tags: Optional[list] = None, **kw) -> dict:
        return self._api.post("/api/2.0/serving-endpoints", {"name": name, "config": {"served_entities": served_entities, "traffic_config": traffic_config}, "tags": tags, **kw})

    def create_for_model(self, name: str, model_name: str, model_version: str | int, workload_size: str = "Small", scale_to_zero: bool = True) -> dict:
        return self.create(name, [{"entity_name": model_name, "entity_version": str(model_version), "workload_size": workload_size, "scale_to_zero_enabled": scale_to_zero}])

    def get(self, name: str) -> dict:
        return self._api.get(f"/api/2.0/serving-endpoints/{name}")

    def update_config(self, name: str, served_entities: list, traffic_config: Optional[dict] = None) -> dict:
        return self._api.put(f"/api/2.0/serving-endpoints/{name}/config", {"served_entities": served_entities, "traffic_config": traffic_config})

    def delete(self, name: str) -> dict:
        return self._api.delete(f"/api/2.0/serving-endpoints/{name}")

    def query(self, name: str, **payload) -> dict:
        return self._api.post(f"/api/2.0/serving-endpoints/{name}/invocations", payload)

    def metrics(self, name: str) -> Any:
        return self._api.get(f"/api/2.0/lakeforge/serving-endpoints/{name}/metrics")

    def logs(self, name: str, served_entity: str) -> Any:
        return self._api.get(f"/api/2.0/serving-endpoints/{name}/served-entities/{served_entity}/logs")

    def build_logs(self, name: str, served_entity: str) -> Any:
        return self._api.get(f"/api/2.0/serving-endpoints/{name}/served-entities/{served_entity}/build-logs")

    def wait_ready(self, name: str, timeout: float = 900) -> dict:
        return _wait(lambda: self.get(name), lambda e: e.get("state", {}).get("ready") == "READY" or e.get("state", {}).get("config_update") in ("UPDATE_FAILED",), timeout, "serving endpoint")


# ------------------------------------------------------------------- helpers --
def _wait(fetch, done, timeout: float, what: str, interval: float = 1.0) -> dict:
    deadline = time.monotonic() + timeout
    delay = interval
    while True:
        cur = fetch()
        if done(cur):
            return cur
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(delay)
        delay = min(delay * 1.5, 10.0)
