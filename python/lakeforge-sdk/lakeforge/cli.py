"""``lakeforge`` command-line interface (mirrors the most-used ``databricks`` CLI commands).

    lakeforge auth login --host http://localhost:8080 --username admin@lakeforge.local
    lakeforge clusters list
    lakeforge sql exec "SELECT 1"
    lakeforge workspace ls /Users
    lakeforge fs cp ./data.csv dbfs:/tmp/data.csv
    lakeforge jobs run-now --job-id 42 --wait
    lakeforge api get /api/2.0/clusters/list
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from typing import Any, Callable, Optional

from . import WorkspaceClient, __version__
from .client import LakeforgeError
from .config import save_profile


def _print(obj: Any, fmt: str) -> None:
    if fmt == "json" or not isinstance(obj, list) or not obj or not all(isinstance(r, dict) for r in obj):
        print(json.dumps(obj, indent=2, default=str))
        return
    cols: list[str] = []
    for r in obj:
        for k in r:
            if k not in cols and not isinstance(r[k], (dict, list)):
                cols.append(k)
    cols = cols[:8]
    rows = [[_cell(r.get(c)) for c in cols] for r in obj]
    widths = [max(len(c), *(len(r[i]) for r in rows)) for i, c in enumerate(cols)]
    print("  ".join(c.ljust(widths[i]) for i, c in enumerate(cols)))
    for r in rows:
        print("  ".join(v.ljust(widths[i]) for i, v in enumerate(r)))


def _cell(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    return s if len(s) <= 48 else s[:45] + "..."


def _json_arg(s: Optional[str]) -> Any:
    if s is None:
        return None
    if s.startswith("@"):
        with open(s[1:]) as f:
            return json.load(f)
    return json.loads(s)


def _dbfs(p: str) -> str:
    return p[len("dbfs:") :] if p.startswith("dbfs:") else p


def global_parser() -> argparse.ArgumentParser:
    """Options accepted anywhere on the command line (before or after the subcommand)."""
    gp = argparse.ArgumentParser(add_help=False)
    gp.add_argument("--host")
    gp.add_argument("--token")
    gp.add_argument("--profile", "-p")
    gp.add_argument("--output", "-o", choices=["table", "json"], default="table")
    return gp


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="lakeforge", description="Lakeforge CLI", parents=[global_parser()])
    ap.add_argument("--version", action="version", version=f"lakeforge {__version__}")
    sub = ap.add_subparsers(dest="group", required=True)

    # auth
    g = sub.add_parser("auth").add_subparsers(dest="cmd", required=True)
    p = g.add_parser("login", help="store credentials in ~/.lakeforgecfg")
    p.add_argument("--username", "-u")
    p.add_argument("--password")
    p.add_argument("--pat", action="store_true", help="store a long-lived personal access token instead of a session JWT")
    p.add_argument("--save-profile", default=None)
    g.add_parser("me")
    g.add_parser("info")

    # api
    g = sub.add_parser("api").add_subparsers(dest="cmd", required=True)
    for m in ("get", "post", "put", "patch", "delete"):
        p = g.add_parser(m)
        p.add_argument("path")
        p.add_argument("--json", "-j", help="request body (inline JSON or @file)")

    # clusters
    g = sub.add_parser("clusters").add_subparsers(dest="cmd", required=True)
    g.add_parser("list")
    for c in ("get", "start", "restart", "delete", "permanent-delete", "events", "forge-status"):
        g.add_parser(c).add_argument("cluster_id")
    p = g.add_parser("create")
    p.add_argument("name")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--node-type", default="local-small")
    p.add_argument("--wait", action="store_true")
    p = g.add_parser("resize")
    p.add_argument("cluster_id")
    p.add_argument("--workers", type=int, required=True)
    g.add_parser("node-types")
    g.add_parser("spark-versions")
    p = g.add_parser("exec", help="run code in an execution context")
    p.add_argument("cluster_id")
    p.add_argument("code")
    p.add_argument("--language", default="python", choices=["python", "sql", "scala", "r"])

    # workspace
    g = sub.add_parser("workspace").add_subparsers(dest="cmd", required=True)
    p = g.add_parser("ls")
    p.add_argument("path", nargs="?", default="/")
    p.add_argument("-r", "--recursive", action="store_true")
    g.add_parser("mkdirs").add_argument("path")
    p = g.add_parser("rm")
    p.add_argument("path")
    p.add_argument("-r", "--recursive", action="store_true")
    p = g.add_parser("import")
    p.add_argument("local")
    p.add_argument("path")
    p.add_argument("--language", choices=["PYTHON", "SQL", "SCALA", "R"])
    p.add_argument("--format", default="SOURCE")
    p.add_argument("--overwrite", action="store_true")
    p = g.add_parser("export")
    p.add_argument("path")
    p.add_argument("local", nargs="?")
    p.add_argument("--format", default="SOURCE")
    p = g.add_parser("run", help="run a notebook and print its exit value")
    p.add_argument("path")
    p.add_argument("--cluster-id")
    p.add_argument("--arg", action="append", default=[], metavar="K=V")
    g.add_parser("search").add_argument("query")

    # fs (dbfs)
    g = sub.add_parser("fs").add_subparsers(dest="cmd", required=True)
    g.add_parser("ls").add_argument("path", nargs="?", default="/")
    g.add_parser("mkdirs").add_argument("path")
    p = g.add_parser("rm")
    p.add_argument("path")
    p.add_argument("-r", "--recursive", action="store_true")
    p = g.add_parser("cp", help="copy local<->dbfs: paths (prefix remote paths with dbfs:)")
    p.add_argument("src")
    p.add_argument("dst")
    p.add_argument("--overwrite", action="store_true")
    p = g.add_parser("mv")
    p.add_argument("src")
    p.add_argument("dst")
    g.add_parser("cat").add_argument("path")

    # sql
    g = sub.add_parser("sql").add_subparsers(dest="cmd", required=True)
    p = g.add_parser("exec")
    p.add_argument("statement")
    p.add_argument("--warehouse-id")
    p.add_argument("--catalog")
    p.add_argument("--schema")
    g.add_parser("warehouses")
    for c in ("start-warehouse", "stop-warehouse", "get-warehouse"):
        g.add_parser(c).add_argument("warehouse_id")
    p = g.add_parser("create-warehouse")
    p.add_argument("name")
    p.add_argument("--size", default="2X-Small")
    g.add_parser("history").add_argument("--max", type=int, default=50)
    g.add_parser("queries")
    g.add_parser("alerts")
    g.add_parser("dashboards")

    # jobs
    g = sub.add_parser("jobs").add_subparsers(dest="cmd", required=True)
    g.add_parser("list")
    g.add_parser("get").add_argument("job_id", type=int)
    p = g.add_parser("create")
    p.add_argument("--json", "-j", required=True, help="job settings (inline JSON or @file)")
    g.add_parser("delete").add_argument("job_id", type=int)
    p = g.add_parser("run-now")
    p.add_argument("--job-id", type=int, required=True)
    p.add_argument("--params", help="JSON job/notebook parameters")
    p.add_argument("--wait", action="store_true")
    p = g.add_parser("submit")
    p.add_argument("--json", "-j", required=True)
    p.add_argument("--wait", action="store_true")
    p = g.add_parser("runs")
    p.add_argument("--job-id", type=int)
    p.add_argument("--active", action="store_true")
    p.add_argument("--limit", type=int, default=25)
    g.add_parser("get-run").add_argument("run_id", type=int)
    g.add_parser("run-output").add_argument("run_id", type=int)
    g.add_parser("cancel-run").add_argument("run_id", type=int)

    # pipelines
    g = sub.add_parser("pipelines").add_subparsers(dest="cmd", required=True)
    g.add_parser("list")
    g.add_parser("get").add_argument("pipeline_id")
    p = g.add_parser("create")
    p.add_argument("--json", "-j", required=True)
    g.add_parser("delete").add_argument("pipeline_id")
    p = g.add_parser("start")
    p.add_argument("pipeline_id")
    p.add_argument("--full-refresh", action="store_true")
    p.add_argument("--wait", action="store_true")
    g.add_parser("stop").add_argument("pipeline_id")
    g.add_parser("events").add_argument("pipeline_id")
    g.add_parser("updates").add_argument("pipeline_id")

    # catalog
    g = sub.add_parser("catalog").add_subparsers(dest="cmd", required=True)
    g.add_parser("catalogs")
    g.add_parser("schemas").add_argument("catalog")
    p = g.add_parser("tables")
    p.add_argument("catalog")
    p.add_argument("schema")
    p = g.add_parser("volumes")
    p.add_argument("catalog")
    p.add_argument("schema")
    p = g.add_parser("functions")
    p.add_argument("catalog")
    p.add_argument("schema")
    g.add_parser("get-table").add_argument("full_name")
    p = g.add_parser("create-catalog")
    p.add_argument("name")
    p.add_argument("--comment")
    p = g.add_parser("create-schema")
    p.add_argument("full_name", help="catalog.schema")
    p.add_argument("--comment")
    p = g.add_parser("grants")
    p.add_argument("securable_type")
    p.add_argument("full_name")
    p = g.add_parser("grant")
    p.add_argument("securable_type")
    p.add_argument("full_name")
    p.add_argument("principal")
    p.add_argument("privileges", nargs="+")
    p = g.add_parser("revoke")
    p.add_argument("securable_type")
    p.add_argument("full_name")
    p.add_argument("principal")
    p.add_argument("privileges", nargs="+")

    # secrets
    g = sub.add_parser("secrets").add_subparsers(dest="cmd", required=True)
    g.add_parser("list-scopes")
    g.add_parser("create-scope").add_argument("scope")
    g.add_parser("delete-scope").add_argument("scope")
    g.add_parser("list").add_argument("scope")
    p = g.add_parser("put")
    p.add_argument("scope")
    p.add_argument("key")
    p.add_argument("--value", help="omit to be prompted")
    p = g.add_parser("get")
    p.add_argument("scope")
    p.add_argument("key")
    p = g.add_parser("delete")
    p.add_argument("scope")
    p.add_argument("key")
    g.add_parser("acls").add_argument("scope")
    p = g.add_parser("put-acl")
    p.add_argument("scope")
    p.add_argument("principal")
    p.add_argument("permission", choices=["READ", "WRITE", "MANAGE"])

    # tokens
    g = sub.add_parser("tokens").add_subparsers(dest="cmd", required=True)
    g.add_parser("list")
    p = g.add_parser("create")
    p.add_argument("--comment")
    p.add_argument("--lifetime-days", type=int)
    g.add_parser("delete").add_argument("token_id")

    # users / groups / service principals
    g = sub.add_parser("users").add_subparsers(dest="cmd", required=True)
    g.add_parser("list").add_argument("--filter")
    g.add_parser("get").add_argument("id")
    p = g.add_parser("create")
    p.add_argument("user_name")
    p.add_argument("--display-name")
    p.add_argument("--password")
    p.add_argument("--admin", action="store_true")
    g.add_parser("delete").add_argument("id")
    p = g.add_parser("set-active")
    p.add_argument("id")
    p.add_argument("active", choices=["true", "false"])
    g = sub.add_parser("groups").add_subparsers(dest="cmd", required=True)
    g.add_parser("list")
    g.add_parser("create").add_argument("display_name")
    g.add_parser("delete").add_argument("id")
    p = g.add_parser("add-member")
    p.add_argument("group_id")
    p.add_argument("member_id")
    p = g.add_parser("remove-member")
    p.add_argument("group_id")
    p.add_argument("member_id")
    g = sub.add_parser("service-principals").add_subparsers(dest="cmd", required=True)
    g.add_parser("list")
    g.add_parser("create").add_argument("display_name")
    g.add_parser("delete").add_argument("id")
    g.add_parser("create-secret").add_argument("id")

    # repos
    g = sub.add_parser("repos").add_subparsers(dest="cmd", required=True)
    g.add_parser("list")
    p = g.add_parser("create")
    p.add_argument("url")
    p.add_argument("--path")
    p.add_argument("--branch")
    p.add_argument("--provider")
    g.add_parser("get").add_argument("repo_id", type=int)
    p = g.add_parser("update")
    p.add_argument("repo_id", type=int)
    p.add_argument("--branch")
    p.add_argument("--tag")
    g.add_parser("delete").add_argument("repo_id", type=int)
    g.add_parser("status").add_argument("repo_id", type=int)
    p = g.add_parser("commit")
    p.add_argument("repo_id", type=int)
    p.add_argument("-m", "--message", required=True)
    p.add_argument("--no-push", action="store_true")

    # experiments / models / serving
    g = sub.add_parser("experiments").add_subparsers(dest="cmd", required=True)
    g.add_parser("list")
    g.add_parser("create").add_argument("name")
    g.add_parser("runs").add_argument("experiment_id")
    g.add_parser("get-run").add_argument("run_id")
    g = sub.add_parser("models").add_subparsers(dest="cmd", required=True)
    g.add_parser("list")
    g.add_parser("get").add_argument("name")
    g.add_parser("versions").add_argument("name")
    p = g.add_parser("set-alias")
    p.add_argument("name")
    p.add_argument("alias")
    p.add_argument("version")
    g = sub.add_parser("serving").add_subparsers(dest="cmd", required=True)
    g.add_parser("list")
    g.add_parser("get").add_argument("name")
    p = g.add_parser("create")
    p.add_argument("name")
    p.add_argument("--model", required=True)
    p.add_argument("--version", required=True)
    p.add_argument("--wait", action="store_true")
    g.add_parser("delete").add_argument("name")
    p = g.add_parser("query")
    p.add_argument("name")
    p.add_argument("--json", "-j", required=True)
    return ap


def _client(a: argparse.Namespace) -> WorkspaceClient:
    return WorkspaceClient(host=a.host, token=a.token, profile=a.profile)


def _auth_login(a: argparse.Namespace) -> Any:
    host = a.host or os.environ.get("LAKEFORGE_HOST") or input("Host [http://localhost:8080]: ").strip() or "http://localhost:8080"
    username = a.username or input("Username: ").strip()
    password = a.password or getpass.getpass("Password: ")
    w = WorkspaceClient(host=host, username=username, password=password)
    if a.pat:
        token = w.tokens.create(comment="lakeforge CLI", lifetime_seconds=None)["token_value"]
    else:
        token = w.login(username, password)
    path = save_profile(a.save_profile or a.profile or "DEFAULT", host, token=token)
    me = WorkspaceClient(host=host, token=token).current_user.me()
    return {"host": host, "user": me.get("user_name"), "profile": a.save_profile or a.profile or "DEFAULT", "config_file": str(path), "token_type": "pat" if a.pat else "session"}


def dispatch(a: argparse.Namespace) -> Any:  # noqa: C901 - flat command table
    if a.group == "auth" and a.cmd == "login":
        return _auth_login(a)
    w = _client(a)
    g, c = a.group, a.cmd
    H: dict[tuple[str, str], Callable[[], Any]] = {
        ("auth", "me"): lambda: w.current_user.me(),
        ("auth", "info"): lambda: w.settings.info(),
        ("clusters", "list"): lambda: w.clusters.list(),
        ("clusters", "get"): lambda: w.clusters.get(a.cluster_id),
        ("clusters", "start"): lambda: w.clusters.start(a.cluster_id),
        ("clusters", "restart"): lambda: w.clusters.restart(a.cluster_id),
        ("clusters", "delete"): lambda: w.clusters.delete(a.cluster_id),
        ("clusters", "permanent-delete"): lambda: w.clusters.permanent_delete(a.cluster_id),
        ("clusters", "events"): lambda: w.clusters.events(a.cluster_id),
        ("clusters", "forge-status"): lambda: w.clusters.forge_status(a.cluster_id),
        ("clusters", "resize"): lambda: w.clusters.resize(a.cluster_id, a.workers),
        ("clusters", "node-types"): lambda: w.clusters.list_node_types(),
        ("clusters", "spark-versions"): lambda: w.clusters.spark_versions(),
        ("clusters", "exec"): lambda: w.command_execution.run(a.cluster_id, a.code, a.language),
        ("workspace", "ls"): lambda: w.workspace.list(a.path, recursive=a.recursive),
        ("workspace", "mkdirs"): lambda: w.workspace.mkdirs(a.path),
        ("workspace", "rm"): lambda: w.workspace.delete(a.path, recursive=a.recursive),
        ("workspace", "search"): lambda: w.workspace.search(a.query),
        ("fs", "ls"): lambda: w.dbfs.list(_dbfs(a.path)),
        ("fs", "mkdirs"): lambda: w.dbfs.mkdirs(_dbfs(a.path)),
        ("fs", "rm"): lambda: w.dbfs.delete(_dbfs(a.path), recursive=a.recursive),
        ("fs", "mv"): lambda: w.dbfs.move(_dbfs(a.src), _dbfs(a.dst)),
        ("sql", "exec"): lambda: w.sql(a.statement, warehouse_id=a.warehouse_id, catalog=a.catalog, schema=a.schema),
        ("sql", "warehouses"): lambda: w.warehouses.list(),
        ("sql", "get-warehouse"): lambda: w.warehouses.get(a.warehouse_id),
        ("sql", "start-warehouse"): lambda: w.warehouses.start(a.warehouse_id),
        ("sql", "stop-warehouse"): lambda: w.warehouses.stop(a.warehouse_id),
        ("sql", "create-warehouse"): lambda: w.warehouses.create(a.name, cluster_size=a.size),
        ("sql", "history"): lambda: w.query_history.list(max_results=a.max),
        ("sql", "queries"): lambda: w.queries.list(),
        ("sql", "alerts"): lambda: w.alerts.list(),
        ("sql", "dashboards"): lambda: w.dashboards.list(),
        ("jobs", "list"): lambda: w.jobs.list(),
        ("jobs", "get"): lambda: w.jobs.get(a.job_id),
        ("jobs", "create"): lambda: w.jobs.create(**_json_arg(a.json)),
        ("jobs", "delete"): lambda: w.jobs.delete(a.job_id),
        ("jobs", "runs"): lambda: w.jobs.list_runs(job_id=a.job_id, active_only=a.active, limit=a.limit),
        ("jobs", "get-run"): lambda: w.jobs.get_run(a.run_id),
        ("jobs", "run-output"): lambda: w.jobs.get_run_output(a.run_id),
        ("jobs", "cancel-run"): lambda: w.jobs.cancel_run(a.run_id),
        ("pipelines", "list"): lambda: w.pipelines.list(),
        ("pipelines", "get"): lambda: w.pipelines.get(a.pipeline_id),
        ("pipelines", "create"): lambda: w.pipelines.create(**_json_arg(a.json)),
        ("pipelines", "delete"): lambda: w.pipelines.delete(a.pipeline_id),
        ("pipelines", "stop"): lambda: w.pipelines.stop(a.pipeline_id),
        ("pipelines", "events"): lambda: w.pipelines.events(a.pipeline_id),
        ("pipelines", "updates"): lambda: w.pipelines.list_updates(a.pipeline_id),
        ("catalog", "catalogs"): lambda: w.catalogs.list(),
        ("catalog", "schemas"): lambda: w.schemas.list(a.catalog),
        ("catalog", "tables"): lambda: w.tables.list(a.catalog, a.schema),
        ("catalog", "volumes"): lambda: w.volumes.list(a.catalog, a.schema),
        ("catalog", "functions"): lambda: w.functions.list(a.catalog, a.schema),
        ("catalog", "get-table"): lambda: w.tables.get(a.full_name),
        ("catalog", "create-catalog"): lambda: w.catalogs.create(a.name, comment=a.comment),
        ("catalog", "create-schema"): lambda: w.schemas.create(a.full_name.split(".", 1)[1], a.full_name.split(".", 1)[0], comment=a.comment),
        ("catalog", "grants"): lambda: w.grants.get(a.securable_type, a.full_name),
        ("catalog", "grant"): lambda: w.grants.grant(a.securable_type, a.full_name, a.principal, a.privileges),
        ("catalog", "revoke"): lambda: w.grants.revoke(a.securable_type, a.full_name, a.principal, a.privileges),
        ("secrets", "list-scopes"): lambda: w.secrets.list_scopes(),
        ("secrets", "create-scope"): lambda: w.secrets.create_scope(a.scope),
        ("secrets", "delete-scope"): lambda: w.secrets.delete_scope(a.scope),
        ("secrets", "list"): lambda: w.secrets.list_secrets(a.scope),
        ("secrets", "put"): lambda: w.secrets.put_secret(a.scope, a.key, a.value if a.value is not None else getpass.getpass("Value: ")),
        ("secrets", "get"): lambda: w.secrets.get_secret(a.scope, a.key),
        ("secrets", "delete"): lambda: w.secrets.delete_secret(a.scope, a.key),
        ("secrets", "acls"): lambda: w.secrets.list_acls(a.scope),
        ("secrets", "put-acl"): lambda: w.secrets.put_acl(a.scope, a.principal, a.permission),
        ("tokens", "list"): lambda: w.tokens.list(),
        ("tokens", "create"): lambda: w.tokens.create(a.comment, a.lifetime_days * 86400 if a.lifetime_days else None),
        ("tokens", "delete"): lambda: w.tokens.delete(a.token_id),
        ("users", "list"): lambda: w.users.list(filter=a.filter),
        ("users", "get"): lambda: w.users.get(a.id),
        ("users", "create"): lambda: w.users.create(a.user_name, a.display_name, a.password, groups=["admins"] if a.admin else None),
        ("users", "delete"): lambda: w.users.delete(a.id),
        ("users", "set-active"): lambda: w.users.set_active(a.id, a.active == "true"),
        ("groups", "list"): lambda: w.groups.list(),
        ("groups", "create"): lambda: w.groups.create(a.display_name),
        ("groups", "delete"): lambda: w.groups.delete(a.id),
        ("groups", "add-member"): lambda: w.groups.add_member(a.group_id, a.member_id),
        ("groups", "remove-member"): lambda: w.groups.remove_member(a.group_id, a.member_id),
        ("service-principals", "list"): lambda: w.service_principals.list(),
        ("service-principals", "create"): lambda: w.service_principals.create(a.display_name),
        ("service-principals", "delete"): lambda: w.service_principals.delete(a.id),
        ("service-principals", "create-secret"): lambda: w.service_principals.create_secret(a.id),
        ("repos", "list"): lambda: w.repos.list(),
        ("repos", "create"): lambda: w.repos.create(a.url, provider=a.provider, path=a.path, branch=a.branch),
        ("repos", "get"): lambda: w.repos.get(a.repo_id),
        ("repos", "update"): lambda: w.repos.update(a.repo_id, branch=a.branch, tag=a.tag),
        ("repos", "delete"): lambda: w.repos.delete(a.repo_id),
        ("repos", "status"): lambda: w.repos.status(a.repo_id),
        ("repos", "commit"): lambda: w.repos.commit(a.repo_id, a.message, push=not a.no_push),
        ("experiments", "list"): lambda: w.experiments.search(),
        ("experiments", "create"): lambda: {"experiment_id": w.experiments.create(a.name)},
        ("experiments", "runs"): lambda: w.runs.search([a.experiment_id]),
        ("experiments", "get-run"): lambda: w.runs.get(a.run_id),
        ("models", "list"): lambda: w.model_registry.search_models(),
        ("models", "get"): lambda: w.model_registry.get_model(a.name),
        ("models", "versions"): lambda: w.model_registry.search_versions(f"name='{a.name}'"),
        ("models", "set-alias"): lambda: w.model_registry.set_alias(a.name, a.alias, a.version),
        ("serving", "list"): lambda: w.serving_endpoints.list(),
        ("serving", "get"): lambda: w.serving_endpoints.get(a.name),
        ("serving", "delete"): lambda: w.serving_endpoints.delete(a.name),
        ("serving", "query"): lambda: w.serving_endpoints.query(a.name, **_json_arg(a.json)),
    }
    if (g, c) in H:
        return H[(g, c)]()

    # commands with extra logic
    if g == "api":
        return w.api_client.do(c.upper(), a.path, body=_json_arg(a.json))
    if g == "clusters" and c == "create":
        r = w.clusters.create(a.name, num_workers=a.workers, node_type_id=a.node_type)
        return w.clusters.wait_running(r["cluster_id"]) if a.wait else r
    if g == "workspace" and c == "import":
        with open(a.local, "rb") as f:
            data = f.read()
        lang = a.language or {".py": "PYTHON", ".sql": "SQL", ".scala": "SCALA", ".r": "R"}.get(os.path.splitext(a.local)[1].lower())
        return w.workspace.import_(a.path, data, format=a.format, language=lang, overwrite=a.overwrite)
    if g == "workspace" and c == "export":
        data = w.workspace.export(a.path, a.format)
        if a.local:
            with open(a.local, "wb") as f:
                f.write(data)
            return {"path": a.path, "written": a.local, "bytes": len(data)}
        sys.stdout.write(data.decode(errors="replace"))
        return None
    if g == "workspace" and c == "run":
        args = dict(kv.split("=", 1) for kv in a.arg)
        return w.notebooks.run(a.path, cluster_id=a.cluster_id, arguments=args)
    if g == "fs" and c == "cp":
        s_remote, d_remote = a.src.startswith("dbfs:"), a.dst.startswith("dbfs:")
        if s_remote and d_remote:
            return w.dbfs.copy(_dbfs(a.src), _dbfs(a.dst))
        if s_remote:
            w.dbfs.download(_dbfs(a.src), a.dst)
            return {"downloaded": a.dst}
        if d_remote:
            return w.dbfs.upload(_dbfs(a.dst), a.src, overwrite=a.overwrite)
        raise SystemExit("at least one side must be a dbfs: path")
    if g == "fs" and c == "cat":
        sys.stdout.write(w.dbfs.read_all(_dbfs(a.path)).decode(errors="replace"))
        return None
    if g == "jobs" and c == "run-now":
        params = _json_arg(a.params) or {}
        r = w.jobs.run_now(a.job_id, **params)
        return w.jobs.wait_run(r["run_id"]) if a.wait else r
    if g == "jobs" and c == "submit":
        r = w.jobs.submit(**_json_arg(a.json))
        return w.jobs.wait_run(r["run_id"]) if a.wait else r
    if g == "pipelines" and c == "start":
        r = w.pipelines.start_update(a.pipeline_id, full_refresh=a.full_refresh)
        return w.pipelines.wait_update(a.pipeline_id, r["update_id"]) if a.wait else r
    if g == "serving" and c == "create":
        r = w.serving_endpoints.create_for_model(a.name, a.model, a.version)
        return w.serving_endpoints.wait_ready(a.name) if a.wait else r
    raise SystemExit(f"unknown command {g} {c}")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    argv = list(sys.argv[1:] if argv is None else argv)
    g, rest = global_parser().parse_known_args(argv)
    a = build_parser().parse_args(rest)
    for k, v in vars(g).items():
        setattr(a, k, v)
    return a


def main(argv: Optional[list[str]] = None) -> int:
    a = parse_args(argv)
    try:
        out = dispatch(a)
    except LakeforgeError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except TimeoutError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if out is not None:
        _print(out, a.output)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
