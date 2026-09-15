"""Lakeforge Python SDK.

    from lakeforge import WorkspaceClient

    w = WorkspaceClient()                         # LAKEFORGE_HOST / LAKEFORGE_TOKEN (or DATABRICKS_*)
    for c in w.clusters.list():
        print(c["cluster_id"], c["state"])
    rows = w.statement_execution.rows("SELECT 1 AS one")
"""

from __future__ import annotations

from typing import Optional

from . import services as _s
from .client import (
    AlreadyExists,
    ApiClient,
    InvalidParameterValue,
    LakeforgeError,
    NotFound,
    PermissionDenied,
    Unauthenticated,
)
from .config import Config
from .dbutils import DBUtils

__version__ = "0.1.0"

__all__ = [
    "WorkspaceClient",
    "Config",
    "ApiClient",
    "DBUtils",
    "LakeforgeError",
    "NotFound",
    "PermissionDenied",
    "Unauthenticated",
    "AlreadyExists",
    "InvalidParameterValue",
    "__version__",
]


class WorkspaceClient:
    """Entry point for the SDK; groups every service under one authenticated client."""

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        profile: Optional[str] = None,
        config: Optional[Config] = None,
        **kw,
    ):
        self.config = config or Config.resolve(host=host, token=token, username=username, password=password, profile=profile, **kw)
        self.api_client = ApiClient(self.config)
        a = self.api_client

        self.current_user = _s.CurrentUser(a)
        self.tokens = _s.Tokens(a)
        self.token_management = _s.TokenManagement(a)
        self.settings = _s.Settings(a)

        self.workspace = _s.Workspace(a)
        self.notebooks = _s.Notebooks(a)
        self.repos = _s.Repos(a)
        self.git_credentials = _s.GitCredentials(a)

        self.clusters = _s.Clusters(a)
        self.instance_pools = _s.InstancePools(a)
        self.cluster_policies = _s.ClusterPolicies(a)
        self.libraries = _s.Libraries(a)
        self.command_execution = _s.CommandExecution(a)

        self.jobs = _s.Jobs(a)
        self.pipelines = _s.Pipelines(a)

        self.warehouses = _s.Warehouses(a)
        self.statement_execution = _s.StatementExecution(a)
        self.queries = _s.Queries(a)
        self.query_history = _s.QueryHistory(a)
        self.alerts = _s.Alerts(a)
        self.dashboards = _s.Dashboards(a)

        self.metastores = _s.Metastores(a)
        self.catalogs = _s.Catalogs(a)
        self.schemas = _s.Schemas(a)
        self.tables = _s.Tables(a)
        self.volumes = _s.Volumes(a)
        self.functions = _s.Functions(a)
        self.external_locations = _s.ExternalLocations(a)
        self.storage_credentials = _s.StorageCredentials(a)
        self.connections = _s.Connections(a)
        self.grants = _s.Grants(a)

        self.dbfs = _s.Dbfs(a)
        self.files = _s.Files(a)
        self.secrets = _s.Secrets(a)

        self.users = _s.Users(a)
        self.groups = _s.Groups(a)
        self.service_principals = _s.ServicePrincipals(a)
        self.permissions = _s.Permissions(a)
        self.workspace_conf = _s.WorkspaceConf(a)
        self.ip_access_lists = _s.IpAccessLists(a)
        self.global_init_scripts = _s.GlobalInitScripts(a)

        self.experiments = _s.Experiments(a)
        self.runs = _s.Runs(a)
        self.model_registry = _s.ModelRegistry(a)
        self.serving_endpoints = _s.ServingEndpoints(a)

        self.dbutils = DBUtils(self)

    # -------------------------------------------------------------- helpers --
    def sql(self, statement: str, **kw) -> list:
        """Run a SQL statement on the default (or given) warehouse and return rows as dicts."""
        return self.statement_execution.rows(statement, **kw)

    def login(self, username: str, password: str) -> str:
        """Exchange a username/password for a session JWT and use it for subsequent calls."""
        res = self.api_client.post("/api/2.0/lakeforge/login", {"username": username, "password": password})
        self.config.token = res["access_token"]
        self.config.username = self.config.password = None
        return res["access_token"]

    def __repr__(self) -> str:
        return f"WorkspaceClient(host={self.config.host!r}, auth={self.config.auth_type})"
