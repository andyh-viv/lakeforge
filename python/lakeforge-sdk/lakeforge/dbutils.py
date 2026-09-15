"""Standalone ``dbutils`` for scripts and IDEs (outside a notebook kernel).

Inside Lakeforge notebooks ``dbutils`` is injected by the kernel; this module
gives local code the same surface (``fs``, ``secrets``, ``widgets``,
``notebook``, ``jobs``, ``library``) backed by the REST API:

    from lakeforge import WorkspaceClient
    dbutils = WorkspaceClient().dbutils
    dbutils.fs.ls("/")
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:  # pragma: no cover
    from . import WorkspaceClient


class FileInfo(dict):
    """Row-like record with attribute access (``f.path``, ``f.name``, ``f.size``)."""

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as e:
            raise AttributeError(k) from e

    def isDir(self) -> bool:
        return self["name"].endswith("/")

    def isFile(self) -> bool:
        return not self.isDir()


class NotebookExit(Exception):
    def __init__(self, value: Any):
        super().__init__(str(value))
        self.value = value


class _FS:
    def __init__(self, w: "WorkspaceClient"):
        self._w = w

    def ls(self, path: str) -> list[FileInfo]:
        return [
            FileInfo(path=f["path"], name=f["path"].rstrip("/").split("/")[-1] + ("/" if f["is_dir"] else ""), size=f.get("file_size", 0), modificationTime=f.get("modification_time", 0))
            for f in self._w.dbfs.list(path)
        ]

    def mkdirs(self, path: str) -> bool:
        self._w.dbfs.mkdirs(path)
        return True

    def rm(self, path: str, recurse: bool = False) -> bool:
        self._w.dbfs.delete(path, recursive=recurse)
        return True

    def put(self, path: str, contents: str, overwrite: bool = False) -> bool:
        self._w.dbfs.put(path, contents, overwrite=overwrite)
        return True

    def head(self, path: str, max_bytes: int = 65536) -> str:
        return self._w.dbfs.read(path, 0, max_bytes).decode(errors="replace")

    def cp(self, src: str, dst: str, recurse: bool = False) -> bool:
        self._w.dbfs.copy(src, dst, recursive=recurse)
        return True

    def mv(self, src: str, dst: str, recurse: bool = False) -> bool:
        self._w.dbfs.move(src, dst)
        return True

    def mount(self, *_, **__):
        raise NotImplementedError("DBFS mounts are not supported; use Unity Catalog volumes or external locations")

    def unmount(self, *_, **__):
        raise NotImplementedError("DBFS mounts are not supported; use Unity Catalog volumes or external locations")

    def mounts(self) -> list:
        return []

    def help(self) -> None:
        print("fs: ls, mkdirs, rm, put, head, cp, mv")


class _Secrets:
    def __init__(self, w: "WorkspaceClient"):
        self._w = w

    def get(self, scope: str, key: str) -> str:
        return self._w.secrets.get_secret(scope, key)

    def getBytes(self, scope: str, key: str) -> bytes:
        return self.get(scope, key).encode()

    def list(self, scope: str) -> list[FileInfo]:
        return [FileInfo(key=s["key"]) for s in self._w.secrets.list_secrets(scope)]

    def listScopes(self) -> list[FileInfo]:
        return [FileInfo(name=s["name"]) for s in self._w.secrets.list_scopes()]

    def help(self) -> None:
        print("secrets: get, getBytes, list, listScopes")


class _Widgets:
    def __init__(self, values: Optional[dict] = None):
        self._v = dict(values or {})
        env = os.environ.get("LAKEFORGE_WIDGETS")
        if env:
            self._v.update(json.loads(env))

    def text(self, name: str, defaultValue: str = "", label: Optional[str] = None) -> None:
        self._v.setdefault(name, defaultValue)

    def dropdown(self, name: str, defaultValue: str, choices: Optional[list] = None, label: Optional[str] = None) -> None:
        self._v.setdefault(name, defaultValue)

    combobox = dropdown

    def multiselect(self, name: str, defaultValue: str, choices: Optional[list] = None, label: Optional[str] = None) -> None:
        self._v.setdefault(name, defaultValue)

    def get(self, name: str) -> str:
        if name not in self._v:
            raise KeyError(f"No input widget named {name} is defined")
        return self._v[name]

    def getArgument(self, name: str, defaultValue: Optional[str] = None) -> Optional[str]:
        return self._v.get(name, defaultValue)

    def getAll(self) -> dict:
        return dict(self._v)

    def remove(self, name: str) -> None:
        self._v.pop(name, None)

    def removeAll(self) -> None:
        self._v.clear()

    def help(self) -> None:
        print("widgets: text, dropdown, combobox, multiselect, get, getArgument, getAll, remove, removeAll")


class _Notebook:
    def __init__(self, w: "WorkspaceClient"):
        self._w = w

    def exit(self, value: Any = "") -> None:
        raise NotebookExit(value)

    def run(self, path: str, timeout_seconds: int = 0, arguments: Optional[dict] = None, cluster_id: Optional[str] = None) -> str:
        cid = cluster_id or os.environ.get("LAKEFORGE_CLUSTER_ID")
        return self._w.notebooks.run(path, cluster_id=cid, arguments=arguments, timeout_seconds=timeout_seconds).get("result", "")

    def getContext(self) -> FileInfo:
        return FileInfo(notebookPath=os.environ.get("LAKEFORGE_NOTEBOOK_PATH"), clusterId=os.environ.get("LAKEFORGE_CLUSTER_ID"), apiUrl=self._w.config.host)

    def help(self) -> None:
        print("notebook: exit, run, getContext")


class _TaskValues:
    def __init__(self, w: "WorkspaceClient"):
        self._w = w
        self._local: dict = {}

    def set(self, key: str, value: Any) -> None:
        self._local[key] = value
        ctx = os.environ.get("LAKEFORGE_CONTEXT_ID")
        if ctx:
            self._w.api_client.post("/api/2.0/lakeforge/task-values", {"context_id": ctx, "key": key, "value": value})

    def get(self, taskKey: str, key: str, default: Any = None, debugValue: Any = None) -> Any:
        ctx = os.environ.get("LAKEFORGE_CONTEXT_ID")
        if not ctx:
            return self._local.get(key, debugValue if debugValue is not None else default)
        r = self._w.api_client.get("/api/2.0/lakeforge/task-values", context_id=ctx, task_key=taskKey, key=key)
        return r.get("value", default if default is not None else debugValue)


class _Jobs:
    def __init__(self, w: "WorkspaceClient"):
        self.taskValues = _TaskValues(w)

    def help(self) -> None:
        print("jobs: taskValues.set, taskValues.get")


class _Library:
    def __init__(self, w: "WorkspaceClient"):
        self._w = w

    def install(self, cluster_id: str, *libraries: str) -> None:
        self._w.libraries.install(cluster_id, [{"pypi": {"package": l}} for l in libraries])

    def restartPython(self) -> None:
        pass

    def help(self) -> None:
        print("library: install, restartPython")


class DBUtils:
    def __init__(self, w: "WorkspaceClient", widgets: Optional[dict] = None):
        self.fs = _FS(w)
        self.secrets = _Secrets(w)
        self.widgets = _Widgets(widgets)
        self.notebook = _Notebook(w)
        self.jobs = _Jobs(w)
        self.library = _Library(w)

    def help(self) -> None:
        print("dbutils: fs, secrets, widgets, notebook, jobs, library")
