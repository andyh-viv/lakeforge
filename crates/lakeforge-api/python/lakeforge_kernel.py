"""Lakeforge notebook kernel.

Runs one Python namespace per execution context and speaks JSON lines with the
control plane over stdin/stdout. Provides ``spark`` (SQL on the attached Forge
cluster), ``display`` and ``dbutils`` (fs, secrets, widgets, notebook, jobs)
via the Lakeforge REST API.
"""
import base64
import io
import json
import os
import signal
import sys
import threading
import traceback
import urllib.error
import urllib.request

URL = os.environ.get("LAKEFORGE_URL", "http://127.0.0.1:8080").rstrip("/")
TOKEN = os.environ.get("LAKEFORGE_TOKEN", "")
CLUSTER_ID = os.environ.get("LAKEFORGE_CLUSTER_ID", "")
CONTEXT_ID = os.environ.get("LAKEFORGE_CONTEXT_ID", "")
NOTEBOOK_PATH = os.environ.get("LAKEFORGE_NOTEBOOK_PATH", "")

_out_lock = threading.Lock()
_current_id = None


def _emit(event):
    with _out_lock:
        sys.__stdout__.write(json.dumps(event, default=str) + "\n")
        sys.__stdout__.flush()


def _api(method, path, body=None, raw=False):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(URL + path, data=data, method=method)
    req.add_header("Authorization", "Bearer " + TOKEN)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            payload = r.read()
    except urllib.error.HTTPError as e:
        payload = e.read()
        try:
            msg = json.loads(payload).get("message", payload.decode())
        except Exception:
            msg = payload.decode(errors="replace")
        raise RuntimeError(msg)
    if raw:
        return payload
    return json.loads(payload) if payload else {}


class _Stream(io.TextIOBase):
    def __init__(self, kind):
        self.kind = kind

    def write(self, s):
        if s and _current_id is not None:
            _emit({"id": _current_id, "type": self.kind, "text": s})
        return len(s)

    def flush(self):
        pass


class NotebookExit(Exception):
    def __init__(self, value):
        super().__init__(value)
        self.value = value


# ---------------------------------------------------------------- DataFrame --

class Row(dict):
    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as e:
            raise AttributeError(k) from e

    def asDict(self):
        return dict(self)


class Column:
    def __init__(self, name, type_name, type_text):
        self.name, self.typeName, self.typeText = name, type_name, type_text

    def __repr__(self):
        return f"{self.name}: {self.typeText}"


def _coerce(v, type_name):
    if v is None:
        return None
    try:
        if type_name in ("INT", "LONG", "SHORT", "BYTE"):
            return int(v)
        if type_name in ("DOUBLE", "FLOAT", "DECIMAL"):
            return float(v)
        if type_name == "BOOLEAN":
            return v.lower() == "true"
    except (ValueError, AttributeError):
        pass
    return v


class DataFrame:
    """Materialised result of a SQL statement (Databricks JSON_ARRAY format)."""

    def __init__(self, sql, result=None):
        self._sql = sql
        self._result = result

    def _run(self):
        if self._result is None:
            self._result = spark._execute(self._sql)
        return self._result

    @property
    def schema(self):
        cols = self._run()["manifest"]["schema"]["columns"]
        return [Column(c["name"], c["type_name"], c["type_text"]) for c in cols]

    @property
    def columns(self):
        return [c.name for c in self.schema]

    def collect(self):
        r = self._run()
        cols = r["manifest"]["schema"]["columns"]
        rows = r.get("result", {}).get("data_array", []) or []
        return [Row({c["name"]: _coerce(v, c["type_name"]) for c, v in zip(cols, row)}) for row in rows]

    def count(self):
        return len(self.collect())

    def first(self):
        rows = self.collect()
        return rows[0] if rows else None

    def take(self, n):
        return self.collect()[:n]

    def limit(self, n):
        return DataFrame(f"SELECT * FROM ({self._sql}) __lf LIMIT {int(n)}")

    def filter(self, cond):
        return DataFrame(f"SELECT * FROM ({self._sql}) __lf WHERE {cond}")

    where = filter

    def select(self, *cols):
        return DataFrame(f"SELECT {', '.join(cols)} FROM ({self._sql}) __lf")

    def orderBy(self, *cols):
        return DataFrame(f"SELECT * FROM ({self._sql}) __lf ORDER BY {', '.join(cols)}")

    def groupBy(self, *cols):
        return GroupedData(self, cols)

    def createOrReplaceTempView(self, name):
        spark.sql(f"CREATE OR REPLACE VIEW {name} AS {self._sql}")._run()

    def toPandas(self):
        import pandas as pd  # noqa: WPS433
        rows = self.collect()
        return pd.DataFrame([dict(r) for r in rows], columns=self.columns)

    def show(self, n=20, truncate=True):
        rows = self.collect()[:n]
        cols = self.columns
        widths = [len(c) for c in cols]
        str_rows = []
        for r in rows:
            sr = []
            for i, c in enumerate(cols):
                s = "null" if r[c] is None else str(r[c])
                if truncate and len(s) > 20:
                    s = s[:17] + "..."
                widths[i] = max(widths[i], len(s))
                sr.append(s)
            str_rows.append(sr)
        sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
        print(sep)
        print("|" + "|".join(f" {c:<{w}} " for c, w in zip(cols, widths)) + "|")
        print(sep)
        for sr in str_rows:
            print("|" + "|".join(f" {s:<{w}} " for s, w in zip(sr, widths)) + "|")
        print(sep)

    def display(self):
        display(self)

    def __repr__(self):
        return f"DataFrame[{', '.join(map(repr, self.schema))}]"


class GroupedData:
    def __init__(self, df, cols):
        self.df, self.cols = df, cols

    def agg(self, *exprs):
        g = ", ".join(self.cols)
        return DataFrame(f"SELECT {g}, {', '.join(exprs)} FROM ({self.df._sql}) __lf GROUP BY {g}")

    def count(self):
        return self.agg("count(*) AS count")

    def sum(self, col):
        return self.agg(f"sum({col}) AS sum_{col}")

    def avg(self, col):
        return self.agg(f"avg({col}) AS avg_{col}")


class _Reader:
    def __init__(self, fmt="parquet"):
        self._fmt = fmt
        self._opts = {}

    def format(self, fmt):
        self._fmt = fmt
        return self

    def option(self, k, v):
        self._opts[k] = v
        return self

    def load(self, path):
        opts = "".join(f", '{k}' '{v}'" for k, v in self._opts.items())
        return DataFrame(f"SELECT * FROM {self._fmt}_scan('{path}'{opts})")

    def parquet(self, path):
        return self.format("parquet").load(path)

    def csv(self, path, header=True, **_):
        return self.format("csv").option("has_header", str(header).lower()).load(path)

    def json(self, path):
        return self.format("json").load(path)

    def table(self, name):
        return DataFrame(f"SELECT * FROM {name}")


class SparkSession:
    def __init__(self):
        self.conf = _Conf()
        self.read = _Reader()

    def _execute(self, sql):
        body = {"statement": sql, "cluster_id": CLUSTER_ID, "wait_timeout": "50s", "row_limit": 10000, "on_wait_timeout": "CONTINUE"}
        r = _api("POST", "/api/2.0/sql/statements", body)
        while r.get("status", {}).get("state") in ("PENDING", "RUNNING"):
            r = _api("GET", f"/api/2.0/sql/statements/{r['statement_id']}")
        st = r.get("status", {})
        if st.get("state") != "SUCCEEDED":
            err = st.get("error", {})
            raise RuntimeError(err.get("message", f"statement {st.get('state')}"))
        return r

    def sql(self, query, **kwargs):
        if kwargs:
            query = query.format(**kwargs)
        return DataFrame(query)

    def table(self, name):
        return DataFrame(f"SELECT * FROM {name}")

    def range(self, start, end=None, step=1):
        if end is None:
            start, end = 0, start
        return DataFrame(f"SELECT * FROM range({int(start)}, {int(end)}, {int(step)})")

    def createDataFrame(self, data, schema=None):
        if hasattr(data, "to_dict"):
            cols = list(data.columns)
            data = data.values.tolist()
        else:
            data = [list(r) if not isinstance(r, dict) else list(r.values()) for r in data]
            cols = schema if isinstance(schema, (list, tuple)) else None
        if not data:
            raise ValueError("empty data")
        if cols is None:
            cols = [f"_{i + 1}" for i in range(len(data[0]))]

        def lit(v):
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            if isinstance(v, (int, float)):
                return repr(v)
            return "'" + str(v).replace("'", "''") + "'"

        values = ", ".join("(" + ", ".join(lit(v) for v in row) + ")" for row in data)
        return DataFrame(f"SELECT * FROM (VALUES {values}) AS t({', '.join(cols)})")


class _Conf:
    def __init__(self):
        self._c = {}

    def set(self, k, v):
        self._c[k] = v
        spark._execute(f"SET {k} = {v}")

    def get(self, k, default=None):
        return self._c.get(k, default)


# ------------------------------------------------------------------ dbutils --

class _FS:
    def ls(self, path):
        res = _api("GET", "/api/2.0/dbfs/list?path=" + urllib.request.quote(path, safe="/:"))
        return [Row(path=f["path"], name=f["path"].rstrip("/").split("/")[-1] + ("/" if f["is_dir"] else ""), size=f["file_size"], modificationTime=f.get("modification_time", 0)) for f in res.get("files", [])]

    def mkdirs(self, path):
        _api("POST", "/api/2.0/dbfs/mkdirs", {"path": path})
        return True

    def rm(self, path, recurse=False):
        _api("POST", "/api/2.0/dbfs/delete", {"path": path, "recursive": recurse})
        return True

    def put(self, path, contents, overwrite=False):
        _api("POST", "/api/2.0/dbfs/put", {"path": path, "contents": base64.b64encode(contents.encode()).decode(), "overwrite": overwrite})
        return True

    def head(self, path, max_bytes=65536):
        r = _api("GET", f"/api/2.0/dbfs/read?path={urllib.request.quote(path, safe='/:')}&offset=0&length={max_bytes}")
        return base64.b64decode(r["data"]).decode(errors="replace")

    def cp(self, src, dst, recurse=False):
        _api("POST", "/api/2.0/dbfs/copy", {"source_path": src, "destination_path": dst, "recursive": recurse})
        return True

    def mv(self, src, dst, recurse=False):
        _api("POST", "/api/2.0/dbfs/move", {"source_path": src, "destination_path": dst})
        return True


class _Secrets:
    def get(self, scope, key):
        r = _api("GET", f"/api/2.0/secrets/get?scope={scope}&key={key}")
        return base64.b64decode(r["value"]).decode()

    def list(self, scope):
        return [Row(key=s["key"]) for s in _api("GET", f"/api/2.0/secrets/list?scope={scope}").get("secrets", [])]

    def listScopes(self):
        return [Row(name=s["name"]) for s in _api("GET", "/api/2.0/secrets/scopes/list").get("scopes", [])]


class _Widgets:
    def __init__(self):
        self._v = json.loads(os.environ.get("LAKEFORGE_WIDGETS", "{}") or "{}")

    def text(self, name, default="", label=None):
        self._v.setdefault(name, default)

    dropdown = combobox = multiselect = lambda self, name, default="", choices=None, label=None: self._v.setdefault(name, default)

    def get(self, name):
        if name not in self._v:
            raise KeyError(f"No input widget named {name} is defined")
        return self._v[name]

    def getAll(self):
        return dict(self._v)

    def remove(self, name):
        self._v.pop(name, None)

    def removeAll(self):
        self._v.clear()


class _Notebook:
    def exit(self, value=""):
        raise NotebookExit(str(value))

    def run(self, path, timeout_seconds=0, arguments=None):
        r = _api("POST", "/api/2.0/lakeforge/notebooks/run", {"path": path, "cluster_id": CLUSTER_ID, "arguments": arguments or {}, "timeout_seconds": timeout_seconds})
        return r.get("result", "")

    def getContext(self):
        return Row(notebookPath=NOTEBOOK_PATH, clusterId=CLUSTER_ID)


class _Jobs:
    class _TaskValues:
        def __init__(self):
            self._v = {}

        def set(self, key, value):
            self._v[key] = value
            _api("POST", "/api/2.0/lakeforge/task-values", {"context_id": CONTEXT_ID, "key": key, "value": value})

        def get(self, taskKey, key, default=None, debugValue=None):
            r = _api("GET", f"/api/2.0/lakeforge/task-values?context_id={CONTEXT_ID}&task_key={taskKey}&key={key}")
            return r.get("value", default if default is not None else debugValue)

    def __init__(self):
        self.taskValues = self._TaskValues()


class DBUtils:
    def __init__(self):
        self.fs = _FS()
        self.secrets = _Secrets()
        self.widgets = _Widgets()
        self.notebook = _Notebook()
        self.jobs = _Jobs()

    def help(self):
        print("dbutils.fs, dbutils.secrets, dbutils.widgets, dbutils.notebook, dbutils.jobs")


# ------------------------------------------------------------------ display --

def display(obj, *_, **__):
    if isinstance(obj, DataFrame):
        r = obj._run()
        cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
        rows = r.get("result", {}).get("data_array", []) or []
        _emit({"id": _current_id, "type": "table", "columns": cols, "rows": rows, "truncated": bool(r["manifest"].get("truncated"))})
        return
    if hasattr(obj, "to_dict") and hasattr(obj, "columns"):
        rows = obj.astype(object).where(obj.notna(), None).values.tolist()
        _emit({"id": _current_id, "type": "table", "columns": [str(c) for c in obj.columns], "rows": rows, "truncated": False})
        return
    if hasattr(obj, "_repr_html_"):
        _emit({"id": _current_id, "type": "display", "mime": "text/html", "data": obj._repr_html_()})
        return
    if hasattr(obj, "savefig"):
        buf = io.BytesIO()
        obj.savefig(buf, format="png", bbox_inches="tight")
        _emit({"id": _current_id, "type": "display", "mime": "image/png", "data": base64.b64encode(buf.getvalue()).decode()})
        return
    if isinstance(obj, (dict, list)):
        _emit({"id": _current_id, "type": "display", "mime": "application/json", "data": obj})
        return
    print(obj)


def displayHTML(html):
    _emit({"id": _current_id, "type": "display", "mime": "text/html", "data": str(html)})


spark = SparkSession()
dbutils = DBUtils()
sc = Row(appName="lakeforge", applicationId=CLUSTER_ID, version="forge-0.1")

NAMESPACE = {
    "__name__": "__main__",
    "spark": spark,
    "dbutils": dbutils,
    "display": display,
    "displayHTML": displayHTML,
    "sc": sc,
    "DataFrame": DataFrame,
    "Row": Row,
}


# ----------------------------------------------------------------- magics ---

def _run_magic(magic, arg, body):
    if magic == "sql":
        df = spark.sql(body if body.strip() else arg)
        display(df)
        NAMESPACE["_sqldf"] = df
    elif magic == "md" or magic == "md-sandbox":
        _emit({"id": _current_id, "type": "display", "mime": "text/markdown", "data": body if body.strip() else arg})
    elif magic == "sh":
        import subprocess
        p = subprocess.run(body if body.strip() else arg, shell=True, capture_output=True, text=True)
        if p.stdout:
            print(p.stdout, end="")
        if p.stderr:
            print(p.stderr, end="", file=sys.stderr)
    elif magic == "pip":
        import subprocess
        args = (arg + " " + body).split()
        p = subprocess.run([sys.executable, "-m", "pip"] + args, capture_output=True, text=True)
        print(p.stdout + p.stderr, end="")
    elif magic == "fs":
        parts = (arg + " " + body).split()
        cmd, rest = parts[0], parts[1:]
        if cmd == "ls":
            display(dbutils.fs.ls(rest[0] if rest else "/") and _rows_df(dbutils.fs.ls(rest[0] if rest else "/")))
        elif cmd == "mkdirs":
            dbutils.fs.mkdirs(rest[0])
        elif cmd == "rm":
            dbutils.fs.rm(rest[-1], "-r" in rest)
        elif cmd == "head":
            print(dbutils.fs.head(rest[0]))
        else:
            raise ValueError(f"unsupported %fs command {cmd}")
    elif magic == "run":
        r = _api("POST", "/api/2.0/lakeforge/notebooks/run", {"path": arg.strip(), "cluster_id": CLUSTER_ID, "arguments": {}, "inline_context": CONTEXT_ID})
        if r.get("result"):
            print(r["result"])
    elif magic in ("python", "py"):
        _exec_python(body)
    else:
        raise ValueError(f"unsupported magic %{magic}")


def _rows_df(rows):
    if not rows:
        return DataFrame("SELECT NULL AS path WHERE 1=0")
    cols = list(rows[0].keys())
    return spark.createDataFrame([[r[c] for c in cols] for r in rows], cols)


def _exec_python(code):
    import ast
    tree = ast.parse(code, mode="exec")
    if tree.body and isinstance(tree.body[-1], ast.Expr):
        last = tree.body.pop()
        exec(compile(tree, "<cell>", "exec"), NAMESPACE)
        value = eval(compile(ast.Expression(last.value), "<cell>", "eval"), NAMESPACE)
        if value is not None:
            NAMESPACE["_"] = value
            if isinstance(value, DataFrame):
                display(value)
            else:
                _emit({"id": _current_id, "type": "result", "text": repr(value)})
    else:
        exec(compile(tree, "<cell>", "exec"), NAMESPACE)


def _execute(req):
    global _current_id
    _current_id = req["id"]
    code = req.get("code", "")
    language = (req.get("language") or "python").lower()
    status = "ok"
    try:
        stripped = code.lstrip()
        if stripped.startswith("%"):
            first, _, body = stripped.partition("\n")
            magic, _, arg = first[1:].partition(" ")
            _run_magic(magic.strip(), arg, body)
        elif language == "sql":
            _run_magic("sql", "", code)
        elif language in ("markdown", "md"):
            _run_magic("md", "", code)
        elif language in ("sh", "shell", "bash"):
            _run_magic("sh", "", code)
        else:
            _exec_python(code)
    except NotebookExit as e:
        _emit({"id": _current_id, "type": "exit", "value": e.value})
    except KeyboardInterrupt:
        status = "cancelled"
        _emit({"id": _current_id, "type": "error", "ename": "KeyboardInterrupt", "evalue": "Command cancelled", "traceback": []})
    except BaseException as e:  # noqa: BLE001
        status = "error"
        tb = traceback.format_exception(type(e), e, e.__traceback__)
        tb = [t for t in tb if "lakeforge_kernel.py" not in t]
        _emit({"id": _current_id, "type": "error", "ename": type(e).__name__, "evalue": str(e), "traceback": tb})
    finally:
        _emit({"id": _current_id, "type": "done", "status": status})
        _current_id = None


def main():
    sys.stdout = _Stream("stdout")
    sys.stderr = _Stream("stderr")
    signal.signal(signal.SIGINT, signal.default_int_handler)
    for line in sys.__stdin__:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        if req.get("op") == "interrupt":
            continue  # SIGINT is delivered by the control plane
        _execute(req)


if __name__ == "__main__":
    main()
