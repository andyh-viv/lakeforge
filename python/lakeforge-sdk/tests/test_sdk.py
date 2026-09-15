import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from lakeforge import LakeforgeError, NotFound, WorkspaceClient
from lakeforge.cli import main


class Handler(BaseHTTPRequestHandler):
    calls = []

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}") if n else {}

    def _send(self, code, payload):
        data = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do(self):
        path, _, qs = self.path.partition("?")
        body = self._body() if self.command in ("POST", "PUT", "PATCH", "DELETE") else {}
        Handler.calls.append((self.command, path, qs, body, self.headers.get("Authorization")))
        if path == "/api/2.0/clusters/list":
            return self._send(200, {"clusters": [{"cluster_id": "c1", "state": "RUNNING"}]})
        if path == "/api/2.0/sql/warehouses":
            return self._send(200, {"warehouses": [{"id": "w1", "state": "RUNNING"}]})
        if path == "/api/2.0/sql/statements":
            return self._send(200, {"statement_id": "s1", "status": {"state": "SUCCEEDED"}, "manifest": {"schema": {"columns": [{"name": "one"}]}}, "result": {"data_array": [[1]]}})
        if path == "/api/2.0/secrets/get":
            return self._send(200, {"value": base64.b64encode(b"s3cret").decode()})
        if path == "/api/2.1/jobs/list":
            if "page_token=p2" in qs:
                return self._send(200, {"jobs": [{"job_id": 2}]})
            return self._send(200, {"jobs": [{"job_id": 1}], "next_page_token": "p2"})
        if path == "/api/2.0/lakeforge/login":
            return self._send(200, {"access_token": "jwt-123"})
        if path == "/api/2.0/dbfs/put":
            return self._send(200, {})
        return self._send(404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": f"no {path}"})

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do

    def log_message(self, *_):
        pass


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch, tmp_path):
    for k in ("LAKEFORGE_HOST", "LAKEFORGE_TOKEN", "LAKEFORGE_USER", "LAKEFORGE_PASSWORD",
              "DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_USERNAME", "DATABRICKS_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("LAKEFORGE_CONFIG_FILE", str(tmp_path / "lakeforgecfg"))
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(tmp_path / "databrickscfg"))


@pytest.fixture(scope="module")
def server():
    srv = HTTPServer(("127.0.0.1", 0), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


@pytest.fixture
def w(server):
    Handler.calls.clear()
    return WorkspaceClient(host=server, token="dapi-test", retries=0)


def test_bearer_auth_and_list(w):
    assert w.clusters.list()[0]["cluster_id"] == "c1"
    assert Handler.calls[-1][4] == "Bearer dapi-test"


def test_basic_auth(server):
    c = WorkspaceClient(host=server, username="u", password="p", retries=0)
    c.clusters.list()
    assert Handler.calls[-1][4].startswith("Basic ")


def test_sql_rows_uses_default_warehouse(w):
    assert w.sql("SELECT 1 AS one") == [{"one": 1}]
    post = [c for c in Handler.calls if c[1] == "/api/2.0/sql/statements"][0]
    assert post[3]["warehouse_id"] == "w1" and post[3]["statement"] == "SELECT 1 AS one"


def test_pagination_follows_next_page_token(w):
    assert [j["job_id"] for j in w.jobs.list()] == [1, 2]


def test_secret_roundtrip_and_dbutils(w):
    assert w.secrets.get_secret("sc", "k") == "s3cret"
    assert w.dbutils.secrets.get("sc", "k") == "s3cret"
    w.dbutils.fs.put("/tmp/x", "hello", overwrite=True)
    put = [c for c in Handler.calls if c[1] == "/api/2.0/dbfs/put"][0][3]
    assert base64.b64decode(put["contents"]) == b"hello" and put["overwrite"] is True


def test_errors_are_typed(w):
    with pytest.raises(NotFound) as e:
        w.clusters.get("nope")
    assert e.value.error_code == "RESOURCE_DOES_NOT_EXIST" and e.value.status == 404
    assert isinstance(e.value, LakeforgeError)


def test_login_switches_to_jwt(w):
    assert w.login("a", "b") == "jwt-123"
    w.clusters.list()
    assert Handler.calls[-1][4] == "Bearer jwt-123"


def test_cli_json_output(server, capsys):
    rc = main(["--host", server, "--token", "t", "-o", "json", "clusters", "list"])
    assert rc == 0
    assert json.loads(capsys.readouterr().out)[0]["cluster_id"] == "c1"


def test_cli_error_exit_code(server, capsys):
    rc = main(["--host", server, "--token", "t", "clusters", "get", "nope"])
    assert rc == 1 and "RESOURCE_DOES_NOT_EXIST" in capsys.readouterr().err
