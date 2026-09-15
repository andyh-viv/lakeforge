"""Thin HTTP client over the Lakeforge REST API (stdlib only)."""

from __future__ import annotations

import base64
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Iterator, Optional

from .config import Config


class LakeforgeError(Exception):
    """Raised for any non-2xx API response."""

    def __init__(self, status: int, error_code: str, message: str, method: str = "", path: str = ""):
        super().__init__(f"{error_code}: {message}" + (f" ({method} {path})" if path else ""))
        self.status = status
        self.error_code = error_code
        self.message = message
        self.method = method
        self.path = path


class NotFound(LakeforgeError):
    pass


class PermissionDenied(LakeforgeError):
    pass


class Unauthenticated(LakeforgeError):
    pass


class AlreadyExists(LakeforgeError):
    pass


class InvalidParameterValue(LakeforgeError):
    pass


_ERROR_BY_STATUS = {401: Unauthenticated, 403: PermissionDenied, 404: NotFound, 409: AlreadyExists, 400: InvalidParameterValue}
_RETRY_STATUSES = {429, 502, 503, 504}


class ApiClient:
    def __init__(self, config: Config):
        self.config = config

    # ---------------------------------------------------------------- core --
    def _headers(self, extra: Optional[dict] = None, content_type: Optional[str] = "application/json") -> dict:
        h = {"User-Agent": self.config.user_agent, "Accept": "application/json"}
        if content_type:
            h["Content-Type"] = content_type
        if self.config.token:
            h["Authorization"] = f"Bearer {self.config.token}"
        elif self.config.username and self.config.password:
            cred = base64.b64encode(f"{self.config.username}:{self.config.password}".encode()).decode()
            h["Authorization"] = f"Basic {cred}"
        h.update(self.config.extra_headers)
        if extra:
            h.update(extra)
        return h

    def do(
        self,
        method: str,
        path: str,
        query: Optional[dict] = None,
        body: Any = None,
        raw: bool = False,
        headers: Optional[dict] = None,
        data: Optional[bytes] = None,
        content_type: Optional[str] = "application/json",
    ) -> Any:
        url = self.config.host + path
        if query:
            q = {k: (json.dumps(v) if isinstance(v, (dict, list)) else str(v).lower() if isinstance(v, bool) else v) for k, v in query.items() if v is not None}
            if q:
                url += ("&" if "?" in url else "?") + urllib.parse.urlencode(q, doseq=True)
        payload = data if data is not None else (json.dumps(body).encode() if body is not None else None)
        if payload is None and method in ("POST", "PUT", "PATCH"):
            payload = b"{}"
        last: Optional[Exception] = None
        for attempt in range(self.config.retries + 1):
            req = urllib.request.Request(url, data=payload, method=method, headers=self._headers(headers, content_type))
            try:
                with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                    content = resp.read()
                    if raw:
                        return content
                    if not content:
                        return {}
                    ctype = resp.headers.get("Content-Type", "")
                    return json.loads(content) if "json" in ctype or content[:1] in (b"{", b"[") else content.decode(errors="replace")
            except urllib.error.HTTPError as e:
                content = e.read()
                if e.code in _RETRY_STATUSES and attempt < self.config.retries:
                    time.sleep(min(2**attempt, 8))
                    last = e
                    continue
                raise self._error(e.code, content, method, path) from None
            except urllib.error.URLError as e:
                last = e
                if attempt < self.config.retries:
                    time.sleep(min(2**attempt, 8))
                    continue
                raise LakeforgeError(0, "CONNECTION_ERROR", str(e.reason), method, path) from None
        raise LakeforgeError(0, "RETRY_EXHAUSTED", str(last), method, path)

    @staticmethod
    def _error(status: int, content: bytes, method: str, path: str) -> LakeforgeError:
        code, msg = f"HTTP_{status}", content.decode(errors="replace")[:500]
        try:
            j = json.loads(content)
            code = j.get("error_code", code)
            msg = j.get("message", j.get("error", msg))
        except (ValueError, AttributeError):
            pass
        return _ERROR_BY_STATUS.get(status, LakeforgeError)(status, code, msg, method, path)

    # ------------------------------------------------------------- helpers --
    def get(self, path: str, /, **query) -> Any:
        return self.do("GET", path, query=query or None)

    def post(self, path: str, body: Any = None, /, **query) -> Any:
        return self.do("POST", path, query=query or None, body=body)

    def put(self, path: str, body: Any = None, /, **query) -> Any:
        return self.do("PUT", path, query=query or None, body=body)

    def patch(self, path: str, body: Any = None, /, **query) -> Any:
        return self.do("PATCH", path, query=query or None, body=body)

    def delete(self, path: str, body: Any = None, /, **query) -> Any:
        return self.do("DELETE", path, query=query or None, body=body)

    def paginate(self, method: str, path: str, key: str, query: Optional[dict] = None, body: Optional[dict] = None) -> Iterator[dict]:
        """Follow ``next_page_token`` / ``has_more``+offset pagination."""
        query = dict(query or {})
        body = dict(body or {})
        offset = 0
        while True:
            res = self.do(method, path, query=query if method == "GET" else None, body=body if method != "GET" else None) or {}
            items = res.get(key) or []
            yield from items
            token = res.get("next_page_token")
            if token:
                (query if method == "GET" else body)["page_token"] = token
                continue
            if res.get("has_more") and items:
                offset += len(items)
                (query if method == "GET" else body)["offset"] = offset
                continue
            return

    def upload_multipart(self, path: str, fields: dict, filename: str, content: bytes, file_field: str = "contents") -> Any:
        boundary = "----lakeforge" + uuid.uuid4().hex
        parts = []
        for k, v in fields.items():
            parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode())
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"{file_field}\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n".encode())
        parts.append(content)
        parts.append(f"\r\n--{boundary}--\r\n".encode())
        return self.do("POST", path, data=b"".join(parts), content_type=f"multipart/form-data; boundary={boundary}")
