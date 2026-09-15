"""Client configuration resolution.

Resolution order (first match wins), mirroring the Databricks unified auth
conventions so existing tooling keeps working against Lakeforge:

1. explicit constructor arguments
2. ``LAKEFORGE_HOST`` / ``LAKEFORGE_TOKEN`` (or ``LAKEFORGE_USER`` + ``LAKEFORGE_PASSWORD``)
3. ``DATABRICKS_HOST`` / ``DATABRICKS_TOKEN``
4. the ``[<profile>]`` section of ``~/.lakeforgecfg`` or ``~/.databrickscfg``
"""

from __future__ import annotations

import configparser
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

DEFAULT_HOST = "http://localhost:8080"


@dataclass
class Config:
    host: str = DEFAULT_HOST
    token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    profile: str = "DEFAULT"
    timeout: float = 60.0
    retries: int = 3
    user_agent: str = "lakeforge-sdk/0.1.0"
    extra_headers: dict = field(default_factory=dict)

    @classmethod
    def resolve(
        cls,
        host: Optional[str] = None,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        profile: Optional[str] = None,
        **kw,
    ) -> "Config":
        env = os.environ
        profile = profile or env.get("LAKEFORGE_CONFIG_PROFILE") or env.get("DATABRICKS_CONFIG_PROFILE") or "DEFAULT"
        file_cfg = _read_profile(profile)
        host = host or env.get("LAKEFORGE_HOST") or env.get("DATABRICKS_HOST") or file_cfg.get("host") or DEFAULT_HOST
        explicit = bool(token or (username and password))
        if not explicit:
            token = env.get("LAKEFORGE_TOKEN") or env.get("DATABRICKS_TOKEN")
            username = username or env.get("LAKEFORGE_USER") or env.get("DATABRICKS_USERNAME")
            password = password or env.get("LAKEFORGE_PASSWORD") or env.get("DATABRICKS_PASSWORD")
        if not token and not (username and password):
            token = file_cfg.get("token")
            username = username or file_cfg.get("username")
            password = password or file_cfg.get("password")
        if not host.startswith("http://") and not host.startswith("https://"):
            host = "https://" + host
        return cls(host=host.rstrip("/"), token=token, username=username, password=password, profile=profile, **kw)

    @property
    def auth_type(self) -> str:
        if self.token:
            return "pat"
        if self.username and self.password:
            return "basic"
        return "none"


def config_paths() -> list[Path]:
    home = Path(os.environ.get("LAKEFORGE_CONFIG_FILE") or (Path.home() / ".lakeforgecfg"))
    return [home, Path(os.environ.get("DATABRICKS_CONFIG_FILE") or (Path.home() / ".databrickscfg"))]


def _read_profile(profile: str) -> dict:
    for p in config_paths():
        if not p.exists():
            continue
        cp = configparser.ConfigParser()
        cp.read(p)
        if cp.has_section(profile):
            return dict(cp.items(profile))
        if profile == "DEFAULT" and cp.defaults():
            return dict(cp.defaults())
    return {}


def save_profile(profile: str, host: str, token: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None) -> Path:
    path = config_paths()[0]
    cp = configparser.ConfigParser()
    if path.exists():
        cp.read(path)
    if profile != "DEFAULT" and not cp.has_section(profile):
        cp.add_section(profile)
    section = cp[profile]
    section["host"] = host
    for k in ("token", "username", "password"):
        section.pop(k, None)
    if token:
        section["token"] = token
    if username:
        section["username"] = username
    if password:
        section["password"] = password
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        cp.write(f)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return path
