"""Shared fail-closed validation for bounded local HTTP endpoints."""

from __future__ import annotations

from urllib.parse import urlparse


def validate_local_http_endpoint(raw: str, *, name: str, hosts: set[str], ports: set[int], paths: set[str] | None = None) -> str:
    parsed = urlparse(raw)
    allowed_paths = paths or {"", "/"}
    try:
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError(f"{name} has an invalid port") from exc
    if (
        parsed.scheme != "http"
        or parsed.hostname not in hosts
        or port not in ports
        or parsed.path not in allowed_paths
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise RuntimeError(f"{name} must be an approved local HTTP endpoint")
    return raw.rstrip("/")
