"""Small HTTP helpers for bounded local REST and Trino validation scripts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class HttpJsonError(RuntimeError):
    """HTTP failure with the status and bounded response detail preserved."""

    def __init__(self, url: str, status_code: int, detail: str, *, context: str | None = None) -> None:
        prefix = context or url
        super().__init__(f"{prefix} returned HTTP {status_code}: {detail[:500]}")
        self.url = url
        self.status_code = status_code
        self.detail = detail[:500]


class LocalHttpJsonClient:
    """Dependency-free JSON client with explicit per-request timeout control."""

    def __init__(self, *, opener: Callable[..., Any] = urlopen) -> None:
        self._opener = opener

    def request_json(
        self,
        url: str,
        *,
        method: str = "GET",
        body: Mapping[str, Any] | bytes | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float,
        error_context: str | None = None,
        non_object_error: str | None = None,
    ) -> dict[str, Any]:
        if isinstance(body, Mapping):
            payload: bytes | None = json.dumps(body, separators=(",", ":")).encode()
        else:
            payload = body
        request = Request(url, data=payload, method=method, headers=dict(headers or {}))
        try:
            with self._opener(request, timeout=timeout) as response:
                result = json.load(response)
        except HTTPError as exc:
            detail = exc.read(501).decode("utf-8", errors="replace")
            raise HttpJsonError(url, exc.code, detail, context=error_context) from exc
        if not isinstance(result, dict):
            raise RuntimeError(non_object_error or error_context or f"unexpected response from {url}")
        return result


def trino_query(
    endpoint: str,
    catalog: str,
    schema: str,
    sql: str,
    *,
    user: str = "metabase",
    timeout: float = 30,
    client: LocalHttpJsonClient | None = None,
) -> list[list[object]]:
    """Execute a read-only caller-supplied Trino statement and follow nextUri."""
    http = client or LocalHttpJsonClient()
    payload = http.request_json(
        f"{endpoint.rstrip('/')}/v1/statement",
        method="POST",
        body=sql.encode(),
        headers={
            "X-Trino-User": user,
            "X-Trino-Catalog": catalog,
            "X-Trino-Schema": schema,
        },
        timeout=timeout,
    )
    rows = list(payload.get("data", []))
    while payload.get("nextUri"):
        payload = http.request_json(
            payload["nextUri"],
            headers={"X-Trino-User": user},
            timeout=timeout,
        )
        rows.extend(payload.get("data", []))
    if payload.get("error"):
        raise RuntimeError(payload["error"].get("message", "Trino query failed"))
    return rows
