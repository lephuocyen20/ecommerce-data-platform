"""Bootstrap and exercise the isolated Metabase REST trial without cutover."""

from __future__ import annotations

import json
import os
from urllib.parse import urlparse

from common.http_json import LocalHttpJsonClient

EMAIL = "rest-trial@example.local"
PASSWORD = "TrialPass!2026"
SCHEMA = "gold_run_20261112_gold_06"
CATALOG = "lakehouse_rest_postgres"


def endpoint(raw: str) -> str:
    parsed = urlparse(raw)
    if parsed.scheme != "http" or parsed.username or parsed.password or parsed.path not in ("", "/"):
        raise RuntimeError("METABASE_INTERNAL_URL must be a plain HTTP endpoint")
    if (parsed.hostname == "metabase-rest-trial" and parsed.port == 3000) or (parsed.hostname in {"127.0.0.1", "localhost"} and parsed.port == 3001):
        return raw.rstrip("/")
    raise RuntimeError("METABASE_INTERNAL_URL must target the approved local trial endpoint")


def request(path: str, *, method: str = "GET", body: dict | None = None, session: str | None = None) -> dict:
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session
    return LocalHttpJsonClient().request_json(
        f"{BASE}{path}",
        method=method,
        body=body,
        headers=headers,
        timeout=30,
        error_context=f"Metabase API {path}",
        non_object_error=f"unexpected response from {path}",
    )


def main() -> int:
    global BASE
    BASE = endpoint(os.getenv("METABASE_INTERNAL_URL", "http://metabase-rest-trial:3000"))
    properties = request("/api/session/properties")
    setup_token = properties.get("setup-token")
    if setup_token:
        try:
            request(
                "/api/setup",
                method="POST",
                body={
                    "token": setup_token,
                    "user": {"first_name": "REST", "last_name": "Trial", "email": EMAIL, "password": PASSWORD},
                    "prefs": {"site_name": "REST Trial", "site_locale": "en"},
                },
            )
        except RuntimeError as exc:
            if "HTTP 400" not in str(exc) and "HTTP 403" not in str(exc):
                raise
    session = request("/api/session", method="POST", body={"username": EMAIL, "password": PASSWORD}).get("id")
    if not isinstance(session, str) or not session:
        raise RuntimeError("Metabase trial session was not created")
    database = request(
        "/api/database",
        method="POST",
        session=session,
        body={
            "engine": "presto-jdbc",
            "name": "REST Gold Trial",
            "details": {
                "host": "trino-rest-postgres-validation",
                "port": 8080,
                "catalog": CATALOG,
                "schema": SCHEMA,
                "user": "metabase",
                "ssl": False,
            },
        },
    )
    database_id = database.get("id")
    if not isinstance(database_id, int):
        raise RuntimeError("Metabase trial database id missing")
    result = request(
        "/api/dataset",
        method="POST",
        session=session,
        body={
            "database": database_id,
            "type": "native",
            "native": {"query": f'SELECT count(*) AS row_count FROM "{SCHEMA}"."fact_orders"', "template-tags": {}},
            "constraints": {"max-results": 1},
        },
    )
    rows = result.get("data", {}).get("rows", [])
    if rows != [[1]]:
        raise RuntimeError(f"unexpected fact_orders result: {rows!r}")
    print(json.dumps({"status": "PASS", "database_id": database_id, "catalog": CATALOG, "schema": SCHEMA, "row_count": 1}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
