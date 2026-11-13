"""Read-only validation for the isolated Metabase -> Trino REST trial."""

from __future__ import annotations

import json
import os
import re

from common.http_json import LocalHttpJsonClient
from gold.implementation.gold_table_registry import RUN06_SCHEMA
from gold.implementation.local_endpoint import validate_local_http_endpoint

SCHEMA = RUN06_SCHEMA
CATALOG = "lakehouse_rest_postgres"
QUERY = f'SELECT count(*) AS row_count FROM "{SCHEMA}"."fact_orders"'


def _url(raw: str) -> str:
    return validate_local_http_endpoint(raw, name="METABASE_TRIAL_URL", hosts={"127.0.0.1", "localhost", "metabase-rest-trial"}, ports={3000, 3001})


def _json(base: str, path: str, *, method: str = "GET", body: dict | None = None) -> dict:
    token = os.environ.get("METABASE_TRIAL_SESSION")
    if not token:
        raise RuntimeError("METABASE_TRIAL_SESSION is required")
    return LocalHttpJsonClient().request_json(
        f"{base}{path}",
        method=method,
        body=body,
        headers={"X-Metabase-Session": token, "Content-Type": "application/json", "Accept": "application/json"},
        timeout=30,
        error_context="Metabase response",
        non_object_error="Metabase response is not an object",
    )


def validate(base: str, database_id: int) -> dict:
    database = _json(base, f"/api/database/{database_id}")
    details = database.get("details", {})
    if database.get("engine") not in {"presto-jdbc", "presto", "trino"}:
        raise RuntimeError("trial database is not a Presto/Trino connection")
    if details.get("host") not in {"trino-rest-postgres-validation", "host.docker.internal", "127.0.0.1", "localhost"}:
        raise RuntimeError("trial database host is not local")
    if int(details.get("port", 0)) not in {8080, 18085} or details.get("catalog") != CATALOG or details.get("schema") != SCHEMA:
        raise RuntimeError("trial database target does not match REST Trino validation")
    if re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|CREATE|CALL)\b", QUERY, re.I):
        raise RuntimeError("validation query is not read-only")
    result = _json(base, "/api/dataset", method="POST", body={"database": database_id, "type": "native", "native": {"query": QUERY, "template-tags": {}}, "constraints": {"max-results": 1}})
    rows = result.get("data", {}).get("rows", [])
    if rows != [[1]]:
        raise RuntimeError(f"unexpected fact_orders row result: {rows!r}")
    return {"status": "PASS", "catalog": CATALOG, "schema": SCHEMA, "query": QUERY, "row_count": 1}


if __name__ == "__main__":
    print(json.dumps(validate(_url(os.getenv("METABASE_TRIAL_URL", "http://127.0.0.1:3001")), int(os.environ["METABASE_TRIAL_DATABASE_ID"])), sort_keys=True))
