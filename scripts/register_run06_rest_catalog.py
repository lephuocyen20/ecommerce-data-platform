"""Bounded metadata-only registration into the isolated PostgreSQL REST catalog."""

from __future__ import annotations

import argparse
import json
import os
import re
from urllib.parse import quote

from common.http_json import HttpJsonError, LocalHttpJsonClient
from gold.implementation.gold_table_registry import RUN06_PREFIX, RUN06_SCHEMA, RUN06_TABLES
from gold.implementation.local_endpoint import validate_local_http_endpoint

RUN_ID = "run-20261112-gold-06"
SCHEMA = RUN06_SCHEMA
PREFIX = RUN06_PREFIX
TABLES = RUN06_TABLES
LOCATION_RE = re.compile(
    r"s3://ecommerce-data-lake/quality-it/run-20261112-gold-06/"
    r"[A-Za-z0-9._~/-]+/metadata/[A-Za-z0-9._~-]+\.json"
)


def _endpoint(raw: str, *, name: str, ports: set[int]) -> str:
    return validate_local_http_endpoint(raw, name=name, hosts={"127.0.0.1", "localhost", "rest-catalog", "rest-catalog-postgres"}, ports=ports)


def _json(url: str, *, method: str = "GET", body: dict | None = None) -> dict:
    return LocalHttpJsonClient().request_json(
        url,
        method=method,
        body=body,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=20,
        error_context=f"REST request {url}",
        non_object_error=f"non-object REST response from {url}",
    )


def _metadata_location(payload: dict, table: str) -> str:
    location = payload.get("metadata-location") or payload.get("metadataLocation")
    if not isinstance(location, str) or ".." in location or not LOCATION_RE.fullmatch(location):
        raise RuntimeError(f"unapproved metadata location for {table}")
    return location


def source_locations(source: str) -> dict[str, str]:
    namespace = quote(SCHEMA, safe="")
    listing = _json(f"{source}/v1/namespaces/{namespace}/tables")
    identifiers = sorted(".".join(item["namespace"] + [item["name"]]) for item in listing.get("identifiers", []))
    expected = sorted(f"{SCHEMA}.{table}" for table in TABLES)
    if identifiers != expected:
        raise RuntimeError(f"source REST table set mismatch: {identifiers!r}")
    return {
        table: _metadata_location(
            _json(f"{source}/v1/namespaces/{namespace}/tables/{quote(table, safe='')}"), table
        )
        for table in TABLES
    }


def register(target: str, locations: dict[str, str]) -> None:
    namespace = quote(SCHEMA, safe="")
    try:
        listing = _json(f"{target}/v1/namespaces/{namespace}/tables")
    except HttpJsonError as exc:
        if exc.status_code != 404:
            raise
        listing = {"identifiers": []}
    if listing.get("identifiers"):
        raise RuntimeError("target REST namespace is not empty; refusing overwrite")
    try:
        _json(f"{target}/v1/namespaces", method="POST", body={"namespace": [SCHEMA], "properties": {}})
    except HttpJsonError as exc:
        if exc.status_code != 409:
            raise
    for table, location in locations.items():
        _json(
            f"{target}/v1/namespaces/{namespace}/register",
            method="POST",
            body={"name": table, "metadata-location": location},
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--source", default=os.getenv("ICEBERG_SOURCE_REST_URL", "http://127.0.0.1:8181"))
    parser.add_argument("--target", default=os.getenv("ICEBERG_POSTGRES_REST_URL", "http://127.0.0.1:8182"))
    args = parser.parse_args()
    if args.apply and os.getenv("ICEBERG_REST_PHASE2_APPLY") != "1":
        raise SystemExit("--apply requires ICEBERG_REST_PHASE2_APPLY=1")
    source = _endpoint(args.source, name="source REST URL", ports={8181})
    target = _endpoint(args.target, name="target REST URL", ports={8181, 8182})
    locations = source_locations(source)
    if args.apply:
        register(target, locations)
    print(json.dumps({"run_id": RUN_ID, "schema": SCHEMA, "tables": TABLES, "mode": "apply" if args.apply else "dry-run", "metadata_locations": locations, "metadata_only": True}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
