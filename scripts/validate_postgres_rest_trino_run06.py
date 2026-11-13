"""Compare source REST and persistent PostgreSQL REST reads through Trino."""

from __future__ import annotations

import argparse
import json

from common.http_json import trino_query
from gold.implementation.gold_table_registry import RUN06_EXPECTED_ROW_COUNTS, RUN06_SCHEMA, RUN06_SNAPSHOT_IDS, RUN06_TABLES
from gold.implementation.local_endpoint import validate_local_http_endpoint

SCHEMA = RUN06_SCHEMA
TABLES = RUN06_TABLES
EXPECTED_ROWS = RUN06_EXPECTED_ROW_COUNTS
EXPECTED_SNAPSHOTS = RUN06_SNAPSHOT_IDS


def endpoint(raw: str, *, name: str, port: int) -> str:
    return validate_local_http_endpoint(raw, name=name, hosts={"127.0.0.1", "localhost"}, ports={port})


def query(endpoint: str, catalog: str, sql: str) -> list[list[object]]:
    return trino_query(endpoint, catalog, SCHEMA, sql, timeout=30)


def evidence(endpoint: str, catalog: str) -> dict:
    names = sorted(row[0] for row in query(endpoint, catalog, "SHOW TABLES"))
    if names != sorted(TABLES):
        raise RuntimeError(f"{catalog} table set mismatch: {names!r}")
    result = {}
    for table in TABLES:
        describe = query(endpoint, catalog, f'DESCRIBE "{table}"')
        rows = query(endpoint, catalog, f'SELECT count(*) FROM "{table}"')[0][0]
        snapshots = query(endpoint, catalog, f'SELECT snapshot_id FROM "{table}$snapshots" ORDER BY committed_at DESC LIMIT 1')[0][0]
        physical = query(endpoint, catalog, f'SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM "{table}$files"')[0]
        result[table] = {"describe": describe, "row_count": rows, "snapshot_id": snapshots, "file_count": physical[0], "file_bytes": physical[1]}
        if rows != EXPECTED_ROWS[table] or snapshots != EXPECTED_SNAPSHOTS[table] or physical[0] <= 0 or physical[1] <= 0:
            raise RuntimeError(f"evidence mismatch for {catalog}.{table}: {result[table]!r}")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="http://127.0.0.1:18081")
    parser.add_argument("--target", default="http://127.0.0.1:18085")
    args = parser.parse_args()
    source = evidence(endpoint(args.source, name="source Trino URL", port=18081), "lakehouse")
    target = evidence(endpoint(args.target, name="target Trino URL", port=18085), "lakehouse_rest_postgres")
    for table in TABLES:
        if source[table] != target[table]:
            raise RuntimeError(f"source/target evidence mismatch for {table}")
    print(json.dumps({"status": "PASS", "schema": SCHEMA, "tables": target}, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
