"""Create a bounded, non-destructive Silver Parquet fixture for Gold E2E.

The script is deliberately write-gated. It refuses every scope except the
approved run envelope and fails before Spark/MinIO client creation when the
authorization is not exact. It never overwrites or deletes objects; a
non-empty run prefix aborts the run.

Example (execute only after the bounded fixture approval is active)::

    python tests/run_bounded_gold_parquet_fixture.py --write \
        --lineage-output /tmp/gold-run-20261111-gold-01-lineage.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.bounded_e2e_scope import TEST_BUCKET, build_bounded_e2e_scope
from common.minio import configure_s3a, create_minio_client
from common.manifest import put_json_object_if_absent
from common.object_integrity import discover_parquet_objects
from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
from gold.implementation.dimensional.lineage import (
    build_dimensional_lineage_from_artifacts,
    build_parquet_lineage_artifact,
)
from lakehouse.models.spec import SPECS


APPROVED_RUN_IDS = {
    "run-20261111-gold-01",
    "run-20261111-gold-02",
    "run-20261111-gold-03",
    "run-20261111-gold-04",
}
TABLES = tuple(DIMENSIONAL_SOURCE_TABLES)


def _validate_exact_scope(environ):
    """Validate the immutable scope before creating any external client."""
    scope = build_bounded_e2e_scope(environ)
    if scope.bucket != TEST_BUCKET or scope.run_id not in APPROVED_RUN_IDS:
        raise RuntimeError("Fixture requires the exact approved Gold E2E scope")
    return scope


def _fixture_rows(table):
    """Return one deterministic row per source table using the shared contract."""
    values = {
        "customers": [
            (1, "Gold E2E Customer", "gold-e2e@example.test", "2026-01-01 00:00:00", "2026-01-01 00:00:00")
        ],
        "products": [
            (10, "Gold E2E Product", 5, "12.50", "2026-01-01 00:00:00", "2026-01-01 00:00:00")
        ],
        "orders": [(100, 1, "CONFIRMED", "2026-01-02 00:00:00", "2026-01-02 00:00:00")],
        "order_items": [(1000, 100, 10, 2, "12.50", "1.00")],
        "payments": [(5000, 100, "24.00", "CARD", "SUCCESS", "gold-e2e-tx-1", "2026-01-02 00:00:00")],
        "shipments": [
            (
                7000,
                100,
                "E2E",
                "GOLD-E2E-TRACK-1",
                "Bounded Gold E2E",
                "0.00",
                "PENDING",
                None,
                None,
                "2026-01-05 00:00:00",
                "2026-01-02 00:00:00",
                "2026-01-02 00:00:00",
            )
        ],
    }
    return values[table]


def _typed_fixture_frame(spark, table):
    spec = SPECS[table]
    type_map = {
        "string": StringType(),
        "bigint": LongType(),
        "int": IntegerType(),
        "timestamp": TimestampType(),
        "date": DateType(),
        "decimal(12,2)": DecimalType(12, 2),
    }
    schema = StructType(
        [StructField(column, type_map[spec.casts[column]], True) for column in spec.columns]
    )

    def coerce(value, type_name):
        if value is None:
            return None
        if type_name == "timestamp":
            return datetime.fromisoformat(value) if isinstance(value, str) else value
        if type_name.startswith("decimal"):
            return Decimal(str(value))
        return value

    rows = [
        tuple(coerce(value, spec.casts[column]) for value, column in zip(row, spec.columns))
        for row in _fixture_rows(table)
    ]
    return spark.createDataFrame(rows, schema=schema).select(*spec.columns)


def _batch_id(scope, table):
    return hashlib.sha256(f"{scope.run_id}:batch:{table}".encode()).hexdigest()[:32]


def _run_id(scope, table):
    return hashlib.sha256(f"{scope.run_id}:run:{table}".encode()).hexdigest()[:32]


def _paths(scope, table):
    batch_id = _batch_id(scope, table)
    data_prefix = scope.key(
        f"silver_incremental/{table}/batch_id={batch_id}/runs/{_run_id(scope, table)}/"
    )
    return {
        "batch_id": batch_id,
        "data_prefix": data_prefix,
        "manifest_key": scope.key(
            f"silver_incremental/{table}/batch_id={batch_id}/_BATCH_METADATA.json"
        ),
    }


def _assert_empty_scope(client, scope):
    existing = next(client.list_objects(scope.bucket, prefix=scope.prefix, recursive=True), None)
    if existing is not None:
        raise RuntimeError(
            f"Refusing non-empty bounded fixture prefix: {scope.prefix}{existing.object_name[len(scope.prefix):]}"
        )


def _manifest(table, paths, objects):
    return {
        "status": "COMPLETED",
        "table_name": table,
        "batch_id": paths["batch_id"],
        "data_prefix": paths["data_prefix"],
        "silver_row_count": 1,
        "parquet_file_count": len(objects),
        "parquet_objects": objects,
        "fixture": "bounded-gold-parquet-fixture",
    }


def _canonical_identity(document):
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def create_fixture(scope, *, lineage_output, artifacts_output):
    """Create six immutable Parquet inputs and their completion manifests."""
    client = create_minio_client()
    _assert_empty_scope(client, scope)

    spark = SparkSession.builder.appName("BoundedGoldParquetFixture").getOrCreate()
    try:
        configure_s3a(spark)
        artifacts = {}
        for table in TABLES:
            paths = _paths(scope, table)
            parquet_uri = f"s3a://{scope.bucket}/{paths['data_prefix']}"
            _typed_fixture_frame(spark, table).write.mode("errorifexists").parquet(parquet_uri)

            objects = discover_parquet_objects(
                client, scope.bucket, paths["data_prefix"], include_etag=True
            )
            manifest = _manifest(table, paths, objects)
            manifest_identity = _canonical_identity(manifest)
            put_json_object_if_absent(client, scope.bucket, paths["manifest_key"], manifest)
            artifacts[table] = build_parquet_lineage_artifact(
                table=table,
                bucket=scope.bucket,
                manifest_key=paths["manifest_key"],
                data_prefix=paths["data_prefix"],
                manifest_identity=manifest_identity,
                batch_id=paths["batch_id"],
                row_count=manifest["silver_row_count"],
            )

        lineage = build_dimensional_lineage_from_artifacts(artifacts, scope=scope)
        if lineage_output.exists():
            raise RuntimeError(f"Refusing to overwrite existing lineage output: {lineage_output}")
        if artifacts_output.exists():
            raise RuntimeError(f"Refusing to overwrite existing artifact output: {artifacts_output}")
        lineage_output.parent.mkdir(parents=True, exist_ok=True)
        artifacts_output.parent.mkdir(parents=True, exist_ok=True)
        lineage_output.write_text(
            json.dumps(lineage, indent=2, sort_keys=True), encoding="utf-8"
        )
        artifacts_output.write_text(
            json.dumps(artifacts, indent=2, sort_keys=True), encoding="utf-8"
        )
        return {
            "status": "CREATED",
            "tables": list(TABLES),
            "lineage": str(lineage_output),
            "artifacts": str(artifacts_output),
        }
    finally:
        spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Enable the approved fixture write")
    parser.add_argument(
        "--lineage-output",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--artifacts-output",
        type=Path,
        default=None,
    )
    args = parser.parse_args(argv)

    # This must remain before SparkSession.builder and create_minio_client.
    scope = _validate_exact_scope(os.environ)
    if not args.write:
        raise SystemExit("Refusing fixture write without --write")
    lineage_output = args.lineage_output or Path(f"/tmp/gold-{scope.run_id}-lineage.json")
    artifacts_output = args.artifacts_output or Path(f"/tmp/gold-{scope.run_id}-artifacts.json")
    result = create_fixture(
        scope,
        lineage_output=lineage_output,
        artifacts_output=artifacts_output,
    )
    print("BOUNDED_GOLD_PARQUET_FIXTURE=" + json.dumps(result, sort_keys=True), flush=True)
    return result


if __name__ == "__main__":
    main()
