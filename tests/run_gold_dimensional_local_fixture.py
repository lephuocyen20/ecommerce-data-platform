"""Seed a local-only Silver fixture and execute Gold dimensional DRY_RUN.

This runner is intentionally bounded to the ``ecommerce-quality-it`` bucket.
It never reads production evidence and refuses a production-looking bucket.
Run inside the local quality-it Spark container after the stack is healthy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, DecimalType, IntegerType, LongType,
                               StringType, StructField, StructType, TimestampType)

from common.minio import configure_s3a, create_minio_client
from gold.implementation.dimensional.writer import DRY_RUN
from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
from lakehouse.attestation import build_snapshot_attestation, publish_snapshot_attestation
from lakehouse.evidence import build_evidence, publish_evidence
from lakehouse.models.spec import SPECS

from tests.run_gold_dimensional_iceberg_integration import dry_run_harness


LOCAL_BUCKET = "ecommerce-quality-it"
TABLES = ("customers", "products", "orders", "order_items", "payments", "shipments")
CATALOG = "lakehouse"
NAMESPACE = "silver"
REF = "main"


def _fixture_rows(table):
    """Return tiny deterministic rows satisfying the shared Silver contracts."""
    values = {
        "customers": [(1, "Local Customer", "local@example.test", "2026-01-01 00:00:00", "2026-01-01 00:00:00")],
        "products": [(10, "Local Product", 5, 12.50, "2026-01-01 00:00:00", "2026-01-01 00:00:00")],
        "orders": [(100, 1, "CONFIRMED", "2026-01-02 00:00:00", "2026-01-02 00:00:00")],
        "order_items": [(1000, 100, 10, 2, 12.50, 1.00)],
        "payments": [(5000, 100, 24.00, "CARD", "SUCCESS", "local-tx-1", "2026-01-02 00:00:00")],
        "shipments": [(7000, 100, "LOCAL", "LOCAL-TRACK-1", "Local address", 0.00, "PENDING", None, None, "2026-01-05 00:00:00", "2026-01-02 00:00:00", "2026-01-02 00:00:00")],
    }
    return values[table]


def _manifest_identity(table, snapshot_id):
    return hashlib.sha256(f"local-fixture:{table}:{snapshot_id}".encode()).hexdigest()


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
    schema = StructType([StructField(column, type_map[spec.casts[column]], True) for column in spec.columns])

    def coerce(value, type_name):
        if value is None:
            return None
        if type_name == "timestamp":
            return datetime.fromisoformat(value) if isinstance(value, str) else value
        if type_name.startswith("decimal"):
            return Decimal(str(value))
        return value

    rows = [tuple(coerce(value, spec.casts[column]) for value, column in zip(row, spec.columns)) for row in _fixture_rows(table)]
    frame = spark.createDataFrame(rows, schema=schema)
    expressions = [f"CAST(`{column}` AS {spec.casts[column]}) AS `{column}`" for column in spec.columns]
    return frame.selectExpr(*expressions)


def _publish_lineage_artifacts(client, bucket, table, snapshot_id):
    attestation = build_snapshot_attestation(
        table=table,
        snapshot_id=snapshot_id,
        manifest_identities=[_manifest_identity(table, snapshot_id)],
        catalog=CATALOG,
        namespace=NAMESPACE,
        ref=REF,
    )
    publish_snapshot_attestation(client, bucket, attestation)
    refs = {}
    for kind in ("reconciliation", "quality"):
        evidence = build_evidence(
            kind=kind,
            table=table,
            snapshot_id=snapshot_id,
            status="PASS",
            details={"scope": "local-only-fixture", "rows": 1},
            catalog=CATALOG,
            namespace=NAMESPACE,
            ref=REF,
            snapshot_attestation_identity=attestation["identity"],
        )
        publish_evidence(client, bucket, evidence)
        refs[kind] = {"identity": evidence["identity"], "status": "PASS", "snapshot_id": snapshot_id, "table": table}
    return {
        "source_type": "iceberg_snapshot",
        "table": table,
        "catalog": CATALOG,
        "namespace": NAMESPACE,
        "ref": REF,
        "snapshot_id": snapshot_id,
        "snapshot_attestation_identity": attestation["identity"],
        "bronze_batch_id": None,
        "row_count": 1,
        "reconciliation_evidence": refs["reconciliation"],
        "quality_evidence": refs["quality"],
    }


def seed_sources(spark, client, bucket):
    if bucket != LOCAL_BUCKET:
        raise ValueError(f"Local fixture refuses non-local bucket: {bucket}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    lineage = {}
    for table in TABLES:
        spec = SPECS[table]
        identifier = f"{CATALOG}.{NAMESPACE}.{table}"
        # The runner is bounded and non-destructive: never DROP or clean up a
        # local table. Existing tables are validated by the normal reader path.
        spark.sql(f"CREATE TABLE IF NOT EXISTS {identifier} ({spec.ddl}) USING iceberg")
        # Idempotency guard: MERGE one deterministic source delta per table so
        # a rerun cannot append a second version of the same business key.
        # Existing historical rows are preserved; no cleanup or overwrite is
        # used. This also repairs a partially seeded local stack where only
        # some of the six source tables contain fixture data.
        columns = ", ".join(spec.columns)
        _typed_fixture_frame(spark, table).createOrReplaceTempView(f"_local_{table}")
        key = spec.key
        assignments = ", ".join(
            f"target.{column} = source.{column}" for column in spec.columns if column != key
        )
        spark.sql(
            f"MERGE INTO {identifier} target USING _local_{table} source "
            f"ON target.{key} = source.{key} "
            f"WHEN MATCHED THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({', '.join(f'source.{column}' for column in spec.columns)})"
        )
        snapshot_id = spark.sql(f"SELECT snapshot_id FROM {identifier}.snapshots ORDER BY committed_at DESC LIMIT 1").first().snapshot_id
        lineage[table] = _publish_lineage_artifacts(client, bucket, table, int(snapshot_id))
    return {
        "version": 1,
        "source_type": "iceberg_snapshot",
        "sources": {table: lineage[table] for table in DIMENSIONAL_SOURCE_TABLES},
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=LOCAL_BUCKET)
    parser.add_argument("--lineage-output", type=Path, default=Path("/tmp/gold-local-lineage.json"))
    args = parser.parse_args(argv)
    if args.bucket != LOCAL_BUCKET:
        raise SystemExit(f"Refusing bucket {args.bucket!r}; only {LOCAL_BUCKET!r} is allowed")
    spark = SparkSession.builder.appName("GoldDimensionalLocalFixture").getOrCreate()
    try:
        configure_s3a(spark)
        client = create_minio_client()
        lineage = seed_sources(spark, client, args.bucket)
        args.lineage_output.write_text(json.dumps(lineage, indent=2, sort_keys=True), encoding="utf-8")
        result = dry_run_harness(
            spark,
            client,
            args.bucket,
            lineage,
            dim_date_start="2026-01-01",
            dim_date_end="2026-01-07",
        )
        print("GOLD_LOCAL_FIXTURE_RESULT=" + json.dumps(result, sort_keys=True, default=str), flush=True)
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
