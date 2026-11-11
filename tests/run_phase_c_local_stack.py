"""Bounded local-stack Phase C runner: Silver MERGE + Gold DRY_RUN only.

Run inside the already-provisioned local quality-it Spark stack.  It never
touches Gold targets, production refs, or performs maintenance/cleanup.
"""

from __future__ import annotations

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
from pyspark.sql import functions as F

from common.minio import configure_s3a, create_minio_client
from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
from gold.implementation.dimensional.writer import DRY_RUN, run_dimensional_graph
from lakehouse.attestation import build_snapshot_attestation, publish_snapshot_attestation
from lakehouse.evidence import build_evidence, publish_evidence
from lakehouse.models.spec import SPECS
from tests.run_phase_c_local_runtime import LOCAL_BUCKET, LOCAL_HOSTS, validate_local_scope


TABLES = ("customers", "products", "orders", "order_items", "payments", "shipments")
KEYS = {"customers": "customer_id", "products": "product_id", "orders": "order_id", "order_items": "order_item_id", "payments": "payment_id", "shipments": "shipment_id"}


def _manifest_identity(table, snapshot_id):
    return hashlib.sha256(f"phase-c-local:{table}:{snapshot_id}".encode()).hexdigest()


def _scope(spark, bucket, endpoint, ref):
    validate_local_scope(bucket, endpoint, ref)
    warehouse = spark.conf.get("spark.sql.catalog.lakehouse.warehouse", "")
    if LOCAL_BUCKET not in warehouse:
        raise ValueError("Spark catalog warehouse is not quality-it scoped")
    uri = spark.conf.get("spark.sql.catalog.lakehouse.uri", endpoint)
    if uri != endpoint:
        raise ValueError("Spark catalog endpoint does not match the approved local endpoint")


def _merge_fixture(spark, table, frame):
    identifier = f"lakehouse.silver.{table}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {identifier} ({SPECS[table].ddl}) USING iceberg")
    frame.createOrReplaceTempView(f"_phase_c_{table}")
    columns = list(SPECS[table].columns)
    key = KEYS[table]
    assignments = ", ".join(f"target.{column} = source.{column}" for column in columns if column != key)
    names = ", ".join(columns)
    values = ", ".join(f"source.{column}" for column in columns)
    spark.sql(f"MERGE INTO {identifier} target USING _phase_c_{table} source ON target.{key} = source.{key} WHEN MATCHED THEN UPDATE SET {assignments} WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})")


def _publish_lineage_artifacts(client, bucket, table, snapshot_id, ref):
    attestation = build_snapshot_attestation(table=table, snapshot_id=snapshot_id, manifest_identities=[_manifest_identity(table, snapshot_id)], ref=ref)
    publish_snapshot_attestation(client, bucket, attestation)
    refs = {}
    for kind in ("reconciliation", "quality"):
        evidence = build_evidence(kind=kind, table=table, snapshot_id=snapshot_id, status="PASS", details={"scope": "phase-c-local-stack", "rows": 1}, snapshot_attestation_identity=attestation["identity"], ref=ref)
        publish_evidence(client, bucket, evidence)
        refs[kind] = {"identity": evidence["identity"], "status": "PASS", "snapshot_id": snapshot_id, "table": table}
    return {"source_type": "iceberg_snapshot", "table": table, "catalog": "lakehouse", "namespace": "silver", "ref": ref, "snapshot_id": snapshot_id, "snapshot_attestation_identity": attestation["identity"], "bronze_batch_id": None, "row_count": 1, "reconciliation_evidence": refs["reconciliation"], "quality_evidence": refs["quality"]}


def run_phase_c_local_stack(spark, client, *, bucket=LOCAL_BUCKET, endpoint="http://nessie:19120/api/v2", ref="main"):
    _scope(spark, bucket, endpoint, ref)
    from tests.run_gold_dimensional_local_fixture import _typed_fixture_frame

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    for table in TABLES:
        frame = _typed_fixture_frame(spark, table)
        _merge_fixture(spark, table, frame)
        if table == "orders":
            correction = frame.where("order_id = 100").withColumn("status", F.lit("DELIVERED")).withColumn("updated_at", F.lit(datetime(2026, 1, 3)).cast("timestamp"))
            _merge_fixture(spark, table, correction)
    lineage = {}
    for table in DIMENSIONAL_SOURCE_TABLES:
        identifier = f"lakehouse.silver.{table}"
        snapshot_id = int(spark.sql(f"SELECT snapshot_id FROM {identifier}.snapshots ORDER BY committed_at DESC LIMIT 1").first().snapshot_id)
        lineage[table] = _publish_lineage_artifacts(client, bucket, table, snapshot_id, ref)
    envelope = {"version": 1, "source_type": "iceberg_snapshot", "sources": lineage}
    result = run_dimensional_graph(spark, client, bucket, envelope, dim_date_start="2026-01-01", dim_date_end="2026-01-07", mode=DRY_RUN)
    return {"status": result["status"], "lineage": envelope, "gold": result, "can_write": False}


def main():
    spark = SparkSession.builder.appName("PhaseCLocalStack").getOrCreate()
    try:
        configure_s3a(spark)
        print(json.dumps(run_phase_c_local_stack(spark, create_minio_client()), sort_keys=True, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
