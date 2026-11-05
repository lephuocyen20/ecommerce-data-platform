
import os
import json
from datetime import datetime

import psycopg2
from minio import Minio
from pyspark.sql import SparkSession

from silver.snapshots.batch_discovery import (
    get_committed_batches,
)
from silver.snapshots.continuity import (
    validate_batch_continuity,
)
from silver.snapshots.identity import (
    generate_snapshot_id,
)
from silver.snapshots.recovery import (
    inspect_snapshot_state,
)
from silver.snapshots.publisher import (
    read_published_manifest,
)
from silver.snapshots.verifier import (
    verify_snapshot,
)
from ingestion.incremental.config import get_table_config


TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
]


def configure_s3a(spark):
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    conf.set(
        "fs.s3a.endpoint",
        f"http://{os.environ['MINIO_ENDPOINT']}",
    )
    conf.set(
        "fs.s3a.access.key",
        os.environ["MINIO_ACCESS_KEY"],
    )
    conf.set(
        "fs.s3a.secret.key",
        os.environ["MINIO_SECRET_KEY"],
    )
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def normalize_timestamp(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, str):
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        ).isoformat()

    raise ValueError(
        f"Unsupported timestamp type: {type(value)}"
    )


def audit_table(conn, spark, client, bucket, table_name):
    print(
        f"\n========== AUDIT: {table_name} ==========",
        flush=True,
    )

    config = get_table_config(table_name)

    batches = get_committed_batches(
        conn=conn,
        table_name=table_name,
    )

    if not batches:
        raise ValueError(
            f"{table_name}: no committed batches"
        )

    validate_batch_continuity(batches)

    expected_batch_ids = [
        batch["batch_id"]
        for batch in batches
    ]

    assert len(expected_batch_ids) == len(
        set(expected_batch_ids)
    ), "Duplicate committed batch IDs"

    expected_snapshot_id = generate_snapshot_id(
        table_name=table_name,
        batches=batches,
    )

    state = inspect_snapshot_state(
        client=client,
        bucket=bucket,
        table_name=table_name,
        snapshot_id=expected_snapshot_id,
    )

    assert state["state"] == "PUBLISHED", (
        f"{table_name}: expected PUBLISHED, "
        f"got {state['state']}"
    )

    manifest = read_published_manifest(
        client=client,
        bucket=bucket,
        manifest_key=state["manifest_key"],
    )

    # --------------------------------------------------
    # 1. Snapshot identity
    # --------------------------------------------------

    assert manifest["snapshot_id"] == expected_snapshot_id
    assert manifest["table_name"] == table_name
    assert manifest["status"] == "COMPLETED"
    assert manifest["data_prefix"] == state["data_prefix"]

    print("SNAPSHOT IDENTITY: PASS", flush=True)

    # --------------------------------------------------
    # 2. Source lineage
    # --------------------------------------------------

    assert manifest["source_batch_ids"] == expected_batch_ids

    assert manifest["source_batch_count"] == len(batches)

    assert manifest["watermark_type"] == config["watermark_type"]

    print("SOURCE LINEAGE: PASS", flush=True)

    # --------------------------------------------------
    # 3. Watermark
    # --------------------------------------------------

    last_batch = batches[-1]

    expected_end_timestamp = normalize_timestamp(
        last_batch["end_timestamp"]
    )

    actual_end_timestamp = normalize_timestamp(
        manifest["end_timestamp"]
    )

    assert actual_end_timestamp == expected_end_timestamp

    assert manifest["end_id"] == last_batch["end_id"]

    print("WATERMARK: PASS", flush=True)

    # --------------------------------------------------
    # 4. Metadata structure
    # --------------------------------------------------

    assert isinstance(manifest["created_at"], str)

    datetime.fromisoformat(
        manifest["created_at"].replace("Z", "+00:00")
    )

    assert manifest["parquet_file_count"] > 0

    assert manifest["parquet_file_count"] == len(
        manifest["parquet_objects"]
    )

    print("METADATA STRUCTURE: PASS", flush=True)

    # --------------------------------------------------
    # 5. Snapshot data verification
    # --------------------------------------------------

    verify_snapshot(
        spark=spark,
        client=client,
        bucket=bucket,
        manifest=manifest,
    )

    print("PARQUET INTEGRITY: PASS", flush=True)
    print("ROW COUNT: PASS", flush=True)
    print("PRIMARY KEY INTEGRITY: PASS", flush=True)

    print(
        f"{table_name}: AUDIT PASS",
        flush=True,
    )

    return {
        "table": table_name,
        "snapshot_id": expected_snapshot_id,
        "source_batch_count": len(batches),
        "row_count": manifest["row_count"],
        "parquet_file_count": manifest["parquet_file_count"],
        "status": "PASS",
    }


def main():
    conn = None
    spark = None
    results = []

    try:
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
        )

        conn.set_session(
            readonly=True,
            autocommit=True,
        )

        client = Minio(
            os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )

        bucket = os.environ["BRONZE_BUCKET"]

        spark = (
            SparkSession.builder
            .appName("SilverSnapshotMetadataLineageAudit")
            .master("local[*]")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        configure_s3a(spark)

        for table_name in TABLES:
            result = audit_table(
                conn=conn,
                spark=spark,
                client=client,
                bucket=bucket,
                table_name=table_name,
            )

            results.append(result)

        print("\n========== FINAL AUDIT RESULTS ==========")
        print(json.dumps(results, indent=2))

        print(
            f"FINAL AUDIT RESULT: PASS "
            f"{len(results)}/{len(TABLES)}",
            flush=True,
        )

    finally:
        if spark is not None:
            spark.stop()

        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
