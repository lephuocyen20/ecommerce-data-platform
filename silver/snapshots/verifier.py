from pyspark.sql import functions as F

from ingestion.incremental.config import get_table_config

from silver.snapshots.object_discovery import (
    discover_snapshot_parquet_objects,
)
from common.paths import silver_snapshot_paths


def verify_snapshot(
    spark,
    client,
    bucket,
    manifest,
):
    """
    Verify a Silver Consolidated Snapshot.

    Checks:
        - Snapshot metadata
        - Object integrity
        - Parquet file count
        - Row count
        - Primary key integrity

    Returns the verified manifest.
    """

    if not isinstance(manifest, dict):
        raise ValueError("Invalid snapshot manifest")

    if manifest.get("status") != "COMPLETED":
        raise ValueError("Snapshot is not completed")

    table_name = manifest.get("table_name")
    config = get_table_config(table_name)

    snapshot_id = manifest.get("snapshot_id")
    data_prefix = manifest.get("data_prefix")

    if (
        not isinstance(snapshot_id, str)
        or not snapshot_id
        or not all(
            char.isalnum() or char in "-_"
            for char in snapshot_id
        )
    ):
        raise ValueError("Invalid snapshot ID")

    expected_prefix = silver_snapshot_paths(
        table_name, snapshot_id
    )["data_prefix"]

    if data_prefix != expected_prefix:
        raise ValueError("Snapshot data prefix mismatch")

    if manifest.get("watermark_type") != config["watermark_type"]:
        raise ValueError("Snapshot watermark type mismatch")

    expected_count = manifest.get("row_count")

    if (
        not isinstance(expected_count, int)
        or isinstance(expected_count, bool)
        or expected_count < 0
    ):
        raise ValueError("Invalid snapshot row count")

    expected_objects = manifest.get("parquet_objects")

    if not isinstance(expected_objects, list):
        raise ValueError("Missing snapshot Parquet metadata")

    actual_objects = discover_snapshot_parquet_objects(
        client=client,
        bucket=bucket,
        data_prefix=data_prefix,
    )

    if actual_objects != expected_objects:
        raise ValueError(
            "Snapshot Parquet objects do not match manifest"
        )

    if manifest.get("parquet_file_count") != len(actual_objects):
        raise ValueError(
            "Snapshot Parquet file count mismatch"
        )

    snapshot_path = f"s3a://{bucket}/{data_prefix}"

    df = spark.read.parquet(snapshot_path)

    primary_key = config["primary_key"]

    if primary_key not in df.columns:
        raise ValueError("Snapshot primary key is missing")

    if "_batch_sequence" in df.columns:
        raise ValueError(
            "Snapshot contains internal batch sequence"
        )

    actual_count = df.count()

    if actual_count != expected_count:
        raise ValueError(
            "Snapshot row count mismatch: "
            f"expected={expected_count}, actual={actual_count}"
        )

    if df.filter(F.col(primary_key).isNull()).limit(1).count():
        raise ValueError(
            "Snapshot contains null primary keys"
        )

    duplicate_count = (
        df.groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_count:
        raise ValueError(
            "Snapshot contains duplicate primary keys"
        )

    return manifest
