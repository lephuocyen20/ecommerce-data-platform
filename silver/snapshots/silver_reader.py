from pyspark.sql import functions as F
from silver.incremental import (
    get_completion_manifest,
    verify_published_manifest,
)
from common.paths import silver_incremental_batch_paths


def read_verified_silver_batch(
    spark,
    client,
    bucket,
    batch,
    batch_sequence,
):
    """Read one verified Silver incremental batch."""

    table_name = batch["table_name"]
    batch_id = batch["batch_id"]

    manifest_key = silver_incremental_batch_paths(
        table_name, batch_id, validate_batch_id=False
    )["manifest_key"]

    manifest = get_completion_manifest(
        client,
        bucket,
        manifest_key,
    )

    if manifest is None:
        raise ValueError(
            f"Missing Silver manifest for batch {batch_id}"
        )

    verified = verify_published_manifest(
        spark,
        client,
        bucket,
        manifest,
        table_name,
        batch_id,
        batch["object_key"],
        batch["checksum_sha256"],
    )

    silver_path = (
        f"s3a://{bucket}/{verified['data_prefix']}"
    )

    df = spark.read.parquet(silver_path)

    if "_batch_sequence" in df.columns:
        raise ValueError(
            "Reserved column already exists: _batch_sequence"
        )

    return df.withColumn(
    "_batch_sequence",
    F.lit(batch_sequence),
)   
