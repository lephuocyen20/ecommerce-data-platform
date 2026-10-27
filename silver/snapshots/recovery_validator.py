
def build_silver_snapshot(*args, **kwargs):
    """
    Lazy-load the snapshot builder.

    Allows unit tests to mock this function without
    importing the full Spark transformation pipeline.
    """
    from silver.snapshots.builder import (
        build_silver_snapshot as actual_builder,
    )

    return actual_builder(*args, **kwargs)


def validate_recovery_snapshot(
    spark,
    client,
    bucket,
    table_name,
    batches,
    data_prefix,
):
    """
    Validate an unpublished Silver snapshot against
    a freshly reconstructed snapshot from source batches.

    No writes or publications are performed.
    """

    # ========================================================
    # 1. VALIDATE RECOVERY DATA PREFIX
    # ========================================================

    expected_prefix = (
        f"silver_snapshots/{table_name}/"
        f"snapshots/"
    )

    if (
        not isinstance(data_prefix, str)
        or not data_prefix.startswith(expected_prefix)
        or not data_prefix.endswith("/data/")
    ):
        raise ValueError("Invalid recovery data prefix")

    # ========================================================
    # 2. REBUILD EXPECTED SNAPSHOT
    # ========================================================

    expected_df = build_silver_snapshot(
        spark=spark,
        client=client,
        bucket=bucket,
        batches=batches,
        table_name=table_name,
    )

    if "_batch_sequence" not in expected_df.columns:
        raise ValueError(
            "Expected snapshot missing batch sequence"
        )

    expected_df = expected_df.drop("_batch_sequence")

    # ========================================================
    # 3. READ EXISTING UNPUBLISHED SNAPSHOT
    # ========================================================

    actual_df = spark.read.parquet(
        f"s3a://{bucket}/{data_prefix}"
    )

    # ========================================================
    # 4. VALIDATE SCHEMA
    # ========================================================

    if expected_df.schema != actual_df.schema:
        raise ValueError(
            "Recovery snapshot schema mismatch"
        )

    # ========================================================
    # 5. VALIDATE ROW COUNT
    # ========================================================

    expected_count = expected_df.count()
    actual_count = actual_df.count()

    if expected_count != actual_count:
        raise ValueError(
            "Recovery snapshot row count mismatch"
        )

    # ========================================================
    # 6. VALIDATE DATA CONTENT
    # ========================================================

    columns = expected_df.columns

    # exceptAll preserves duplicate rows.
    # Schema and column order have already been checked.

    missing_rows = (
        expected_df.select(*columns)
        .exceptAll(actual_df.select(*columns))
        .limit(1)
        .count()
    )

    unexpected_rows = (
        actual_df.select(*columns)
        .exceptAll(expected_df.select(*columns))
        .limit(1)
        .count()
    )

    if missing_rows or unexpected_rows:
        raise ValueError(
            "Recovery snapshot data mismatch"
        )

    # ========================================================
    # 7. RETURN VALIDATION RESULT
    # ========================================================

    return {
        "table_name": table_name,
        "data_prefix": data_prefix,
        "row_count": actual_count,
        "validation_status": "VERIFIED",
    }
