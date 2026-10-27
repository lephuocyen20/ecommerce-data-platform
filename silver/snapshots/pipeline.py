
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
    publish_snapshot,
)
from silver.snapshots.verifier import (
    verify_snapshot,
)
from silver.snapshots.recovery_validator import (
    validate_recovery_snapshot,
)
from silver.snapshots.object_discovery import (
    discover_snapshot_parquet_objects,
)
from silver.snapshots.metadata import (
    build_snapshot_metadata,
)
from silver.snapshots.writer import (
    write_snapshot_parquet,
)


def build_silver_snapshot(*args, **kwargs):
    """
    Lazy-load the snapshot builder.

    Prevents importing the full Spark transformation
    pipeline during local Windows unit tests.

    This wrapper can also be mocked in pipeline tests.
    """
    from silver.snapshots.builder import (
        build_silver_snapshot as actual_builder,
    )

    return actual_builder(*args, **kwargs)


def prepare_snapshot_batches(conn, table_name):
    """
    Discover and validate committed batches
    before building a Silver snapshot.

    No Spark processing or MinIO writes.
    """

    batches = get_committed_batches(
        conn=conn,
        table_name=table_name,
    )

    if not batches:
        raise ValueError(
            f"No committed batches for table: {table_name}"
        )

    validate_batch_continuity(batches)

    batch_ids = [
        batch["batch_id"]
        for batch in batches
    ]

    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError(
            "Duplicate committed batch IDs"
        )

    return batches


def run_snapshot_pipeline(
    conn,
    spark,
    client,
    bucket,
    table_name,
):
    """
    Orchestrate a Silver Consolidated Snapshot.

    Supported states:
        NEW:
            Build, write, verify and publish snapshot.

        UNPUBLISHED:
            Validate existing Parquet against source
            batches, then publish the manifest.

        PUBLISHED:
            Verify the published snapshot and return
            an idempotent result.

        INCOMPLETE:
            Reject automatic recovery to avoid
            overwriting potentially partial data.
    """

    # ========================================================
    # 1. DISCOVER AND VALIDATE SOURCE BATCHES
    # ========================================================

    batches = prepare_snapshot_batches(
        conn=conn,
        table_name=table_name,
    )

    # ========================================================
    # 2. GENERATE DETERMINISTIC SNAPSHOT ID
    # ========================================================

    snapshot_id = generate_snapshot_id(
        table_name=table_name,
        batches=batches,
    )

    # ========================================================
    # 3. INSPECT EXISTING SNAPSHOT STATE
    # ========================================================

    snapshot_state = inspect_snapshot_state(
        client=client,
        bucket=bucket,
        table_name=table_name,
        snapshot_id=snapshot_id,
    )

    state = snapshot_state["state"]

    # ========================================================
    # 4. INCOMPLETE SNAPSHOT
    # ========================================================

    if state == "INCOMPLETE":
        raise RuntimeError(
            f"Incomplete snapshot detected: {snapshot_id}. "
            "Manual inspection required; refusing to overwrite."
        )

    # ========================================================
    # 5. PUBLISHED SNAPSHOT
    # ========================================================

    if state == "PUBLISHED":
        manifest = read_published_manifest(
            client=client,
            bucket=bucket,
            manifest_key=snapshot_state["manifest_key"],
        )

        if not isinstance(manifest, dict):
            raise ValueError(
                "Invalid published snapshot manifest"
            )

        if (
            manifest.get("snapshot_id") != snapshot_id
            or manifest.get("table_name") != table_name
        ):
            raise ValueError(
                "Published snapshot identity mismatch"
            )

        expected_batch_ids = [
            batch["batch_id"]
            for batch in batches
        ]

        if (
            manifest.get("source_batch_ids")
            != expected_batch_ids
        ):
            raise ValueError(
                "Published snapshot source batches mismatch"
            )

        if (
            manifest.get("source_batch_count")
            != len(batches)
        ):
            raise ValueError(
                "Published snapshot source batch count mismatch"
            )

        last_batch = batches[-1]

        expected_end_timestamp = (
            last_batch["end_timestamp"].isoformat()
            if last_batch["end_timestamp"] is not None
            else None
        )

        if (
            manifest.get("end_timestamp")
            != expected_end_timestamp
            or manifest.get("end_id")
            != last_batch["end_id"]
        ):
            raise ValueError(
                "Published snapshot watermark mismatch"
            )

        verify_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        return {
            "snapshot_id": snapshot_id,
            "table_name": table_name,
            "manifest_key": snapshot_state["manifest_key"],
            "status": "COMPLETED",
            "publication_status": "ALREADY_PUBLISHED",
        }

    # ========================================================
    # 6. UNPUBLISHED SNAPSHOT RECOVERY
    # ========================================================

    if state == "UNPUBLISHED":
        data_prefix = snapshot_state["data_prefix"]

        # 6.1 Rebuild and validate existing snapshot data
        recovery_result = validate_recovery_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            table_name=table_name,
            batches=batches,
            data_prefix=data_prefix,
        )

        if (
            recovery_result["validation_status"]
            != "VERIFIED"
        ):
            raise ValueError(
                "Snapshot recovery validation failed"
            )

        # 6.2 Discover existing Parquet objects
        parquet_objects = discover_snapshot_parquet_objects(
            client=client,
            bucket=bucket,
            data_prefix=data_prefix,
        )

        # 6.3 Reconstruct snapshot manifest
        manifest = build_snapshot_metadata(
            table_name=table_name,
            snapshot_id=snapshot_id,
            data_prefix=data_prefix,
            batches=batches,
            row_count=recovery_result["row_count"],
            parquet_objects=parquet_objects,
        )

        # 6.4 Verify and publish existing snapshot
        return publish_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

    # ========================================================
    # 7. NEW SNAPSHOT
    # ========================================================

    if state == "NEW":

        # 7.1 Build consolidated snapshot
        snapshot_df = build_silver_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            batches=batches,
            table_name=table_name,
        )

        # 7.2 Count snapshot rows
        row_count = snapshot_df.count()

        # 7.3 Write immutable snapshot Parquet
        write_result = write_snapshot_parquet(
            spark=spark,
            snapshot_df=snapshot_df,
            bucket=bucket,
            table_name=table_name,
            snapshot_id=snapshot_id,
        )

        # 7.4 Discover written Parquet objects
        parquet_objects = discover_snapshot_parquet_objects(
            client=client,
            bucket=bucket,
            data_prefix=write_result["data_prefix"],
        )

        # 7.5 Build snapshot metadata
        manifest = build_snapshot_metadata(
            table_name=table_name,
            snapshot_id=snapshot_id,
            data_prefix=write_result["data_prefix"],
            batches=batches,
            row_count=row_count,
            parquet_objects=parquet_objects,
        )

        # 7.6 Verify and publish manifest
        return publish_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

    # ========================================================
    # 8. UNKNOWN STATE
    # ========================================================

    raise NotImplementedError(
        f"Snapshot state not implemented yet: {state}"
    )
