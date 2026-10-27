from datetime import datetime, timezone
from ingestion.incremental.config import get_table_config

def build_snapshot_metadata(
    table_name,
    snapshot_id,
    data_prefix,
    batches,
    row_count,
    parquet_objects,
    created_at=None,
):
    """
    Build metadata for an immutable Silver snapshot.

    This function only constructs metadata.
    It does not write anything to MinIO.
    """

    if not batches:
        raise ValueError(
            "Cannot create snapshot metadata without source batches"
        )

    if row_count < 0:
        raise ValueError("Invalid snapshot row count")

    if not parquet_objects:
        raise ValueError(
            "Snapshot must contain Parquet objects"
        )


    if created_at is None:
        created_at = datetime.now(timezone.utc).isoformat()

    if (
        not isinstance(created_at, str)
        or not created_at.strip()
    ):
        raise ValueError("Invalid snapshot creation timestamp")


    if not isinstance(snapshot_id, str) or not snapshot_id.strip():
        raise ValueError("Invalid snapshot ID")

    if (
        not isinstance(data_prefix, str)
        or not data_prefix
        or not data_prefix.endswith("/")
    ):
        raise ValueError("Invalid snapshot data prefix")

    if any(
        batch.get("table_name") != table_name
        for batch in batches
    ):
        raise ValueError("Snapshot source table mismatch")

    config = get_table_config(table_name)

    if any(
        batch.get("watermark_type") != config["watermark_type"]
        for batch in batches
    ):
        raise ValueError("Snapshot watermark type mismatch")

    batch_ids = [batch["batch_id"] for batch in batches]

    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("Duplicate snapshot source batches")

    object_keys = []

    for obj in parquet_objects:
        key = obj.get("object_key")
        size = obj.get("size")

        if (
            not isinstance(key, str)
            or not key.startswith(data_prefix)
            or not key.endswith(".parquet")
        ):
            raise ValueError("Invalid snapshot Parquet object path")

        if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
            raise ValueError("Invalid snapshot Parquet object size")

        object_keys.append(key)

    if len(object_keys) != len(set(object_keys)):
        raise ValueError("Duplicate snapshot Parquet objects")

    return {
        "snapshot_id": snapshot_id,
        "table_name": table_name,
        "status": "COMPLETED",
        "created_at": created_at,
        "data_prefix": data_prefix,
        "row_count": row_count,
        "parquet_file_count": len(parquet_objects),
        "parquet_objects": parquet_objects,
        "source_batch_ids": [
            batch["batch_id"]
            for batch in batches
        ],
        "source_batch_count": len(batches),
        "watermark_type": batches[0]["watermark_type"],
        "end_timestamp": (
            batches[-1]["end_timestamp"].isoformat()
            if batches[-1]["end_timestamp"] is not None
            else None
        ),
        "end_id": batches[-1]["end_id"],
    }