"""Watermark reads, validation, locking, and checkpoint commits."""

from .batch_repository import acquire_table_lock
from .config import TABLE_CONFIG, get_table_config


def get_watermark(conn, table_name):
    config = get_table_config(table_name)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                watermark_type,
                last_timestamp,
                last_id,
                last_batch_id
            FROM public.ingestion_watermarks
            WHERE table_name = %s
            """,
            (table_name,),
        )
        row = cursor.fetchone()

    if row is None:
        return {
            "table_name": table_name,
            "watermark_type": config["watermark_type"],
            "last_timestamp": None,
            "last_id": 0,
            "last_batch_id": None,
            "is_initial_load": True,
        }

    watermark_type, last_timestamp, last_id, last_batch_id = row
    if watermark_type != config["watermark_type"]:
        raise RuntimeError(
            f"Watermark type mismatch for {table_name}: "
            f"expected {config['watermark_type']}, "
            f"got {watermark_type}"
        )
    return {
        "table_name": table_name,
        "watermark_type": watermark_type,
        "last_timestamp": last_timestamp,
        "last_id": last_id,
        "last_batch_id": last_batch_id,
        "is_initial_load": False,
    }


def watermark_matches_batch_start(watermark, metadata):
    """Check that the current checkpoint equals a batch start checkpoint."""
    if watermark["watermark_type"] != metadata["watermark_type"]:
        return False
    if watermark["last_id"] != metadata["start_id"]:
        return False
    if metadata["watermark_type"] == "TIMESTAMP":
        return watermark["last_timestamp"] == metadata["start_timestamp"]
    if metadata["watermark_type"] == "ID":
        return watermark["last_timestamp"] is None and metadata["start_timestamp"] is None
    return False


def get_locked_watermark(conn, table_name):
    """Acquire the table lock and re-read its current watermark."""
    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table_name}")
    if not acquire_table_lock(conn, table_name):
        raise RuntimeError(f"Table ingestion lock is busy: {table_name}")
    watermark = get_watermark(conn, table_name)
    if watermark["watermark_type"] != TABLE_CONFIG[table_name]["watermark_type"]:
        raise ValueError(f"Watermark type mismatch: {table_name}")
    return watermark


def commit_uploaded_batch(conn, client, batch_id):
    """Atomically advance a verified UPLOADED batch checkpoint."""
    from .batch_repository import get_batch_metadata, lock_batch_metadata
    from .minio_storage import check_minio_bucket, verify_minio_object
    from .recovery import validate_recovery_metadata

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")
    metadata = get_batch_metadata(conn, batch_id)
    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")
    validate_recovery_metadata(metadata)
    table_name = metadata["table_name"]
    watermark = get_locked_watermark(conn, table_name)
    metadata = lock_batch_metadata(conn, batch_id)
    if metadata is None:
        raise RuntimeError("Batch disappeared")
    validate_recovery_metadata(metadata)
    if metadata["status"] != "UPLOADED":
        raise ValueError(f"Batch must be UPLOADED, got {metadata['status']}")
    if not watermark_matches_batch_start(watermark, metadata):
        raise RuntimeError("Watermark does not match batch start")
    bucket = check_minio_bucket(client)
    if not verify_minio_object(
        client, bucket, metadata["object_key"], metadata["checksum_sha256"]
    ):
        raise RuntimeError("MinIO object does not exist")
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO public.ingestion_watermarks (
                table_name, watermark_type, last_timestamp, last_id, last_batch_id,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name)
            DO UPDATE SET
                watermark_type = EXCLUDED.watermark_type,
                last_timestamp = EXCLUDED.last_timestamp,
                last_id = EXCLUDED.last_id,
                last_batch_id = EXCLUDED.last_batch_id,
                updated_at = CURRENT_TIMESTAMP
            """,
            (table_name, metadata["watermark_type"], metadata["end_timestamp"],
             metadata["end_id"], batch_id),
        )
        cursor.execute(
            """
            UPDATE public.ingestion_batches
            SET status = 'COMMITTED', updated_at = CURRENT_TIMESTAMP
            WHERE batch_id = %s AND status = 'UPLOADED'
            RETURNING batch_id
            """,
            (batch_id,),
        )
        if cursor.fetchone() is None:
            raise RuntimeError("Batch status changed before COMMIT")
    return {
        "batch_id": batch_id, "table_name": table_name, "status": "COMMITTED",
        "last_timestamp": metadata["end_timestamp"], "last_id": metadata["end_id"],
    }

__all__ = [
    "commit_uploaded_batch",
    "get_locked_watermark",
    "get_watermark",
    "watermark_matches_batch_start",
]
