"""Repository operations and advisory locking for ingestion batches."""

from .config import TABLE_CONFIG, get_table_config, validate_batch_id


def has_failed_batch(conn, table_name):
    """Return whether *table_name* has a batch requiring investigation."""
    get_table_config(table_name)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM public.ingestion_batches
                WHERE table_name = %s AND status = 'FAILED'
            )
            """,
            (table_name,),
        )
        return cursor.fetchone()[0]


def acquire_table_lock(conn, table_name):
    """Acquire the table's transaction-level advisory lock."""
    if table_name not in TABLE_CONFIG:
        raise ValueError("Unsupported table")
    lock_ids = {
        "customers": 61001, "products": 61002, "orders": 61003,
        "order_items": 61004, "payments": 61005, "shipments": 61006,
    }
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT pg_try_advisory_xact_lock(%s, %s)",
            (62001, lock_ids[table_name]),
        )
        return cursor.fetchone()[0]


def has_unfinished_batch(conn, table_name):
    get_table_config(table_name)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM public.ingestion_batches
                WHERE table_name = %s
                  AND status IN ('PENDING', 'UPLOADED')
            )
            """,
            (table_name,),
        )
        return cursor.fetchone()[0]


def has_blocking_batch(conn, table_name):
    get_table_config(table_name)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM public.ingestion_batches
                WHERE table_name = %s
                  AND status IN ('PENDING', 'UPLOADED', 'FAILED')
            )
            """,
            (table_name,),
        )
        return cursor.fetchone()[0]


def register_pending_batch(conn, metadata):
    """Insert a PENDING batch without committing the caller transaction."""
    required_fields = [
        "batch_id", "table_name", "watermark_type", "start_timestamp", "start_id",
        "end_timestamp", "end_id", "row_count", "object_key", "checksum_sha256",
        "status",
    ]
    for field in required_fields:
        if field not in metadata:
            raise ValueError(f"Missing metadata field: {field}")
    if metadata["status"] != "PENDING":
        raise ValueError("Only PENDING batches can be registered")
    if metadata["table_name"] not in TABLE_CONFIG:
        raise ValueError("Unsupported table")

    query = """
        INSERT INTO public.ingestion_batches (
            batch_id,
            table_name,
            watermark_type,
            start_timestamp,
            start_id,
            end_timestamp,
            end_id,
            row_count,
            object_key,
            checksum_sha256,
            status
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        RETURNING batch_id, table_name, status
    """
    values = (
        metadata["batch_id"], metadata["table_name"], metadata["watermark_type"],
        metadata["start_timestamp"], metadata["start_id"], metadata["end_timestamp"],
        metadata["end_id"], metadata["row_count"], metadata["object_key"],
        metadata["checksum_sha256"], metadata["status"],
    )
    with conn.cursor() as cursor:
        cursor.execute(query, values)
        result = cursor.fetchone()
    return {"batch_id": result[0], "table_name": result[1], "status": result[2]}


def get_batch_metadata(conn, batch_id):
    """Read batch metadata; return ``None`` when the batch does not exist."""
    validate_batch_id(batch_id)
    query = """
        SELECT
            batch_id,
            table_name,
            watermark_type,
            start_timestamp,
            start_id,
            end_timestamp,
            end_id,
            row_count,
            object_key,
            checksum_sha256,
            status,
            created_at,
            updated_at
        FROM public.ingestion_batches
        WHERE batch_id = %s
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (batch_id,))
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def get_recoverable_batches(conn, table_name=None):
    """Return PENDING and UPLOADED batches in recovery order."""
    if table_name is not None:
        get_table_config(table_name)
    query = """
        SELECT batch_id
        FROM public.ingestion_batches
        WHERE status IN ('PENDING', 'UPLOADED')
    """
    params = []
    if table_name is not None:
        query += " AND table_name = %s"
        params.append(table_name)
    query += " ORDER BY created_at, batch_id"
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        batch_ids = [row[0] for row in cursor.fetchall()]
    return [get_batch_metadata(conn, batch_id) for batch_id in batch_ids]


def lock_batch_metadata(conn, batch_id):
    """Lock a batch row and return freshly-read metadata."""
    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")
    metadata = get_batch_metadata(conn, batch_id)
    if metadata is None:
        return None
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT batch_id
            FROM public.ingestion_batches
            WHERE batch_id = %s
            FOR UPDATE
            """,
            (batch_id,),
        )
        locked = cursor.fetchone()
    if locked is None:
        raise RuntimeError("Batch disappeared before row lock")
    return get_batch_metadata(conn, batch_id)


def mark_batch_uploaded(conn, client, batch_id):
    """Verify MinIO and transition a PENDING batch to UPLOADED."""
    from .minio_storage import check_minio_bucket, verify_minio_object
    from .recovery import validate_recovery_metadata

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")
    metadata = lock_batch_metadata(conn, batch_id)
    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")
    validate_recovery_metadata(metadata)
    if metadata["status"] not in ("PENDING", "UPLOADED"):
        raise ValueError(f"Invalid batch status: {metadata['status']}")
    bucket = check_minio_bucket(client)
    if not verify_minio_object(
        client, bucket, metadata["object_key"], metadata["checksum_sha256"]
    ):
        raise RuntimeError("MinIO object does not exist")
    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE public.ingestion_batches
            SET status = 'UPLOADED',
                updated_at = CURRENT_TIMESTAMP
            WHERE batch_id = %s
              AND status IN ('PENDING', 'UPLOADED')
            RETURNING batch_id
            """,
            (batch_id,),
        )
        result = cursor.fetchone()
    if result is None:
        raise RuntimeError("Batch status changed concurrently")
    return {
        "batch_id": batch_id, "status": "UPLOADED",
        "object_key": metadata["object_key"],
        "checksum_sha256": metadata["checksum_sha256"],
    }

__all__ = [
    "acquire_table_lock",
    "get_batch_metadata",
    "get_recoverable_batches",
    "has_blocking_batch",
    "has_unfinished_batch",
    "lock_batch_metadata",
    "mark_batch_uploaded",
    "register_pending_batch",
]
