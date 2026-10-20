"""Recovery of PENDING and UPLOADED ingestion batches."""

from .config import RECOVERABLE_STATUSES, VALID_CHECKSUM, get_table_config, validate_batch_id
from .minio_storage import build_object_key
from .staging import load_staged_batch


def validate_recovery_metadata(metadata):
    """Validate recovery metadata without accessing PostgreSQL or MinIO."""
    if not isinstance(metadata, dict):
        raise ValueError("Metadata must be a dictionary")
    required_fields = (
        "batch_id", "table_name", "watermark_type", "start_timestamp", "start_id",
        "end_timestamp", "end_id", "row_count", "object_key", "checksum_sha256",
        "status",
    )
    for field in required_fields:
        if field not in metadata:
            raise ValueError(f"Missing metadata field: {field}")

    table_name = metadata["table_name"]
    config = get_table_config(table_name)
    batch_id = metadata["batch_id"]
    validate_batch_id(batch_id)
    watermark_type = metadata["watermark_type"]
    if watermark_type != config["watermark_type"]:
        raise ValueError("Watermark type mismatch")
    if metadata["status"] not in RECOVERABLE_STATUSES:
        raise ValueError("Batch is not recoverable")
    row_count = metadata["row_count"]
    if not isinstance(row_count, int) or isinstance(row_count, bool) or row_count <= 0:
        raise ValueError("Invalid row_count")
    start_id, end_id = metadata["start_id"], metadata["end_id"]
    for value in (start_id, end_id):
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError("Invalid watermark ID")
    if start_id < 0 or end_id <= 0:
        raise ValueError("Invalid watermark range")
    if watermark_type == "ID":
        if end_id <= start_id:
            raise ValueError("Invalid ID watermark order")
        if metadata["start_timestamp"] is not None or metadata["end_timestamp"] is not None:
            raise ValueError("ID watermark cannot have timestamps")
    else:
        start_timestamp, end_timestamp = (
            metadata["start_timestamp"], metadata["end_timestamp"]
        )
        if end_timestamp is None:
            raise ValueError("Missing end_timestamp")
        if start_timestamp is not None and (end_timestamp, end_id) <= (
            start_timestamp, start_id
        ):
            raise ValueError("Invalid timestamp watermark order")
    if metadata["object_key"] != build_object_key(table_name, batch_id):
        raise ValueError("Object key mismatch")
    checksum = metadata["checksum_sha256"]
    if not isinstance(checksum, str) or not VALID_CHECKSUM.fullmatch(checksum):
        raise ValueError("Invalid SHA256 checksum")
    return True


def recover_staged_batch(metadata, staging_root="staging"):
    """Read, checksum-validate, and structurally validate a staged CSV."""
    import csv
    import io

    validate_recovery_metadata(metadata)
    csv_bytes = load_staged_batch(metadata, staging_root=staging_root)
    try:
        reader = csv.reader(
            io.StringIO(csv_bytes.decode("utf-8", errors="strict"), newline=""),
            strict=True,
        )
        header = next(reader, None)
        if not header:
            raise ValueError("CSV header is missing")
        if any(not column for column in header):
            raise ValueError("CSV contains an empty column name")
        if len(header) != len(set(header)):
            raise ValueError("CSV contains duplicate column names")
        actual_row_count = 0
        for row in reader:
            if len(row) != len(header):
                raise ValueError("CSV column count mismatch")
            actual_row_count += 1
    except (UnicodeDecodeError, csv.Error) as exc:
        raise ValueError("Invalid staged CSV") from exc
    if actual_row_count != metadata["row_count"]:
        raise ValueError(
            f"CSV row count mismatch: expected={metadata['row_count']}, "
            f"actual={actual_row_count}"
        )
    return {
        "batch_id": metadata["batch_id"], "table_name": metadata["table_name"],
        "row_count": actual_row_count, "object_key": metadata["object_key"],
        "checksum_sha256": metadata["checksum_sha256"], "csv_bytes": csv_bytes,
    }


def upload_recovered_batch(client, metadata, staging_root="staging"):
    """Upload a recovered CSV without overwriting an existing object."""
    from minio.error import S3Error

    from .minio_storage import (
        check_minio_bucket,
        put_object_if_absent,
        verify_minio_object,
    )

    recovered = recover_staged_batch(metadata, staging_root=staging_root)
    bucket = check_minio_bucket(client)
    object_key = recovered["object_key"]
    checksum = recovered["checksum_sha256"]
    csv_bytes = recovered["csv_bytes"]
    if verify_minio_object(client, bucket, object_key, checksum):
        return {
            "status": "ALREADY_EXISTS", "object_key": object_key,
            "checksum_sha256": checksum,
        }
    try:
        put_object_if_absent(client, bucket, object_key, csv_bytes)
    except S3Error as exc:
        if exc.code in ("PreconditionFailed", "ConditionalRequestConflict") and (
            verify_minio_object(client, bucket, object_key, checksum)
        ):
            return {
                "status": "ALREADY_EXISTS", "object_key": object_key,
                "checksum_sha256": checksum,
            }
        raise
    if not verify_minio_object(client, bucket, object_key, checksum):
        raise RuntimeError("Uploaded object could not be verified")
    return {"status": "UPLOADED", "object_key": object_key, "checksum_sha256": checksum}


def recover_uploaded_batch(conn, client, batch_id):
    """Commit an UPLOADED batch's checkpoint in the caller transaction."""
    from .batch_repository import get_batch_metadata
    from .watermark import commit_uploaded_batch

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")
    metadata = get_batch_metadata(conn, batch_id)
    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")
    validate_recovery_metadata(metadata)
    if metadata["status"] != "UPLOADED":
        raise ValueError("Expected UPLOADED batch, " f"got {metadata['status']}")
    result = commit_uploaded_batch(conn, client, batch_id)
    return {
        "batch_id": result["batch_id"], "table_name": result["table_name"],
        "status": result["status"], "last_timestamp": result["last_timestamp"],
        "last_id": result["last_id"], "recovered": True,
    }


def execute_uploaded_recovery(conn, client, batch_id):
    """Commit an UPLOADED batch inside the caller-owned transaction."""
    from .batch_repository import get_batch_metadata
    from .database import require_transaction

    require_transaction(conn)
    metadata = get_batch_metadata(conn, batch_id)
    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")
    if metadata["status"] != "UPLOADED":
        raise ValueError(f"Expected UPLOADED, got {metadata['status']}")
    return {"action": "RECOVER_UPLOADED", **recover_uploaded_batch(conn, client, batch_id)}


def execute_pending_recovery(conn, client, batch_id, staging_root="staging"):
    """Recover a PENDING batch without committing the caller transaction."""
    from .batch_repository import get_batch_metadata, mark_batch_uploaded

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")
    metadata = get_batch_metadata(conn, batch_id)
    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")
    validate_recovery_metadata(metadata)
    if metadata["status"] != "PENDING":
        raise ValueError(f"Expected PENDING, got {metadata['status']}")
    recover_staged_batch(metadata, staging_root=staging_root)
    upload_result = upload_recovered_batch(client, metadata, staging_root=staging_root)
    mark_batch_uploaded(conn, client, batch_id)
    result = recover_uploaded_batch(conn, client, batch_id)
    return {
        "action": "RECOVER_PENDING", "batch_id": result["batch_id"],
        "table_name": result["table_name"], "status": result["status"],
        "recovered": True, "upload_result": upload_result,
        "last_timestamp": result["last_timestamp"], "last_id": result["last_id"],
    }


__all__ = [
    "execute_pending_recovery",
    "execute_uploaded_recovery",
    "recover_staged_batch",
    "recover_uploaded_batch",
    "upload_recovered_batch",
    "validate_recovery_metadata",
]
