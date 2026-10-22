"""Read-only Data Quality validation for committed incremental Bronze batches."""

import csv
import io
from contextlib import closing

from ingestion.incremental.batch_repository import get_batch_metadata
from ingestion.incremental.config import get_table_config, validate_batch_id
from ingestion.incremental.database import get_db_connection
from ingestion.incremental.minio_storage import (
    build_object_key,
    check_minio_bucket,
    get_minio_client,
    verify_minio_object,
)


def validate_incremental_batch(table_name, batch_id):
    """
    Validate one committed incremental Bronze batch.

    Checks:
        - Supported table and valid batch ID
        - Batch metadata and COMMITTED status
        - Expected MinIO object key
        - SHA-256 checksum
        - CSV header and primary key
        - Non-empty primary keys
        - Duplicate primary keys
        - CSV row count against batch metadata

    This function never modifies PostgreSQL or MinIO.
    """

    table_config = get_table_config(table_name)
    validate_batch_id(batch_id)

    # 1. Read exact batch metadata.
    with closing(get_db_connection()) as conn:
        metadata = get_batch_metadata(conn, batch_id)

        # End the read-only transaction.
        conn.rollback()

    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")

    if metadata["table_name"] != table_name:
        raise ValueError(
            f"Batch {batch_id} belongs to {metadata['table_name']}, "
            f"not {table_name}"
        )

    if metadata["status"] != "COMMITTED":
        raise ValueError(
            f"Batch {batch_id} is not COMMITTED: {metadata['status']}"
        )

    # 2. Verify object location.
    expected_object_key = build_object_key(table_name, batch_id)
    object_key = metadata["object_key"]

    if object_key != expected_object_key:
        raise ValueError(
            f"Unexpected object key for batch {batch_id}: {object_key}"
        )

    # 3. Verify checksum before parsing CSV.
    client = get_minio_client()
    bucket_name = check_minio_bucket(client)

    checksum_valid = verify_minio_object(
        client,
        bucket_name,
        object_key,
        metadata["checksum_sha256"],
    )

    if not checksum_valid:
        raise ValueError(f"MinIO object not found: {object_key}")

    # 4. Read the exact object.
    response = client.get_object(bucket_name, object_key)

    try:
        csv_content = response.read().decode("utf-8-sig")
    finally:
        response.close()
        response.release_conn()

    # 5. Parse CSV.
    reader = csv.DictReader(io.StringIO(csv_content))

    primary_key = table_config["primary_key"]

    if not reader.fieldnames or primary_key not in reader.fieldnames:
        raise ValueError(
            f"Missing primary key column '{primary_key}' "
            f"in {object_key}"
        )

    if len(reader.fieldnames) != len(set(reader.fieldnames)):
        raise ValueError(f"Duplicate CSV column names in {object_key}")

    rows = list(reader)

    if any(None in row for row in rows):
        raise ValueError(f"Malformed CSV row in {object_key}")

    # 6. Validate primary keys.
    primary_key_values = [
        row[primary_key]
        for row in rows
    ]

    null_primary_keys = sum(
        value is None or not value.strip()
        for value in primary_key_values
    )

    duplicate_rows = (
        len(primary_key_values)
        - len(set(primary_key_values))
    )

    # 7. Validate batch row count.
    expected_row_count = metadata["row_count"]
    actual_row_count = len(rows)

    if (
        not isinstance(expected_row_count, int)
        or isinstance(expected_row_count, bool)
        or expected_row_count <= 0
    ):
        raise ValueError(
            f"Invalid metadata row_count: {expected_row_count}"
        )

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"Row count mismatch for {table_name}: "
            f"expected={expected_row_count}, actual={actual_row_count}"
        )

    if null_primary_keys:
        raise ValueError(
            f"Found {null_primary_keys} empty primary keys in {object_key}"
        )

    if duplicate_rows:
        raise ValueError(
            f"Found {duplicate_rows} duplicate primary keys in {object_key}"
        )

    # 8. Return Airflow-compatible result.
    return {
        "table_name": table_name,
        "batch_id": batch_id,
        "status": "PASS",
        "row_count": actual_row_count,
        "duplicate_rows": duplicate_rows,
        "null_primary_keys": null_primary_keys,
        "checksum_verified": True,
        "bronze_checksum_sha256": metadata["checksum_sha256"],
        "object_key": object_key,
    }