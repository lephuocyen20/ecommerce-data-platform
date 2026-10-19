import csv
import hashlib
import io
import os
import re
import tempfile
import uuid
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from psycopg2 import sql

load_dotenv(dotenv_path=".env")


# Incremental configuration
TABLE_CONFIG = {
    "customers": {
        "primary_key": "customer_id",
        "watermark_column": "updated_at",
        "watermark_type": "TIMESTAMP",
    },
    "products": {
        "primary_key": "product_id",
        "watermark_column": "updated_at",
        "watermark_type": "TIMESTAMP",
    },
    "orders": {
        "primary_key": "order_id",
        "watermark_column": "updated_at",
        "watermark_type": "TIMESTAMP",
    },
    "shipments": {
        "primary_key": "shipment_id",
        "watermark_column": "updated_at",
        "watermark_type": "TIMESTAMP",
    },
    "order_items": {
        "primary_key": "order_item_id",
        "watermark_column": "order_item_id",
        "watermark_type": "ID",
    },
    "payments": {
        "primary_key": "payment_id",
        "watermark_column": "payment_id",
        "watermark_type": "ID",
    },
}

EXPECTED_DB_NAME = "ecommerce_test"
RECOVERABLE_STATUSES = ("PENDING", "UPLOADED")
BLOCKING_STATUSES = (*RECOVERABLE_STATUSES, "FAILED")
VALID_BATCH_ID = re.compile(r"[0-9a-f]{32}")
VALID_CHECKSUM = re.compile(r"[0-9a-f]{64}")


def get_table_config(table_name):
    """Return the supported table configuration for *table_name*."""
    try:
        return TABLE_CONFIG[table_name]
    except KeyError:
        raise ValueError(f"Unsupported table: {table_name}") from None


def validate_positive_integer(value, name):
    """Validate an integer while rejecting bool, which is an int subclass."""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def validate_batch_id(batch_id):
    if not isinstance(batch_id, str) or not VALID_BATCH_ID.fullmatch(batch_id):
        raise ValueError("Invalid batch ID")


def require_transaction(conn):
    """Ensure a caller-owned PostgreSQL transaction is active."""
    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")


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


def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    with conn.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        actual_db_name = cursor.fetchone()[0]

    if actual_db_name != EXPECTED_DB_NAME:
        conn.close()
        raise RuntimeError(
            f"Unsafe database connection: {actual_db_name}. "
            f"Expected: {EXPECTED_DB_NAME}"
        )

    return conn


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


def extract_incremental(conn, table_name, watermark, batch_size=1000):
    config = get_table_config(table_name)
    validate_positive_integer(batch_size, "batch_size")
    primary_key = config["primary_key"]
    watermark_column = config["watermark_column"]
    watermark_type = config["watermark_type"]

    if watermark["watermark_type"] != watermark_type:
        raise ValueError("Watermark type mismatch")

    with conn.cursor() as cursor:
        if watermark_type == "TIMESTAMP":
            if watermark["last_timestamp"] is None:
                query = sql.SQL("""
                    SELECT *
                    FROM public.{}
                    ORDER BY {} ASC, {} ASC
                    LIMIT %s
                """).format(
                    sql.Identifier(table_name),
                    sql.Identifier(watermark_column),
                    sql.Identifier(primary_key),
                )
                params = (batch_size,)
            else:
                query = sql.SQL("""
                    SELECT *
                    FROM public.{}
                    WHERE ({}, {}) > (%s, %s)
                    ORDER BY {} ASC, {} ASC
                    LIMIT %s
                """).format(
                    sql.Identifier(table_name),
                    sql.Identifier(watermark_column),
                    sql.Identifier(primary_key),
                    sql.Identifier(watermark_column),
                    sql.Identifier(primary_key),
                )
                params = (
                    watermark["last_timestamp"],
                    watermark["last_id"],
                    batch_size,
                )

        else:
            query = sql.SQL("""
                SELECT *
                FROM public.{}
                WHERE {} > %s
                ORDER BY {} ASC
                LIMIT %s
            """).format(
                sql.Identifier(table_name),
                sql.Identifier(primary_key),
                sql.Identifier(primary_key),
            )
            params = (watermark["last_id"], batch_size)

        cursor.execute(query, params)

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

    return {
        "table_name": table_name,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
    }


def records_to_csv(columns, rows):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)

    writer.writerow(columns)
    writer.writerows(rows)

    return buffer.getvalue()


def generate_batch_id():
    return uuid.uuid4().hex


def build_object_key(table_name, batch_id):
    get_table_config(table_name)

    try:
        validated_batch_id = uuid.UUID(hex=batch_id)
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Invalid batch_id") from None

    if validated_batch_id.hex != batch_id:
        raise ValueError("batch_id must be a canonical UUID hex string")

    return f"bronze_incremental/{table_name}/" f"batch_id={batch_id}/data.csv"


def get_minio_client():
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )


def check_minio_bucket(client):
    bucket_name = os.getenv("BRONZE_BUCKET")

    if not bucket_name:
        raise RuntimeError("BRONZE_BUCKET is not configured")

    if not client.bucket_exists(bucket_name):
        raise RuntimeError(f"MinIO bucket does not exist: {bucket_name}")

    return bucket_name


def object_exists(client, bucket_name, object_key):
    try:
        client.stat_object(bucket_name, object_key)
        return True
    except S3Error as error:
        if error.code in ("NoSuchKey", "NoSuchObject"):
            return False
        raise


def upload_incremental_batch(
    client,
    table_name,
    batch_id,
    csv_content,
    dry_run=True,
):
    bucket_name = check_minio_bucket(client)
    object_key = build_object_key(table_name, batch_id)

    if not isinstance(csv_content, str) or not csv_content:
        raise ValueError("csv_content must be a non-empty string")

    if object_exists(client, bucket_name, object_key):
        raise RuntimeError(f"Object already exists, refusing overwrite: {object_key}")

    data = csv_content.encode("utf-8")
    if dry_run:
        return {
            "status": "DRY_RUN",
            "bucket": bucket_name,
            "object_key": object_key,
            "size_bytes": len(data),
        }

    raise NotImplementedError("Actual MinIO upload is not enabled yet")


def prepare_batch_metadata(table_name, watermark, batch, csv_content):
    config = get_table_config(table_name)

    if batch["table_name"] != table_name:
        raise ValueError("Batch table mismatch")

    if batch["row_count"] <= 0:
        raise ValueError("Cannot register an empty batch")

    if batch["row_count"] != len(batch["rows"]):
        raise ValueError("Batch row count mismatch")

    if watermark["watermark_type"] != config["watermark_type"]:
        raise ValueError("Watermark type mismatch")

    columns = batch["columns"]

    if config["primary_key"] not in columns:
        raise ValueError("Primary key column missing")

    if config["watermark_column"] not in columns:
        raise ValueError("Watermark column missing")

    expected_csv = records_to_csv(columns, batch["rows"])

    if csv_content != expected_csv:
        raise ValueError("CSV content does not match batch")

    last_row = dict(zip(columns, batch["rows"][-1]))

    batch_id = generate_batch_id()
    object_key = build_object_key(table_name, batch_id)

    checksum = hashlib.sha256(csv_content.encode("utf-8")).hexdigest()

    return {
        "batch_id": batch_id,
        "table_name": table_name,
        "watermark_type": config["watermark_type"],
        "start_timestamp": watermark["last_timestamp"],
        "start_id": watermark["last_id"],
        "end_timestamp": (
            last_row[config["watermark_column"]]
            if config["watermark_type"] == "TIMESTAMP"
            else None
        ),
        "end_id": last_row[config["primary_key"]],
        "row_count": batch["row_count"],
        "object_key": object_key,
        "checksum_sha256": checksum,
        "status": "PENDING",
    }


def register_pending_batch(conn, metadata):
    """
    Insert a PENDING batch into ingestion_batches.

    IMPORTANT:
    - Does not commit the transaction.
    - Caller is responsible for commit or rollback.
    """

    required_fields = [
        "batch_id",
        "table_name",
        "watermark_type",
        "start_timestamp",
        "start_id",
        "end_timestamp",
        "end_id",
        "row_count",
        "object_key",
        "checksum_sha256",
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
        metadata["batch_id"],
        metadata["table_name"],
        metadata["watermark_type"],
        metadata["start_timestamp"],
        metadata["start_id"],
        metadata["end_timestamp"],
        metadata["end_id"],
        metadata["row_count"],
        metadata["object_key"],
        metadata["checksum_sha256"],
        metadata["status"],
    )

    with conn.cursor() as cursor:
        cursor.execute(query, values)
        result = cursor.fetchone()

    return {
        "batch_id": result[0],
        "table_name": result[1],
        "status": result[2],
    }


def stage_batch_csv(metadata, csv_content, staging_root="staging"):
    """
    Stage CSV using a temporary file and no-overwrite publish.

    - Verify CSV checksum.
    - Write and fsync temporary file.
    - Publish without overwriting existing data.csv.
    - Verify existing file when retrying.
    """

    batch_id = metadata["batch_id"]
    table_name = metadata["table_name"]

    get_table_config(table_name)
    validate_batch_id(batch_id)

    if not isinstance(csv_content, str) or not csv_content:
        raise ValueError("Empty CSV content")

    csv_bytes = csv_content.encode("utf-8")
    expected_checksum = metadata["checksum_sha256"]
    actual_checksum = hashlib.sha256(csv_bytes).hexdigest()

    if actual_checksum != expected_checksum:
        raise ValueError("CSV checksum mismatch")

    staging_dir = Path(staging_root) / table_name / f"batch_id={batch_id}"
    staging_dir.mkdir(parents=True, exist_ok=True)

    final_path = staging_dir / "data.csv"

    def verify_existing():
        existing_bytes = final_path.read_bytes()
        existing_checksum = hashlib.sha256(existing_bytes).hexdigest()

        if existing_checksum != expected_checksum:
            raise RuntimeError("Existing staged CSV has a different checksum")

    # Retry: file already exists.
    if final_path.exists():
        verify_existing()
        return str(final_path)

    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=staging_dir,
            prefix=".tmp-",
            suffix=".csv",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            temp_file.write(csv_bytes)
            temp_file.flush()
            os.fsync(temp_file.fileno())

        # Atomic creation of destination, without overwriting.
        try:
            os.link(temp_path, final_path)
        except FileExistsError:
            verify_existing()

        return str(final_path)

    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def load_staged_batch(metadata, staging_root="staging"):
    """
    Load an existing staged CSV and verify its SHA-256.
    Read-only: does not create or modify files.
    """
    batch_id = metadata["batch_id"]
    table_name = metadata["table_name"]

    get_table_config(table_name)
    validate_batch_id(batch_id)

    expected_checksum = metadata["checksum_sha256"]

    if not isinstance(expected_checksum, str) or not VALID_CHECKSUM.fullmatch(
        expected_checksum
    ):
        raise ValueError("Invalid checksum")

    staged_path = Path(staging_root) / table_name / f"batch_id={batch_id}" / "data.csv"

    if not staged_path.is_file():
        raise FileNotFoundError(f"Staged CSV not found: {staged_path}")

    csv_bytes = staged_path.read_bytes()
    actual_checksum = hashlib.sha256(csv_bytes).hexdigest()

    if actual_checksum != expected_checksum:
        raise RuntimeError(f"Staged CSV checksum mismatch: {staged_path}")

    return csv_bytes


def get_batch_metadata(conn, batch_id):
    """
    Read batch metadata from PostgreSQL.

    Read-only. Returns None if batch does not exist.
    """
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
    """
    Find batches that may require recovery.

    Read-only.
    Returns PENDING and UPLOADED batches.
    """
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


def acquire_table_lock(conn, table_name):
    """
    Acquire a transaction-level advisory lock for one table.

    Returns True if acquired, False if already locked.
    Lock is released automatically on COMMIT or ROLLBACK.
    """
    if table_name not in TABLE_CONFIG:
        raise ValueError("Unsupported table")

    # Stable lock IDs reserved for this ingestion pipeline.
    lock_ids = {
        "customers": 61001,
        "products": 61002,
        "orders": 61003,
        "order_items": 61004,
        "payments": 61005,
        "shipments": 61006,
    }

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT pg_try_advisory_xact_lock(%s, %s)",
            (62001, lock_ids[table_name]),
        )
        acquired = cursor.fetchone()[0]

    return acquired


def has_unfinished_batch(conn, table_name):
    """Return whether a table has a PENDING or UPLOADED batch."""
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
    """
    Return True if the table has a batch that blocks new ingestion.

    PENDING  -> waiting for upload/recovery
    UPLOADED -> waiting for checkpoint
    FAILED   -> requires investigation/recovery

    Read-only. Call while holding the table advisory lock.
    """
    get_table_config(table_name)

    query = """
        SELECT EXISTS (
            SELECT 1
            FROM public.ingestion_batches
            WHERE table_name = %s
              AND status IN ('PENDING', 'UPLOADED', 'FAILED')
        )
    """

    with conn.cursor() as cursor:
        cursor.execute(query, (table_name,))
        return cursor.fetchone()[0]


def prepare_pending_batch(
    conn,
    table_name,
    batch_size=1000,
    staging_root="staging",
):
    """
    Prepare and register one PENDING batch safely.

    Does not COMMIT.
    Caller must COMMIT or ROLLBACK the transaction.
    """

    get_table_config(table_name)
    validate_positive_integer(batch_size, "batch_size")
    require_transaction(conn)

    # 1. Acquire per-table transaction lock
    if not acquire_table_lock(conn, table_name):
        raise RuntimeError(f"Table is locked: {table_name}")

    # 2. Block new ingestion if recovery is required
    if has_blocking_batch(conn, table_name):
        raise RuntimeError(f"Blocking batch exists for {table_name}")

    # 3. Read current watermark
    watermark = get_watermark(conn, table_name)

    # 4. Extract incremental rows
    batch = extract_incremental(
        conn,
        table_name,
        watermark,
        batch_size=batch_size,
    )

    if batch["row_count"] == 0:
        return None

    # 5. Build CSV and metadata
    csv_content = records_to_csv(
        batch["columns"],
        batch["rows"],
    )

    metadata = prepare_batch_metadata(
        table_name,
        watermark,
        batch,
        csv_content,
    )

    # 6. Persist CSV before registering PENDING
    staged_path = stage_batch_csv(
        metadata,
        csv_content,
        staging_root=staging_root,
    )

    # 7. Register PENDING in current transaction
    register_pending_batch(conn, metadata)

    return {
        "metadata": metadata,
        "staged_path": staged_path,
    }


def validate_recovery_metadata(metadata):
    """
    Validate batch metadata before recovery.

    Read-only. Does not access PostgreSQL or MinIO.
    """
    if not isinstance(metadata, dict):
        raise ValueError("Metadata must be a dictionary")

    required_fields = (
        "batch_id",
        "table_name",
        "watermark_type",
        "start_timestamp",
        "start_id",
        "end_timestamp",
        "end_id",
        "row_count",
        "object_key",
        "checksum_sha256",
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

    start_id = metadata["start_id"]
    end_id = metadata["end_id"]

    for value in (start_id, end_id):
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError("Invalid watermark ID")

    if start_id < 0 or end_id <= 0:
        raise ValueError("Invalid watermark range")

    if watermark_type == "ID":
        if end_id <= start_id:
            raise ValueError("Invalid ID watermark order")

        if (
            metadata["start_timestamp"] is not None
            or metadata["end_timestamp"] is not None
        ):
            raise ValueError("ID watermark cannot have timestamps")

    else:
        end_timestamp = metadata["end_timestamp"]
        start_timestamp = metadata["start_timestamp"]

        if end_timestamp is None:
            raise ValueError("Missing end_timestamp")

        if start_timestamp is not None and (
            end_timestamp,
            end_id,
        ) <= (
            start_timestamp,
            start_id,
        ):
            raise ValueError("Invalid timestamp watermark order")

    expected_key = build_object_key(
        table_name,
        batch_id,
    )

    if metadata["object_key"] != expected_key:
        raise ValueError("Object key mismatch")

    checksum = metadata["checksum_sha256"]

    if not isinstance(checksum, str) or not VALID_CHECKSUM.fullmatch(checksum):
        raise ValueError("Invalid SHA256 checksum")

    return True


def recover_staged_batch(
    metadata,
    staging_root="staging",
):
    """
    Recover a staged CSV with metadata, checksum
    and row-count validation.

    Read-only. No PostgreSQL or MinIO writes.
    """

    validate_recovery_metadata(metadata)

    csv_bytes = load_staged_batch(
        metadata,
        staging_root=staging_root,
    )

    try:
        csv_text = csv_bytes.decode("utf-8", errors="strict")

        reader = csv.reader(
            io.StringIO(csv_text, newline=""),
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
            f"CSV row count mismatch: "
            f"expected={metadata['row_count']}, "
            f"actual={actual_row_count}"
        )

    return {
        "batch_id": metadata["batch_id"],
        "table_name": metadata["table_name"],
        "row_count": actual_row_count,
        "object_key": metadata["object_key"],
        "checksum_sha256": metadata["checksum_sha256"],
        "csv_bytes": csv_bytes,
    }


def verify_minio_object(
    client,
    bucket_name,
    object_key,
    expected_checksum,
):
    """
    Verify an existing MinIO object's SHA256.

    Read-only. Does not upload or modify objects.
    """

    if not isinstance(expected_checksum, str) or not VALID_CHECKSUM.fullmatch(
        expected_checksum
    ):
        raise ValueError("Invalid expected checksum")

    if not object_exists(client, bucket_name, object_key):
        return False

    response = None

    try:
        response = client.get_object(
            bucket_name,
            object_key,
        )

        hasher = hashlib.sha256()

        for chunk in response.stream(amt=1024 * 1024):
            hasher.update(chunk)

        actual_checksum = hasher.hexdigest()

        if actual_checksum != expected_checksum:
            raise ValueError("MinIO object checksum mismatch")

        return True

    finally:
        if response is not None:
            response.close()
            response.release_conn()


def upload_recovered_batch(
    client,
    metadata,
    staging_root="staging",
):
    """
    Upload a recovered batch with conditional creation.

    Returns:
      UPLOADED       - new object created and verified
      ALREADY_EXISTS - existing object matches checksum

    Never intentionally overwrites an existing object.
    """

    recovered = recover_staged_batch(
        metadata,
        staging_root=staging_root,
    )

    bucket = check_minio_bucket(client)
    object_key = recovered["object_key"]
    checksum = recovered["checksum_sha256"]
    csv_bytes = recovered["csv_bytes"]

    if verify_minio_object(
        client,
        bucket,
        object_key,
        checksum,
    ):
        return {
            "status": "ALREADY_EXISTS",
            "object_key": object_key,
            "checksum_sha256": checksum,
        }

    try:
        put_object_if_absent(
            client,
            bucket,
            object_key,
            csv_bytes,
        )

    except S3Error as exc:
        if exc.code in (
            "PreconditionFailed",
            "ConditionalRequestConflict",
        ):
            if verify_minio_object(
                client,
                bucket,
                object_key,
                checksum,
            ):
                return {
                    "status": "ALREADY_EXISTS",
                    "object_key": object_key,
                    "checksum_sha256": checksum,
                }

        raise

    if not verify_minio_object(
        client,
        bucket,
        object_key,
        checksum,
    ):
        raise RuntimeError("Uploaded object could not be verified")

    return {
        "status": "UPLOADED",
        "object_key": object_key,
        "checksum_sha256": checksum,
    }


def put_object_if_absent(
    client,
    bucket_name,
    object_key,
    content,
):
    """
    Create a MinIO object only if it does not exist.

    Uses MinIO SDK 7.2.20 internal API.
    Never falls back to unconditional upload.
    """

    if not isinstance(content, bytes) or not content:
        raise ValueError("Content must be non-empty bytes")

    return client._put_object(
        bucket_name,
        object_key,
        content,
        headers={
            "Content-Type": "text/csv",
            "If-None-Match": "*",
        },
    )


def mark_batch_uploaded(conn, client, batch_id):
    """
    Mark a PENDING batch as UPLOADED only after
    verifying the corresponding MinIO object.

    Does not commit the PostgreSQL transaction.
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    metadata = lock_batch_metadata(conn, batch_id)
    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")

    validate_recovery_metadata(metadata)

    if metadata["status"] not in ("PENDING", "UPLOADED"):
        raise ValueError(f"Invalid batch status: {metadata['status']}")

    bucket = check_minio_bucket(client)

    verified = verify_minio_object(
        client,
        bucket,
        metadata["object_key"],
        metadata["checksum_sha256"],
    )

    if not verified:
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
        "batch_id": batch_id,
        "status": "UPLOADED",
        "object_key": metadata["object_key"],
        "checksum_sha256": metadata["checksum_sha256"],
    }


def lock_batch_metadata(conn, batch_id):
    """
    Read batch metadata while locking its row.

    Lock remains active until COMMIT or ROLLBACK.
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    # Reuse the existing batch-id validation.
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

    # Re-read after acquiring the lock to obtain
    # the latest committed metadata.
    return get_batch_metadata(conn, batch_id)


def watermark_matches_batch_start(watermark, metadata):
    """
    Verify that the current checkpoint matches
    the starting checkpoint of a batch.

    Read-only validation. No database writes.
    """

    if watermark["watermark_type"] != metadata["watermark_type"]:
        return False

    if watermark["last_id"] != metadata["start_id"]:
        return False

    if metadata["watermark_type"] == "TIMESTAMP":
        return watermark["last_timestamp"] == metadata["start_timestamp"]

    if metadata["watermark_type"] == "ID":
        return (
            watermark["last_timestamp"] is None and metadata["start_timestamp"] is None
        )

    return False


def get_locked_watermark(conn, table_name):
    """
    Acquire a transaction-level advisory lock and
    read the current ingestion watermark.

    The caller must COMMIT or ROLLBACK.
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table_name}")

    acquired = acquire_table_lock(conn, table_name)

    if not acquired:
        raise RuntimeError(f"Table ingestion lock is busy: {table_name}")

    watermark = get_watermark(conn, table_name)

    if watermark["watermark_type"] != (TABLE_CONFIG[table_name]["watermark_type"]):
        raise ValueError(f"Watermark type mismatch: {table_name}")

    return watermark


def commit_uploaded_batch(conn, client, batch_id):
    """
    Atomically advance the watermark and mark a verified
    UPLOADED batch as COMMITTED.

    Does not commit or rollback the transaction.
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    # Read metadata to identify the table.
    metadata = get_batch_metadata(conn, batch_id)

    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")

    validate_recovery_metadata(metadata)

    table_name = metadata["table_name"]

    # Serialize ingestion operations for this table.
    watermark = get_locked_watermark(conn, table_name)

    # Lock and re-read the batch after acquiring the table lock.
    metadata = lock_batch_metadata(conn, batch_id)

    if metadata is None:
        raise RuntimeError("Batch disappeared")

    validate_recovery_metadata(metadata)

    if metadata["status"] != "UPLOADED":
        raise ValueError(f"Batch must be UPLOADED, got {metadata['status']}")

    if not watermark_matches_batch_start(watermark, metadata):
        raise RuntimeError("Watermark does not match batch start")

    # Verify that the object still exists with the
    # expected content before advancing the checkpoint.
    bucket = check_minio_bucket(client)

    if not verify_minio_object(
        client,
        bucket,
        metadata["object_key"],
        metadata["checksum_sha256"],
    ):
        raise RuntimeError("MinIO object does not exist")

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO public.ingestion_watermarks (
                table_name,
                watermark_type,
                last_timestamp,
                last_id,
                last_batch_id,
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
            (
                table_name,
                metadata["watermark_type"],
                metadata["end_timestamp"],
                metadata["end_id"],
                batch_id,
            ),
        )

        cursor.execute(
            """
            UPDATE public.ingestion_batches
            SET status = 'COMMITTED',
                updated_at = CURRENT_TIMESTAMP
            WHERE batch_id = %s
              AND status = 'UPLOADED'
            RETURNING batch_id
            """,
            (batch_id,),
        )

        if cursor.fetchone() is None:
            raise RuntimeError("Batch status changed before COMMIT")

    return {
        "batch_id": batch_id,
        "table_name": table_name,
        "status": "COMMITTED",
        "last_timestamp": metadata["end_timestamp"],
        "last_id": metadata["end_id"],
    }


def select_ingestion_action(
    table_name,
    recoverable_batches,
    has_blocking,
):
    """
    Decide the next ingestion action without writing data.
    """

    get_table_config(table_name)

    if len(recoverable_batches) > 1:
        raise RuntimeError(f"Multiple unfinished batches: {table_name}")

    if recoverable_batches:
        batch = recoverable_batches[0]
        status = batch["status"]

        if status == "PENDING":
            return "RECOVER_PENDING"

        if status == "UPLOADED":
            return "RECOVER_UPLOADED"

        raise RuntimeError(f"Unexpected recoverable status: {status}")

    if has_blocking:
        raise RuntimeError(f"Blocking batch requires investigation: {table_name}")

    return "NEW_BATCH"


def run_incremental_ingestion(
    table_name,
    batch_size=1000,
    staging_root="staging",
    dry_run=True,
    rollback_test=False,
):
    """
    Recovery-first orchestrator.

    dry_run=True:
        Inspect metadata only.

    rollback_test=True:
        Execute RECOVER_UPLOADED, then always ROLLBACK.

    Production write-mode is not enabled yet.
    """

    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table_name}")

    validate_positive_integer(batch_size, "batch_size")

    if not isinstance(dry_run, bool):
        raise ValueError("dry_run must be boolean")

    if not isinstance(rollback_test, bool):
        raise ValueError("rollback_test must be boolean")

    if dry_run and rollback_test:
        raise ValueError("dry_run and rollback_test cannot both be True")

    conn = get_db_connection()

    try:
        if dry_run:
            recoverable = get_recoverable_batches(conn, table_name)

            blocking = has_blocking_batch(conn, table_name)

            if has_failed_batch(conn, table_name):
                raise RuntimeError(f"FAILED batch requires investigation: {table_name}")

            action = select_ingestion_action(
                table_name,
                recoverable,
                blocking,
            )

            return {
                "table_name": table_name,
                "mode": "DRY_RUN",
                "action": action,
                "watermark": get_watermark(conn, table_name),
                "recoverable_batches": [item["batch_id"] for item in recoverable],
            }

        if rollback_test:
            # Lock first, then re-read batch state.
            client = get_minio_client()

            result = execute_ingestion_action_locked(
                conn=conn,
                client=client,
                table_name=table_name,
                batch_size=batch_size,
                staging_root=staging_root,
            )

            return {
                **result,
                "mode": "ROLLBACK_TEST",
                "persisted": False,
            }

        raise NotImplementedError("Production write-mode is not enabled yet")

    finally:
        conn.rollback()
        conn.close()


def recover_uploaded_batch(conn, client, batch_id):
    """
    Recover an UPLOADED batch by committing its
    watermark and COMMITTED status atomically.

    The caller owns the PostgreSQL transaction.
    This function does not commit or rollback.
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    metadata = get_batch_metadata(conn, batch_id)

    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")

    validate_recovery_metadata(metadata)

    if metadata["status"] != "UPLOADED":
        raise ValueError("Expected UPLOADED batch, " f"got {metadata['status']}")

    result = commit_uploaded_batch(
        conn,
        client,
        batch_id,
    )

    return {
        "batch_id": result["batch_id"],
        "table_name": result["table_name"],
        "status": result["status"],
        "last_timestamp": result["last_timestamp"],
        "last_id": result["last_id"],
        "recovered": True,
    }


def execute_uploaded_recovery(conn, client, batch_id):
    """Commit an UPLOADED batch inside the caller-owned transaction."""
    require_transaction(conn)
    metadata = get_batch_metadata(conn, batch_id)

    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")
    if metadata["status"] != "UPLOADED":
        raise ValueError(f"Expected UPLOADED, got {metadata['status']}")

    return {
        "action": "RECOVER_UPLOADED",
        **recover_uploaded_batch(conn, client, batch_id),
    }


def execute_new_batch(
    conn,
    table_name,
    batch_size=1000,
    staging_root="staging",
):
    """
    Prepare a new incremental batch.

    Requires an active PostgreSQL transaction.

    Creates:
      - staged CSV file
      - PENDING batch metadata (uncommitted)

    Does NOT:
      - upload to MinIO
      - advance watermark
      - commit PostgreSQL
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table_name}")

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    # prepare_pending_batch() acquires the table advisory
    # transaction lock and checks for blocking batches.
    result = prepare_pending_batch(
        conn=conn,
        table_name=table_name,
        batch_size=batch_size,
        staging_root=staging_root,
    )

    if result is None:
        return {
            "action": "NEW_BATCH",
            "table_name": table_name,
            "status": "NO_DATA",
            "row_count": 0,
            "batch_id": None,
        }

    metadata = result["metadata"]

    if metadata["table_name"] != table_name:
        raise RuntimeError("Prepared batch table mismatch")

    if metadata["status"] != "PENDING":
        raise RuntimeError("Expected PENDING batch")

    return {
        "action": "NEW_BATCH",
        "table_name": table_name,
        "status": "PENDING",
        "batch_id": metadata["batch_id"],
        "row_count": metadata["row_count"],
        "object_key": metadata["object_key"],
        "staged_path": result["staged_path"],
    }


def execute_ingestion_action(
    conn,
    client,
    table_name,
    action,
    recoverable_batches,
    staging_root="staging",
):
    """Execute one selected recovery action in a caller-owned transaction."""
    require_transaction(conn)
    get_table_config(table_name)

    if action not in ("RECOVER_PENDING", "RECOVER_UPLOADED"):
        raise ValueError(f"Unsupported action: {action}")
    if len(recoverable_batches) != 1:
        raise RuntimeError("Expected exactly one recoverable batch")

    batch = recoverable_batches[0]
    if batch["table_name"] != table_name:
        raise RuntimeError("Recovery batch table mismatch")

    if action == "RECOVER_PENDING":
        if batch["status"] != "PENDING":
            raise ValueError("Expected PENDING batch")
        return execute_pending_recovery(
            conn, client, batch["batch_id"], staging_root=staging_root
        )

    if batch["status"] != "UPLOADED":
        raise ValueError("Expected UPLOADED batch")
    return execute_uploaded_recovery(conn, client, batch["batch_id"])


def execute_ingestion_action_locked(
    conn,
    client,
    table_name,
    batch_size=1000,
    staging_root="staging",
):
    """
    Re-select and execute ingestion action under
    a PostgreSQL table advisory transaction lock.

    Supports recovery of unfinished batches and creation of a new PENDING batch.

    Caller owns commit/rollback.
    """

    require_transaction(conn)
    get_table_config(table_name)

    # 1. Acquire table-level advisory transaction lock
    if not acquire_table_lock(conn, table_name):
        raise RuntimeError(f"Table ingestion lock is busy: {table_name}")

    # 2. Re-read batch state after acquiring lock
    recoverable = get_recoverable_batches(
        conn,
        table_name,
    )

    blocking = has_blocking_batch(
        conn,
        table_name,
    )

    # 3. Block ingestion if FAILED batches exist.
    if has_failed_batch(conn, table_name):
        raise RuntimeError(f"FAILED batch requires investigation: {table_name}")

    # 4. Select action while holding advisory lock
    action = select_ingestion_action(
        table_name,
        recoverable,
        blocking,
    )

    # 5. Execute while retaining the advisory lock.
    if action == "NEW_BATCH":
        return execute_new_batch(
            conn=conn,
            table_name=table_name,
            batch_size=batch_size,
            staging_root=staging_root,
        )

    return execute_ingestion_action(
        conn=conn,
        client=client,
        table_name=table_name,
        action=action,
        recoverable_batches=recoverable,
        staging_root=staging_root,
    )


def execute_pending_recovery(
    conn,
    client,
    batch_id,
    staging_root="staging",
):
    """
    Recover a PENDING batch.

    MinIO upload is durable and cannot be rolled back
    with the PostgreSQL transaction.

    Caller owns PostgreSQL commit/rollback.
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    metadata = get_batch_metadata(conn, batch_id)

    if metadata is None:
        raise ValueError(f"Batch not found: {batch_id}")

    validate_recovery_metadata(metadata)

    if metadata["status"] != "PENDING":
        raise ValueError(f"Expected PENDING, got {metadata['status']}")

    recover_staged_batch(metadata, staging_root=staging_root)

    upload_result = upload_recovered_batch(
        client,
        metadata,
        staging_root=staging_root,
    )

    # Verify object and transition PENDING -> UPLOADED.
    mark_batch_uploaded(
        conn,
        client,
        batch_id,
    )

    # Reuse the already-tested UPLOADED recovery path.
    result = recover_uploaded_batch(
        conn,
        client,
        batch_id,
    )

    return {
        "action": "RECOVER_PENDING",
        "batch_id": result["batch_id"],
        "table_name": result["table_name"],
        "status": result["status"],
        "recovered": True,
        "upload_result": upload_result,
        "last_timestamp": result["last_timestamp"],
        "last_id": result["last_id"],
    }


def persist_new_pending_batch(
    table_name,
    batch_size=1000,
    staging_root="staging",
    allow_commit=False,
):
    """
    Prepare a new PENDING batch and optionally commit it.

    Default:
        allow_commit=False -> always rollback PostgreSQL.

    Explicit commit:
        allow_commit=True -> commit PENDING metadata only.

    Does not upload to MinIO or advance watermark.
    """

    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table_name}")

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    if not isinstance(allow_commit, bool):
        raise ValueError("allow_commit must be boolean")

    conn = get_db_connection()

    try:
        # Independent guard before any writes.
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            actual_database = cursor.fetchone()[0]

        if actual_database != "ecommerce_test":
            raise RuntimeError("Refusing to write outside ecommerce_test")

        result = execute_new_batch(
            conn=conn,
            table_name=table_name,
            batch_size=batch_size,
            staging_root=staging_root,
        )

        if result["status"] == "NO_DATA":
            conn.rollback()

            return {
                **result,
                "persisted": False,
            }

        if result["status"] != "PENDING":
            raise RuntimeError(f"Unexpected batch status: {result['status']}")

        # Re-check that the staged file is present.
        staged_path = Path(result["staged_path"])

        if not staged_path.is_file():
            raise RuntimeError("Staged CSV is missing")

        if allow_commit:
            conn.commit()
        else:
            conn.rollback()

        return {
            **result,
            "persisted": allow_commit,
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def _execute_two_transaction_ingestion(
    table_name,
    batch_size=1000,
    staging_root="staging",
):
    """
    Internal two-transaction ingestion orchestrator.

    Transaction 1:
        - Acquire lock and re-read state.
        - Recover an existing batch, OR
        - Prepare and commit a new PENDING batch.

    Transaction 2:
        - Recover the newly committed PENDING batch.
        - Commit the checkpoint.

    This function is not connected to the public
    write entrypoint yet.

    IMPORTANT:
        Must only be invoked through a guarded caller.
    """

    # ----------------------------------------------------
    # Transaction 1
    # ----------------------------------------------------
    conn = get_db_connection()

    try:
        if conn.autocommit:
            raise RuntimeError("Autocommit must be disabled")

        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database = cur.fetchone()[0]

        if database != "ecommerce_test":
            raise RuntimeError(f"Write blocked for database: {database}")

        client = get_minio_client()

        result = execute_ingestion_action_locked(
            conn=conn,
            client=client,
            table_name=table_name,
            batch_size=batch_size,
            staging_root=staging_root,
        )

        status = result["status"]

        if status == "NO_DATA":
            conn.rollback()

            return {
                **result,
                "persisted": False,
            }

        if status == "COMMITTED":
            if result.get("action") not in (
                "RECOVER_PENDING",
                "RECOVER_UPLOADED",
            ):
                raise RuntimeError("Unexpected Transaction 1 committed action")

            if result.get("table_name") != table_name:
                raise RuntimeError("Transaction 1 recovery table mismatch")

            if not result.get("batch_id"):
                raise RuntimeError("Transaction 1 recovery batch_id missing")

            conn.commit()

            return {
                **result,
                "persisted": True,
            }

        if status != "PENDING":
            raise RuntimeError(f"Unexpected transaction 1 status: {status}")

        if result.get("action") != "NEW_BATCH":
            raise RuntimeError("Transaction 1 PENDING action mismatch")

        if result.get("table_name") != table_name:
            raise RuntimeError("Transaction 1 PENDING table mismatch")

        batch_id = result.get("batch_id")

        if (
            not isinstance(batch_id, str)
            or len(batch_id) != 32
            or any(c not in "0123456789abcdef" for c in batch_id)
        ):
            raise RuntimeError("Transaction 1 PENDING batch_id invalid")

        staged_path = result.get("staged_path")

        if not isinstance(staged_path, (str, Path)):
            raise RuntimeError("Transaction 1 staged_path missing")

        if not Path(staged_path).is_file():
            raise RuntimeError("Transaction 1 staged CSV missing")

        # Persist PENDING metadata before recovery.
        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    # ----------------------------------------------------
    # Transaction 2
    # ----------------------------------------------------
    expected_batch_id = result["batch_id"]

    if not expected_batch_id:
        raise RuntimeError("Transaction 1 did not return a batch_id")

    conn = get_db_connection()

    try:
        if conn.autocommit:
            raise RuntimeError("Autocommit must be disabled")

        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database = cur.fetchone()[0]

        if database != "ecommerce_test":
            raise RuntimeError(f"Write blocked for database: {database}")

        # Acquire a new transaction-scoped advisory lock.
        if not acquire_table_lock(conn, table_name):
            raise RuntimeError("Table ingestion lock is busy")

        # Read exactly the batch created by Transaction 1.
        metadata = get_batch_metadata(
            conn,
            expected_batch_id,
        )

        if metadata is None:
            raise RuntimeError("Expected batch disappeared after commit")

        if metadata["table_name"] != table_name:
            raise RuntimeError("Transaction 2 batch table mismatch")

        if metadata["batch_id"] != expected_batch_id:
            raise RuntimeError("Transaction 2 batch ID mismatch")

        if metadata["status"] == "COMMITTED":
            # Another worker may have completed this batch.
            # Verify its checkpoint before reporting success.
            watermark = get_watermark(conn, table_name)

            if watermark["last_batch_id"] != expected_batch_id:
                raise RuntimeError("ALREADY_COMMITTED checkpoint mismatch")

            conn.rollback()

            return {
                "action": "ALREADY_COMMITTED",
                "table_name": table_name,
                "batch_id": expected_batch_id,
                "status": "COMMITTED",
                "persisted": True,
                "recovered": False,
            }

        if metadata["status"] not in RECOVERABLE_STATUSES:
            raise RuntimeError(
                "Unexpected Transaction 2 batch status: " f"{metadata['status']}"
            )

        # Ensure the expected batch is the only recoverable
        # batch and that no FAILED batch is present.
        recoverable = get_recoverable_batches(
            conn,
            table_name,
        )

        if (
            len(recoverable) != 1
            or recoverable[0]["batch_id"] != expected_batch_id
            or recoverable[0]["status"] != metadata["status"]
        ):
            raise RuntimeError("Transaction 2 recovery state mismatch")

        if has_failed_batch(conn, table_name):
            raise RuntimeError("FAILED batch requires investigation")

        action = (
            "RECOVER_PENDING" if metadata["status"] == "PENDING" else "RECOVER_UPLOADED"
        )

        recovery_result = execute_ingestion_action(
            conn=conn,
            client=client,
            table_name=table_name,
            action=action,
            recoverable_batches=recoverable,
            staging_root=staging_root,
        )

        if (
            recovery_result["batch_id"] != expected_batch_id
            or recovery_result["table_name"] != table_name
            or recovery_result["status"] != "COMMITTED"
        ):
            raise RuntimeError("Transaction 2 recovery result mismatch")

        conn.commit()

        return {
            **recovery_result,
            "persisted": True,
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def run_incremental_ingestion_write(
    table_name,
    batch_size=1000,
    staging_root="staging",
    allow_write=False,
):
    """
    Controlled incremental ingestion write entrypoint.

    Current phase:
        - Validate arguments.
        - Require explicit write permission.
        - Validate PostgreSQL database and transaction mode.
        - Block actual write execution.

    No batch creation, MinIO upload, or checkpoint commit.
    """

    # 1. Validate input arguments.
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table_name}")

    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or batch_size <= 0
    ):
        raise ValueError("batch_size must be a positive integer")

    if not isinstance(staging_root, (str, Path)):
        raise ValueError("staging_root must be a path")

    if not str(staging_root).strip():
        raise ValueError("staging_root cannot be empty")

    if not isinstance(allow_write, bool):
        raise ValueError("allow_write must be boolean")

    # 2. Default: do not connect or write.
    if not allow_write:
        return {
            "table_name": table_name,
            "mode": "WRITE_DISABLED",
            "persisted": False,
            "reason": "Explicit allow_write=True is required",
        }

    # 3. Validate PostgreSQL before any future write.
    conn = get_db_connection()

    try:
        if conn.autocommit:
            raise RuntimeError("Write-mode requires autocommit=False")

        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database = cur.fetchone()[0]

        if database != "ecommerce_test":
            raise RuntimeError(f"Write-mode blocked for database: {database}")

        # 4. Write execution is intentionally disabled.
        raise NotImplementedError("Production write-mode is not enabled yet")

    finally:
        conn.rollback()
        conn.close()
