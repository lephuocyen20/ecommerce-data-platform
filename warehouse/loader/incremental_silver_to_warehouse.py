"""Incremental Silver to PostgreSQL Raw Warehouse loader."""
import json
import re

from minio import Minio
from minio.error import S3Error
import os

import psycopg2
from psycopg2.extras import RealDictCursor


SUPPORTED_TABLES = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
)


def get_connection():
    """Create a PostgreSQL connection using environment variables."""
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_DB_HOST", "localhost"),
        port=int(os.environ.get("WAREHOUSE_DB_PORT", "5432")),
        dbname=os.environ.get("WAREHOUSE_DB_NAME", "ecommerce_test"),
        user=os.environ.get("WAREHOUSE_DB_USER", "ecommerce_user"),
        password=os.environ["WAREHOUSE_DB_PASSWORD"],
    )


def get_committed_batches(conn, selected_tables=None, selected_batch_ids=None,):
    """
    Read committed ingestion batches not yet loaded into Warehouse.

    selected_tables=None:
        Process all supported tables (backward compatible).

    selected_tables=[...]:
        Process only the explicitly selected tables.
    """

    if selected_tables is None:
        tables = list(SUPPORTED_TABLES)
    else:
        if (
            not isinstance(selected_tables, (list, tuple))
            or not selected_tables
            or any(not isinstance(t, str) for t in selected_tables)
        ):
            raise ValueError(
                "selected_tables must be a non-empty list or tuple of strings"
            )

        if len(selected_tables) != len(set(selected_tables)):
            raise ValueError("Duplicate tables are not allowed")

        unsupported = set(selected_tables) - set(SUPPORTED_TABLES)

        if unsupported:
            raise ValueError(
                f"Unsupported tables: {sorted(unsupported)}"
            )

        tables = list(selected_tables)


    if selected_batch_ids is not None:
        if (
            not isinstance(selected_batch_ids, (list, tuple))
            or not selected_batch_ids
            or any(
                not isinstance(batch_id, str)
                for batch_id in selected_batch_ids
            )
        ):
            raise ValueError(
                "selected_batch_ids must be a non-empty list or tuple of strings"
            )

        if len(selected_batch_ids) != len(set(selected_batch_ids)):
            raise ValueError("Duplicate batch IDs are not allowed")

        for batch_id in selected_batch_ids:
            if not re.fullmatch(r"[0-9a-f]{32}", batch_id):
                raise ValueError(
                    f"Invalid batch ID format: {batch_id}"
                )

    query = """
        SELECT
            b.batch_id,
            b.table_name,
            b.watermark_type,
            b.start_timestamp,
            b.start_id,
            b.end_timestamp,
            b.end_id,
            b.row_count,
            b.object_key,
            b.checksum_sha256
        FROM public.ingestion_batches AS b
        WHERE b.status = 'COMMITTED'
            AND b.table_name = ANY(%s)
            AND (%s::text[] IS NULL OR b.batch_id = ANY(%s::text[]))
            AND NOT EXISTS (
              SELECT 1
              FROM warehouse.loaded_batches AS l
              WHERE l.batch_id = b.batch_id
          )
        ORDER BY
            b.table_name,
            CASE
                WHEN b.watermark_type = 'TIMESTAMP'
                THEN b.end_timestamp
            END NULLS FIRST,
            b.end_id,
            b.batch_id
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            query,
            (
                tables,
                selected_batch_ids,
                selected_batch_ids,
            ),
        )
        return [dict(row) for row in cursor.fetchall()]

def get_minio_client():
    return Minio(
        os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


def check_silver_manifest(client, bucket, batch):
    table_name = batch["table_name"]
    batch_id = batch["batch_id"]

    manifest_key = (
        f"silver_incremental/{table_name}/"
        f"batch_id={batch_id}/_BATCH_METADATA.json"
    )

    try:
        response = client.get_object(bucket, manifest_key)
    except S3Error as exc:
        if exc.code in ("NoSuchKey", "NoSuchObject"):
            return "MISSING", manifest_key
        raise

    try:
        manifest = json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()

    expected_prefix = (
        f"silver_incremental/{table_name}/"
        f"batch_id={batch_id}/runs/"
    )

    data_prefix = manifest.get("data_prefix")

    if (
        manifest.get("status") != "COMPLETED"
        or manifest.get("table_name") != table_name
        or manifest.get("batch_id") != batch_id
        or manifest.get("bronze_object_key") != batch["object_key"]
        or manifest.get("bronze_checksum_sha256") != batch["checksum_sha256"]
        or not isinstance(data_prefix, str)
        or not data_prefix.startswith(expected_prefix)
        or not re.fullmatch(
            r"[0-9a-f]{32}/",
            data_prefix[len(expected_prefix):],
        )
    ):
        return "INVALID", manifest_key

    return "VALID_METADATA", manifest_key

def verify_parquet_objects(client, bucket, manifest):
    """Verify published Parquet objects against Silver manifest."""

    objects = manifest.get("parquet_objects")
    expected_count = manifest.get("parquet_file_count")
    data_prefix = manifest.get("data_prefix")
    expected_rows = manifest.get("silver_row_count")

    if (
        not isinstance(objects, list)
        or not isinstance(expected_count, int)
        or isinstance(expected_count, bool)
        or expected_count < 1
        or len(objects) != expected_count
        or not isinstance(data_prefix, str)
        or not isinstance(expected_rows, int)
        or isinstance(expected_rows, bool)
        or expected_rows < 0
    ):
        return "INVALID_MANIFEST"

    seen_keys = set()

    for obj in objects:
        if not isinstance(obj, dict):
            return "INVALID_MANIFEST"

        key = obj.get("object_key")
        expected_size = obj.get("size")
        expected_etag = obj.get("etag")

        if (
            not isinstance(key, str)
            or not key.startswith(data_prefix)
            or not key.endswith(".parquet")
            or key in seen_keys
            or not isinstance(expected_size, int)
            or isinstance(expected_size, bool)
            or expected_size <= 0
            or not isinstance(expected_etag, str)
            or not expected_etag
        ):
            return "INVALID_MANIFEST"

        seen_keys.add(key)

        try:
            stat = client.stat_object(bucket, key)
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject"):
                return "MISSING_PARQUET"
            raise

        if (
            stat.size != expected_size
            or stat.etag.strip('"') != expected_etag.strip('"')
        ):
            return "PARQUET_MISMATCH"

    return "VERIFIED"

def read_silver_manifest(client, bucket, manifest_key):
    response = client.get_object(bucket, manifest_key)

    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()



def main():
    bucket = os.environ.get(
        "MINIO_BUCKET",
        "ecommerce-data-lake",
    )

    client = get_minio_client()

    with get_connection() as conn:
        batches = get_committed_batches(conn)

        print(f"PENDING WAREHOUSE BATCHES: {len(batches)}")

        counts = {
            "VERIFIED": 0,
            "INVALID_MANIFEST": 0,
            "MISSING_PARQUET": 0,
            "PARQUET_MISMATCH": 0,
            "MANIFEST_NOT_VALID": 0,
        }

        for batch in batches:
            manifest_status, manifest_key = check_silver_manifest(
                client,
                bucket,
                batch,
            )

            if manifest_status != "VALID_METADATA":
                status = "MANIFEST_NOT_VALID"
            else:
                manifest = read_silver_manifest(
                    client,
                    bucket,
                    manifest_key,
                )

                status = verify_parquet_objects(
                    client,
                    bucket,
                    manifest,
                )

            counts[status] += 1

            print(
                f"table={batch['table_name']} "
                f"batch_id={batch['batch_id']} "
                f"parquet_status={status}"
            )

        print(
            "SILVER PARQUET CHECK: "
            + " ".join(
                f"{key}={value}"
                for key, value in counts.items()
            )
        )


if __name__ == "__main__":
    main()