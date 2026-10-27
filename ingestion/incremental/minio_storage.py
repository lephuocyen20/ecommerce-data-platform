"""MinIO client operations, uploads, and object verification."""

import hashlib
import os
import uuid

from minio import Minio
from minio.error import S3Error

try:
    from .config import get_table_config
except ImportError:
    from config import get_table_config
try:
    from common.paths import bronze_incremental_object_key
except ModuleNotFoundError:
    # Support invoking this legacy module directly from ingestion/incremental.
    def bronze_incremental_object_key(table_name, batch_id):
        return f"bronze_incremental/{table_name}/batch_id={batch_id}/data.csv"


def build_object_key(table_name, batch_id):
    get_table_config(table_name)

    try:
        validated_batch_id = uuid.UUID(hex=batch_id)
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Invalid batch_id") from None

    if validated_batch_id.hex != batch_id:
        raise ValueError("batch_id must be a canonical UUID hex string")

    return bronze_incremental_object_key(table_name, batch_id)


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


def upload_incremental_batch(client, table_name, batch_id, csv_content, dry_run=True):
    bucket_name = check_minio_bucket(client)
    object_key = build_object_key(table_name, batch_id)
    if not isinstance(csv_content, str) or not csv_content:
        raise ValueError("csv_content must be a non-empty string")
    if object_exists(client, bucket_name, object_key):
        raise RuntimeError(f"Object already exists, refusing overwrite: {object_key}")
    data = csv_content.encode("utf-8")
    if dry_run:
        return {
            "status": "DRY_RUN", "bucket": bucket_name,
            "object_key": object_key, "size_bytes": len(data),
        }
    raise NotImplementedError("Actual MinIO upload is not enabled yet")


def verify_minio_object(client, bucket_name, object_key, expected_checksum):
    """Verify an existing object's SHA-256 without modifying MinIO."""
    from .config import VALID_CHECKSUM

    if not isinstance(expected_checksum, str) or not VALID_CHECKSUM.fullmatch(
        expected_checksum
    ):
        raise ValueError("Invalid expected checksum")
    if not object_exists(client, bucket_name, object_key):
        return False
    response = None
    try:
        response = client.get_object(bucket_name, object_key)
        hasher = hashlib.sha256()
        for chunk in response.stream(amt=1024 * 1024):
            hasher.update(chunk)
        if hasher.hexdigest() != expected_checksum:
            raise ValueError("MinIO object checksum mismatch")
        return True
    finally:
        if response is not None:
            response.close()
            response.release_conn()


def put_object_if_absent(client, bucket_name, object_key, content):
    """Create an object with an If-None-Match precondition."""
    if not isinstance(content, bytes) or not content:
        raise ValueError("Content must be non-empty bytes")
    return client._put_object(
        bucket_name,
        object_key,
        content,
        headers={"Content-Type": "text/csv", "If-None-Match": "*"},
    )


__all__ = [
    "build_object_key",
    "check_minio_bucket",
    "get_minio_client",
    "object_exists",
    "put_object_if_absent",
    "upload_incremental_batch",
    "verify_minio_object",
]
