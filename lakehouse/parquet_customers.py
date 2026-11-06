"""Verified Silver Parquet to Iceberg customers migration primitives."""

import hashlib
import json
from datetime import datetime, timezone

from minio import Minio
from minio.error import S3Error
from pyspark.sql import functions as F

from lakehouse.customers import latest_snapshot_id, merge_customers, prepare_customers, validate_customers
from common.object_integrity import discover_parquet_objects


TABLE = "lakehouse.silver.customers"
PROGRESS_TABLE = "lakehouse.metadata.customers_parquet_migration_progress"
MANIFEST_PREFIX = "silver_incremental/customers/"


def manifest_identity(manifest):
    """Return a stable digest used to make a manifest replay idempotent."""
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_manifest(manifest_key, manifest):
    if not isinstance(manifest, dict) or manifest.get("status") != "COMPLETED":
        raise ValueError("Invalid Silver manifest status")
    if manifest.get("table_name") != "customers":
        raise ValueError("Silver manifest table must be customers")
    batch_id, prefix, row_count, objects = (manifest.get("batch_id"), manifest.get("data_prefix"), manifest.get("silver_row_count"), manifest.get("parquet_objects"))
    if not isinstance(batch_id, str) or len(batch_id) != 32:
        raise ValueError("Invalid Silver manifest batch ID")
    expected = f"silver_incremental/customers/batch_id={batch_id}/_BATCH_METADATA.json"
    if manifest_key != expected or not isinstance(prefix, str) or not prefix.startswith(f"silver_incremental/customers/batch_id={batch_id}/runs/"):
        raise ValueError("Silver manifest identity mismatch")
    if type(row_count) is not int or row_count < 0 or not isinstance(objects, list) or not objects:
        raise ValueError("Silver manifest output metadata is invalid")
    keys = [item.get("object_key") for item in objects if isinstance(item, dict)]
    if (len(keys) != len(objects) or len(set(keys)) != len(keys)
            or any(not isinstance(key, str) or not key.startswith(prefix) or not key.endswith(".parquet")
                   or type(item.get("size")) is not int or item["size"] <= 0
                   or not isinstance(item.get("etag"), str) or not item["etag"]
                   for item, key in zip(objects, keys))):
        raise ValueError("Silver manifest parquet objects are invalid")
    return batch_id, prefix


def verify_manifest_objects(client, bucket, manifest):
    """Require the live Parquet inventory to match the verified manifest."""
    actual = discover_parquet_objects(client, bucket, manifest["data_prefix"], include_etag=True)
    expected = sorted(manifest["parquet_objects"], key=lambda item: item["object_key"])
    if actual != expected:
        raise ValueError("Silver Parquet object inventory no longer matches manifest")


def read_manifest(client, bucket, manifest_key):
    response = client.get_object(bucket, manifest_key)
    try:
        manifest = json.loads(response.read().decode("utf-8"))
    finally:
        response.close(); response.release_conn()
    validate_manifest(manifest_key, manifest)
    return manifest


def discover_manifests(client, bucket):
    """Discover completed customers manifests in stable completion/batch order."""
    found = []
    for item in client.list_objects(bucket, prefix=MANIFEST_PREFIX, recursive=True):
        if not item.object_name.endswith("/_BATCH_METADATA.json"):
            continue
        try:
            manifest = read_manifest(client, bucket, item.object_name)
        except S3Error:
            raise
        found.append((item.object_name, manifest))
    return sorted(found, key=lambda pair: (pair[1].get("completed_at", ""), pair[1]["batch_id"]))


def ensure_progress_table(spark, progress_table=PROGRESS_TABLE):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.metadata")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {progress_table} (
        batch_id STRING, manifest_key STRING, manifest_identity STRING,
        snapshot_id BIGINT, completed_at TIMESTAMP
    ) USING iceberg""")


def migrate_manifest(spark, client, bucket, manifest_key, manifest=None, *, table=TABLE, progress_table=PROGRESS_TABLE):
    """Merge one verified manifest and record immutable progress in Iceberg."""
    manifest = read_manifest(client, bucket, manifest_key) if manifest is None else manifest
    batch_id, prefix = validate_manifest(manifest_key, manifest)
    ensure_progress_table(spark, progress_table)
    verify_manifest_objects(client, bucket, manifest)
    digest = manifest_identity(manifest)
    existing = spark.sql(f"SELECT manifest_identity, snapshot_id FROM {progress_table} WHERE batch_id = '{batch_id}'").collect()
    if existing:
        if existing[0].manifest_identity != digest:
            raise RuntimeError(f"Conflicting Silver manifest for already migrated batch: {batch_id}")
        return {"status": "ALREADY_MIGRATED", "batch_id": batch_id, "snapshot_id": int(existing[0].snapshot_id)}
    source = spark.read.parquet(f"s3a://{bucket}/{prefix}")
    if source.count() != manifest["silver_row_count"]:
        raise ValueError("Silver Parquet row count does not match manifest")
    merge_customers(spark, prepare_customers(source, batch_id), table)
    quality = validate_customers(spark, table)
    snapshot_id = latest_snapshot_id(spark, table)
    spark.sql(f"""INSERT INTO {progress_table} VALUES (
        '{batch_id}', '{manifest_key}', '{digest}', {snapshot_id}, current_timestamp()
    )""")
    return {"status": "MIGRATED", "batch_id": batch_id, "snapshot_id": snapshot_id, **quality}


def migrate_all_manifests(spark, client, bucket):
    return [migrate_manifest(spark, client, bucket, key, manifest) for key, manifest in discover_manifests(client, bucket)]
