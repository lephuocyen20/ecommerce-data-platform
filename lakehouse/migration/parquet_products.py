"""Verified Silver Parquet to Iceberg products migration primitives."""

import hashlib
import json
from common.object_integrity import discover_parquet_objects

from lakehouse.models.products import merge_products, prepare_products


TABLE = "lakehouse.silver.products"
PROGRESS_TABLE = "lakehouse.metadata.products_parquet_migration_progress"
PREFIX = "silver_incremental/products/"


def manifest_identity(manifest):
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_manifest(key, manifest):
    if not isinstance(manifest, dict) or manifest.get("status") != "COMPLETED" or manifest.get("table_name") != "products":
        raise ValueError("Invalid products Silver manifest")
    batch_id, prefix, rows, objects = manifest.get("batch_id"), manifest.get("data_prefix"), manifest.get("silver_row_count"), manifest.get("parquet_objects")
    if not isinstance(batch_id, str) or len(batch_id) != 32 or key != f"silver_incremental/products/batch_id={batch_id}/_BATCH_METADATA.json":
        raise ValueError("Products manifest identity mismatch")
    if not isinstance(prefix, str) or not prefix.startswith(f"silver_incremental/products/batch_id={batch_id}/runs/") or type(rows) is not int or rows < 0 or not isinstance(objects, list) or not objects:
        raise ValueError("Products manifest output metadata invalid")
    expected = sorted(objects, key=lambda item: item.get("object_key", ""))
    if any(not isinstance(item, dict) or not isinstance(item.get("object_key"), str)
           or not item["object_key"].startswith(prefix) or not item["object_key"].endswith(".parquet")
           or type(item.get("size")) is not int or item["size"] <= 0
           or not isinstance(item.get("etag"), str) or not item["etag"] for item in expected):
        raise ValueError("Products manifest parquet objects invalid")
    return batch_id, prefix


def verify_manifest_objects(client, bucket, manifest):
    actual = discover_parquet_objects(client, bucket, manifest["data_prefix"], include_etag=True)
    expected = sorted(manifest["parquet_objects"], key=lambda item: item["object_key"])
    if actual != expected:
        raise ValueError("Products Parquet object inventory no longer matches manifest")


def read_manifest(client, bucket, key):
    response = client.get_object(bucket, key)
    try:
        manifest = json.loads(response.read().decode("utf-8"))
    finally:
        response.close(); response.release_conn()
    validate_manifest(key, manifest)
    return manifest


def discover_manifests(client, bucket):
    manifests = [(item.object_name, read_manifest(client, bucket, item.object_name)) for item in client.list_objects(bucket, prefix=PREFIX, recursive=True) if item.object_name.endswith("/_BATCH_METADATA.json")]
    return sorted(manifests, key=lambda item: (item[1].get("completed_at", ""), item[1]["batch_id"]))


def ensure_progress(spark, progress_table=PROGRESS_TABLE):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.metadata")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {progress_table} (batch_id STRING, manifest_key STRING, manifest_identity STRING, snapshot_id BIGINT, completed_at TIMESTAMP) USING iceberg")


def latest_snapshot_id(spark, table=TABLE):
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    if not rows: raise ValueError("Products Iceberg table has no snapshot")
    return int(rows[0].snapshot_id)


def validate_products(spark, table=TABLE):
    frame = spark.table(table); count = frame.count(); nulls = frame.filter("product_id IS NULL").count(); duplicates = frame.groupBy("product_id").count().filter("count > 1").count()
    if nulls or duplicates: raise ValueError(f"Products quality failed: nulls={nulls}, duplicates={duplicates}")
    return {"row_count": count, "null_keys": nulls, "duplicate_keys": duplicates}


def migrate_manifest(spark, client, bucket, key, manifest=None, *, table=TABLE, progress_table=PROGRESS_TABLE):
    manifest = read_manifest(client, bucket, key) if manifest is None else manifest
    batch_id, prefix = validate_manifest(key, manifest); ensure_progress(spark, progress_table); verify_manifest_objects(client, bucket, manifest); digest = manifest_identity(manifest)
    existing = spark.sql(f"SELECT manifest_identity, snapshot_id FROM {progress_table} WHERE batch_id = '{batch_id}'").collect()
    if existing:
        if existing[0].manifest_identity != digest: raise RuntimeError(f"Conflicting products manifest: {batch_id}")
        return {"status": "ALREADY_MIGRATED", "batch_id": batch_id, "snapshot_id": int(existing[0].snapshot_id)}
    source = spark.read.parquet(f"s3a://{bucket}/{prefix}")
    if source.count() != manifest["silver_row_count"]: raise ValueError("Products Parquet row count mismatch")
    merge_products(spark, prepare_products(source, batch_id), table); quality = validate_products(spark, table); snapshot = latest_snapshot_id(spark, table)
    spark.sql(f"INSERT INTO {progress_table} VALUES ('{batch_id}', '{key}', '{digest}', {snapshot}, current_timestamp())")
    return {"status": "MIGRATED", "batch_id": batch_id, "snapshot_id": snapshot, **quality}


def migrate_all_manifests(spark, client, bucket):
    return [migrate_manifest(spark, client, bucket, key, manifest) for key, manifest in discover_manifests(client, bucket)]
