"""Verified Silver Parquet to Iceberg shipments migration."""
import hashlib
import json
from common.object_integrity import discover_parquet_objects
from lakehouse.shipments import TABLE, merge_shipments, prepare_shipments, validate_shipments

PREFIX = "silver_incremental/shipments/"
PROGRESS_TABLE = "lakehouse.metadata.shipments_parquet_migration_progress"

def identity(manifest): return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

def validate_manifest(key, manifest):
    if not isinstance(manifest, dict) or manifest.get("status") != "COMPLETED" or manifest.get("table_name") != "shipments": raise ValueError("Invalid shipments Silver manifest")
    batch_id, prefix, rows, objects = manifest.get("batch_id"), manifest.get("data_prefix"), manifest.get("silver_row_count"), manifest.get("parquet_objects")
    if not isinstance(batch_id, str) or len(batch_id) != 32 or key != f"{PREFIX}batch_id={batch_id}/_BATCH_METADATA.json": raise ValueError("Shipments manifest identity mismatch")
    if not isinstance(prefix, str) or not prefix.startswith(f"{PREFIX}batch_id={batch_id}/runs/") or type(rows) is not int or rows < 0 or not isinstance(objects, list) or not objects: raise ValueError("Shipments manifest output metadata invalid")
    if any(not isinstance(item, dict) or not isinstance(item.get("object_key"), str) or not item["object_key"].startswith(prefix) or not item["object_key"].endswith(".parquet") or type(item.get("size")) is not int or item["size"] <= 0 or not item.get("etag") for item in objects): raise ValueError("Shipments manifest parquet objects invalid")
    return batch_id, prefix

def read_manifest(client, bucket, key):
    response = client.get_object(bucket, key)
    try: manifest = json.loads(response.read().decode())
    finally: response.close(); response.release_conn()
    validate_manifest(key, manifest); return manifest

def discover_manifests(client, bucket):
    manifests = [(item.object_name, read_manifest(client, bucket, item.object_name)) for item in client.list_objects(bucket, prefix=PREFIX, recursive=True) if item.object_name.endswith("/_BATCH_METADATA.json")]
    return sorted(manifests, key=lambda item: (item[1].get("completed_at", ""), item[1]["batch_id"]))

def migrate_manifest(spark, client, bucket, key, manifest=None, *, table=TABLE, progress_table=PROGRESS_TABLE):
    manifest = read_manifest(client, bucket, key) if manifest is None else manifest; batch_id, prefix = validate_manifest(key, manifest)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.metadata"); spark.sql(f"CREATE TABLE IF NOT EXISTS {progress_table} (batch_id STRING, manifest_key STRING, manifest_identity STRING, snapshot_id BIGINT, completed_at TIMESTAMP) USING iceberg")
    if discover_parquet_objects(client, bucket, prefix, include_etag=True) != sorted(manifest["parquet_objects"], key=lambda item: item["object_key"]): raise ValueError("Shipments Parquet object inventory no longer matches manifest")
    digest = identity(manifest); existing = spark.sql(f"SELECT manifest_identity, snapshot_id FROM {progress_table} WHERE batch_id = '{batch_id}'").collect()
    if existing:
        if existing[0].manifest_identity != digest: raise RuntimeError(f"Conflicting shipments manifest: {batch_id}")
        return {"status": "ALREADY_MIGRATED", "batch_id": batch_id, "snapshot_id": int(existing[0].snapshot_id)}
    source = spark.read.parquet(f"s3a://{bucket}/{prefix}")
    if source.count() != manifest["silver_row_count"]: raise ValueError("Shipments Parquet row count mismatch")
    merge_shipments(spark, prepare_shipments(source, batch_id), table); quality = validate_shipments(spark, table); snapshot = int(spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first().snapshot_id)
    spark.sql(f"INSERT INTO {progress_table} VALUES ('{batch_id}', '{key}', '{digest}', {snapshot}, current_timestamp())")
    return {"status": "MIGRATED", "batch_id": batch_id, "snapshot_id": snapshot, **quality}
