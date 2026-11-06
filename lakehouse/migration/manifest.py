"""Shared verified Silver Parquet-to-Iceberg manifest migration."""
import hashlib, json
from common.object_integrity import discover_parquet_objects
from lakehouse.migration.registry import TABLES
from lakehouse.models.engine import merge, prepare
from lakehouse.models.spec import SPECS
def _identity(manifest): return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
def _prefix(name): return f"silver_incremental/{name}/"
def _validate(name, key, manifest):
    batch, prefix, rows, objects = manifest.get("batch_id"), manifest.get("data_prefix"), manifest.get("silver_row_count"), manifest.get("parquet_objects")
    if not isinstance(manifest, dict) or manifest.get("status") != "COMPLETED" or manifest.get("table_name") != name: raise ValueError(f"Invalid {name} manifest")
    if not isinstance(batch, str) or len(batch) != 32 or key != f"{_prefix(name)}batch_id={batch}/_BATCH_METADATA.json": raise ValueError(f"{name} manifest identity mismatch")
    if not isinstance(prefix, str) or not prefix.startswith(f"{_prefix(name)}batch_id={batch}/runs/") or type(rows) is not int or rows < 0 or not isinstance(objects, list) or not objects: raise ValueError(f"{name} manifest output metadata invalid")
    if any(not isinstance(item, dict) or not isinstance(item.get("object_key"), str) or not item["object_key"].startswith(prefix) or not item["object_key"].endswith(".parquet") or type(item.get("size")) is not int or item["size"] <= 0 or not item.get("etag") for item in objects): raise ValueError(f"{name} manifest objects invalid")
    return batch, prefix
def read_manifest(client, bucket, key, table_name):
    response = client.get_object(bucket, key)
    try: manifest = json.loads(response.read().decode())
    finally: response.close(); response.release_conn()
    _validate(table_name, key, manifest); return manifest
def discover_manifests(client, bucket, table_name):
    manifests = [(item.object_name, read_manifest(client, bucket, item.object_name, table_name)) for item in client.list_objects(bucket, prefix=_prefix(table_name), recursive=True) if item.object_name.endswith("/_BATCH_METADATA.json")]
    return sorted(manifests, key=lambda item: (item[1].get("completed_at", ""), item[1]["batch_id"]))
def migrate_manifest(spark, client, bucket, key, table_name, manifest=None, *, table=None, progress_table=None):
    manifest = read_manifest(client, bucket, key, table_name) if manifest is None else manifest; batch, prefix = _validate(table_name, key, manifest); progress = progress_table or f"lakehouse.metadata.{table_name}_parquet_migration_progress"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.metadata"); spark.sql(f"CREATE TABLE IF NOT EXISTS {progress} (batch_id STRING, manifest_key STRING, manifest_identity STRING, snapshot_id BIGINT, completed_at TIMESTAMP) USING iceberg")
    try: spark.sql(f"ALTER TABLE {progress} ADD COLUMN manifest_key STRING")
    except Exception: pass
    if discover_parquet_objects(client, bucket, prefix, include_etag=True) != sorted(manifest["parquet_objects"], key=lambda item: item["object_key"]): raise ValueError(f"{table_name} Parquet inventory mismatch")
    digest = _identity(manifest); existing = spark.sql(f"SELECT manifest_identity, snapshot_id FROM {progress} WHERE batch_id = '{batch}'").collect()
    if existing:
        if existing[0].manifest_identity != digest: raise RuntimeError(f"Conflicting {table_name} manifest: {batch}")
        return {"status":"ALREADY_MIGRATED", "batch_id":batch, "snapshot_id":int(existing[0].snapshot_id)}
    spec = SPECS[table_name]; source = spark.read.parquet(f"s3a://{bucket}/{prefix}")
    if source.count() != manifest["silver_row_count"]: raise ValueError(f"{table_name} row count mismatch")
    target_table = table or f"lakehouse.silver.{table_name}"; merge(spark, prepare(source, spec, batch), spec, target_table); snapshot = int(spark.sql(f"SELECT snapshot_id FROM {target_table}.snapshots ORDER BY committed_at DESC LIMIT 1").first().snapshot_id)
    spark.sql(f"INSERT INTO {progress} (batch_id, manifest_key, manifest_identity, snapshot_id, completed_at) VALUES ('{batch}', '{key}', '{digest}', {snapshot}, current_timestamp())")
    return {"status":"MIGRATED", "batch_id":batch, "snapshot_id":snapshot, "row_count":spark.table(target_table).count()}
