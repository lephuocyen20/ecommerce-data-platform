"""Shared verified Silver Parquet-to-Iceberg manifest migration."""
import hashlib, json
from common.object_integrity import discover_parquet_objects
from lakehouse.migration.registry import TABLES
from lakehouse.models.engine import merge, prepare
from lakehouse.models.spec import SPECS
from lakehouse.attestation import build_snapshot_attestation, publish_snapshot_attestation
def _identity(manifest): return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
def _prefix(name, scope=None): return scope.key(f"silver_incremental/{name}/") if scope is not None else f"silver_incremental/{name}/"
def _validate(name, key, manifest, scope=None):
    if not isinstance(manifest, dict): raise ValueError(f"Invalid {name} manifest")
    batch, prefix, rows, objects = manifest.get("batch_id"), manifest.get("data_prefix"), manifest.get("silver_row_count"), manifest.get("parquet_objects")
    if manifest.get("status") != "COMPLETED" or manifest.get("table_name") != name: raise ValueError(f"Invalid {name} manifest")
    if not isinstance(batch, str) or len(batch) != 32 or key != f"{_prefix(name, scope)}batch_id={batch}/_BATCH_METADATA.json": raise ValueError(f"{name} manifest identity mismatch")
    if not isinstance(prefix, str) or not prefix.startswith(f"{_prefix(name, scope)}batch_id={batch}/runs/") or type(rows) is not int or rows < 0 or not isinstance(objects, list) or not objects: raise ValueError(f"{name} manifest output metadata invalid")
    if any(not isinstance(item, dict) or not isinstance(item.get("object_key"), str) or not item["object_key"].startswith(prefix) or not item["object_key"].endswith(".parquet") or type(item.get("size")) is not int or item["size"] <= 0 or not item.get("etag") for item in objects): raise ValueError(f"{name} manifest objects invalid")
    return batch, prefix
def read_manifest(client, bucket, key, table_name, scope=None):
    response = client.get_object(bucket, key)
    try: manifest = json.loads(response.read().decode())
    finally: response.close(); response.release_conn()
    _validate(table_name, key, manifest, scope); return manifest
def discover_manifests(client, bucket, table_name, scope=None):
    manifests = [(item.object_name, read_manifest(client, bucket, item.object_name, table_name, scope)) for item in client.list_objects(bucket, prefix=_prefix(table_name, scope), recursive=True) if item.object_name.endswith("/_BATCH_METADATA.json")]
    return sorted(manifests, key=lambda item: (item[1].get("completed_at", ""), item[1]["batch_id"]))
def migrate_manifest(spark, client, bucket, key, table_name, manifest=None, *, table=None, progress_table=None, scope=None):
    manifest = read_manifest(client, bucket, key, table_name, scope) if manifest is None else manifest; batch, prefix = _validate(table_name, key, manifest, scope); progress = progress_table or f"lakehouse.metadata.{table_name}_parquet_migration_progress"
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
    target_table = table or f"lakehouse.{scope.silver_namespace if scope is not None else 'silver'}.{table_name}"
    # A retry after a process failure between the Iceberg MERGE and progress
    # recording replays the deterministic merge.  The progress write itself is
    # idempotent, so that recovery cannot create competing records for a batch.
    merge(spark, prepare(source, spec, batch), spec, target_table)
    snapshot = int(spark.sql(f"SELECT snapshot_id FROM {target_table}.snapshots ORDER BY committed_at DESC LIMIT 1").first().snapshot_id)
    safe_key, safe_digest = key.replace("'", "''"), digest.replace("'", "''")
    spark.sql(
        f"MERGE INTO {progress} p USING (SELECT '{batch}' batch_id, "
        f"'{safe_key}' manifest_key, '{safe_digest}' manifest_identity, {snapshot} snapshot_id) s "
        "ON p.batch_id = s.batch_id "
        "WHEN MATCHED THEN UPDATE SET manifest_identity = p.manifest_identity "
        "WHEN NOT MATCHED THEN INSERT (batch_id, manifest_key, manifest_identity, snapshot_id, completed_at) "
        "VALUES (s.batch_id, s.manifest_key, s.manifest_identity, s.snapshot_id, current_timestamp())"
    )
    recorded = spark.sql(f"SELECT manifest_identity FROM {progress} WHERE batch_id = '{batch}'").first()
    if recorded is None or recorded.manifest_identity != digest:
        raise RuntimeError(f"Conflicting {table_name} migration progress: {batch}")
    return {"status":"MIGRATED", "batch_id":batch, "snapshot_id":snapshot, "row_count":spark.table(target_table).count()}


def publish_snapshot_attestation_for_progress(spark, client, bucket, table_name, snapshot_id, *,
                                              catalog="lakehouse", namespace="silver", ref="main",
                                              progress_table=None, scope=None):
    """Publish cumulative proof for exactly one Iceberg snapshot.

    This is deliberately separate from migration so a failed attestation cannot
    be mistaken for a successful authorization of a Gold read.
    """
    if table_name not in SPECS:
        raise ValueError("Unknown Lakehouse table")
    if not isinstance(snapshot_id, int) or isinstance(snapshot_id, bool) or snapshot_id <= 0:
        raise ValueError("Invalid snapshot_id")
    progress = progress_table or f"lakehouse.metadata.{table_name}_parquet_migration_progress"
    rows = spark.sql(
        f"SELECT manifest_identity FROM {progress} WHERE snapshot_id = {snapshot_id}"
    ).collect()
    identities = [row.manifest_identity for row in rows]
    attestation = build_snapshot_attestation(
        table=table_name, snapshot_id=snapshot_id, manifest_identities=identities,
        catalog=catalog, namespace=namespace, ref=ref,
    )
    publication = publish_snapshot_attestation(client, bucket, attestation, scope=scope)
    return {**publication, "attestation": attestation}
