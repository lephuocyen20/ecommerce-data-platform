
"""Transform one validated incremental Bronze batch into Silver Parquet.

Run with spark-submit inside the ecommerce-spark image.

A batch is published only after a complete, verified Parquet write.
The immutable run prefix is referenced by a completion manifest.
"""

import argparse
import hashlib
import io
import json
import os
import re
import uuid
from datetime import datetime, timezone

from minio import Minio
from minio.error import S3Error
from pyspark.sql import SparkSession

try:
    # Package imports are required by the snapshot/Gold pipeline and tests.
    from transformation.config import BRONZE_BUCKET, MINIO_ENDPOINT
    from transformation.bronze_to_silver import configure_s3, TRANSFORMS
    from common.paths import (
        bronze_incremental_object_key,
        silver_incremental_batch_paths,
    )
    from common.bounded_e2e_scope import TEST_BUCKET, build_bounded_e2e_scope
    from common.storage_layout import validate_storage_environment
    from common.object_integrity import (
        calculate_object_sha256,
        discover_parquet_objects,
    )
    from common.manifest import canonical_manifest_identity
except ImportError:
    # Keep compatibility with direct spark-submit from transformation/.
    from config import BRONZE_BUCKET, MINIO_ENDPOINT
    from bronze_to_silver import configure_s3, TRANSFORMS
    from shared.paths import bronze_incremental_object_key, silver_incremental_batch_paths
    from shared.object_integrity import calculate_object_sha256, discover_parquet_objects
    from common.bounded_e2e_scope import TEST_BUCKET, build_bounded_e2e_scope
    from common.storage_layout import validate_storage_environment
    from common.manifest import canonical_manifest_identity


VALID_BATCH_ID = re.compile(r"^[0-9a-f]{32}$")
VALID_RUNTIME_WINDOW_ID = re.compile(r"^[A-Za-z0-9._:+\-]{1,256}$")

TABLE_PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "shipments": "shipment_id",
}

SILVER_TABLES = tuple(TABLE_PRIMARY_KEYS)
LINEAGE_EVIDENCE_VERSION = 1
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _identity(value):
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _evidence_identity_payload(evidence, identity_field):
    if not isinstance(evidence, dict):
        raise ValueError("Persisted Silver evidence must be a JSON object")
    return {key: value for key, value in evidence.items() if key != identity_field}


def validate_persisted_evidence(evidence, identity_field, *, expected_identity=None,
                                expected_data_prefix=None, expected_schema_fp=None):
    """Recompute and validate the identity of the complete persisted JSON."""
    if not isinstance(evidence, dict):
        raise ValueError("Persisted Silver evidence must be a JSON object")
    stored_identity = evidence.get(identity_field)
    recomputed_identity = _identity(_evidence_identity_payload(evidence, identity_field))
    if stored_identity != recomputed_identity:
        raise ValueError(f"Persisted Silver {identity_field} canonical identity mismatch")
    if expected_identity is not None and recomputed_identity != expected_identity:
        raise ValueError(f"Persisted Silver {identity_field} mismatch")
    if expected_data_prefix is not None and evidence.get("data_prefix") != expected_data_prefix:
        raise ValueError("Persisted Silver evidence data prefix mismatch")
    if expected_schema_fp is not None and evidence.get("schema_fingerprint") != expected_schema_fp:
        raise ValueError("Persisted Silver evidence schema fingerprint mismatch")
    return recomputed_identity


def _manifest_identity_payload(manifest):
    # manifest_identities contains the resulting identity, so it is excluded
    # together with manifest_identity to avoid a circular hash.
    return {
        key: value for key, value in manifest.items()
        if key not in {"manifest_identity", "manifest_identities"}
    }


def schema_fingerprint(schema):
    """Return a deterministic fingerprint for the persisted Spark schema."""
    if hasattr(schema, "jsonValue"):
        schema = schema.jsonValue()
    elif isinstance(schema, str):
        schema = json.loads(schema)
    if not isinstance(schema, dict):
        raise ValueError("Persisted Silver schema is required")
    return _identity(schema)


def build_quality_evidence(*, table_name, batch_id, run_id, data_prefix,
                           schema_fp, row_count, primary_key,
                           null_primary_key_count, duplicate_primary_key_count,
                           parquet_objects):
    """Build evidence only from the persisted/read-back Silver artifact."""
    if table_name not in SILVER_TABLES or not VALID_BATCH_ID.fullmatch(batch_id):
        raise ValueError("Invalid Silver quality evidence binding")
    if not re.fullmatch(r"[0-9a-f]{32}", run_id) or not data_prefix.endswith("/"):
        raise ValueError("Invalid Silver quality evidence run binding")
    if not HEX_SHA256.fullmatch(schema_fp):
        raise ValueError("Invalid Silver schema fingerprint")
    if type(row_count) is not int or row_count < 0 or type(null_primary_key_count) is not int or type(duplicate_primary_key_count) is not int:
        raise ValueError("Invalid Silver quality metrics")
    if not isinstance(parquet_objects, list) or not parquet_objects:
        raise ValueError("Silver Parquet inventory is required for quality evidence")
    evidence = {
        "version": LINEAGE_EVIDENCE_VERSION,
        "status": "PASS" if null_primary_key_count == 0 and duplicate_primary_key_count == 0 else "FAIL",
        "table_name": table_name,
        "batch_id": batch_id,
        "run_id": run_id,
        "data_prefix": data_prefix,
        "schema_fingerprint": schema_fp,
        "row_count": row_count,
        "primary_key": primary_key,
        "null_primary_key_count": null_primary_key_count,
        "duplicate_primary_key_count": duplicate_primary_key_count,
        "parquet_file_count": len(parquet_objects),
        "parquet_objects": parquet_objects,
    }
    evidence["quality_evidence_identity"] = _identity({k: v for k, v in evidence.items() if k != "quality_evidence_identity"})
    return evidence


def build_reconciliation_evidence(*, table_name, batch_id, run_id, bronze_object_key,
                                  bronze_checksum, data_prefix, schema_fp,
                                  row_count, parquet_objects, quality_identity):
    """Describe the exact Bronze binding and persisted Silver read-back."""
    if not HEX_SHA256.fullmatch(bronze_checksum) or not HEX_SHA256.fullmatch(quality_identity):
        raise ValueError("Invalid reconciliation evidence identity input")
    evidence = {
        "version": LINEAGE_EVIDENCE_VERSION,
        "status": "PASS",
        "table_name": table_name,
        "batch_id": batch_id,
        "run_id": run_id,
        "bronze_object_key": bronze_object_key,
        "bronze_checksum_sha256": bronze_checksum,
        "data_prefix": data_prefix,
        "schema_fingerprint": schema_fp,
        "row_count": row_count,
        "parquet_objects": parquet_objects,
        "quality_evidence_identity": quality_identity,
    }
    evidence["reconciliation_evidence_identity"] = _identity({k: v for k, v in evidence.items() if k != "reconciliation_evidence_identity"})
    return evidence


def validate_lineage_evidence_contract(manifest):
    """Validate evidence identities and bindings without creating clients."""
    if not isinstance(manifest, dict):
        raise ValueError("Silver lineage manifest is required")
    required = ("source_type", "bucket", "manifest_key", "manifest_identity", "manifest_keys",
                "manifest_identities", "data_prefix", "data_prefixes", "run_id",
                "schema_fingerprint", "quality_evidence_identity",
                "reconciliation_evidence_identity")
    scalar_required = tuple(name for name in required if name not in {"manifest_keys", "manifest_identities", "data_prefixes"})
    if any(not isinstance(manifest.get(name), str) or not manifest[name] for name in scalar_required):
        raise ValueError("Silver manifest missing complete lineage evidence")
    for name in ("schema_fingerprint", "quality_evidence_identity", "reconciliation_evidence_identity"):
        if not HEX_SHA256.fullmatch(manifest[name]):
            raise ValueError(f"Invalid Silver {name}")
    if not re.fullmatch(r"[0-9a-f]{32}", manifest["run_id"]):
        raise ValueError("Invalid Silver run binding")
    if manifest["source_type"] != "parquet_manifest":
        raise ValueError("Invalid Silver manifest source type")
    if (not isinstance(manifest["manifest_keys"], list)
            or not isinstance(manifest["manifest_identities"], list)
            or not isinstance(manifest["data_prefixes"], list)
            or manifest["manifest_key"] not in manifest["manifest_keys"]
            or manifest["manifest_identity"] not in manifest["manifest_identities"]
            or manifest["data_prefix"] not in manifest["data_prefixes"]):
        raise ValueError("Silver manifest plural lineage fields mismatch")
    if (len(manifest["manifest_keys"]) != len(manifest["manifest_identities"])
            or len(manifest["manifest_keys"]) != len(manifest["data_prefixes"])
            or not manifest["manifest_keys"]):
        raise ValueError("Silver manifest plural lineage fields have unequal lengths")
    if any(not isinstance(value, str) or not value for value in manifest["manifest_keys"]):
        raise ValueError("Invalid Silver manifest key list")
    if any(not isinstance(value, str) or not HEX_SHA256.fullmatch(value) for value in manifest["manifest_identities"]):
        raise ValueError("Invalid Silver manifest identity list")
    if any(not isinstance(value, str) or not value.endswith("/") for value in manifest["data_prefixes"]):
        raise ValueError("Invalid Silver data prefix list")
    lineage = manifest.get("lineage_evidence")
    if not isinstance(lineage, dict) or set(lineage) != {"quality", "reconciliation"}:
        raise ValueError("Silver manifest missing lineage evidence keys")
    for kind in lineage.values():
        if not isinstance(kind, dict) or not isinstance(kind.get("key"), str) or not isinstance(kind.get("identity"), str):
            raise ValueError("Invalid Silver lineage evidence reference")
    data_prefix = manifest["data_prefix"]
    expected_evidence_keys = {
        "quality": data_prefix + "_lineage/quality.json",
        "reconciliation": data_prefix + "_lineage/reconciliation.json",
    }
    for evidence_kind, reference in lineage.items():
        if reference["key"] != expected_evidence_keys[evidence_kind] or not reference["key"].startswith(data_prefix):
            raise ValueError("Silver lineage evidence key escapes immutable data prefix")
    if lineage["quality"]["identity"] != manifest["quality_evidence_identity"] or lineage["reconciliation"]["identity"] != manifest["reconciliation_evidence_identity"]:
        raise ValueError("Silver lineage evidence identity mismatch")
    expected_manifest_identity = canonical_manifest_identity(manifest)
    if manifest["manifest_identity"] != expected_manifest_identity:
        raise ValueError("Silver manifest identity mismatch")
    return True


def preflight_producer_contract(table_name, batch_id, object_key, bronze_checksum,
                                runtime_window_id, parent_orders_batch_id=None, *, environ=None):
    """Pure fail-closed gate; must run before Spark or MinIO construction."""
    validate_arguments(table_name, batch_id, object_key, bronze_checksum, runtime_window_id, parent_orders_batch_id)
    values = dict(os.environ if environ is None else environ)
    for flag in ("GOLD_DIMENSIONAL_ENABLED", "GOLD_DIMENSIONAL_WRITE_ENABLED", "HISTORICAL_BACKFILL_WRITE_APPROVED"):
        if values.get(flag, "").lower() in {"1", "true", "yes", "write"}:
            raise RuntimeError(f"Silver producer requires {flag} disabled")
    return validate_storage_environment(values)

PARENT_ORDERS_TABLES = {"payments", "shipments"}



def _resolve_parent_orders_manifest(client, bucket, batch_id, runtime_window_id, *, scope=None):
    manifest_key = silver_incremental_batch_paths(
        "orders", batch_id, scope=scope
    )["manifest_key"]
    manifest = get_completion_manifest(client, bucket, manifest_key)
    if not isinstance(manifest, dict):
        raise ValueError("Current orders completion manifest is required")
    if manifest.get("status") != "COMPLETED":
        raise ValueError("Current orders completion manifest is not COMPLETED")
    if manifest.get("table_name") != "orders":
        raise ValueError("Current orders completion manifest table mismatch")
    if manifest.get("batch_id") != batch_id:
        raise ValueError("Current orders completion manifest batch mismatch")
    parent_coverage = manifest.get("coverage")
    expected_coverage_identity = hashlib.sha256(
        f"{runtime_window_id}:orders:{batch_id}".encode("utf-8")
    ).hexdigest()
    if (
        not isinstance(parent_coverage, dict)
        or parent_coverage.get("runtime_window_id") != runtime_window_id
        or not isinstance(parent_coverage.get("coverage_identity"), str)
        or not VALID_RUNTIME_WINDOW_ID.fullmatch(parent_coverage["runtime_window_id"])
        or not re.fullmatch(r"[0-9a-f]{64}", parent_coverage["coverage_identity"])
        or parent_coverage["coverage_identity"] != expected_coverage_identity
    ):
        raise ValueError("Current orders completion manifest runtime coverage mismatch")
    return manifest_key, manifest, parent_coverage["coverage_identity"]


def _validate_parent_orders_data_prefix(parent_manifest, *, scope=None):
    if not isinstance(parent_manifest, dict):
        raise ValueError("Current orders manifest is required")
    if parent_manifest.get("table_name") != "orders":
        raise ValueError("Current orders manifest table mismatch")
    parent_batch_id = parent_manifest.get("batch_id")
    if not isinstance(parent_batch_id, str) or not VALID_BATCH_ID.fullmatch(parent_batch_id):
        raise ValueError("Current orders manifest batch mismatch")

    expected_prefix = silver_incremental_batch_paths(
        "orders", parent_batch_id, scope=scope
    )["runs_prefix"]
    parent_prefix = parent_manifest.get("data_prefix")
    if (
        not isinstance(parent_prefix, str)
        or not parent_prefix.startswith(expected_prefix)
        or not re.fullmatch(r"[0-9a-f]{32}/", parent_prefix[len(expected_prefix):])
    ):
        raise ValueError("Current orders manifest data prefix mismatch")
    return parent_prefix


def validate_child_parent_scope(spark, table_name, silver_df, parent_manifest, *, scope=None):
    """Fail closed on any child order outside the verified orders Silver batch."""
    if table_name not in PARENT_ORDERS_TABLES:
        return
    parent_prefix = _validate_parent_orders_data_prefix(parent_manifest, scope=scope)
    parent_df = spark.read.parquet(f"s3a://{BRONZE_BUCKET}/{parent_prefix}").select("order_id").distinct()
    if silver_df.join(parent_df, on="order_id", how="left_anti").limit(1).count():
        raise ValueError(f"Silver {table_name} contains order_id outside verified orders scope")


def build_coverage_metadata(table_name, batch_id, runtime_window_id, *, parent_orders_batch_id=None, scope=None, client=None, bucket=None):
    """Build immutable coverage metadata for the current Airflow runtime."""
    if not isinstance(runtime_window_id, str) or not VALID_RUNTIME_WINDOW_ID.fullmatch(runtime_window_id):
        raise ValueError("Invalid runtime window ID")
    if table_name not in TABLE_PRIMARY_KEYS:
        raise ValueError(f"Unsupported table: {table_name}")
    if not VALID_BATCH_ID.fullmatch(batch_id):
        raise ValueError(f"Invalid batch ID: {batch_id}")
    if table_name == "orders":
        if parent_orders_batch_id is not None:
            raise ValueError("Orders cannot declare a parent orders batch")
        parent_identity = batch_id
    elif table_name in PARENT_ORDERS_TABLES:
        if not isinstance(parent_orders_batch_id, str) or not VALID_BATCH_ID.fullmatch(parent_orders_batch_id):
            raise ValueError(f"{table_name} requires the current orders batch ID")
        parent_identity = parent_orders_batch_id
    else:
        parent_identity = None
    coverage_identity = hashlib.sha256(
        f"{runtime_window_id}:{table_name}:{batch_id}".encode("utf-8")
    ).hexdigest()
    coverage = {
        "runtime_window_id": runtime_window_id,
        "coverage_identity": coverage_identity,
    }
    if table_name in PARENT_ORDERS_TABLES:
        if client is None:
            raise ValueError(f"{table_name} requires the current orders completion manifest")
        orders_manifest_key, orders_manifest, parent_coverage_identity = _resolve_parent_orders_manifest(
            client,
            bucket or BRONZE_BUCKET,
            parent_orders_batch_id,
            runtime_window_id,
            scope=scope,
        )
        coverage.update({
            "parent_coverage_identity": parent_coverage_identity,
            "parent_orders_reference": {
                "table": "orders",
                "batch_id": parent_orders_batch_id,
                "manifest_key": orders_manifest_key,
                "manifest_identity": canonical_manifest_identity(orders_manifest),
            },
        })
    return coverage


def validate_arguments(table_name, batch_id, object_key, bronze_checksum, runtime_window_id, parent_orders_batch_id=None):
    if table_name not in TABLE_PRIMARY_KEYS:
        raise ValueError(f"Unsupported table: {table_name}")

    if not VALID_BATCH_ID.fullmatch(batch_id):
        raise ValueError(f"Invalid batch ID: {batch_id}")

    expected_key = bronze_incremental_object_key(table_name, batch_id)

    if object_key != expected_key:
        raise ValueError(
            f"Unexpected Bronze object key: {object_key}"
        )

    if not re.fullmatch(r"[0-9a-f]{64}", bronze_checksum):
        raise ValueError("Invalid Bronze SHA-256 checksum")
    if not isinstance(runtime_window_id, str) or not VALID_RUNTIME_WINDOW_ID.fullmatch(runtime_window_id):
        raise ValueError("Invalid runtime window ID")
    if table_name in PARENT_ORDERS_TABLES and (
        not isinstance(parent_orders_batch_id, str) or not VALID_BATCH_ID.fullmatch(parent_orders_batch_id)
    ):
        raise ValueError(f"{table_name} requires the current orders batch ID")


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


def verify_bronze_checksum(
    client,
    bucket,
    object_key,
    expected_checksum,
):
    """Verify the exact Bronze object before Spark reads it."""

    actual_checksum = calculate_object_sha256(client, bucket, object_key)

    if actual_checksum != expected_checksum:
        raise ValueError(
            f"Bronze checksum mismatch for {object_key}: "
            f"expected={expected_checksum}, "
            f"actual={actual_checksum}"
        )

    return True



def read_json_object(client, bucket, object_key):
    response = client.get_object(bucket, object_key)

    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def get_completion_manifest(client, bucket, manifest_key):
    try:
        client.stat_object(bucket, manifest_key)
    except Exception as exc:
        from minio.error import S3Error

        if isinstance(exc, S3Error) and exc.code in (
            "NoSuchKey",
            "NoSuchObject",
        ):
            return None

        raise

    return read_json_object(client, bucket, manifest_key)


def verify_published_manifest(
    spark,
    client,
    bucket,
    manifest,
    table_name,
    batch_id,
    object_key,
    bronze_checksum,
    coverage,
    scope=None,
):
    validate_lineage_evidence_contract(manifest)
    expected_manifest_key = silver_incremental_batch_paths(
        table_name, batch_id, scope=scope
    )["manifest_key"]
    if manifest.get("bucket") != bucket or manifest.get("manifest_key") != expected_manifest_key:
        raise ValueError("Silver manifest bucket/key identity mismatch")
    if manifest.get("status") != "COMPLETED":
        raise ValueError("Invalid Silver manifest status")

    if manifest.get("table_name") != table_name:
        raise ValueError("Silver manifest table mismatch")

    if manifest.get("batch_id") != batch_id:
        raise ValueError("Silver manifest batch ID mismatch")

    if manifest.get("bronze_object_key") != object_key:
        raise ValueError("Silver manifest Bronze key mismatch")
    if manifest.get("bronze_checksum_sha256") != bronze_checksum:
        raise ValueError("Silver manifest Bronze checksum mismatch")
    if manifest.get("coverage") != coverage:
        raise ValueError("Silver manifest coverage metadata mismatch")
    expected_prefix = silver_incremental_batch_paths(
        table_name, batch_id, scope=scope
    )["runs_prefix"]

    data_prefix = manifest.get("data_prefix")

    if (
        not isinstance(data_prefix, str)
        or not data_prefix.startswith(expected_prefix)
        or not re.fullmatch(
            r"[0-9a-f]{32}/",
            data_prefix[len(expected_prefix):],
        )
    ):
        raise ValueError("Invalid Silver data prefix")

    expected_count = manifest.get("silver_row_count")

    if (
        not isinstance(expected_count, int)
        or isinstance(expected_count, bool)
        or expected_count < 0
    ):
        raise ValueError("Invalid Silver row count")

    actual_parquet_objects = discover_parquet_objects(
        client,
        bucket,
        data_prefix,
        include_etag=True,
    )
    
    expected_parquet_objects = manifest.get("parquet_objects")

    if not isinstance(expected_parquet_objects, list):
        raise ValueError(
            "Silver manifest missing Parquet object metadata"
        )

    if actual_parquet_objects != expected_parquet_objects:
        raise ValueError(
            "Published Silver Parquet objects do not match manifest"
        )

    if manifest.get("parquet_file_count") != len(actual_parquet_objects):
        raise ValueError(
            "Published Silver Parquet file count mismatch"
        )

    silver_path = f"s3a://{bucket}/{data_prefix}"

    df = spark.read.parquet(silver_path)

    if TABLE_PRIMARY_KEYS[table_name] not in df.columns:
        raise ValueError("Published Silver primary key is missing")

    actual_count = df.count()

    if actual_count != expected_count:
        raise ValueError(
            "Published Silver row count mismatch: "
            f"expected={expected_count}, actual={actual_count}"
        )

    
    # Verify primary key integrity when reusing published Silver.
    primary_key = TABLE_PRIMARY_KEYS[table_name]

    if df.filter(
        df[primary_key].isNull()
    ).limit(1).count():
        raise ValueError(
        "Published Silver contains null primary keys"
        )

    duplicate_pk_count = (
        df.groupBy(primary_key)
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
        )

    if duplicate_pk_count:
        raise ValueError(
        "Published Silver contains duplicate primary keys"
    )

    actual_schema_fp = schema_fingerprint(df.schema)
    if manifest["schema_fingerprint"] != actual_schema_fp:
        raise ValueError("Published Silver schema fingerprint mismatch")

    lineage = manifest["lineage_evidence"]
    persisted_evidence = {}
    for evidence_kind, identity_field in (("quality", "quality_evidence_identity"), ("reconciliation", "reconciliation_evidence_identity")):
        persisted = read_json_object(client, bucket, lineage[evidence_kind]["key"])
        validate_persisted_evidence(
            persisted,
            identity_field,
            expected_identity=manifest[identity_field],
            expected_data_prefix=data_prefix,
            expected_schema_fp=actual_schema_fp,
        )
        persisted_evidence[evidence_kind] = persisted
    if persisted_evidence["reconciliation"].get("quality_evidence_identity") != manifest["quality_evidence_identity"]:
        raise ValueError("Published Silver reconciliation quality evidence binding mismatch")


    return manifest



def publish_manifest(client, bucket, manifest_key, manifest):
    """
    Publish a completion manifest only when its key does not exist.

    Uses the MinIO SDK 7.2.10 private API because the public
    put_object() method does not expose conditional headers.
    """
    payload = json.dumps(
        manifest,
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")

    return client._put_object(
        bucket_name=bucket,
        object_name=manifest_key,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "If-None-Match": "*",
        },
    )


def publish_evidence_and_verify(client, bucket, evidence_key, evidence, identity_field, *,
                                expected_data_prefix=None, expected_schema_fp=None):
    """Create evidence once, then verify the exact persisted bytes semantically."""
    try:
        publish_manifest(client, bucket, evidence_key, evidence)
    except S3Error as exc:
        if exc.code not in ("PreconditionFailed", "ConditionalRequestConflict"):
            raise
    persisted = read_json_object(client, bucket, evidence_key)
    validate_persisted_evidence(
        persisted,
        identity_field,
        expected_identity=evidence.get(identity_field),
        expected_data_prefix=expected_data_prefix,
        expected_schema_fp=expected_schema_fp,
    )
    return persisted



def transform_batch( spark, client, table_name, batch_id, object_key, bronze_checksum, runtime_window_id, parent_orders_batch_id=None, scope=None):
    bucket = BRONZE_BUCKET

    if not client.bucket_exists(bucket):
        raise ValueError(f"MinIO bucket does not exist: {bucket}")
    verify_bronze_checksum(
            client,
            bucket,
         object_key,
            bronze_checksum,)
    silver_paths = silver_incremental_batch_paths(table_name, batch_id, scope=scope)
    batch_prefix = silver_paths["batch_prefix"]
    manifest_key = silver_paths["manifest_key"]
    coverage = build_coverage_metadata(
        table_name,
        batch_id,
        runtime_window_id,
        parent_orders_batch_id=parent_orders_batch_id,
        scope=scope,
        client=client,
        bucket=bucket,
    )

    existing_manifest = get_completion_manifest(
        client,
        bucket,
        manifest_key,
    )

    if existing_manifest is not None:
        verified = verify_published_manifest(
            spark,
            client,
            bucket,
            existing_manifest,
            table_name,
            batch_id,
            object_key,
            bronze_checksum,
            coverage,
            scope=scope,
        )

        print(
            json.dumps(
                {
                    "status": "ALREADY_COMPLETED",
                    "table_name": table_name,
                    "batch_id": batch_id,
                    "manifest_key": manifest_key,
                    "silver_row_count": verified["silver_row_count"],
                }
            )
        )
        return

    bronze_path = f"s3a://{bucket}/{object_key}"

    bronze_df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .option("mode", "FAILFAST")
    .csv(bronze_path)
)

    transform_function = TRANSFORMS[table_name]

    silver_df = transform_function(bronze_df)
    if table_name in PARENT_ORDERS_TABLES:
        _, parent_manifest, _ = _resolve_parent_orders_manifest(
            client, bucket, parent_orders_batch_id, runtime_window_id, scope=scope
        )
        validate_child_parent_scope(spark, table_name, silver_df, parent_manifest, scope=scope)

    run_id = uuid.uuid4().hex
    data_prefix = batch_prefix + f"runs/{run_id}/"
    silver_path = f"s3a://{bucket}/{data_prefix}"

    # Each attempt uses a unique prefix.
    # Never overwrite an existing published batch.
    silver_df.write.mode("errorifexists").parquet(silver_path)

    verified_df = spark.read.parquet(silver_path)

    primary_key = TABLE_PRIMARY_KEYS[table_name]

    if primary_key not in verified_df.columns:
        raise ValueError("Silver primary key is missing")

    silver_count = verified_df.count()

    null_primary_key_count = verified_df.filter(
        verified_df[primary_key].isNull()
    ).count()
    if null_primary_key_count:
        raise ValueError("Silver contains null primary keys")

    
    # Verify primary key uniqueness in persisted Silver data.
    duplicate_pk_count = (
        verified_df.groupBy(primary_key)
        .count()
        .filter("count > 1")
        .count()
    )   

    if duplicate_pk_count:
        raise ValueError(
            f"Silver contains duplicate primary keys: {primary_key}"
        )

    
    parquet_objects = sorted(
    [
        {
            "object_key": obj.object_name,
            "size": obj.size,
            "etag": obj.etag.strip('"'),
        }
        for obj in client.list_objects(
            bucket,
            prefix=data_prefix,
            recursive=True,
        )
        if obj.object_name.endswith(".parquet")
    ],
    key=lambda item: item["object_key"],
)

    if not parquet_objects:
        raise ValueError("Spark did not produce Parquet files")

    schema_fp = schema_fingerprint(verified_df.schema)
    quality_evidence = build_quality_evidence(
        table_name=table_name,
        batch_id=batch_id,
        run_id=run_id,
        data_prefix=data_prefix,
        schema_fp=schema_fp,
        row_count=silver_count,
        primary_key=primary_key,
        null_primary_key_count=null_primary_key_count,
        duplicate_primary_key_count=duplicate_pk_count,
        parquet_objects=parquet_objects,
    )
    reconciliation_evidence = build_reconciliation_evidence(
        table_name=table_name,
        batch_id=batch_id,
        run_id=run_id,
        bronze_object_key=object_key,
        bronze_checksum=bronze_checksum,
        data_prefix=data_prefix,
        schema_fp=schema_fp,
        row_count=silver_count,
        parquet_objects=parquet_objects,
        quality_identity=quality_evidence["quality_evidence_identity"],
    )
    quality_key = data_prefix + "_lineage/quality.json"
    reconciliation_key = data_prefix + "_lineage/reconciliation.json"
    persisted_quality = publish_evidence_and_verify(
        client, bucket, quality_key, quality_evidence, "quality_evidence_identity",
        expected_data_prefix=data_prefix, expected_schema_fp=schema_fp,
    )
    persisted_reconciliation = publish_evidence_and_verify(
        client, bucket, reconciliation_key, reconciliation_evidence,
        "reconciliation_evidence_identity", expected_data_prefix=data_prefix,
        expected_schema_fp=schema_fp,
    )
    if persisted_quality != quality_evidence or persisted_reconciliation != reconciliation_evidence:
        raise ValueError("Persisted Silver lineage evidence does not match producer evidence")

    manifest = {
        "status": "COMPLETED",
        "source_type": "parquet_manifest",
        "table_name": table_name,
        "batch_id": batch_id,
        "bronze_object_key": object_key,
        "data_prefix": data_prefix,
        "silver_row_count": silver_count,
        "parquet_file_count": len(parquet_objects),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "bronze_checksum_sha256": bronze_checksum,
        "bucket": bucket,
        "run_id": run_id,
        "schema_fingerprint": schema_fp,
        "quality_evidence_identity": quality_evidence["quality_evidence_identity"],
        "reconciliation_evidence_identity": reconciliation_evidence["reconciliation_evidence_identity"],
        "lineage_evidence": {
            "quality": {"key": quality_key, "identity": quality_evidence["quality_evidence_identity"]},
            "reconciliation": {"key": reconciliation_key, "identity": reconciliation_evidence["reconciliation_evidence_identity"]},
        },
        "parquet_objects": parquet_objects,
        "coverage": coverage,
    }
    manifest["manifest_key"] = manifest_key
    manifest["manifest_keys"] = [manifest_key]
    manifest["data_prefixes"] = [data_prefix]
    manifest["manifest_identity"] = canonical_manifest_identity(manifest)
    manifest["manifest_identities"] = [manifest["manifest_identity"]]
    validate_lineage_evidence_contract(manifest)

    try:
        publish_manifest(
            client,
            bucket,
            manifest_key,
            manifest,
        )
    except S3Error as exc:
        if exc.code not in (
        "PreconditionFailed",
        "ConditionalRequestConflict",
    ):
            raise

    # Another attempt may have published the manifest first.
        existing_manifest = get_completion_manifest(
            client,
            bucket,
            manifest_key,
        )

        if existing_manifest is None:
            raise

        verify_published_manifest(
            spark,
            client,
            bucket,
            existing_manifest,
            table_name,
            batch_id,
            object_key,
            bronze_checksum,
            coverage,
            scope=scope,
        )

        print(
            json.dumps(
                {
                    "status": "ALREADY_COMPLETED",
                    "table_name": table_name,
                    "batch_id": batch_id,
                    "manifest_key": manifest_key,
                }
            )
        )
        return

    print(
        json.dumps(
            {
                "status": "COMPLETED",
                "table_name": table_name,
                "batch_id": batch_id,
                "silver_row_count": silver_count,
                "manifest_key": manifest_key,
                "data_prefix": data_prefix,
            }
        )
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--table-name", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--object-key", required=True)
    parser.add_argument("--bronze-checksum", required=True)
    parser.add_argument("--runtime-window-id", required=True)
    parser.add_argument("--parent-orders-batch-id")

    args = parser.parse_args()

    storage_environment = dict(os.environ)
    if "BRONZE_BUCKET" in storage_environment and storage_environment["BRONZE_BUCKET"] != BRONZE_BUCKET:
        raise RuntimeError("BRONZE_BUCKET changed after module configuration")
    storage_environment.setdefault("BRONZE_BUCKET", BRONZE_BUCKET)
    scope = preflight_producer_contract(
        args.table_name,
        args.batch_id,
        args.object_key,
        args.bronze_checksum,
        args.runtime_window_id,
        args.parent_orders_batch_id,
        environ=storage_environment,
    ).scope

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("IncrementalBronzeToSilver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        configure_s3(spark)

        client = get_minio_client()

        transform_batch(
            spark,
            client,
            args.table_name,
            args.batch_id,
            args.object_key,
            args.bronze_checksum,
            args.runtime_window_id,
            parent_orders_batch_id=args.parent_orders_batch_id,
            scope=scope,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
