"""Guarded historical source -> Bronze -> Silver execution primitives.

This module is intentionally independent from ``ingestion.incremental``.  It
does not read or mutate incremental watermarks/checkpoints and creates no
external client until the explicit historical WRITE guards have passed.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import csv
from datetime import datetime
from typing import Callable, Mapping

from common.manifest import put_json_object_if_absent, read_json_object
from common.object_integrity import discover_parquet_objects
from common.storage_layout import bronze_table_key, silver_table_key
from gold.implementation.dimensional.lineage import build_parquet_lineage_artifact

from .config import HistoricalBackfillConfig, PRODUCTION_BUCKET, production_environment, require_write_approval
from .extractor import begin_snapshot, extract_page
from .orchestrator import PARENT_FIRST_ORDER, require_verified_silver_artifacts
from .staging import bronze_manifest, csv_bytes, deduplicate_records, validate_fk_parity


class HistoricalPipelineUnavailable(RuntimeError):
    """Raised when a guarded historical runtime dependency is unavailable."""


def _records(columns, rows):
    return [dict(zip(columns, row)) for row in rows]


def _connection_factory(environ):
    from common.database import connect_postgres

    return connect_postgres(
        host=environ.get("DB_HOST"), port=environ.get("DB_PORT", "5432"),
        dbname=environ.get("DB_NAME"), user=environ.get("DB_USER"),
        password=environ.get("DB_PASSWORD"),
    )


def _minio_factory():
    from common.minio import create_minio_client

    return create_minio_client()


def _extract_all(conn, capture_end, batch_size):
    begin_snapshot(conn, capture_end)
    extracted = {}
    for table in PARENT_FIRST_ORDER:
        rows, columns, cursor = [], (), None
        while True:
            page = extract_page(conn, table, capture_end, batch_size=batch_size, after=cursor)
            columns = page.columns
            rows.extend(page.rows)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        extracted[table] = {"columns": columns, "rows": tuple(rows), "records": _records(columns, rows)}
    return extracted


def _assert_parent_first_fk(extracted):
    customer_ids = [item["customer_id"] for item in extracted["customers"]["records"]]
    product_ids = [item["product_id"] for item in extracted["products"]["records"]]
    order_ids = [item["order_id"] for item in extracted["orders"]["records"]]
    for table in ("order_items", "payments", "shipments"):
        validate_fk_parity(table, extracted[table]["records"], order_ids, product_ids)


def _read_object(client, bucket, object_key):
    response = client.get_object(bucket, object_key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def _verify_bronze_readback(client, bucket, object_key, manifest_key, expected_manifest, expected_data, expected_row_count):
    expected_object_key = bronze_table_key(
        expected_manifest["table"],
        f"run_id={expected_manifest['run_id']}/data.csv",
        environ={"BRONZE_BUCKET": bucket, "LAKEHOUSE_STORAGE_ENV": "production", "NESSIE_REF": "gold_prod_current"},
    )
    if object_key != expected_object_key or expected_manifest.get("object_key") != expected_object_key:
        raise HistoricalPipelineUnavailable(f"Bronze object key mismatch: {object_key}")

    persisted_data = _read_object(client, bucket, object_key)
    actual_checksum = hashlib.sha256(persisted_data).hexdigest()
    if persisted_data != expected_data or actual_checksum != expected_manifest["checksum_sha256"]:
        raise HistoricalPipelineUnavailable(f"Bronze checksum mismatch: {object_key}")
    try:
        rows = list(csv.reader(io.StringIO(persisted_data.decode("utf-8"))))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise HistoricalPipelineUnavailable(f"Bronze data is not valid CSV: {object_key}") from exc
    if len(rows) < 1 or len(rows) - 1 != expected_row_count:
        raise HistoricalPipelineUnavailable(f"Bronze row count mismatch: {object_key}")

    persisted_manifest = read_json_object(client, bucket, manifest_key)
    for field in ("table", "run_id", "object_key", "row_count", "checksum_sha256"):
        if persisted_manifest.get(field) != expected_manifest.get(field):
            raise HistoricalPipelineUnavailable(f"Bronze manifest mismatch: {manifest_key}")
    if persisted_manifest.get("status") != "VERIFIED":
        raise HistoricalPipelineUnavailable(f"Bronze manifest is not verified: {manifest_key}")


def _silver_output_checksum(client, bucket, objects):
    checksums = []
    for object_key in objects:
        data = _read_object(client, bucket, object_key)
        checksums.append({"object_key": object_key, "checksum_sha256": hashlib.sha256(data).hexdigest()})
    canonical = json.dumps(checksums, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest(), checksums


def _verify_silver_readback(client, spark, bucket, target, data_prefix, manifest_key, expected_payload):
    expected_objects = sorted(expected_payload.get("objects", []))
    persisted_objects = sorted(
        item.object_name
        for item in client.list_objects(bucket, prefix=data_prefix, recursive=True)
        if item.object_name.endswith(".parquet")
    )
    if not expected_objects or persisted_objects != expected_objects:
        raise HistoricalPipelineUnavailable(f"Silver Parquet object set mismatch: {expected_payload['table']}")
    parquet_targets = [f"s3a://{bucket}/{object_key}" for object_key in persisted_objects]
    try:
        persisted_frame = spark.read.parquet(*parquet_targets)
        persisted_row_count = persisted_frame.count()
    except Exception as exc:
        raise HistoricalPipelineUnavailable(f"Silver persisted Parquet readback failed: {expected_payload['table']}") from exc
    if persisted_row_count != expected_payload["row_count"]:
        raise HistoricalPipelineUnavailable(f"Silver persisted row count mismatch: {expected_payload['table']}")
    actual_checksum, object_checksums = _silver_output_checksum(client, bucket, expected_payload["objects"])
    if actual_checksum != expected_payload["checksum_sha256"]:
        raise HistoricalPipelineUnavailable(f"Silver output checksum mismatch: {expected_payload['table']}")
    persisted_manifest = read_json_object(client, bucket, manifest_key)
    expected_manifest = {**expected_payload, "object_checksums": object_checksums}
    expected_manifest.pop("manifest_identity", None)
    if persisted_manifest != expected_manifest:
        raise HistoricalPipelineUnavailable(f"Silver manifest mismatch: {manifest_key}")


def _stop_spark_without_masking(spark):
    stop = getattr(spark, "stop", None)
    if callable(stop):
        try:
            stop()
        except Exception:
            # Never replace the original extraction/write/initialization error.
            pass


def run_historical_bronze(
    plan: Mapping,
    environ=None,
    *,
    connection_factory: Callable | None = None,
    minio_factory: Callable | None = None,
):
    """Extract one fixed snapshot and publish immutable Bronze manifests/data."""
    values = dict(os.environ if environ is None else environ)
    if plan.get("mode") == "DRY_RUN":
        return {"status": "DRY_RUN", "run_id": plan["run_id"], "tables": list(PARENT_FIRST_ORDER)}
    if plan.get("mode") != "WRITE":
        raise HistoricalPipelineUnavailable("historical mode is invalid")
    production_environment(values, candidate_ref=plan["candidate_ref"])
    require_write_approval(values)
    config = HistoricalBackfillConfig(plan["run_id"], datetime.fromisoformat(plan["capture_end"]), plan.get("batch_size", 1000), dry_run=False).validate()
    connect = connection_factory or (lambda: _connection_factory(values))
    make_minio = minio_factory or _minio_factory
    conn = connect()
    try:
        extracted = _extract_all(conn, config.capture_end, config.batch_size)
        _assert_parent_first_fk(extracted)
        client = make_minio()
        manifests = {}
        for table in PARENT_FIRST_ORDER:
            item = extracted[table]
            data = csv_bytes(item["columns"], item["rows"])
            object_key = bronze_table_key(table, f"run_id={config.run_id}/data.csv", environ=values)
            manifest = bronze_manifest(table, config.run_id, item["columns"], item["rows"], bucket=PRODUCTION_BUCKET)
            manifest["object_key"] = object_key
            manifest_key = bronze_table_key(table, f"run_id={config.run_id}/manifest.json", environ=values)
            client._put_object(PRODUCTION_BUCKET, object_key, data, headers={"Content-Type": "text/csv", "If-None-Match": "*"})
            put_json_object_if_absent(client, PRODUCTION_BUCKET, manifest_key, manifest)
            _verify_bronze_readback(client, PRODUCTION_BUCKET, object_key, manifest_key, manifest, data, len(item["rows"]))
            manifests[table] = {"status": "PASS", "table": table, "object_key": object_key, "manifest_key": manifest_key, "row_count": len(item["rows"]), "checksum_sha256": manifest["checksum_sha256"], "records": item["records"]}
        return {"status": "PASS", "run_id": config.run_id, "bronze": manifests}
    except Exception as exc:
        if isinstance(exc, HistoricalPipelineUnavailable):
            raise
        raise HistoricalPipelineUnavailable("historical Bronze extraction/upload failed") from exc
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()


def run_historical_silver(
    bronze_result: Mapping,
    plan: Mapping,
    environ=None,
    *,
    runtime_factory: Callable | None = None,
):
    """Build six immutable Silver Parquet artifacts from verified Bronze."""
    if plan.get("mode") == "DRY_RUN":
        return {"status": "DRY_RUN", "artifacts": {}}
    if not isinstance(bronze_result, Mapping) or bronze_result.get("status") != "PASS":
        raise HistoricalPipelineUnavailable("Silver requires verified Bronze output")
    values = dict(os.environ if environ is None else environ)
    production_environment(values, candidate_ref=plan["candidate_ref"])
    factory = runtime_factory
    spark = None
    if factory is None:
        try:
            from common.minio import configure_s3a, create_minio_client
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("HistoricalSilver").getOrCreate()
            configure_s3a(spark)
            runtime = {"spark": spark, "client": create_minio_client()}
        except Exception as exc:
            _stop_spark_without_masking(spark)
            raise HistoricalPipelineUnavailable("Spark/MinIO runtime is unavailable") from exc
    else:
        runtime = factory(plan, values)
    spark, client = runtime.get("spark"), runtime.get("client")
    if spark is None or client is None:
        raise HistoricalPipelineUnavailable("Silver runtime did not provide Spark and MinIO clients")
    artifacts = {}
    orders_coverage = None
    orders_reference = None
    try:
        for table in PARENT_FIRST_ORDER:
            bronze = bronze_result["bronze"][table]
            source = f"s3a://{PRODUCTION_BUCKET}/{bronze['object_key']}"
            data_prefix = silver_table_key(table, f"run_id={plan['run_id']}/", environ=values)
            target = f"s3a://{PRODUCTION_BUCKET}/{data_prefix}"
            frame = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", "true")
                .csv(source)
            )
            row_count = frame.count()
            if row_count != bronze["row_count"] or row_count <= 0:
                raise HistoricalPipelineUnavailable(f"Silver row count mismatch: {table}")
            frame.write.mode("errorifexists").parquet(target)
            parquet_objects = discover_parquet_objects(
                client, PRODUCTION_BUCKET, data_prefix, include_etag=True
            )
            objects = [item["object_key"] for item in parquet_objects]
            batch_id = hashlib.sha256(f"{plan['run_id']}:{table}".encode()).hexdigest()[:32]
            output_checksum, object_checksums = _silver_output_checksum(client, PRODUCTION_BUCKET, objects)
            manifest_payload = {
                "status": "COMPLETED",
                "table_name": table,
                "data_prefix": data_prefix,
                "parquet_objects": parquet_objects,
                # Keep the historical verification and coverage fields.
                "table": table,
                "run_id": plan["run_id"],
                "batch_id": batch_id,
                "row_count": row_count,
                "objects": objects,
                "object_checksums": object_checksums,
                "checksum_sha256": output_checksum,
            }
            manifest_identity = hashlib.sha256(json.dumps(manifest_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            manifest_key = silver_table_key(table, f"run_id={plan['run_id']}/manifest.json", environ=values)
            put_json_object_if_absent(client, PRODUCTION_BUCKET, manifest_key, manifest_payload)
            _verify_silver_readback(client, spark, PRODUCTION_BUCKET, target, data_prefix, manifest_key, {**manifest_payload, "manifest_identity": manifest_identity})
            coverage = {
                "runtime_window_id": plan["run_id"],
                "coverage_identity": hashlib.sha256(
                    f"{plan['run_id']}:{table}:{batch_id}".encode("utf-8")
                ).hexdigest(),
            }
            if table in ("payments", "shipments"):
                if orders_coverage is None or orders_reference is None:
                    raise HistoricalPipelineUnavailable(
                        f"Historical Silver parent coverage is unavailable: {table}"
                    )
                coverage.update(
                    {
                        "parent_coverage_identity": orders_coverage["coverage_identity"],
                        "parent_orders_reference": dict(orders_reference),
                    }
                )
            lineage_artifact = build_parquet_lineage_artifact(
                table=table,
                bucket=PRODUCTION_BUCKET,
                manifest_key=manifest_key,
                data_prefix=data_prefix,
                manifest_identity=manifest_identity,
                batch_id=batch_id,
                row_count=row_count,
                coverage=coverage,
            )
            artifacts[table] = {
                **lineage_artifact,
                "source_table": table,
                "artifact_identity": manifest_identity,
                "object_key": objects[0],
                "batch_id": batch_id,
                "checksum_sha256": output_checksum,
                "row_count": row_count,
            }
            if table == "orders":
                orders_coverage = coverage
                orders_reference = {
                    "table": "orders",
                    "batch_id": batch_id,
                    "manifest_key": manifest_key,
                    "manifest_identity": manifest_identity,
                }
        return {"status": "PASS", "artifacts": require_verified_silver_artifacts(artifacts)}
    finally:
        _stop_spark_without_masking(spark)


__all__ = ["HistoricalPipelineUnavailable", "run_historical_bronze", "run_historical_silver"]
