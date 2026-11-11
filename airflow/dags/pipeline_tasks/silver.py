"""Silver TaskFlow tasks for the controlled incremental DAG."""

import json
import logging
import os
import re

from airflow.exceptions import AirflowSkipException
try:
    from airflow.sdk import get_current_context, task
except ImportError:
    try:
        from airflow.decorators import get_current_context, task
    except ImportError:
        def get_current_context():
            raise RuntimeError("Airflow runtime context is unavailable")
        def task(*_args, **_kwargs):
            return lambda fn: fn
from minio import Minio

from pipeline_contracts import TABLES
from pipeline_silver import get_silver_data_prefix, get_silver_manifest_key
from common.paths import bronze_incremental_object_key


@task.branch(retries=0)
def check_silver_eligibility(table_name, validation_result):
    if not isinstance(validation_result, dict):
        raise ValueError("Missing Bronze validation result")
    if validation_result.get("table_name") != table_name:
        raise ValueError("Bronze validation table mismatch")
    status, reason, mode = (
        validation_result.get("status"),
        validation_result.get("reason"),
        validation_result.get("mode"),
    )
    if mode == "DRY_RUN":
        if status != "SKIPPED" or reason != "DRY_RUN":
            raise ValueError("Invalid DRY_RUN validation result")
        logging.getLogger("airflow.task").info("Silver transformation disabled in DRY_RUN | table=%s", table_name)
        return f"skip_silver_{table_name}"
    if mode != "WRITE":
        raise ValueError(f"Unsupported mode: {mode}")
    if status == "SKIPPED" and reason == "NO_DATA":
        return f"skip_silver_{table_name}"
    if status != "PASS" or validation_result.get("checksum_verified") is not True:
        raise ValueError(f"Bronze validation did not PASS: {table_name}")
    batch_id = validation_result.get("batch_id")
    if not isinstance(batch_id, str) or not re.fullmatch(r"[0-9a-f]{32}", batch_id):
        raise ValueError("Invalid Bronze batch ID")
    if validation_result.get("object_key") != bronze_incremental_object_key(table_name, batch_id):
        raise ValueError("Bronze object key mismatch")
    checksum = validation_result.get("bronze_checksum_sha256")
    if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
        raise ValueError("Invalid Bronze checksum")
    return f"bronze_to_silver_{table_name}"


@task(retries=0)
def skip_silver(table_name, validation_result):
    if not isinstance(validation_result, dict) or validation_result.get("table_name") != table_name:
        raise ValueError("Silver skip Bronze table mismatch")
    reason = validation_result.get("reason")
    if validation_result.get("status") != "SKIPPED" or reason not in ("DRY_RUN", "NO_DATA"):
        raise ValueError(f"Invalid Silver skip condition: {table_name}")
    return {"table_name": table_name, "status": "SKIPPED", "reason": reason}


@task(retries=0)
def verify_silver_manifest(table_name, validation_result):
    if not isinstance(validation_result, dict) or validation_result.get("table_name") != table_name or validation_result.get("status") != "PASS" or validation_result.get("checksum_verified") is not True:
        raise ValueError(f"Invalid Bronze validation for {table_name}")
    batch_id = validation_result.get("batch_id")
    manifest_key = get_silver_manifest_key(table_name, batch_id)
    client = Minio("minio:9000", access_key=os.environ["MINIO_ACCESS_KEY"], secret_key=os.environ["MINIO_SECRET_KEY"], secure=False)
    response = client.get_object(os.environ["BRONZE_BUCKET"], manifest_key)
    try:
        manifest = json.loads(response.read().decode("utf-8"))
    finally:
        response.close(); response.release_conn()
    prefix = get_silver_data_prefix(table_name, batch_id)
    if not isinstance(manifest, dict) or manifest.get("status") != "COMPLETED" or manifest.get("table_name") != table_name or manifest.get("batch_id") != batch_id or manifest.get("bronze_object_key") != validation_result["object_key"] or manifest.get("bronze_checksum_sha256") != validation_result["bronze_checksum_sha256"]:
        raise ValueError("Silver manifest metadata mismatch")
    row_count, data_prefix, objects = manifest.get("silver_row_count"), manifest.get("data_prefix"), manifest.get("parquet_objects")
    if type(row_count) is not int or row_count < 0 or not isinstance(data_prefix, str) or not data_prefix.startswith(prefix) or not re.fullmatch(rf"(?:[A-Za-z0-9._-]+/[A-Za-z0-9._-]+/)?silver_incremental/{table_name}/batch_id={batch_id}/runs/[0-9a-f]{{32}}/", data_prefix):
        raise ValueError("Invalid Silver manifest output metadata")
    if not isinstance(objects, list) or not objects or manifest.get("parquet_file_count") != len(objects):
        raise ValueError("Silver manifest has invalid Parquet metadata")
    expected = sorted(objects, key=lambda item: item["object_key"])
    if len({item["object_key"] for item in expected}) != len(expected) or any(not isinstance(item, dict) or not isinstance(item.get("object_key"), str) or not item["object_key"].startswith(data_prefix) or not item["object_key"].endswith(".parquet") or type(item.get("size")) is not int or item["size"] <= 0 for item in expected):
        raise ValueError("Invalid Silver Parquet metadata")
    actual = sorted([{"object_key": item.object_name, "size": item.size, "etag": item.etag.strip('"')} for item in client.list_objects(os.environ["BRONZE_BUCKET"], prefix=data_prefix, recursive=True) if item.object_name.endswith(".parquet")], key=lambda item: item["object_key"])
    if actual != expected:
        raise ValueError("Silver Parquet objects do not match completion manifest")
    result = {"table_name": table_name, "status": "PASS", "batch_id": batch_id, "manifest_key": manifest_key, "silver_row_count": row_count}
    # Native Gold consumes the verified Silver Parquet manifest directly;
    # absence remains fail-closed in the lineage builder.
    if "dimensional_artifact" in manifest:
        result["dimensional_artifact"] = manifest["dimensional_artifact"]
    return result


@task(trigger_rule="none_failed_min_one_success", retries=0)
def silver_result(table_name, bronze_result):
    ti = get_current_context()["ti"]
    skip_result = ti.xcom_pull(task_ids=f"skip_silver_{table_name}")
    verify_result = ti.xcom_pull(task_ids=f"verify_silver_{table_name}")
    if not isinstance(bronze_result, dict) or bronze_result.get("table_name") != table_name:
        raise ValueError("Bronze table mismatch")
    mode = bronze_result.get("mode")
    if mode == "DRY_RUN": expected_status, expected_reason = "SKIPPED", "DRY_RUN"
    elif mode == "WRITE" and bronze_result.get("status") == "SKIPPED" and bronze_result.get("reason") == "NO_DATA": expected_status, expected_reason = "SKIPPED", "NO_DATA"
    elif mode == "WRITE" and bronze_result.get("status") == "PASS": expected_status, expected_reason = "PASS", None
    else: raise ValueError("Invalid pipeline Bronze status")
    if expected_status == "SKIPPED":
        if not isinstance(skip_result, dict) or skip_result.get("table_name") != table_name or skip_result.get("status") != "SKIPPED" or skip_result.get("reason") != expected_reason or verify_result is not None:
            raise RuntimeError("Invalid Silver skip result")
        return skip_result
    if not isinstance(verify_result, dict) or verify_result.get("table_name") != table_name or verify_result.get("status") != "PASS" or verify_result.get("batch_id") != bronze_result.get("batch_id") or skip_result is not None:
        raise RuntimeError("Invalid Silver verification result")
    return verify_result


def build_pipeline_summary(silver_results: dict, config: dict):
    """Validate every table result passed explicitly by the Silver graph."""
    if not isinstance(config, dict) or config.get("mode") not in ("DRY_RUN", "WRITE") or not isinstance(config.get("tables"), list):
        raise RuntimeError("PIPELINE_ABORTED: validate_config failed or produced no valid configuration")
    if not isinstance(silver_results, dict) or set(silver_results) != set(config["tables"]):
        raise RuntimeError("PIPELINE_ABORTED: incomplete Silver result inputs")
    completed, failed = {}, []
    for table_name in config["tables"]:
        result = silver_results[table_name]
        valid = isinstance(result, dict) and result.get("table_name") == table_name
        if valid and config["mode"] == "DRY_RUN":
            valid = result.get("status") == "SKIPPED" and result.get("reason") == "DRY_RUN"
        elif valid and config["mode"] == "WRITE":
            valid = (result.get("status") == "SKIPPED" and result.get("reason") == "NO_DATA") or (result.get("status") == "PASS" and isinstance(result.get("batch_id"), str) and bool(re.fullmatch(r"[0-9a-f]{32}", result["batch_id"])) and result.get("manifest_key") == get_silver_manifest_key(table_name, result["batch_id"]) and type(result.get("silver_row_count")) is int and result["silver_row_count"] >= 0)
        else:
            valid = False
        if valid: completed[table_name] = result
        else: failed.append(table_name)
    if failed:
        raise RuntimeError(f"Incremental validation incomplete or failed: {sorted(failed)}")
    return {"mode": config["mode"], "status": "PASS", "tables": sorted(completed), "results": completed}


@task(trigger_rule="all_done", retries=0)
def pipeline_summary(silver_results: dict):
    """Task wrapper retained for compatibility with standalone Silver runs."""
    ti = get_current_context()["ti"]
    config = ti.xcom_pull(task_ids="validate_config")
    return build_pipeline_summary(silver_results, config)
