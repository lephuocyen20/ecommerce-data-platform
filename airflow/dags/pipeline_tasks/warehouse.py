"""Warehouse and dbt TaskFlow tasks for the incremental DAG."""

import logging
import os
import re
import sys

from airflow.sdk import task

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from common.runtime_policy import is_feature_enabled, require_feature_enabled
from common.lakehouse_policy import ICEBERG, customers_silver_source, require_iceberg_cutover_enabled
from pipeline_contracts import TABLES


@task(trigger_rule="none_failed_min_one_success", retries=0)
def warehouse_gate(summary: dict, iceberg_checkpoint=None):
    """Validate pipeline summary before Warehouse loading."""
    if not isinstance(summary, dict):
        raise ValueError("Missing pipeline summary")
    if summary.get("status") != "PASS":
        raise RuntimeError("Pipeline summary is not PASS")
    mode, tables, results = summary.get("mode"), summary.get("tables"), summary.get("results")
    if mode not in ("DRY_RUN", "WRITE"):
        raise ValueError("Invalid pipeline mode")
    if (not isinstance(tables, list) or not tables or any(not isinstance(t, str) for t in tables)
            or len(tables) != len(set(tables)) or set(tables) - set(TABLES)):
        raise ValueError("Invalid Warehouse table selection")
    if not isinstance(results, dict):
        raise ValueError("Missing Silver results")
    if set(results) != set(tables):
        raise ValueError("Silver results do not match selected tables")
    for table_name in tables:
        result = results[table_name]
        if not isinstance(result, dict):
            raise ValueError(f"Invalid Silver result: {table_name}")
        if result.get("table_name") != table_name:
            raise ValueError(f"Silver table mismatch: {table_name}")
        if mode == "DRY_RUN":
            valid = result.get("status") == "SKIPPED" and result.get("reason") == "DRY_RUN"
        else:
            valid = (
                result.get("status") == "PASS"
                and isinstance(result.get("batch_id"), str)
                and bool(re.fullmatch(r"[0-9a-f]{32}", result["batch_id"]))
                and result.get("manifest_key") == f"silver_incremental/{table_name}/batch_id={result['batch_id']}/_BATCH_METADATA.json"
                and type(result.get("silver_row_count")) is int
                and result["silver_row_count"] >= 0
            ) or (result.get("status") == "SKIPPED" and result.get("reason") == "NO_DATA")
        if not valid:
            raise RuntimeError(f"Warehouse Gate rejected Silver result: {table_name}")
    if mode == "DRY_RUN":
        return {
            "status": "SKIP",
            "reason": "DRY_RUN",
            "mode": mode,
            "tables": tables,
        }
    ready_tables = [table_name for table_name in tables if results[table_name]["status"] == "PASS"]
    # Customers are loaded by the separately gated Iceberg adapter after its
    # dual-run reconciliation. Do not present the same batch to strict legacy
    # Parquet selection, otherwise its existing checkpoint is interpreted as a
    # missing batch.
    if customers_silver_source() == ICEBERG:
        require_iceberg_cutover_enabled()
        if not isinstance(iceberg_checkpoint, dict) or iceberg_checkpoint.get("source_type") != ICEBERG:
            raise RuntimeError("Iceberg customers checkpoint was not verified before Warehouse cutover")
        ready_tables = [table_name for table_name in ready_tables if table_name != "customers"]
    if not ready_tables:
        return {
            "status": "SKIP",
            "reason": "NO_DATA",
            "mode": mode,
            "tables": tables,
        }
    ready_batch_ids = [results[table_name]["batch_id"] for table_name in ready_tables]
    if len(ready_batch_ids) != len(set(ready_batch_ids)):
        raise ValueError("Duplicate Warehouse batch IDs")
    return {
        "status": "READY",
        "reason": "SILVER_VERIFIED",
        "mode": mode,
        "tables": ready_tables,
        "batch_ids": ready_batch_ids,
    }


@task(retries=0)
def load_warehouse(gate_result: dict):
    """Load verified Silver batches into Warehouse Raw."""
    logger = logging.getLogger("airflow.task")
    if not isinstance(gate_result, dict):
        raise ValueError("Missing Warehouse Gate result")
    mode, status = gate_result.get("mode"), gate_result.get("status")
    if mode not in ("DRY_RUN", "WRITE"):
        raise ValueError("Invalid pipeline mode")
    if status == "SKIP":
        reason = gate_result.get("reason")
        if (mode == "DRY_RUN" and reason != "DRY_RUN") or (mode == "WRITE" and reason != "NO_DATA"):
            raise ValueError("Invalid Warehouse skip reason")
        logger.info("Warehouse loading skipped | reason=%s", reason)
        return {"status": "SKIPPED", "reason": reason}
    if status != "READY" or mode != "WRITE":
        raise ValueError("Invalid Warehouse loading state")
    if gate_result.get("reason") != "SILVER_VERIFIED":
        raise ValueError("Warehouse Gate verification missing")
    tables, batch_ids = gate_result.get("tables"), gate_result.get("batch_ids")
    if (not isinstance(tables, list) or not tables or any(t not in TABLES for t in tables)
            or len(tables) != len(set(tables))):
        raise ValueError("Invalid Warehouse table selection")
    if (not isinstance(batch_ids, list) or len(batch_ids) != len(tables)
            or any(not isinstance(b, str) or not re.fullmatch(r"[0-9a-f]{32}", b) for b in batch_ids)
            or len(batch_ids) != len(set(batch_ids))):
        raise ValueError("Invalid Warehouse batch selection")
    if not is_feature_enabled("WAREHOUSE_WRITE_ENABLED"):
        logger.info("Warehouse WRITE disabled | tables=%s | batches=%s", tables, batch_ids)
        return {"status": "SKIPPED", "reason": "WAREHOUSE_WRITE_DISABLED", "tables": tables, "batch_ids": batch_ids}
    require_feature_enabled("INGESTION_WRITE_ENABLED", "Ingestion WRITE permission revoked")
    project_path = "/opt/airflow/project/warehouse/loader"
    if project_path not in sys.path:
        sys.path.insert(0, project_path)
    from warehouse_orchestrator import process_batches
    logger.info("Starting Warehouse loading | tables=%s | batches=%s", tables, batch_ids)
    result = process_batches(dry_run=False, selected_tables=tables, selected_batch_ids=batch_ids, strict_batch_selection=True)
    expected_count = len(batch_ids)
    if (not isinstance(result, dict) or result.get("status") != "PASS" or result.get("mode") != "COMMIT"
            or result.get("strict_batch_selection") is not True or result.get("selected_tables") != tables
            or result.get("selected_batch_ids") != batch_ids or type(result.get("pending_batches")) is not int
            or result["pending_batches"] != expected_count or type(result.get("loaded_batches")) is not int
            or result["loaded_batches"] != expected_count or type(result.get("skipped_batches")) is not int
            or result["skipped_batches"] != 0 or type(result.get("loaded_rows")) is not int or result["loaded_rows"] < 0):
        raise RuntimeError(f"Warehouse Orchestrator result verification failed: {result}")
    from incremental_silver_to_warehouse import get_connection
    verify_conn = get_connection()
    try:
        with verify_conn.cursor() as cursor:
            cursor.execute("SELECT batch_id, table_name, loaded_row_count FROM warehouse.loaded_batches WHERE batch_id = ANY(%s)", (batch_ids,))
            checkpoint_rows = cursor.fetchall()
    finally:
        verify_conn.close()
    checkpoints = {batch_id: (table_name, row_count) for batch_id, table_name, row_count in checkpoint_rows}
    if len(checkpoints) != len(batch_ids):
        raise RuntimeError("Warehouse checkpoint verification failed: missing or duplicate batch IDs")
    for batch_id, expected_table in dict(zip(batch_ids, tables)).items():
        checkpoint = checkpoints.get(batch_id)
        if checkpoint is None:
            raise RuntimeError(f"Missing Warehouse checkpoint: {batch_id}")
        actual_table, loaded_row_count = checkpoint
        if actual_table != expected_table:
            raise RuntimeError(f"Warehouse checkpoint table mismatch: {batch_id}")
        if type(loaded_row_count) is not int or loaded_row_count < 0:
            raise RuntimeError(f"Invalid Warehouse checkpoint row count: {batch_id}")
    checkpoint_total_rows = sum(checkpoint[1] for checkpoint in checkpoints.values())
    if checkpoint_total_rows != result["loaded_rows"]:
        raise RuntimeError("Warehouse loaded row count does not match checkpoints")
    logger.info("Warehouse checkpoints verified | batches=%s | rows=%s", len(checkpoints), checkpoint_total_rows)
    logger.info("Warehouse loading verified | batches=%s | rows=%s", result["loaded_batches"], result["loaded_rows"])
    return {"status": "COMPLETED", "mode": "COMMIT", "tables": tables, "batch_ids": batch_ids,
            "loaded_batches": result["loaded_batches"], "loaded_rows": result["loaded_rows"], "skipped_batches": result["skipped_batches"]}


@task.branch
def check_dbt_eligibility(warehouse_result: dict):
    if not isinstance(warehouse_result, dict):
        raise RuntimeError("Invalid Warehouse result")
    status = warehouse_result.get("status")
    if status == "SKIPPED":
        return "skip_dbt"
    if status != "COMPLETED":
        raise RuntimeError(f"Unexpected Warehouse status: {status}")
    if warehouse_result.get("mode") != "COMMIT":
        raise RuntimeError("dbt requires Warehouse COMMIT mode")
    if not warehouse_result.get("batch_ids"):
        raise RuntimeError("Warehouse completed without batch IDs")
    return "dbt_build"


@task(task_id="skip_dbt")
def skip_dbt():
    return {"status": "SKIPPED", "reason": "WAREHOUSE_NOT_COMPLETED"}
