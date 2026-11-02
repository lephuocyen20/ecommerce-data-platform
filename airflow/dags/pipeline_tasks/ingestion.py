"""Ingestion TaskFlow tasks for the incremental DAG."""

import logging
import sys
from datetime import datetime

from airflow.exceptions import AirflowSkipException
from airflow.sdk import get_current_context, task

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from common.runtime_policy import DRY_RUN, WRITE, require_feature_enabled
from pipeline_config import validate_incremental_run_config


STAGING_ROOT = "/opt/airflow/project/staging"
BATCH_SIZE = 1000


@task(task_id="validate_config", retries=0)
def validate_config():
    """Validate and normalize the configuration for one DAG run."""
    conf = get_current_context()["dag_run"].conf or {}
    result = validate_incremental_run_config(conf)
    logging.getLogger("airflow.task").info(
        "Incremental configuration validated | mode=%s | tables=%s",
        result["mode"],
        result["tables"],
    )
    return result


def build_ingestion_summary(table_name, result):
    """Build the JSON-serializable ingestion payload stored in XCom."""
    if not isinstance(result, dict):
        raise ValueError(f"Invalid ingestion return type: {table_name}")

    def serialize(value):
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {key: serialize(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [serialize(item) for item in value]
        return value

    return serialize({
        "table_name": table_name,
        "mode": result.get("mode"),
        "action": result.get("action"),
        "status": result.get("status"),
        "batch_id": result.get("batch_id"),
        "row_count": result.get("row_count"),
        "persisted": result.get("persisted"),
        "watermark": result.get("watermark"),
        "recoverable_batches": result.get("recoverable_batches", []),
    })


@task
def ingest_incremental(table_name: str, config: dict):
    if table_name not in config["tables"]:
        raise AirflowSkipException(f"Table {table_name} was not selected")
    project_path = "/opt/airflow/project"
    if project_path not in sys.path:
        sys.path.insert(0, project_path)
    from ingestion.incremental.orchestrator import (
        run_incremental_ingestion,
        run_incremental_ingestion_write,
    )
    logger = logging.getLogger("airflow.task")
    mode = config["mode"]
    logger.info("Starting incremental ingestion | table=%s | mode=%s", table_name, mode)
    if mode == DRY_RUN:
        result = run_incremental_ingestion(
            table_name=table_name, batch_size=BATCH_SIZE, dry_run=True, rollback_test=False
        )
    elif mode == WRITE:
        require_feature_enabled("INGESTION_WRITE_ENABLED", "WRITE blocked at ingestion task")
        result = run_incremental_ingestion_write(
            table_name=table_name, batch_size=BATCH_SIZE, staging_root=STAGING_ROOT, allow_write=True
        )
    else:
        raise ValueError(f"Unsupported mode: {mode}")
    summary = build_ingestion_summary(table_name, result)
    logger.info("Incremental ingestion completed | result=%s", summary)
    return summary


@task
def validate_incremental(table_name: str, ingestion_result: dict):
    logger = logging.getLogger("airflow.task")
    if not isinstance(ingestion_result, dict):
        raise ValueError(f"Missing ingestion result for {table_name}")
    if ingestion_result.get("table_name") != table_name:
        raise ValueError(f"Ingestion result table mismatch: {table_name}")
    mode, status, batch_id = (
        ingestion_result.get("mode"), ingestion_result.get("status"), ingestion_result.get("batch_id")
    )
    if mode == DRY_RUN:
        if (
            status is not None or batch_id is not None or ingestion_result.get("persisted") is not None
            or not isinstance(ingestion_result.get("action"), str) or not ingestion_result["action"]
            or not isinstance(ingestion_result.get("recoverable_batches"), list)
        ):
            raise ValueError(f"Invalid DRY_RUN ingestion result: {table_name}")
        logger.info("Skipping Bronze validation in DRY_RUN | table=%s", table_name)
        return {
            "table_name": table_name,
            "mode": mode,
            "status": "SKIPPED",
            "reason": "DRY_RUN",
        }
    if mode != WRITE:
        raise ValueError(f"Unexpected ingestion mode: {mode}")
    if status == "NO_DATA":
        if batch_id is not None:
            raise ValueError("NO_DATA must not contain a batch_id")
        logger.info("No incremental batch to validate | table=%s", table_name)
        return {
            "table_name": table_name,
            "mode": mode,
            "status": "SKIPPED",
            "reason": "NO_DATA",
        }
    if status != "COMMITTED":
        raise RuntimeError(f"Unexpected ingestion status: {status}")
    if not isinstance(batch_id, str) or not batch_id:
        raise ValueError(f"COMMITTED result missing batch_id: {table_name}")
    project_path = "/opt/airflow/project"
    if project_path not in sys.path:
        sys.path.insert(0, project_path)
    from validation.validate_incremental_bronze import validate_incremental_batch
    result = validate_incremental_batch(table_name=table_name, batch_id=batch_id)
    logger.info("Incremental Bronze validation completed | result=%s", result)
    return {**result, "mode": mode}
