"""Pure validation for incremental Airflow DAG run configuration."""

import json
import sys
import os

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from common.runtime_policy import DRY_RUN, WRITE, require_feature_enabled, validate_pipeline_mode
from common.storage_layout import QUALITY_BUCKET, validate_storage_environment
try:
    from pipeline_contracts import TABLES
except ImportError:
    from airflow.dags.pipeline_contracts import TABLES


def _validate_json_payload(payload):
    """Reject payloads that cannot safely cross the Airflow XCom boundary."""
    json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return payload


def validate_incremental_run_config(conf):
    """Validate a DAG run config and return its canonical mode/table selection."""
    if not isinstance(conf, dict):
        raise ValueError("DAG configuration must be an object")

    # Validate explicit production/quality identities before downstream tasks
    # can construct a storage client. Legacy local profiles remain compatible
    # until they opt into the canonical layout contract.
    if os.environ.get("LAKEHOUSE_STORAGE_ENV") in {"production", "quality-it"}:
        validate_storage_environment(os.environ)

    # Explicitly bounded local E2E convenience only. Normal DAG runs still
    # require dag_run.conf; production-looking buckets cannot use this path.
    if not conf and os.environ.get("BRONZE_BUCKET") == QUALITY_BUCKET and os.environ.get("AIRFLOW_LOCAL_E2E_MODE") == WRITE:
        conf = {"mode": WRITE, "tables": list(TABLES)}

    mode = validate_pipeline_mode(conf.get("mode", DRY_RUN))
    selected_tables = conf.get("tables")

    if selected_tables is None:
        if mode == WRITE:
            raise ValueError("WRITE requires explicit table selection")
        selected_tables = list(TABLES)

    if (
        not isinstance(selected_tables, list)
        or not selected_tables
        or any(not isinstance(table_name, str) for table_name in selected_tables)
    ):
        raise ValueError("tables must be a non-empty list of strings")

    if len(selected_tables) != len(set(selected_tables)):
        raise ValueError("Duplicate tables are not allowed")

    unsupported = set(selected_tables) - set(TABLES)
    if unsupported:
        raise ValueError(f"Unsupported tables: {sorted(unsupported)}")

    if mode == WRITE:
        require_feature_enabled("INGESTION_WRITE_ENABLED")

    result = {"mode": mode, "tables": selected_tables}
    # Preserve only the explicitly named, JSON-safe controlled DRY_RUN input
    # for the downstream task.  The task validates its shape and remains
    # default-off; arbitrary runtime objects cannot enter this payload.
    if "controlled_dry_run_spec" in conf:
        result["controlled_dry_run_spec"] = _validate_json_payload(conf["controlled_dry_run_spec"])
    if "gold_snapshot_reuse_artifacts" in conf:
        result["gold_snapshot_reuse_artifacts"] = _validate_json_payload(conf["gold_snapshot_reuse_artifacts"])
    if "gold_snapshot_reuse_registry" in conf:
        from gold.implementation.dimensional.snapshot_reuse_registry import (
            validate_gold_snapshot_reuse_registry,
        )
        try:
            result["gold_snapshot_reuse_registry"] = validate_gold_snapshot_reuse_registry(
                conf["gold_snapshot_reuse_registry"]
            )
        except ValueError as exc:
            raise ValueError(f"Invalid gold_snapshot_reuse_registry: {exc}") from exc
    return result
