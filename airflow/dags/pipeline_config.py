"""Pure validation for incremental Airflow DAG run configuration."""

import sys

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from common.runtime_policy import DRY_RUN, WRITE, require_feature_enabled, validate_pipeline_mode
from pipeline_contracts import TABLES


def validate_incremental_run_config(conf):
    """Validate a DAG run config and return its canonical mode/table selection."""
    if not isinstance(conf, dict):
        raise ValueError("DAG configuration must be an object")

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

    return {"mode": mode, "tables": selected_tables}
