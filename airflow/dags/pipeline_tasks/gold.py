"""Gold TaskFlow tasks for the incremental DAG."""

import sys

from airflow.sdk import task

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from common.runtime_policy import is_feature_enabled
from gold_gate_policy import decide_gold_snapshot_gate
from pipeline_gold import (
    build_gold_source_snapshot_payload,
    resolve_silver_snapshot_lineage,
)


@task.branch(retries=0)
def gold_snapshot_gate(pipeline_result: dict):
    return decide_gold_snapshot_gate(
        pipeline_result,
        write_enabled=is_feature_enabled("GOLD_SNAPSHOT_WRITE_ENABLED"),
    )


@task(retries=0)
def skip_gold_layer():
    return {
        "status": "SKIPPED",
        "reason": "GOLD_SNAPSHOT_NOT_ENABLED_OR_SILVER_NOT_WRITABLE",
    }


@task(task_id="resolve_gold_snapshot_lineage", retries=0)
def resolve_gold_snapshot_lineage_task():
    return resolve_silver_snapshot_lineage()


@task(task_id="prepare_gold_dataset_sources", retries=0)
def prepare_gold_dataset_sources_task(dataset_name: str, silver_snapshots: dict):
    return build_gold_source_snapshot_payload(dataset_name, silver_snapshots)
