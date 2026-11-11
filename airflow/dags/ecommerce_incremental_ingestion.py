"""Controlled Bronze -> Silver -> Gold DAG; Warehouse/dbt is retired."""

import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import TaskGroup, dag

PROJECT_ROOT = "/opt/airflow/project"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline_contracts import TABLES
from pipeline_operators import (create_gold_dimensional_confirmation_operator, create_gold_dimensional_operator, create_incremental_silver_operator)
from pipeline_tasks.gold import (build_dimensional_parquet_artifact_task, gold_dimensional_gate, resolve_gold_dimensional_lineage_task, skip_gold_dimensional, verify_gold_dimensional_graph_manifest_task)
from pipeline_tasks.ingestion import ingest_incremental, validate_config, validate_incremental
from pipeline_tasks.silver import check_silver_eligibility, pipeline_summary, silver_result, skip_silver, verify_silver_manifest

PROJECT_PATH = os.environ.get("AIRFLOW_DOCKER_PROJECT_PATH", PROJECT_ROOT)


@dag(
    dag_id="ecommerce_incremental_ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["ecommerce", "incremental", "bronze-silver-gold"],
    max_active_runs=1,
)
def ecommerce_incremental_ingestion():
    config = validate_config()
    ingestion = {table: ingest_incremental.override(task_id=f"ingest_{table}")(table, config) for table in TABLES}
    validation = {table: validate_incremental.override(task_id=f"validate_{table}")(table, ingestion[table]) for table in TABLES}
    with TaskGroup(group_id="silver", prefix_group_id=False):
        eligibility = {table: check_silver_eligibility.override(task_id=f"check_silver_eligibility_{table}")(table, validation[table]) for table in TABLES}
        transforms = {table: create_incremental_silver_operator(table_name=table, project_path=PROJECT_PATH) for table in TABLES}
        verify = {table: verify_silver_manifest.override(task_id=f"verify_silver_{table}")(table, validation[table]) for table in TABLES}
        results = {table: silver_result.override(task_id=f"silver_result_{table}")(table, validation[table]) for table in TABLES}
        for table in TABLES:
            skip = skip_silver.override(task_id=f"skip_silver_{table}")(table, validation[table])
            eligibility[table] >> [skip, transforms[table]]
            transforms[table] >> verify[table]
            [skip, verify[table]] >> results[table]
    summary = pipeline_summary(results)
    dimensional_artifacts = {}
    for table in TABLES:
        artifact = build_dimensional_parquet_artifact_task.override(task_id=f"dimensional_parquet_artifact_{table}")(
            table, results[table]
        )
        verify[table] >> artifact
        dimensional_artifacts[table] = artifact

    dimensional_gate = gold_dimensional_gate(summary)
    # The dimensional branch must not decide before all six artifact contracts
    # have completed; otherwise a slow Silver artifact task can cause the
    # writer to be skipped while the DAG is still running.
    list(dimensional_artifacts.values()) >> dimensional_gate
    dimensional_lineage = resolve_gold_dimensional_lineage_task(dimensional_artifacts)
    dimensional_writer = create_gold_dimensional_operator(
        source_task_id="resolve_gold_dimensional_lineage",
        project_path=PROJECT_PATH,
        mode=os.environ.get("GOLD_DIMENSIONAL_MODE", "DRY_RUN"),
    )
    dimensional_skip = skip_gold_dimensional()
    dimensional_gate >> [dimensional_lineage, dimensional_skip]
    dimensional_lineage >> dimensional_writer
    dimensional_verify = verify_gold_dimensional_graph_manifest_task(dimensional_writer)
    dimensional_writer >> dimensional_verify
    confirmations = [
        create_gold_dimensional_confirmation_operator(table_name=table)
        for table in ("dim_date", "dim_customer", "dim_product", "fact_orders", "fact_order_items", "fact_payments", "fact_shipments")
    ]
    for confirmation in confirmations:
        dimensional_verify >> confirmation


ecommerce_incremental_ingestion()
