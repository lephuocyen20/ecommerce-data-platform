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
from pipeline_tasks.gold import build_dimensional_parquet_artifact_task
from pipeline_tasks.ingestion import ingest_incremental, validate_config, validate_incremental
from pipeline_tasks.silver import check_silver_eligibility, silver_result, skip_silver, verify_silver_manifest

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
    ingestion = {}
    validation = {}
    with TaskGroup(group_id="bronze", prefix_group_id=False):
        for table in TABLES:
            with TaskGroup(group_id=f"bronze_{table}", prefix_group_id=False):
                ingestion[table] = ingest_incremental.override(task_id=f"ingest_{table}")(table, config)
                validation[table] = validate_incremental.override(task_id=f"validate_{table}")(table, ingestion[table])

    eligibility = {}
    transforms = {}
    verify = {}
    results = {}
    dimensional_artifacts = {}
    with TaskGroup(group_id="silver_parquet", prefix_group_id=False):
        for table in TABLES:
            with TaskGroup(group_id=f"silver_{table}", prefix_group_id=False):
                eligibility[table] = check_silver_eligibility.override(task_id=f"check_silver_eligibility_{table}")(table, validation[table])
                transforms[table] = create_incremental_silver_operator(table_name=table, project_path=PROJECT_PATH)
                verify[table] = verify_silver_manifest.override(task_id=f"verify_silver_{table}")(table, validation[table])
                results[table] = silver_result.override(task_id=f"silver_result_{table}")(table, validation[table])
                skip = skip_silver.override(task_id=f"skip_silver_{table}")(table, validation[table])
                eligibility[table] >> [skip, transforms[table]]
                transforms[table] >> verify[table]
                [skip, verify[table]] >> results[table]
                dimensional_artifacts[table] = build_dimensional_parquet_artifact_task.override(
                    task_id=f"dimensional_parquet_artifact_{table}"
                )(table, results[table])
                verify[table] >> dimensional_artifacts[table]
    dimensional_writer = create_gold_dimensional_operator(
        project_path=PROJECT_PATH,
        mode=os.environ.get("GOLD_DIMENSIONAL_MODE", "DRY_RUN"),
    )
    list(dimensional_artifacts.values()) >> dimensional_writer

    with TaskGroup(group_id="gold_dimensions", prefix_group_id=False):
        dimension_confirmations = [
            create_gold_dimensional_confirmation_operator(table_name=table)
            for table in ("dim_date", "dim_customer", "dim_product")
        ]
    with TaskGroup(group_id="gold_facts", prefix_group_id=False):
        fact_confirmations = [
            create_gold_dimensional_confirmation_operator(table_name=table)
            for table in ("fact_orders", "fact_order_items", "fact_payments", "fact_shipments")
        ]
    confirmations = dimension_confirmations + fact_confirmations
    for confirmation in confirmations:
        dimensional_writer >> confirmation


ecommerce_incremental_ingestion()
