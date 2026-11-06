"""Airflow graph for controlled incremental ingestion."""

import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import TaskGroup, dag

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from pipeline_contracts import GOLD_DATASET_SOURCES, TABLES
from pipeline_operators import (create_dbt_build_operator, create_gold_write_operator, create_incremental_silver_operator, create_silver_snapshot_operator, create_silver_parquet_to_iceberg_customers_operator, create_reconcile_iceberg_customers_operator, create_iceberg_customers_warehouse_operator, create_silver_parquet_to_iceberg_products_operator, create_reconcile_iceberg_products_operator, create_iceberg_products_warehouse_operator, create_silver_parquet_to_iceberg_orders_operator, create_reconcile_iceberg_orders_operator, create_iceberg_orders_warehouse_operator)
from pipeline_tasks.lakehouse import (check_lakehouse_customers_eligibility, skip_iceberg_customers, verify_iceberg_customers_checkpoint, check_lakehouse_products_eligibility, skip_iceberg_products, verify_iceberg_products_checkpoint, check_lakehouse_orders_eligibility, skip_iceberg_orders, verify_iceberg_orders_checkpoint)
from pipeline_tasks.gold import (gold_snapshot_gate, prepare_gold_dataset_sources_task, resolve_gold_snapshot_lineage_task, skip_gold_layer)
from pipeline_tasks.ingestion import ingest_incremental, validate_config, validate_incremental
from pipeline_tasks.silver import (check_silver_eligibility, pipeline_summary, silver_result, skip_silver, verify_silver_manifest)
from pipeline_tasks.warehouse import check_dbt_eligibility, load_warehouse, skip_dbt, warehouse_gate

PROJECT_PATH = os.environ.get("AIRFLOW_DOCKER_PROJECT_PATH", "/opt/airflow/project")


@dag(
    dag_id="ecommerce_incremental_ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["ecommerce", "incremental", "controlled"],
    max_active_runs=1,
)
def ecommerce_incremental_ingestion():
    def create_silver_transform_operator(table_name):
        return create_incremental_silver_operator(table_name=table_name, project_path=PROJECT_PATH)

    dbt_build_task = create_dbt_build_operator(project_path=PROJECT_PATH)

    def create_snapshot_operator(table_name):
        return create_silver_snapshot_operator(
            table_name=table_name, project_path=PROJECT_PATH,
            database_environment={
                "DB_HOST": "postgres", "DB_PORT": os.environ["DB_PORT"],
                "DB_NAME": os.environ["DB_NAME"], "DB_USER": os.environ["DB_USER"],
                "DB_PASSWORD": os.environ["DB_PASSWORD"],
            },
        )

    def create_gold_operator(dataset_name, source_task_id):
        return create_gold_write_operator(dataset_name=dataset_name, source_task_id=source_task_id, project_path=PROJECT_PATH)

    config = validate_config()
    ingestion_results, validation_results = {}, {}
    eligibility_tasks, silver_skip_tasks = {}, {}
    silver_transform_tasks, silver_verify_tasks, silver_result_tasks = {}, {}, {}

    for table_name in TABLES:
        ingestion_results[table_name] = ingest_incremental.override(task_id=f"ingest_{table_name}")(table_name, config)

    # Keep the task IDs stable: branch targets and XCom consumers refer to them
    # directly, while the groups keep Airflow's graph view readable.
    with TaskGroup(group_id="bronze_validation", prefix_group_id=False):
        for table_name in TABLES:
            validation_results[table_name] = validate_incremental.override(task_id=f"validate_{table_name}")(table_name, ingestion_results[table_name])

    with TaskGroup(group_id="silver_eligibility", prefix_group_id=False):
        for table_name in TABLES:
            eligibility_tasks[table_name] = check_silver_eligibility.override(task_id=f"check_silver_eligibility_{table_name}")(table_name, validation_results[table_name])

    with TaskGroup(group_id="silver_skip", prefix_group_id=False):
        for table_name in TABLES:
            silver_skip_tasks[table_name] = skip_silver.override(task_id=f"skip_silver_{table_name}")(table_name, validation_results[table_name])

    for table_name in TABLES:
        silver_transform_tasks[table_name] = create_silver_transform_operator(table_name)

    with TaskGroup(group_id="silver_verification", prefix_group_id=False):
        for table_name in TABLES:
            silver_verify_tasks[table_name] = verify_silver_manifest.override(task_id=f"verify_silver_{table_name}")(table_name, validation_results[table_name])

    with TaskGroup(group_id="silver_result_aggregation", prefix_group_id=False):
        for table_name in TABLES:
            silver_result_tasks[table_name] = silver_result.override(task_id=f"silver_result_{table_name}")(table_name, validation_results[table_name])

    for table_name in TABLES:
        eligibility_tasks[table_name] >> silver_skip_tasks[table_name]
        eligibility_tasks[table_name] >> silver_transform_tasks[table_name]
        silver_transform_tasks[table_name] >> silver_verify_tasks[table_name]
        silver_skip_tasks[table_name] >> silver_result_tasks[table_name]
        silver_verify_tasks[table_name] >> silver_result_tasks[table_name]

    lakehouse_gate = check_lakehouse_customers_eligibility(validation_results["customers"])
    iceberg_customers = create_silver_parquet_to_iceberg_customers_operator(project_path=PROJECT_PATH)
    iceberg_skip = skip_iceberg_customers(validation_results["customers"])
    iceberg_reconcile = create_reconcile_iceberg_customers_operator(project_path=PROJECT_PATH)
    iceberg_warehouse_loader = create_iceberg_customers_warehouse_operator(project_path=PROJECT_PATH)
    iceberg_checkpoint = verify_iceberg_customers_checkpoint(validation_results["customers"])
    silver_verify_tasks["customers"] >> lakehouse_gate
    lakehouse_gate >> [iceberg_customers, iceberg_skip]
    iceberg_customers >> iceberg_reconcile
    iceberg_reconcile >> iceberg_warehouse_loader >> iceberg_checkpoint

    products_lakehouse_gate = check_lakehouse_products_eligibility(validation_results["products"])
    iceberg_products = create_silver_parquet_to_iceberg_products_operator(project_path=PROJECT_PATH)
    iceberg_products_skip = skip_iceberg_products(validation_results["products"])
    iceberg_products_reconcile = create_reconcile_iceberg_products_operator(project_path=PROJECT_PATH)
    iceberg_products_warehouse_loader = create_iceberg_products_warehouse_operator(project_path=PROJECT_PATH)
    iceberg_products_checkpoint = verify_iceberg_products_checkpoint(validation_results["products"])
    silver_verify_tasks["products"] >> products_lakehouse_gate
    products_lakehouse_gate >> [iceberg_products, iceberg_products_skip]
    iceberg_products >> iceberg_products_reconcile
    iceberg_products_reconcile >> iceberg_products_warehouse_loader >> iceberg_products_checkpoint

    orders_lakehouse_gate = check_lakehouse_orders_eligibility(validation_results["orders"])
    iceberg_orders = create_silver_parquet_to_iceberg_orders_operator(project_path=PROJECT_PATH)
    iceberg_orders_skip = skip_iceberg_orders(validation_results["orders"])
    iceberg_orders_reconcile = create_reconcile_iceberg_orders_operator(project_path=PROJECT_PATH)
    iceberg_orders_warehouse_loader = create_iceberg_orders_warehouse_operator(project_path=PROJECT_PATH)
    iceberg_orders_checkpoint = verify_iceberg_orders_checkpoint(validation_results["orders"])
    silver_verify_tasks["orders"] >> orders_lakehouse_gate
    orders_lakehouse_gate >> [iceberg_orders, iceberg_orders_skip]
    iceberg_orders >> iceberg_orders_reconcile
    iceberg_orders_reconcile >> iceberg_orders_warehouse_loader >> iceberg_orders_checkpoint

    pipeline_summary_task = pipeline_summary()
    for table_name in TABLES:
        silver_result_tasks[table_name] >> pipeline_summary_task
    warehouse_gate_task = warehouse_gate(pipeline_summary_task, {"customers": iceberg_checkpoint, "products": iceberg_products_checkpoint, "orders": iceberg_orders_checkpoint})
    warehouse_loader_task = load_warehouse(warehouse_gate_task)
    dbt_eligibility_task = check_dbt_eligibility(warehouse_loader_task)
    dbt_eligibility_task >> [dbt_build_task, skip_dbt()]

    gold_gate_task = gold_snapshot_gate(warehouse_loader_task)
    gold_skip_task = skip_gold_layer()
    gold_lineage_task = resolve_gold_snapshot_lineage_task()
    gold_gate_task >> [gold_skip_task, gold_lineage_task]
    dbt_build_task >> gold_gate_task
    snapshot_tasks = {}
    for table_name in TABLES:
        snapshot_tasks[table_name] = create_snapshot_operator(table_name)
        gold_lineage_task >> snapshot_tasks[table_name]
    for dataset_name in GOLD_DATASET_SOURCES:
        source_task_id = f"gold_sources_{dataset_name}"
        gold_sources_task = prepare_gold_dataset_sources_task.override(task_id=source_task_id)(dataset_name, gold_lineage_task)
        for source_table in GOLD_DATASET_SOURCES[dataset_name]:
            snapshot_tasks[source_table] >> gold_sources_task
        gold_sources_task >> create_gold_operator(dataset_name, source_task_id)


ecommerce_incremental_ingestion()
