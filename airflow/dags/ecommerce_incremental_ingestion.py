"""Airflow graph for controlled incremental ingestion."""

import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import TaskGroup, dag

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from pipeline_contracts import GOLD_DATASET_SOURCES, TABLES
from lakehouse_dependencies import foreign_key_parents
from pipeline_operators import (create_dbt_build_operator, create_gold_write_operator, create_incremental_silver_operator, create_silver_snapshot_operator, create_lakehouse_migration_operator, create_lakehouse_reconcile_operator, create_lakehouse_attestation_operator, create_lakehouse_quality_operator, create_lakehouse_completion_operator, create_lakehouse_warehouse_operator)
from pipeline_tasks.lakehouse import (check_lakehouse_customers_eligibility, skip_iceberg_customers, verify_iceberg_customers_checkpoint, check_lakehouse_products_eligibility, skip_iceberg_products, verify_iceberg_products_checkpoint, check_lakehouse_orders_eligibility, skip_iceberg_orders, verify_iceberg_orders_checkpoint, check_lakehouse_order_items_eligibility, skip_iceberg_order_items, check_iceberg_order_items_cutover, skip_iceberg_order_items_warehouse, verify_iceberg_order_items_checkpoint, check_lakehouse_payments_eligibility, skip_iceberg_payments, check_iceberg_payments_cutover, skip_iceberg_payments_warehouse, verify_iceberg_payments_checkpoint, check_lakehouse_shipments_eligibility, skip_iceberg_shipments, check_iceberg_shipments_cutover, skip_iceberg_shipments_warehouse, verify_iceberg_shipments_checkpoint)
from pipeline_gold import link_gold_orchestration, link_gold_iceberg_orchestration
from gold_source_mode import ICEBERG_SNAPSHOT, resolve_gold_source_mode
from pipeline_tasks.gold import (gold_snapshot_gate, prepare_gold_dataset_sources_task, resolve_gold_snapshot_lineage_task, resolve_gold_iceberg_lineage_task, skip_gold_layer)
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
    gold_source_mode = resolve_gold_source_mode(os.environ.get("GOLD_SOURCE_MODE"))
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
        return create_gold_write_operator(dataset_name=dataset_name, source_task_id=source_task_id, project_path=PROJECT_PATH, source_mode=gold_source_mode)

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
    iceberg_customers = create_lakehouse_migration_operator(table_name="customers", project_path=PROJECT_PATH)
    iceberg_customers_attestation = create_lakehouse_attestation_operator(table_name="customers", project_path=PROJECT_PATH)
    iceberg_skip = skip_iceberg_customers(validation_results["customers"])
    iceberg_reconcile = create_lakehouse_reconcile_operator(table_name="customers", project_path=PROJECT_PATH)
    iceberg_customers_quality = create_lakehouse_quality_operator(table_name="customers", project_path=PROJECT_PATH)
    iceberg_customers_completion = create_lakehouse_completion_operator(table_name="customers")
    iceberg_warehouse_loader = create_lakehouse_warehouse_operator(table_name="customers", project_path=PROJECT_PATH)
    iceberg_checkpoint = verify_iceberg_customers_checkpoint(validation_results["customers"])
    silver_verify_tasks["customers"] >> lakehouse_gate
    lakehouse_gate >> [iceberg_customers, iceberg_skip]
    iceberg_customers >> iceberg_customers_attestation >> iceberg_reconcile >> iceberg_customers_quality
    [iceberg_customers_quality, iceberg_skip] >> iceberg_customers_completion
    iceberg_customers_quality >> iceberg_warehouse_loader >> iceberg_checkpoint

    products_lakehouse_gate = check_lakehouse_products_eligibility(validation_results["products"])
    iceberg_products = create_lakehouse_migration_operator(table_name="products", project_path=PROJECT_PATH)
    iceberg_products_attestation = create_lakehouse_attestation_operator(table_name="products", project_path=PROJECT_PATH)
    iceberg_products_skip = skip_iceberg_products(validation_results["products"])
    iceberg_products_reconcile = create_lakehouse_reconcile_operator(table_name="products", project_path=PROJECT_PATH)
    iceberg_products_quality = create_lakehouse_quality_operator(table_name="products", project_path=PROJECT_PATH)
    iceberg_products_completion = create_lakehouse_completion_operator(table_name="products")
    iceberg_products_warehouse_loader = create_lakehouse_warehouse_operator(table_name="products", project_path=PROJECT_PATH)
    iceberg_products_checkpoint = verify_iceberg_products_checkpoint(validation_results["products"])
    silver_verify_tasks["products"] >> products_lakehouse_gate
    products_lakehouse_gate >> [iceberg_products, iceberg_products_skip]
    iceberg_products >> iceberg_products_attestation >> iceberg_products_reconcile >> iceberg_products_quality
    [iceberg_products_quality, iceberg_products_skip] >> iceberg_products_completion
    iceberg_products_quality >> iceberg_products_warehouse_loader >> iceberg_products_checkpoint

    orders_lakehouse_gate = check_lakehouse_orders_eligibility(validation_results["orders"])
    iceberg_orders = create_lakehouse_migration_operator(table_name="orders", project_path=PROJECT_PATH)
    iceberg_orders_attestation = create_lakehouse_attestation_operator(table_name="orders", project_path=PROJECT_PATH)
    iceberg_orders_skip = skip_iceberg_orders(validation_results["orders"])
    iceberg_orders_reconcile = create_lakehouse_reconcile_operator(table_name="orders", project_path=PROJECT_PATH)
    iceberg_orders_quality = create_lakehouse_quality_operator(table_name="orders", project_path=PROJECT_PATH)
    iceberg_orders_completion = create_lakehouse_completion_operator(table_name="orders")
    iceberg_orders_warehouse_loader = create_lakehouse_warehouse_operator(table_name="orders", project_path=PROJECT_PATH)
    iceberg_orders_checkpoint = verify_iceberg_orders_checkpoint(validation_results["orders"])
    silver_verify_tasks["orders"] >> orders_lakehouse_gate
    orders_lakehouse_gate >> [iceberg_orders, iceberg_orders_skip]
    iceberg_orders >> iceberg_orders_attestation >> iceberg_orders_reconcile >> iceberg_orders_quality
    [iceberg_orders_quality, iceberg_orders_skip] >> iceberg_orders_completion
    iceberg_orders_quality >> iceberg_orders_warehouse_loader >> iceberg_orders_checkpoint

    order_items_lakehouse_gate = check_lakehouse_order_items_eligibility(validation_results["order_items"])
    iceberg_order_items = create_lakehouse_migration_operator(table_name="order_items", project_path=PROJECT_PATH)
    iceberg_order_items_attestation = create_lakehouse_attestation_operator(table_name="order_items", project_path=PROJECT_PATH)
    iceberg_order_items_skip = skip_iceberg_order_items(validation_results["order_items"])
    iceberg_order_items_reconcile = create_lakehouse_reconcile_operator(table_name="order_items", project_path=PROJECT_PATH)
    iceberg_order_items_quality = create_lakehouse_quality_operator(table_name="order_items", project_path=PROJECT_PATH)
    iceberg_order_items_cutover = check_iceberg_order_items_cutover()
    iceberg_order_items_warehouse_skip = skip_iceberg_order_items_warehouse()
    iceberg_order_items_warehouse_loader = create_lakehouse_warehouse_operator(table_name="order_items", project_path=PROJECT_PATH)
    iceberg_order_items_checkpoint = verify_iceberg_order_items_checkpoint(validation_results["order_items"])
    silver_verify_tasks["order_items"] >> order_items_lakehouse_gate
    order_items_lakehouse_gate >> [iceberg_order_items, iceberg_order_items_skip]
    iceberg_order_items >> iceberg_order_items_attestation >> iceberg_order_items_reconcile >> iceberg_order_items_quality
    iceberg_order_items_quality >> iceberg_order_items_cutover
    iceberg_order_items_cutover >> [iceberg_order_items_warehouse_loader, iceberg_order_items_warehouse_skip]
    iceberg_order_items_warehouse_loader >> iceberg_order_items_checkpoint

    payments_lakehouse_gate = check_lakehouse_payments_eligibility(validation_results["payments"])
    iceberg_payments = create_lakehouse_migration_operator(table_name="payments", project_path=PROJECT_PATH)
    iceberg_payments_attestation = create_lakehouse_attestation_operator(table_name="payments", project_path=PROJECT_PATH)
    iceberg_payments_skip = skip_iceberg_payments(validation_results["payments"])
    iceberg_payments_reconcile = create_lakehouse_reconcile_operator(table_name="payments", project_path=PROJECT_PATH)
    iceberg_payments_quality = create_lakehouse_quality_operator(table_name="payments", project_path=PROJECT_PATH)
    iceberg_payments_cutover = check_iceberg_payments_cutover()
    iceberg_payments_warehouse_skip = skip_iceberg_payments_warehouse()
    iceberg_payments_warehouse_loader = create_lakehouse_warehouse_operator(table_name="payments", project_path=PROJECT_PATH)
    iceberg_payments_checkpoint = verify_iceberg_payments_checkpoint(validation_results["payments"])
    silver_verify_tasks["payments"] >> payments_lakehouse_gate
    payments_lakehouse_gate >> [iceberg_payments, iceberg_payments_skip]
    iceberg_payments >> iceberg_payments_attestation >> iceberg_payments_reconcile >> iceberg_payments_quality >> iceberg_payments_cutover
    iceberg_payments_cutover >> [iceberg_payments_warehouse_loader, iceberg_payments_warehouse_skip]
    iceberg_payments_warehouse_loader >> iceberg_payments_checkpoint

    shipments_lakehouse_gate = check_lakehouse_shipments_eligibility(validation_results["shipments"])
    iceberg_shipments = create_lakehouse_migration_operator(table_name="shipments", project_path=PROJECT_PATH)
    iceberg_shipments_attestation = create_lakehouse_attestation_operator(table_name="shipments", project_path=PROJECT_PATH)
    iceberg_shipments_skip = skip_iceberg_shipments(validation_results["shipments"])
    iceberg_shipments_reconcile = create_lakehouse_reconcile_operator(table_name="shipments", project_path=PROJECT_PATH)
    iceberg_shipments_quality = create_lakehouse_quality_operator(table_name="shipments", project_path=PROJECT_PATH)
    iceberg_shipments_cutover = check_iceberg_shipments_cutover()
    iceberg_shipments_warehouse_skip = skip_iceberg_shipments_warehouse()
    iceberg_shipments_warehouse_loader = create_lakehouse_warehouse_operator(table_name="shipments", project_path=PROJECT_PATH)
    iceberg_shipments_checkpoint = verify_iceberg_shipments_checkpoint(validation_results["shipments"])
    silver_verify_tasks["shipments"] >> shipments_lakehouse_gate
    shipments_lakehouse_gate >> [iceberg_shipments, iceberg_shipments_skip]
    iceberg_shipments >> iceberg_shipments_attestation >> iceberg_shipments_reconcile >> iceberg_shipments_quality >> iceberg_shipments_cutover
    iceberg_shipments_cutover >> [iceberg_shipments_warehouse_loader, iceberg_shipments_warehouse_skip]
    iceberg_shipments_warehouse_loader >> iceberg_shipments_checkpoint

    # Parent reconciliation and quality must complete before a child table is
    # migrated; the completion gate also accepts an intentional parent no-op.
    iceberg_migrations = {
        "customers": iceberg_customers, "products": iceberg_products,
        "orders": iceberg_orders, "order_items": iceberg_order_items,
        "payments": iceberg_payments, "shipments": iceberg_shipments,
    }
    iceberg_completions = {
        "customers": iceberg_customers_completion,
        "products": iceberg_products_completion,
        "orders": iceberg_orders_completion,
    }
    for child, parents in foreign_key_parents().items():
        for parent in parents:
            iceberg_completions[parent] >> iceberg_migrations[child]

    pipeline_summary_task = pipeline_summary()
    for table_name in TABLES:
        silver_result_tasks[table_name] >> pipeline_summary_task
    warehouse_gate_task = warehouse_gate(pipeline_summary_task, {"customers": iceberg_checkpoint, "products": iceberg_products_checkpoint, "orders": iceberg_orders_checkpoint, "order_items": iceberg_order_items_checkpoint, "payments": iceberg_payments_checkpoint, "shipments": iceberg_shipments_checkpoint})
    warehouse_loader_task = load_warehouse(warehouse_gate_task)
    dbt_eligibility_task = check_dbt_eligibility(warehouse_loader_task)
    dbt_eligibility_task >> [dbt_build_task, skip_dbt()]

    gold_gate_task = gold_snapshot_gate(pipeline_summary_task)
    gold_skip_task = skip_gold_layer()
    gold_lineage_task = resolve_gold_iceberg_lineage_task() if gold_source_mode == ICEBERG_SNAPSHOT else resolve_gold_snapshot_lineage_task()
    snapshot_tasks = {}
    if gold_source_mode != ICEBERG_SNAPSHOT:
        for table_name in TABLES:
            snapshot_tasks[table_name] = create_snapshot_operator(table_name)
    gold_source_tasks, gold_write_tasks = {}, {}
    for dataset_name in GOLD_DATASET_SOURCES:
        source_task_id = f"gold_sources_{dataset_name}"
        gold_source_tasks[dataset_name] = prepare_gold_dataset_sources_task.override(task_id=source_task_id)(dataset_name, gold_lineage_task)
        gold_write_tasks[dataset_name] = create_gold_operator(dataset_name, source_task_id)

    if gold_source_mode == ICEBERG_SNAPSHOT:
        link_gold_iceberg_orchestration(
            pipeline_summary_task=pipeline_summary_task, gold_gate_task=gold_gate_task,
            gold_skip_task=gold_skip_task, gold_lineage_task=gold_lineage_task,
            evidence_tasks={"customers": iceberg_customers_quality, "products": iceberg_products_quality,
                            "orders": iceberg_orders_quality, "order_items": iceberg_order_items_quality,
                            "payments": iceberg_payments_quality, "shipments": iceberg_shipments_quality},
            dataset_source_tasks=gold_source_tasks, gold_write_tasks=gold_write_tasks,
        )
    else:
        link_gold_orchestration(
            pipeline_summary_task=pipeline_summary_task,
            gold_gate_task=gold_gate_task,
            gold_skip_task=gold_skip_task,
            gold_lineage_task=gold_lineage_task,
            snapshot_tasks=snapshot_tasks,
            dataset_source_tasks=gold_source_tasks,
            gold_write_tasks=gold_write_tasks,
        )


ecommerce_incremental_ingestion()
