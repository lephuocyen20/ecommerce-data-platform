
"""Airflow DAG for read-only incremental ingestion inspection.

This DAG intentionally runs metadata dry-runs only.
It does not create batches, upload objects, or advance watermarks.
"""

import logging
import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task


TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
]


@dag(
    dag_id="ecommerce_incremental_ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ecommerce", "incremental", "dry-run"],
    max_active_runs=1,
)
def ecommerce_incremental_ingestion():

    @task
    def inspect_incremental(table_name: str):
        # Project source is mounted into Airflow containers.
        project_path = "/opt/airflow/project"

        if project_path not in sys.path:
            sys.path.insert(0, project_path)

        from ingestion.incremental.orchestrator import (
            run_incremental_ingestion,
        )

        logger = logging.getLogger("airflow.task")

        logger.info(
            "Starting incremental dry-run | table=%s",
            table_name,
        )

        result = run_incremental_ingestion(
            table_name=table_name,
            batch_size=1000,
            dry_run=True,
            rollback_test=False,
        )

        # Keep XCom JSON-serializable.
        watermark = result.get("watermark")

        if watermark is not None:
            watermark = {
                key: (
                    value.isoformat()
                    if isinstance(value, datetime)
                    else value
                )
                for key, value in watermark.items()
            }

        summary = {
            "table_name": result["table_name"],
            "mode": result["mode"],
            "action": result["action"],
            "watermark": watermark,
            "recoverable_batches": result["recoverable_batches"],
        }

        logger.info(
            "Incremental dry-run completed | table=%s | action=%s",
            table_name,
            summary["action"],
        )

        return summary

    for table_name in TABLES:
        inspect_incremental.override(
            task_id=f"inspect_{table_name}",
        )(table_name)


ecommerce_incremental_ingestion()
