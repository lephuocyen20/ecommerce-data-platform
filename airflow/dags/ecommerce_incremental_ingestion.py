
"""Airflow DAG for controlled incremental ingestion.

Default: DRY_RUN for all supported tables.
WRITE requires explicit table selection and environment permission.
"""

import logging
import os
import re
import sys
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from airflow.sdk import dag, get_current_context, task
from airflow.exceptions import AirflowSkipException


TABLES = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
)

STAGING_ROOT = "/opt/airflow/project/staging"
BATCH_SIZE = 1000
SPARK_IMAGE = "ecommerce-spark:3.5.6"
DOCKER_NETWORK = "ecommerce-data-platform_default"

PROJECT_PATH = r"C:\Users\HITLAB\Projects\ecommerce-data-platform"

SPARK_TRANSFORMATION_PATH = (
    "/opt/spark/work-dir/transformation"
)

SPARK_PYTHONPATH = (
    "/opt/spark/work-dir:"
    "/opt/spark/work-dir/transformation"
)




@dag(
    dag_id="ecommerce_incremental_ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ecommerce", "incremental", "controlled"],
    max_active_runs=1,
)
def ecommerce_incremental_ingestion():

    @task(retries=0)
    def validate_config():
        context = get_current_context()
        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        if not isinstance(conf, dict):
            raise ValueError("DAG configuration must be an object")

        mode = conf.get("mode", "DRY_RUN")

        if mode not in ("DRY_RUN", "WRITE"):
            raise ValueError(f"Unsupported mode: {mode}")

        selected_tables = conf.get("tables")

        if selected_tables is None:
            if mode == "WRITE":
                raise ValueError(
                    "WRITE requires explicit table selection"
                )
            selected_tables = list(TABLES)

        if (
            not isinstance(selected_tables, list)
            or not selected_tables
            or any(not isinstance(t, str) for t in selected_tables)
        ):
            raise ValueError("tables must be a non-empty list of strings")

        if len(selected_tables) != len(set(selected_tables)):
            raise ValueError("Duplicate tables are not allowed")

        unsupported = set(selected_tables) - set(TABLES)

        if unsupported:
            raise ValueError(
                f"Unsupported tables: {sorted(unsupported)}"
            )

        if mode == "WRITE":
            if os.getenv("INGESTION_WRITE_ENABLED", "").lower() != "true":
                raise RuntimeError(
                    "WRITE blocked: INGESTION_WRITE_ENABLED=true required"
                )

        logging.getLogger("airflow.task").info(
            "Incremental configuration validated | mode=%s | tables=%s",
            mode,
            selected_tables,
        )

        return {
            "mode": mode,
            "tables": selected_tables,
        }

    @task
    def ingest_incremental(table_name: str, config: dict):
        if table_name not in config["tables"]:
            raise AirflowSkipException(
                f"Table {table_name} was not selected"
            )

        project_path = "/opt/airflow/project"

        if project_path not in sys.path:
            sys.path.insert(0, project_path)

        from ingestion.incremental.orchestrator import (
            run_incremental_ingestion,
            run_incremental_ingestion_write,
        )

        logger = logging.getLogger("airflow.task")
        mode = config["mode"]

        logger.info(
            "Starting incremental ingestion | table=%s | mode=%s",
            table_name,
            mode,
        )

        if mode == "DRY_RUN":
            result = run_incremental_ingestion(
                table_name=table_name,
                batch_size=BATCH_SIZE,
                dry_run=True,
                rollback_test=False,
            )
        elif mode == "WRITE":
            # Recheck permission at execution time, not only in validate_config.
            if os.getenv("INGESTION_WRITE_ENABLED", "").lower() != "true":
                raise RuntimeError("WRITE blocked at ingestion task")
            result = run_incremental_ingestion_write(
                table_name=table_name,
                batch_size=BATCH_SIZE,
                staging_root=STAGING_ROOT,
                allow_write=True,
            )
        else:
            raise ValueError(f"Unsupported mode: {mode}")

        def serialize(value):
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, dict):
                return {k: serialize(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return [serialize(v) for v in value]
            return value

        if not isinstance(result, dict):
            raise ValueError(f"Invalid ingestion return type: {table_name}")

        summary = serialize({
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

        logger.info(
            "Incremental ingestion completed | result=%s",
            summary,
        )

        return summary

    @task
    def validate_incremental(table_name: str, ingestion_result: dict):
        logger = logging.getLogger("airflow.task")

        if not isinstance(ingestion_result, dict):
            raise ValueError(
                f"Missing ingestion result for {table_name}"
            )

        if ingestion_result.get("table_name") != table_name:
            raise ValueError(
                f"Ingestion result table mismatch: {table_name}"
            )

        mode = ingestion_result.get("mode")
        status = ingestion_result.get("status")
        batch_id = ingestion_result.get("batch_id")

        if mode == "DRY_RUN":
            # The orchestrator's DRY_RUN returns no status, batch or persistence.
            if (
                status is not None
                or batch_id is not None
                or ingestion_result.get("persisted") is not None
                or not isinstance(ingestion_result.get("action"), str)
                or not ingestion_result["action"]
                or not isinstance(ingestion_result.get("recoverable_batches"), list)
            ):
                raise ValueError(f"Invalid DRY_RUN ingestion result: {table_name}")
            logger.info(
                "Skipping Bronze validation in DRY_RUN | table=%s",
                table_name,
            )
            return {
                "table_name": table_name,
                "status": "SKIPPED",
                "reason": "DRY_RUN",
            }

        if mode != "WRITE":
            raise ValueError(f"Unexpected ingestion mode: {mode}")

        if status == "NO_DATA":
            if batch_id is not None:
                raise ValueError(
                    "NO_DATA must not contain a batch_id"
                )

            logger.info(
                "No incremental batch to validate | table=%s",
                table_name,
            )
            return {
                "table_name": table_name,
                "status": "SKIPPED",
                "reason": "NO_DATA",
            }

        if status != "COMMITTED":
            raise RuntimeError(
                f"Unexpected ingestion status: {status}"
            )

        if not isinstance(batch_id, str) or not batch_id:
            raise ValueError(
                f"COMMITTED result missing batch_id: {table_name}"
            )

        project_path = "/opt/airflow/project"
        if project_path not in sys.path:
            sys.path.insert(0, project_path)

        from validation.validate_incremental_bronze import (
            validate_incremental_batch,
        )

        result = validate_incremental_batch(
            table_name=table_name,
            batch_id=batch_id,
        )

        logger.info(
            "Incremental Bronze validation completed | result=%s",
            result,
        )

        return result


    @task.branch(retries=0)
    def check_silver_eligibility(
        table_name: str,
        config: dict,
        validation_result: dict,
    ):
        logger = logging.getLogger("airflow.task")

        if table_name not in config["tables"]:
            raise AirflowSkipException(
                f"Table {table_name} was not selected"
            )

        if not isinstance(validation_result, dict):
            raise ValueError("Missing Bronze validation result")

        if validation_result.get("table_name") != table_name:
            raise ValueError("Bronze validation table mismatch")

        status = validation_result.get("status")
        reason = validation_result.get("reason")
        mode = config["mode"]

        if mode == "DRY_RUN":
            if status != "SKIPPED" or reason != "DRY_RUN":
                raise ValueError("Invalid DRY_RUN validation result")

            logger.info(
                "Silver transformation disabled in DRY_RUN | table=%s",
                table_name,
            )
            return f"skip_silver_{table_name}"

        if mode != "WRITE":
            raise ValueError(f"Unsupported mode: {mode}")

        if status == "SKIPPED" and reason == "NO_DATA":
            return f"skip_silver_{table_name}"

        if status != "PASS":
            raise ValueError(
                f"Bronze validation did not PASS: {table_name}"
            )

        if validation_result.get("checksum_verified") is not True:
            raise ValueError("Bronze checksum was not verified")

        batch_id = validation_result.get("batch_id")
        object_key = validation_result.get("object_key")
        checksum = validation_result.get("bronze_checksum_sha256")

        if not isinstance(batch_id, str) or not re.fullmatch(
            r"[0-9a-f]{32}", batch_id
        ):
            raise ValueError("Invalid Bronze batch ID")

        expected_key = (
            f"bronze_incremental/{table_name}/"
            f"batch_id={batch_id}/data.csv"
        )

        if object_key != expected_key:
            raise ValueError("Bronze object key mismatch")

        if not isinstance(checksum, str) or not re.fullmatch(
            r"[0-9a-f]{64}", checksum
        ):
            raise ValueError("Invalid Bronze checksum")

        return f"transform_{table_name}"

    @task(retries=0)
    def skip_silver(table_name: str, validation_result: dict):
        if not isinstance(validation_result, dict):
            raise ValueError("Missing Bronze validation result")
        if validation_result.get("table_name") != table_name:
            raise ValueError("Silver skip Bronze table mismatch")
        status = validation_result.get("status")
        reason = validation_result.get("reason")

        if status != "SKIPPED" or reason not in (
            "DRY_RUN",
            "NO_DATA",
        ):
            raise ValueError(
                f"Invalid Silver skip condition: {table_name}"
            )

        logging.getLogger("airflow.task").info(
            "Silver transformation skipped | table=%s | reason=%s",
            table_name,
            reason,
        )

        return {
            "table_name": table_name,
            "status": "SKIPPED",
            "reason": reason,
        }


    @task(trigger_rule="all_done", retries=0)
    def pipeline_summary():
        logger = logging.getLogger("airflow.task")
        ti = get_current_context()["ti"]

        config = ti.xcom_pull(task_ids="validate_config")

        if not isinstance(config, dict) or config.get("mode") not in ("DRY_RUN", "WRITE"):
            raise RuntimeError("PIPELINE_ABORTED: validate_config failed or produced no valid configuration")

        results = {
            table_name: ti.xcom_pull(
                task_ids=f"silver_result_{table_name}"
            )
            for table_name in TABLES
        }
        if not isinstance(config.get("tables"), list):
            raise ValueError("Missing pipeline table selection")
        selected = set(config["tables"])
        completed = {}
        failed = []

        for table_name in TABLES:
            if table_name not in selected:
                continue

            result = results.get(table_name)

            if not isinstance(result, dict):
                failed.append(table_name)
                continue

            status = result.get("status")
            reason = result.get("reason")

            if result.get("table_name") != table_name:
                valid = False

            elif config["mode"] == "DRY_RUN":
                valid = (
                    status == "SKIPPED"
                    and reason == "DRY_RUN"
                )

            elif config["mode"] == "WRITE":
                valid = (
                    status == "PASS"
                    and isinstance(result.get("batch_id"), str)
                    and bool(re.fullmatch(r"[0-9a-f]{32}", result["batch_id"]))
                    and result.get("manifest_key") == (
                        f"silver_incremental/{table_name}/batch_id={result['batch_id']}/_BATCH_METADATA.json"
                    )
                    and isinstance(
                        result.get("silver_row_count"), int
                    )
                    and not isinstance(
                        result.get("silver_row_count"), bool
                    )
                    and result["silver_row_count"] >= 0
                ) or (
                    status == "SKIPPED"
                    and reason == "NO_DATA"
                )

            else:
                valid = False

            if not valid:
                failed.append(table_name)
                continue

            completed[table_name] = result

        if failed:
            raise RuntimeError(
                f"Incremental validation incomplete or failed: "
                f"{sorted(failed)}"
            )

        logger.info(
            "Incremental pipeline completed | mode=%s | tables=%s",
            config["mode"],
            sorted(completed),
        )

        return {
            "mode": config["mode"],
            "status": "PASS",
            "tables": sorted(completed),
            "results": completed,
        }

    @task(retries=0)
    def warehouse_gate(summary: dict):
        """Validate pipeline summary before Warehouse loading."""

        if not isinstance(summary, dict):
            raise ValueError("Missing pipeline summary")

        if summary.get("status") != "PASS":
            raise RuntimeError("Pipeline summary is not PASS")

        mode = summary.get("mode")
        tables = summary.get("tables")
        results = summary.get("results")

        if mode not in ("DRY_RUN", "WRITE"):
            raise ValueError("Invalid pipeline mode")

        if (
            not isinstance(tables, list)
            or not tables
            or any(not isinstance(t, str) for t in tables)
            or len(tables) != len(set(tables))
            or set(tables) - set(TABLES)
        ):
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
                valid = (
                    result.get("status") == "SKIPPED"
                    and result.get("reason") == "DRY_RUN"
                )
            else:
                valid = (
                    result.get("status") == "PASS"
                    and isinstance(result.get("batch_id"), str)
                    and bool(
                        re.fullmatch(
                            r"[0-9a-f]{32}",
                            result["batch_id"],
                        )
                    )
                    and result.get("manifest_key") == (
                        f"silver_incremental/{table_name}/"
                        f"batch_id={result['batch_id']}/"
                        "_BATCH_METADATA.json"
                    )
                    and type(result.get("silver_row_count")) is int
                    and result["silver_row_count"] >= 0
                ) or (
                    result.get("status") == "SKIPPED"
                    and result.get("reason") == "NO_DATA"
                )

            if not valid:
                raise RuntimeError(
                    f"Warehouse Gate rejected Silver result: {table_name}"
                )

        if mode == "DRY_RUN":
            return {
                "status": "SKIP",
                "reason": "DRY_RUN",
                "tables": tables,
            }

        ready_tables = [
            table_name
            for table_name in tables
            if results[table_name]["status"] == "PASS"
        ]

        if not ready_tables:
            return {
                "status": "SKIP",
                "reason": "NO_DATA",
                "tables": tables,
            }

        ready_batch_ids = [
            results[table_name]["batch_id"]
            for table_name in ready_tables
        ]

        if len(ready_batch_ids) != len(set(ready_batch_ids)):
            raise ValueError("Duplicate Warehouse batch IDs")

        return {
            "status": "READY",
            "reason": "SILVER_VERIFIED",
            "tables": ready_tables,
            "batch_ids": ready_batch_ids,
        }

    @task(retries=0)
    def load_warehouse(gate_result: dict, config: dict):
        """Load verified Silver batches into Warehouse Raw."""

        logger = logging.getLogger("airflow.task")

        if not isinstance(gate_result, dict):
            raise ValueError("Missing Warehouse Gate result")

        if not isinstance(config, dict):
            raise ValueError("Missing pipeline configuration")

        mode = config.get("mode")
        status = gate_result.get("status")

        if mode not in ("DRY_RUN", "WRITE"):
            raise ValueError("Invalid pipeline mode")

        if status == "SKIP":
            reason = gate_result.get("reason")

            if (
                (mode == "DRY_RUN" and reason != "DRY_RUN")
                or (mode == "WRITE" and reason != "NO_DATA")
            ):
                raise ValueError("Invalid Warehouse skip reason")

            logger.info(
                "Warehouse loading skipped | reason=%s",
                reason,
            )

            return {
                "status": "SKIPPED",
                "reason": reason,
            }

        if status != "READY" or mode != "WRITE":
            raise ValueError("Invalid Warehouse loading state")

        if gate_result.get("reason") != "SILVER_VERIFIED":
            raise ValueError("Warehouse Gate verification missing")

        tables = gate_result.get("tables")
        batch_ids = gate_result.get("batch_ids")

        if (
            not isinstance(tables, list)
            or not tables
            or any(t not in TABLES for t in tables)
            or len(tables) != len(set(tables))
        ):
            raise ValueError("Invalid Warehouse table selection")

        if (
            not isinstance(batch_ids, list)
            or len(batch_ids) != len(tables)
            or any(
                not isinstance(b, str)
                or not re.fullmatch(r"[0-9a-f]{32}", b)
                for b in batch_ids
            )
            or len(batch_ids) != len(set(batch_ids))
        ):
            raise ValueError("Invalid Warehouse batch selection")

        if not set(tables).issubset(set(config.get("tables", []))):
            raise ValueError("Warehouse tables exceed DAG selection")

        # Independent safety gate for Warehouse writes.
        if os.getenv("WAREHOUSE_WRITE_ENABLED", "").lower() != "true":
            logger.info(
                "Warehouse WRITE disabled | tables=%s | batches=%s",
                tables,
                batch_ids,
            )

            return {
                "status": "SKIPPED",
                "reason": "WAREHOUSE_WRITE_DISABLED",
                "tables": tables,
                "batch_ids": batch_ids,
            }

        # Recheck ingestion permission at execution time.
        if os.getenv("INGESTION_WRITE_ENABLED", "").lower() != "true":
            raise RuntimeError("Ingestion WRITE permission revoked")

        project_path = "/opt/airflow/project/warehouse/loader"

        if project_path not in sys.path:
            sys.path.insert(0, project_path)

        from warehouse_orchestrator import process_batches

        logger.info(
            "Starting Warehouse loading | tables=%s | batches=%s",
            tables,
            batch_ids,
        )

        result = process_batches(
            dry_run=False,
            selected_tables=tables,
            selected_batch_ids=batch_ids,
            strict_batch_selection=True,
        )

        # Verify Warehouse Orchestrator result before reporting success.
        if not isinstance(result, dict):
            raise RuntimeError(
                "Warehouse Orchestrator returned an invalid result"
            )

        expected_count = len(batch_ids)

        if (
            result.get("status") != "PASS"
            or result.get("mode") != "COMMIT"
            or result.get("strict_batch_selection") is not True
            or result.get("selected_tables") != tables
            or result.get("selected_batch_ids") != batch_ids
            or type(result.get("pending_batches")) is not int
            or result["pending_batches"] != expected_count
            or type(result.get("loaded_batches")) is not int
            or result["loaded_batches"] != expected_count
            or type(result.get("skipped_batches")) is not int
            or result["skipped_batches"] != 0
            or type(result.get("loaded_rows")) is not int
            or result["loaded_rows"] < 0
        ):
            raise RuntimeError(
                f"Warehouse Orchestrator result verification failed: "
                f"{result}"
            )

        # Verify committed checkpoints using a fresh DB connection.
        from incremental_silver_to_warehouse import get_connection

        verify_conn = get_connection()

        try:
            with verify_conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        batch_id,
                        table_name,
                        loaded_row_count
                    FROM warehouse.loaded_batches
                    WHERE batch_id = ANY(%s)
                    """,
                    (batch_ids,),
                )

                checkpoint_rows = cursor.fetchall()

        finally:
            verify_conn.close()

        checkpoints = {
            batch_id: (table_name, row_count)
            for batch_id, table_name, row_count in checkpoint_rows
        }

        if len(checkpoints) != len(batch_ids):
            raise RuntimeError(
                "Warehouse checkpoint verification failed: "
                "missing or duplicate batch IDs"
            )

        expected_mapping = dict(zip(batch_ids, tables))

        for batch_id, expected_table in expected_mapping.items():
            checkpoint = checkpoints.get(batch_id)

            if checkpoint is None:
                raise RuntimeError(
                    f"Missing Warehouse checkpoint: {batch_id}"
                )

            actual_table, loaded_row_count = checkpoint

            if actual_table != expected_table:
                raise RuntimeError(
                    f"Warehouse checkpoint table mismatch: {batch_id}"
                )

            if (
                type(loaded_row_count) is not int
                or loaded_row_count < 0
            ):
                raise RuntimeError(
                    f"Invalid Warehouse checkpoint row count: {batch_id}"
                )

        checkpoint_total_rows = sum(
            checkpoint[1]
            for checkpoint in checkpoints.values()
        )

        if checkpoint_total_rows != result["loaded_rows"]:
            raise RuntimeError(
                "Warehouse loaded row count does not match checkpoints"
            )

        logger.info(
            "Warehouse checkpoints verified | batches=%s | rows=%s",
            len(checkpoints),
            checkpoint_total_rows,
        )


        logger.info(
            "Warehouse loading verified | batches=%s | rows=%s",
            result["loaded_batches"],
            result["loaded_rows"],
        )

        return {
            "status": "COMPLETED",
            "mode": "COMMIT",
            "tables": tables,
            "batch_ids": batch_ids,
            "loaded_batches": result["loaded_batches"],
            "loaded_rows": result["loaded_rows"],
            "skipped_batches": result["skipped_batches"],
        }


    @task.branch
    def check_dbt_eligibility(warehouse_result: dict):
        """
        Run dbt only after a successful Warehouse COMMIT.
        """

        if not isinstance(warehouse_result, dict):
            raise RuntimeError("Invalid Warehouse result")

        status = warehouse_result.get("status")

        if status == "SKIPPED":
            return "skip_dbt"

        if status != "COMPLETED":
            raise RuntimeError(
                f"Unexpected Warehouse status: {status}"
            )

        if warehouse_result.get("mode") != "COMMIT":
            raise RuntimeError(
                "dbt requires Warehouse COMMIT mode"
            )

        if not warehouse_result.get("batch_ids"):
            raise RuntimeError(
                "Warehouse completed without batch IDs"
            )

        return "dbt_build"


    @task(retries=0)
    def verify_silver_manifest(
        table_name: str,
        validation_result: dict,
    ):
        import json
        from minio import Minio

        logger = logging.getLogger("airflow.task")

        if not isinstance(validation_result, dict):
            raise ValueError("Missing Bronze validation result")
        if (
            validation_result.get("table_name") != table_name
            or validation_result.get("status") != "PASS"
            or validation_result.get("checksum_verified") is not True
        ):
            raise ValueError(f"Invalid Bronze validation for {table_name}")

        batch_id = validation_result.get("batch_id")
        if not isinstance(batch_id, str) or not re.fullmatch(r"[0-9a-f]{32}", batch_id):
            raise ValueError("Invalid Bronze batch ID")

        manifest_key = (
            f"silver_incremental/{table_name}/"
            f"batch_id={batch_id}/_BATCH_METADATA.json"
        )

        client = Minio(
            "minio:9000",
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )

        bucket = os.environ["BRONZE_BUCKET"]

        response = client.get_object(bucket, manifest_key)

        try:
            manifest = json.loads(
                response.read().decode("utf-8")
            )
        finally:
            response.close()
            response.release_conn()

        if not isinstance(manifest, dict):
            raise ValueError("Silver manifest must be a JSON object")

        if manifest.get("status") != "COMPLETED":
            raise ValueError("Silver manifest is not COMPLETED")

        if manifest.get("table_name") != table_name:
            raise ValueError("Silver manifest table mismatch")

        if manifest.get("batch_id") != batch_id:
            raise ValueError("Silver manifest batch mismatch")

        if (
            manifest.get("bronze_object_key")
            != validation_result["object_key"]
        ):
            raise ValueError("Silver manifest Bronze key mismatch")

        if (
            manifest.get("bronze_checksum_sha256")
            != validation_result["bronze_checksum_sha256"]
        ):
            raise ValueError("Silver manifest checksum mismatch")

        row_count = manifest.get("silver_row_count")
        if type(row_count) is not int or row_count < 0:
            raise ValueError("Invalid Silver manifest row count")
        data_prefix = manifest.get("data_prefix")
        expected_prefix = f"silver_incremental/{table_name}/batch_id={batch_id}/runs/"
        if not isinstance(data_prefix, str) or not data_prefix.startswith(expected_prefix):
            raise ValueError("Invalid Silver manifest data prefix")
        if not re.fullmatch(
            rf"silver_incremental/{table_name}/batch_id={batch_id}/runs/[0-9a-f]{{32}}/",
            data_prefix,
        ):
            raise ValueError("Invalid Silver manifest run prefix")
        parquet_objects = manifest.get("parquet_objects")
        if not isinstance(parquet_objects, list) or not parquet_objects:
            raise ValueError("Silver manifest has no Parquet objects")
        if any(
            not isinstance(obj, dict)
            or not isinstance(obj.get("object_key"), str)
            or not obj["object_key"].startswith(data_prefix)
            or not obj["object_key"].endswith(".parquet")
            or type(obj.get("size")) is not int
            or obj["size"] <= 0
            for obj in parquet_objects
        ):
            raise ValueError("Invalid Silver Parquet metadata")
        if manifest.get("parquet_file_count") != len(parquet_objects):
            raise ValueError("Silver Parquet file count mismatch")

        expected_objects = sorted(
            parquet_objects,
            key=lambda obj: obj["object_key"],
        )

        if len({obj["object_key"] for obj in expected_objects}) != len(expected_objects):
            raise ValueError("Duplicate Silver Parquet object keys")

        actual_objects = sorted(
            [
                {
                    "object_key": obj.object_name,
                    "size": obj.size,
                    "etag": obj.etag.strip('"'),
                }
                for obj in client.list_objects(
                    bucket,
                    prefix=data_prefix,
                    recursive=True,
                )
                if obj.object_name.endswith(".parquet")
            ],
            key=lambda obj: obj["object_key"],
        )

        if actual_objects != expected_objects:
            raise ValueError(
                "Silver Parquet objects do not match completion manifest"
            )

        logger.info(
            "Silver manifest verified | table=%s | batch=%s",
            table_name,
            batch_id,
        )

        return {
            "table_name": table_name,
            "status": "PASS",
            "batch_id": batch_id,
            "manifest_key": manifest_key,
            "silver_row_count": row_count,
        }


    @task(trigger_rule="none_failed_min_one_success", retries=0)
    def silver_result(
        table_name: str,
        config: dict,
        bronze_result: dict,
    ):
        from airflow.sdk import get_current_context

        context = get_current_context()
        ti = context["ti"]

        if table_name not in config["tables"]:
            raise AirflowSkipException(
                f"Silver result skipped: table {table_name} not selected"
            )

        skip_result = ti.xcom_pull(
            task_ids=f"skip_silver_{table_name}"
        )

        verify_result = ti.xcom_pull(
            task_ids=f"verify_silver_{table_name}"
        )

        if not isinstance(bronze_result, dict):
            raise ValueError("Missing Bronze result")
        if bronze_result.get("table_name") != table_name:
            raise ValueError("Bronze table mismatch")

        if config["mode"] == "DRY_RUN":
            expected_status = "SKIPPED"
            expected_reason = "DRY_RUN"
        elif config["mode"] == "WRITE":
            if bronze_result.get("status") == "SKIPPED":
                if bronze_result.get("reason") != "NO_DATA":
                    raise ValueError("Invalid WRITE skip reason")
                expected_status = "SKIPPED"
                expected_reason = "NO_DATA"
            elif bronze_result.get("status") == "PASS":
                expected_status = "PASS"
                expected_reason = None
            else:
                raise ValueError("Invalid WRITE Bronze status")
        else:
            raise ValueError("Unsupported pipeline mode")

        if expected_status == "SKIPPED":
            if not isinstance(skip_result, dict):
                raise RuntimeError("Missing Silver skip result")

            if (
                skip_result.get("table_name") != table_name
                or skip_result.get("status") != "SKIPPED"
                or skip_result.get("reason") != expected_reason
            ):
                raise RuntimeError("Invalid Silver skip result")

            if verify_result is not None:
                raise RuntimeError("Unexpected Silver verification result")

            return skip_result

        if not isinstance(verify_result, dict):
            raise RuntimeError("Missing Silver verification result")

        if (
            verify_result.get("table_name") != table_name
            or verify_result.get("status") != "PASS"
            or verify_result.get("batch_id")
            != bronze_result.get("batch_id")
        ):
            raise RuntimeError("Invalid Silver verification result")

        if skip_result is not None:
            raise RuntimeError("Unexpected Silver skip result")

        return verify_result


    def create_silver_transform_operator(table_name: str):
        validation_task_id = f"validate_{table_name}"

        def xcom_value(field: str):
            return (
                "{{ ti.xcom_pull(task_ids='"
                + validation_task_id
                + "')['"
                + field
                + "'] }}"
            )

        return DockerOperator(
            task_id=f"transform_{table_name}",
            image=SPARK_IMAGE,
            network_mode=DOCKER_NETWORK,
            private_environment={
                "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
                "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
                "BRONZE_BUCKET": os.environ["BRONZE_BUCKET"],
                "PYTHONPATH": SPARK_PYTHONPATH,
            },
            command=[
                "/opt/spark/bin/spark-submit",
                (
                    f"{SPARK_TRANSFORMATION_PATH}/"
                    "incremental_bronze_to_silver.py"
                ),
                "--table-name",
                table_name,
                "--batch-id",
                xcom_value("batch_id"),
                "--object-key",
                xcom_value("object_key"),
                "--bronze-checksum",
                xcom_value("bronze_checksum_sha256"),
            ],
            mounts=[
                Mount(
                    source=PROJECT_PATH + r"\transformation",
                    target=SPARK_TRANSFORMATION_PATH,
                    type="bind",
                    read_only=True,
                ),
            ],
            mount_tmp_dir=False,
            do_xcom_push=False,
            retries=0,
        )


    @task(task_id="skip_dbt")
    def skip_dbt():
        return {
            "status": "SKIPPED",
            "reason": "WAREHOUSE_NOT_COMPLETED",
    }

    dbt_build_task = DockerOperator(
        task_id="dbt_build",
        image="ecommerce-data-platform-dbt-runner:latest",
        network_mode=DOCKER_NETWORK,
        command=["build"],
        private_environment={
            "DBT_HOST": "postgres",
            "DBT_PORT": "5432",
            "DBT_NAME": os.environ["WAREHOUSE_DB_NAME"],
            "DBT_USER": os.environ["WAREHOUSE_DB_USER"],
            "DBT_PASSWORD": os.environ["WAREHOUSE_DB_PASSWORD"],
            "DBT_PROFILES_DIR": "/app/profiles",
        },
        mounts=[
            Mount(
                source=PROJECT_PATH + r"\warehouse\ecommerce_dbt",
                target="/app",
                type="bind",
            ),
            Mount(
                source=PROJECT_PATH + r"\warehouse\dbt_profiles",
                target="/app/profiles",
                type="bind",
                read_only=True,
            ),
        ],
        working_dir="/app",
        mount_tmp_dir=False,
        do_xcom_push=False,
        retries=0,
)


    config = validate_config()

    ingestion_results = {}
    validation_results = {}
    eligibility_tasks = {}
    silver_skip_tasks = {}
    silver_transform_tasks = {}
    silver_verify_tasks = {}
    silver_result_tasks = {}


    for table_name in TABLES:
        ingestion_results[table_name] = ingest_incremental.override(
            task_id=f"ingest_{table_name}",
        )(table_name, config)

        validation_results[table_name] = validate_incremental.override(
            task_id=f"validate_{table_name}",
        )(table_name, ingestion_results[table_name])

        eligibility_tasks[table_name] = (
            check_silver_eligibility.override(
                task_id=f"check_silver_eligibility_{table_name}",
            )(
                table_name,
                config,
                validation_results[table_name],
            )
        )

        silver_skip_tasks[table_name] = skip_silver.override(
            task_id=f"skip_silver_{table_name}",
        )(
            table_name,
            validation_results[table_name],
        )

        eligibility_tasks[table_name] >> silver_skip_tasks[table_name]

        silver_transform_tasks[table_name] = (
            create_silver_transform_operator(table_name)
        )

        eligibility_tasks[table_name] >> silver_transform_tasks[table_name]

        silver_verify_tasks[table_name] = (
            verify_silver_manifest.override(
                task_id=f"verify_silver_{table_name}",
            )(
                table_name,
                validation_results[table_name],
            )
        )

        silver_transform_tasks[table_name] >> silver_verify_tasks[table_name]

        silver_result_tasks[table_name] = silver_result.override(
            task_id=f"silver_result_{table_name}",
        )(
            table_name,
            config,
            validation_results[table_name],
        )

        silver_skip_tasks[table_name] >> silver_result_tasks[table_name]

        silver_verify_tasks[table_name] >> silver_result_tasks[table_name]

    pipeline_summary_task = pipeline_summary()

    config >> pipeline_summary_task

    for table_name in TABLES:
        silver_result_tasks[table_name] >> pipeline_summary_task

    warehouse_gate_task = warehouse_gate(pipeline_summary_task)
    warehouse_loader_task = load_warehouse(warehouse_gate_task, config,)
    dbt_eligibility_task = check_dbt_eligibility(warehouse_loader_task)
    skip_dbt_task = skip_dbt()

    dbt_eligibility_task >> [dbt_build_task,skip_dbt_task,]

ecommerce_incremental_ingestion()
