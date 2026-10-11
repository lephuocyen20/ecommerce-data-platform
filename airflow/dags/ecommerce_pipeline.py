
from datetime import datetime, timedelta
import logging
import os

from airflow.sdk import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
]

SPARK_IMAGE = "ecommerce-spark:3.5.6"
DOCKER_NETWORK = "ecommerce-data-platform_default"

PROJECT_PATH = "C:\\Users\\HITLAB\\Projects\\ecommerce-data-platform"


@dag(
    dag_id="ecommerce_pipeline",
    schedule="0 19 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ecommerce", "data-engineering"],
)
def ecommerce_pipeline():

    # ==========================================
    # 1. PostgreSQL -> MinIO Bronze
    # ==========================================

    @task
    def ingest(table_name):
        import sys

        sys.path.insert(0, "/opt/airflow/project")

        from ingestion.postgres_to_minio import ingest_table

        logger = logging.getLogger("airflow.task")

        logger.info(
            "Starting ingestion for table: %s",
            table_name,
        )

        object_name = ingest_table(table_name)

        logger.info(
            "Ingestion completed | table=%s | object=%s",
            table_name,
            object_name,
        )

        return object_name

    # ==========================================
    # 2. Bronze Data Quality
    # ==========================================

    @task
    def validate(table_name, object_name):
        import sys

        sys.path.insert(0, "/opt/airflow/project")

        from validation.validate_bronze import validate_table

        logger = logging.getLogger("airflow.task")

        logger.info(
            "Starting validation | table=%s | object=%s",
            table_name,
            object_name,
        )

        validate_table(table_name)

        logger.info(
            "Validation completed | table=%s",
            table_name,
        )

    # ==========================================
    # 3. Create 6 ingestion/validation pairs
    # ==========================================
    bronze_objects = {}
    bronze_validation_tasks = []

    for table_name in TABLES:

        uploaded_object = ingest.override(
            task_id=f"ingest_{table_name}"
        )(table_name)

        bronze_objects[table_name] = uploaded_object

        validation_task = validate.override(
            task_id=f"validate_{table_name}"
        )(table_name, uploaded_object)

        bronze_validation_tasks.append(validation_task)

    # ==========================================
    # 4. Spark: Bronze -> Silver
    # ==========================================

    bronze_to_silver = DockerOperator(
        task_id="bronze_to_silver",
        image=SPARK_IMAGE,
        network_mode=DOCKER_NETWORK,
        private_environment={
            "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
            "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
            "PYTHONPATH": (
                "/opt/spark/work-dir:"
                "/opt/spark/work-dir/transformation"
            ),
        },
        command=[
            "/opt/spark/bin/spark-submit",
            "/opt/spark/work-dir/transformation/bronze_to_silver.py",
        ],
        mounts=[
            Mount(
                source=PROJECT_PATH + r"\transformation",
                target="/opt/spark/work-dir/transformation",
                type="bind",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        do_xcom_push=False,
    )

    # ==========================================
    # 5. Spark: Silver Data Quality
    # ==========================================

    validate_silver = DockerOperator(
        task_id="validate_silver",
        image=SPARK_IMAGE,
        network_mode=DOCKER_NETWORK,
        private_environment={
            "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
            "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
            "PYTHONPATH": (
                "/opt/spark/work-dir:"
                "/opt/spark/work-dir/transformation"
            ),
        },
        command=[
            "/opt/spark/bin/spark-submit",
            "/opt/spark/work-dir/validation/validate_silver_optimized.py",
        ],
        mounts=[
            Mount(
                source=PROJECT_PATH + r"\validation",
                target="/opt/spark/work-dir/validation",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=PROJECT_PATH + r"\transformation",
                target="/opt/spark/work-dir/transformation",
                type="bind",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        do_xcom_push=False,
    )

    # ==========================================
    # 6. Pipeline dependencies
    # ==========================================

    bronze_validation_tasks >> bronze_to_silver
    bronze_to_silver >> validate_silver


ecommerce_pipeline()
