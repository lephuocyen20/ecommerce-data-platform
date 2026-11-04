"""Reusable Airflow DockerOperator factories for Spark pipeline stages."""

import os

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


SPARK_IMAGE = "ecommerce-spark:3.5.6"
DOCKER_NETWORK = "ecommerce-data-platform_default"
SPARK_WORKDIR = "/opt/spark/work-dir"
TRANSFORMATION_TARGET = f"{SPARK_WORKDIR}/transformation"


def create_incremental_silver_operator(*, table_name, project_path):
    """Create the Bronze-to-Silver operator for one validated table batch."""
    validation_task_id = f"validate_{table_name}"

    def xcom_value(field):
        return (
            "{{ ti.xcom_pull(task_ids='"
            + validation_task_id
            + "')['"
            + field
            + "'] }}"
        )

    return create_spark_submit_operator(
        task_id=f"bronze_to_silver_{table_name}",
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/silver/run_incremental.py",
        arguments=[
            "--table-name", table_name,
            "--batch-id", xcom_value("batch_id"),
            "--object-key", xcom_value("object_key"),
            "--bronze-checksum", xcom_value("bronze_checksum_sha256"),
        ],
        retries=0,
    )


def create_dbt_build_operator(*, project_path):
    """Create the dbt build task used after a successful Warehouse load."""
    return DockerOperator(
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
                source=project_path + r"\warehouse\ecommerce_dbt",
                target="/app",
                type="bind",
            ),
            Mount(
                source=project_path + r"\warehouse\dbt_profiles",
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


def create_silver_snapshot_operator(*, table_name, project_path, database_environment):
    """Create the immutable Silver snapshot task for one table."""
    return create_spark_submit_operator(
        task_id=f"snapshot_{table_name}",
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/silver/run_snapshot.py",
        extra_environment=database_environment,
        arguments=["--table-name", table_name],
        extra_mounts=[
            Mount(
                source=project_path + r"\ingestion",
                target=f"{SPARK_WORKDIR}/ingestion",
                type="bind",
                read_only=True,
            ),
        ],
        retries=0,
    )


def create_gold_write_operator(*, dataset_name, source_task_id, project_path):
    """Create the guarded Gold write task for one Gold dataset."""
    return create_spark_submit_operator(
        task_id=f"gold_{dataset_name}",
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/gold/run_pipeline.py",
        arguments=[
            "--dataset", dataset_name,
            "--source-snapshots",
            "{{{{ ti.xcom_pull(task_ids='{}') }}}}".format(source_task_id),
            "--mode", "WRITE",
        ],
        extra_environment={
            "GOLD_SNAPSHOT_WRITE_ENABLED": os.environ.get(
                "GOLD_SNAPSHOT_WRITE_ENABLED", ""
            ),
            "DB_HOST": "postgres",
            "DB_PORT": "5432",
            "DB_NAME": os.environ["DB_NAME"],
            "DB_USER": os.environ["DB_USER"],
            "DB_PASSWORD": os.environ["DB_PASSWORD"],
        },
        extra_mounts=[
            Mount(
                source=project_path + r"\ingestion",
                target=f"{SPARK_WORKDIR}/ingestion",
                type="bind",
                read_only=True,
            ),
        ],
        retries=0,
    )


def create_spark_submit_operator(
    *,
    task_id,
    project_path,
    transformation_script=None,
    script_path=None,
    arguments=(),
    extra_environment=None,
    extra_mounts=(),
    retries=0,
):
    """Create a consistently configured Spark DockerOperator.

    Every stage receives the same MinIO credentials and read-only source mounts.
    Pass a transformation-relative script or an explicit absolute ``script_path``.
    """
    environment = {
        "MINIO_ENDPOINT": os.environ["MINIO_ENDPOINT"],
        "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
        "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
        "BRONZE_BUCKET": os.environ["BRONZE_BUCKET"],
        "PYTHONPATH": SPARK_WORKDIR,
    }
    environment.update(extra_environment or {})

    if (transformation_script is None) == (script_path is None):
        raise ValueError(
            "Provide exactly one of transformation_script or script_path"
        )
    submit_script = (
        script_path
        if script_path is not None
        else f"{TRANSFORMATION_TARGET}/{transformation_script}"
    )

    mounts = [
        Mount(
            source=project_path + r"\common",
            target=f"{SPARK_WORKDIR}/common",
            type="bind",
            read_only=True,
        ),
        Mount(
            source=project_path + r"\transformation",
            target=TRANSFORMATION_TARGET,
            type="bind",
            read_only=True,
        ),
        Mount(
            source=project_path + r"\silver",
            target=f"{SPARK_WORKDIR}/silver",
            type="bind",
            read_only=True,
        ),
        Mount(
            source=project_path + r"\gold",
            target=f"{SPARK_WORKDIR}/gold",
            type="bind",
            read_only=True,
        ),
    ]
    mounts.extend(extra_mounts)

    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        network_mode=DOCKER_NETWORK,
        private_environment=environment,
        command=[
            "/opt/spark/bin/spark-submit",
            submit_script,
            *arguments,
        ],
        mounts=mounts,
        mount_tmp_dir=False,
        do_xcom_push=False,
        retries=retries,
    )
