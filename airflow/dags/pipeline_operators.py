"""Reusable Airflow DockerOperator factories for Spark pipeline stages."""

import os

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


SPARK_IMAGE = "ecommerce-spark:3.5.6"
DOCKER_NETWORK = "ecommerce-data-platform_default"
SPARK_WORKDIR = "/opt/spark/work-dir"
TRANSFORMATION_TARGET = f"{SPARK_WORKDIR}/transformation"


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
