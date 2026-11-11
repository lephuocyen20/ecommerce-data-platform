"""Reusable Airflow DockerOperator factories for Spark pipeline stages."""

import json
import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from docker.types import Mount
from lakehouse_dependencies import PARENT_COMPLETION_TRIGGER_RULE
from common.bounded_e2e_scope import BoundedE2EScopeError, build_bounded_e2e_scope


SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "ecommerce-spark:3.5.6")
DOCKER_NETWORK = os.environ.get(
    "AIRFLOW_DOCKER_NETWORK", "ecommerce-data-platform_default"
)
SPARK_WORKDIR = "/opt/spark/work-dir"
SPARK_PYTHONPATH = ":".join(
    (
        SPARK_WORKDIR,
        "/opt/spark/python",
        "/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
    )
)
TRANSFORMATION_TARGET = f"{SPARK_WORKDIR}/transformation"
IDEMPOTENT_RETRIES = 2
IDEMPOTENT_RETRY_DELAY = timedelta(minutes=1)
IDEMPOTENT_EXECUTION_TIMEOUT = timedelta(hours=2)
DOCKER_API_TIMEOUT_SECONDS = 60


def _bounded_scope_environment():
    """Propagate an already-authorized shared-bucket scope to Spark tasks."""
    if os.environ.get("BRONZE_BUCKET") != "ecommerce-data-lake":
        return {}
    scope = build_bounded_e2e_scope(os.environ)
    return {
        "LAKEHOUSE_E2E_APPROVED": "1",
        "LAKEHOUSE_E2E_TEST_BUCKET": scope.bucket,
        "LAKEHOUSE_E2E_RUN_ID": scope.run_id,
        "LAKEHOUSE_E2E_PREFIX": scope.prefix,
        "LAKEHOUSE_E2E_WAREHOUSE": scope.warehouse,
        "LAKEHOUSE_E2E_NESSIE_REF": scope.nessie_ref,
        "LAKEHOUSE_E2E_SILVER_NAMESPACE": scope.silver_namespace,
    }


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
        extra_environment=_bounded_scope_environment(),
    )


def create_lakehouse_migration_operator(*, table_name, project_path):
    """Generic verified-Parquet to Iceberg task; table behavior lives in TableSpec."""
    manifest_key = "{{ ti.xcom_pull(task_ids='verify_silver_" + table_name + "')['manifest_key'] }}"
    return create_spark_submit_operator(task_id=f"silver_parquet_to_iceberg_{table_name}", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/run.py", arguments=["--table-name", table_name, "--manifest-key", manifest_key], extra_environment={"LAKEHOUSE_WRITE_ENABLED": os.environ.get("LAKEHOUSE_WRITE_ENABLED", "false"), **_bounded_scope_environment()}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], do_xcom_push=True)


def create_lakehouse_reconcile_operator(*, table_name, project_path):
    manifest_key = "{{ ti.xcom_pull(task_ids='verify_silver_" + table_name + "')['manifest_key'] }}"
    return create_spark_submit_operator(task_id=f"verify_iceberg_{table_name}", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/reconcile.py", arguments=["--table-name", table_name, "--manifest-key", manifest_key], extra_environment=_bounded_scope_environment(), extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], do_xcom_push=True, xcom_all=True)


def create_lakehouse_attestation_operator(*, table_name, project_path):
    manifest_key = "{{ ti.xcom_pull(task_ids='verify_silver_" + table_name + "')['manifest_key'] }}"
    return create_spark_submit_operator(task_id=f"attest_iceberg_{table_name}", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/attestation_cli.py", arguments=["--table-name", table_name, "--manifest-key", manifest_key], extra_environment=_bounded_scope_environment(), extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], do_xcom_push=True, xcom_all=True)


def create_lakehouse_quality_operator(*, table_name, project_path):
    """Run the read-only quality contract; a FAIL/ERROR blocks downstream work."""
    return create_spark_submit_operator(
        task_id=f"quality_iceberg_{table_name}",
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/lakehouse/quality/cli.py",
        arguments=["--table-name", table_name, "--manifest-key", "{{ ti.xcom_pull(task_ids='verify_silver_" + table_name + "')['manifest_key'] }}"],
        extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)],
        extra_environment=_bounded_scope_environment(),
        do_xcom_push=True,
        xcom_all=True,
    )


def create_lakehouse_completion_operator(*, table_name):
    """Join an Iceberg parent write and its intentional no-op branch.

    A skipped/no-op parent permits FK children; a failed parent does not.
    """
    return EmptyOperator(
        task_id=f"iceberg_{table_name}_completion",
        trigger_rule=PARENT_COMPLETION_TRIGGER_RULE,
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
    )


def create_gold_dimensional_operator(*, source_task_id, project_path, mode="DRY_RUN"):
    """Create the single native dimensional graph writer.

    This operator has no PostgreSQL/Warehouse dependency; lineage and Silver
    reads are resolved by the Spark dimensional CLI from attested Iceberg data.
    """
    if mode not in {"DRY_RUN", "WRITE"}:
        raise ValueError("Unsupported dimensional Gold mode")
    bucket = os.environ.get("BRONZE_BUCKET", "ecommerce-quality-it")
    scope = None
    if bucket == "ecommerce-data-lake":
        try:
            scope = build_bounded_e2e_scope(os.environ)
        except BoundedE2EScopeError as exc:
            raise ValueError(f"Refusing unscoped ecommerce-data-lake Gold operator: {exc}") from exc
    arguments = [
        "--source-lineage",
        "{{{{ ti.xcom_pull(task_ids='{}') | tojson }}}}".format(source_task_id),
        "--dim-date-start", os.environ.get("GOLD_DIM_DATE_START", "2020-01-01"),
        "--dim-date-end", os.environ.get("GOLD_DIM_DATE_END", "2035-12-31"),
        "--mode", mode,
        "--bucket", bucket,
    ]
    extra_environment = {
        "GOLD_DIMENSIONAL_WRITE_ENABLED": os.environ.get("GOLD_DIMENSIONAL_WRITE_ENABLED", "false"),
        "GOLD_RUN_METADATA_PERSIST_ENABLED": os.environ.get("GOLD_RUN_METADATA_PERSIST_ENABLED", "false"),
    }
    if extra_environment["GOLD_RUN_METADATA_PERSIST_ENABLED"].lower() == "true":
        extra_environment.update({
            "DB_HOST": os.environ.get("DB_HOST", "postgres"),
            "DB_PORT": os.environ.get("DB_PORT", "5432"),
            "DB_NAME": os.environ.get("DB_NAME", "ecommerce_platform"),
            "DB_USER": os.environ.get("DB_USER", "postgres"),
            "DB_PASSWORD": os.environ.get("DB_PASSWORD", ""),
            "GOLD_RUN_ID": os.environ.get("GOLD_RUN_ID", ""),
            "GOLD_CODE_SHA": os.environ.get("GOLD_CODE_SHA", "local000"),
        })
    if scope is not None:
        arguments.extend(["--warehouse", scope.warehouse, "--nessie-ref", scope.nessie_ref])
        extra_environment.update({
            "LAKEHOUSE_E2E_APPROVED": "1",
            "LAKEHOUSE_E2E_TEST_BUCKET": scope.bucket,
            "LAKEHOUSE_E2E_RUN_ID": scope.run_id,
            "LAKEHOUSE_E2E_PREFIX": scope.prefix,
        })
    return create_spark_submit_operator(
        task_id="gold_dimensional_write",
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/gold/implementation/dimensional/run_pipeline.py",
        arguments=arguments,
        extra_environment=extra_environment,
        extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)],
        do_xcom_push=True,
    )


def create_gold_dimensional_confirmation_operator(*, table_name):
    """Non-writing confirmation node for one native dimensional output."""
    return EmptyOperator(
        task_id=f"confirm_gold_dimensional_{table_name}",
        trigger_rule="none_failed_min_one_success",
        retries=0,
    )


def create_gold_dimensional_graph_verification_operator():
    """Non-writing barrier: the native writer returns only after graph manifest verification."""
    return EmptyOperator(
        task_id="verify_gold_dimensional_graph_manifest",
        trigger_rule="all_success",
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
    retries=IDEMPOTENT_RETRIES,
    do_xcom_push=False,
    xcom_all=False,
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
        "NESSIE_URI": os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2"),
        "NESSIE_REF": os.environ.get("NESSIE_REF", "main"),
        "PYTHONPATH": SPARK_PYTHONPATH,
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
        do_xcom_push=do_xcom_push,
        xcom_all=xcom_all,
        retries=retries,
        retry_delay=IDEMPOTENT_RETRY_DELAY,
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
        execution_timeout=IDEMPOTENT_EXECUTION_TIMEOUT,
        timeout=DOCKER_API_TIMEOUT_SECONDS,
    )
