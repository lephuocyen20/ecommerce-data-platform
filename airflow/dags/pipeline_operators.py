"""Reusable Airflow DockerOperator factories for Spark pipeline stages."""

import os

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "ecommerce-spark:3.5.6")
DBT_RUNNER_IMAGE = os.environ.get(
    "DBT_RUNNER_IMAGE", "ecommerce-data-platform-dbt-runner:latest"
)
DOCKER_NETWORK = os.environ.get(
    "AIRFLOW_DOCKER_NETWORK", "ecommerce-data-platform_default"
)
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


def create_lakehouse_migration_operator(*, table_name, project_path):
    """Generic verified-Parquet to Iceberg task; table behavior lives in TableSpec."""
    manifest_key = "{{ ti.xcom_pull(task_ids='verify_silver_" + table_name + "')['manifest_key'] }}"
    return create_spark_submit_operator(task_id=f"silver_parquet_to_iceberg_{table_name}", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/run.py", arguments=["--table-name", table_name, "--manifest-key", manifest_key], extra_environment={"LAKEHOUSE_WRITE_ENABLED": os.environ.get("LAKEHOUSE_WRITE_ENABLED", "false")}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0)


def create_lakehouse_reconcile_operator(*, table_name, project_path):
    return create_spark_submit_operator(task_id=f"verify_iceberg_{table_name}", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/reconcile.py", arguments=["--table-name", table_name], extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0)


def create_lakehouse_warehouse_operator(*, table_name, project_path):
    batch_id = "{{ ti.xcom_pull(task_ids='validate_" + table_name + "')['batch_id'] }}"
    return create_spark_submit_operator(task_id=f"iceberg_{table_name}_to_warehouse", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/warehouse/load.py", arguments=["--table-name", table_name, "--batch-id", batch_id], extra_environment={"WAREHOUSE_DB_HOST": "postgres", "WAREHOUSE_DB_PORT": os.environ["DB_PORT"], "WAREHOUSE_DB_NAME": os.environ["DB_NAME"], "WAREHOUSE_DB_USER": os.environ["DB_USER"], "WAREHOUSE_DB_PASSWORD": os.environ["DB_PASSWORD"]}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True), Mount(source=project_path + r"\warehouse", target=f"{SPARK_WORKDIR}/warehouse", type="bind", read_only=True)], retries=0)


def create_silver_parquet_to_iceberg_customers_operator(*, project_path):
    """Migrate the verified Silver Parquet customers manifest into Iceberg."""
    manifest_key = "{{ ti.xcom_pull(task_ids='verify_silver_customers')['manifest_key'] }}"
    return create_spark_submit_operator(
        task_id="silver_parquet_to_iceberg_customers",
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/lakehouse/migration/run.py",
        arguments=["--table-name", "customers", "--manifest-key", manifest_key],
        extra_environment={"LAKEHOUSE_WRITE_ENABLED": os.environ.get("LAKEHOUSE_WRITE_ENABLED", "false")},
        extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)],
        retries=0,
    )


def create_silver_parquet_to_iceberg_products_operator(*, project_path):
    """Migrate the verified Silver Parquet products manifest into Iceberg."""
    manifest_key = "{{ ti.xcom_pull(task_ids='verify_silver_products')['manifest_key'] }}"
    return create_spark_submit_operator(
        task_id="silver_parquet_to_iceberg_products", project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/lakehouse/migration/run.py",
        arguments=["--table-name", "products", "--manifest-key", manifest_key],
        extra_environment={"LAKEHOUSE_WRITE_ENABLED": os.environ.get("LAKEHOUSE_WRITE_ENABLED", "false")},
        extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0,
    )

def create_silver_parquet_to_iceberg_orders_operator(*, project_path):
    key = "{{ ti.xcom_pull(task_ids='verify_silver_orders')['manifest_key'] }}"
    return create_spark_submit_operator(task_id="silver_parquet_to_iceberg_orders", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/run.py", arguments=["--table-name", "orders", "--manifest-key", key], extra_environment={"LAKEHOUSE_WRITE_ENABLED": os.environ.get("LAKEHOUSE_WRITE_ENABLED", "false")}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0)


def create_silver_parquet_to_iceberg_order_items_operator(*, project_path):
    """Migrate the verified Silver order-items manifest into Iceberg."""
    key = "{{ ti.xcom_pull(task_ids='verify_silver_order_items')['manifest_key'] }}"
    return create_spark_submit_operator(task_id="silver_parquet_to_iceberg_order_items", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/run.py", arguments=["--table-name", "order_items", "--manifest-key", key], extra_environment={"LAKEHOUSE_WRITE_ENABLED": os.environ.get("LAKEHOUSE_WRITE_ENABLED", "false")}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0)


def create_silver_parquet_to_iceberg_payments_operator(*, project_path):
    key = "{{ ti.xcom_pull(task_ids='verify_silver_payments')['manifest_key'] }}"
    return create_spark_submit_operator(task_id="silver_parquet_to_iceberg_payments", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/run.py", arguments=["--table-name", "payments", "--manifest-key", key], extra_environment={"LAKEHOUSE_WRITE_ENABLED": os.environ.get("LAKEHOUSE_WRITE_ENABLED", "false")}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0)

def create_silver_parquet_to_iceberg_shipments_operator(*, project_path):
    key = "{{ ti.xcom_pull(task_ids='verify_silver_shipments')['manifest_key'] }}"
    return create_spark_submit_operator(task_id="silver_parquet_to_iceberg_shipments", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/run.py", arguments=["--table-name", "shipments", "--manifest-key", key], extra_environment={"LAKEHOUSE_WRITE_ENABLED": os.environ.get("LAKEHOUSE_WRITE_ENABLED", "false")}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0)


def create_reconcile_iceberg_customers_operator(*, project_path):
    manifest_key = "{{ ti.xcom_pull(task_ids='verify_silver_customers')['manifest_key'] }}"
    return create_spark_submit_operator(
        task_id="verify_iceberg_customers",
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/lakehouse/migration/reconcile.py",
        arguments=["--table-name", "customers"],
        extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)],
        retries=0,
    )


def create_reconcile_iceberg_products_operator(*, project_path):
    manifest_key = "{{ ti.xcom_pull(task_ids='verify_silver_products')['manifest_key'] }}"
    return create_spark_submit_operator(
        task_id="verify_iceberg_products", project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/lakehouse/migration/reconcile.py", arguments=["--table-name", "products"],
        extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0,
    )

def create_reconcile_iceberg_orders_operator(*, project_path):
    key = "{{ ti.xcom_pull(task_ids='verify_silver_orders')['manifest_key'] }}"
    return create_spark_submit_operator(task_id="verify_iceberg_orders", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/reconcile.py", arguments=["--table-name", "orders"], extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0)


def create_reconcile_iceberg_order_items_operator(*, project_path):
    return create_spark_submit_operator(task_id="verify_iceberg_order_items", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/reconcile.py", arguments=["--table-name", "order_items"], extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0)


def create_reconcile_iceberg_payments_operator(*, project_path):
    return create_spark_submit_operator(task_id="verify_iceberg_payments", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/reconcile.py", arguments=["--table-name", "payments"], extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0)

def create_reconcile_iceberg_shipments_operator(*, project_path):
    return create_spark_submit_operator(task_id="verify_iceberg_shipments", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/migration/reconcile.py", arguments=["--table-name", "shipments"], extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True)], retries=0)


def create_iceberg_customers_warehouse_operator(*, project_path):
    batch_id = "{{ ti.xcom_pull(task_ids='validate_customers')['batch_id'] }}"
    return create_spark_submit_operator(
        task_id="iceberg_customers_to_warehouse",
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/lakehouse/warehouse/load.py",
        arguments=["--table-name", "customers", "--batch-id", batch_id],
        extra_environment={"WAREHOUSE_DB_HOST": "postgres", "WAREHOUSE_DB_PORT": os.environ["DB_PORT"], "WAREHOUSE_DB_NAME": os.environ["DB_NAME"], "WAREHOUSE_DB_USER": os.environ["DB_USER"], "WAREHOUSE_DB_PASSWORD": os.environ["DB_PASSWORD"]},
        extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True), Mount(source=project_path + r"\warehouse", target=f"{SPARK_WORKDIR}/warehouse", type="bind", read_only=True)],
        retries=0,
    )


def create_iceberg_products_warehouse_operator(*, project_path):
    batch_id = "{{ ti.xcom_pull(task_ids='validate_products')['batch_id'] }}"
    return create_spark_submit_operator(
        task_id="iceberg_products_to_warehouse", project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/lakehouse/warehouse/load.py", arguments=["--table-name", "products", "--batch-id", batch_id],
        extra_environment={"WAREHOUSE_DB_HOST": "postgres", "WAREHOUSE_DB_PORT": os.environ["DB_PORT"], "WAREHOUSE_DB_NAME": os.environ["DB_NAME"], "WAREHOUSE_DB_USER": os.environ["DB_USER"], "WAREHOUSE_DB_PASSWORD": os.environ["DB_PASSWORD"]},
        extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True), Mount(source=project_path + r"\warehouse", target=f"{SPARK_WORKDIR}/warehouse", type="bind", read_only=True)], retries=0,
    )

def create_iceberg_orders_warehouse_operator(*, project_path):
    batch_id = "{{ ti.xcom_pull(task_ids='validate_orders')['batch_id'] }}"
    return create_spark_submit_operator(task_id="iceberg_orders_to_warehouse", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/warehouse/load.py", arguments=["--table-name", "orders", "--batch-id", batch_id], extra_environment={"WAREHOUSE_DB_HOST": "postgres", "WAREHOUSE_DB_PORT": os.environ["DB_PORT"], "WAREHOUSE_DB_NAME": os.environ["DB_NAME"], "WAREHOUSE_DB_USER": os.environ["DB_USER"], "WAREHOUSE_DB_PASSWORD": os.environ["DB_PASSWORD"]}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True), Mount(source=project_path + r"\warehouse", target=f"{SPARK_WORKDIR}/warehouse", type="bind", read_only=True)], retries=0)


def create_iceberg_order_items_warehouse_operator(*, project_path):
    batch_id = "{{ ti.xcom_pull(task_ids='validate_order_items')['batch_id'] }}"
    return create_spark_submit_operator(task_id="iceberg_order_items_to_warehouse", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/warehouse/load.py", arguments=["--table-name", "order_items", "--batch-id", batch_id], extra_environment={"WAREHOUSE_DB_HOST": "postgres", "WAREHOUSE_DB_PORT": os.environ["DB_PORT"], "WAREHOUSE_DB_NAME": os.environ["DB_NAME"], "WAREHOUSE_DB_USER": os.environ["DB_USER"], "WAREHOUSE_DB_PASSWORD": os.environ["DB_PASSWORD"]}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True), Mount(source=project_path + r"\warehouse", target=f"{SPARK_WORKDIR}/warehouse", type="bind", read_only=True)], retries=0)


def create_iceberg_payments_warehouse_operator(*, project_path):
    batch_id = "{{ ti.xcom_pull(task_ids='validate_payments')['batch_id'] }}"
    return create_spark_submit_operator(task_id="iceberg_payments_to_warehouse", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/warehouse/load.py", arguments=["--table-name", "payments", "--batch-id", batch_id], extra_environment={"WAREHOUSE_DB_HOST": "postgres", "WAREHOUSE_DB_PORT": os.environ["DB_PORT"], "WAREHOUSE_DB_NAME": os.environ["DB_NAME"], "WAREHOUSE_DB_USER": os.environ["DB_USER"], "WAREHOUSE_DB_PASSWORD": os.environ["DB_PASSWORD"]}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True), Mount(source=project_path + r"\warehouse", target=f"{SPARK_WORKDIR}/warehouse", type="bind", read_only=True)], retries=0)

def create_iceberg_shipments_warehouse_operator(*, project_path):
    batch_id = "{{ ti.xcom_pull(task_ids='validate_shipments')['batch_id'] }}"
    return create_spark_submit_operator(task_id="iceberg_shipments_to_warehouse", project_path=project_path, script_path=f"{SPARK_WORKDIR}/lakehouse/warehouse/load.py", arguments=["--table-name", "shipments", "--batch-id", batch_id], extra_environment={"WAREHOUSE_DB_HOST": "postgres", "WAREHOUSE_DB_PORT": os.environ["DB_PORT"], "WAREHOUSE_DB_NAME": os.environ["DB_NAME"], "WAREHOUSE_DB_USER": os.environ["DB_USER"], "WAREHOUSE_DB_PASSWORD": os.environ["DB_PASSWORD"]}, extra_mounts=[Mount(source=project_path + r"\lakehouse", target=f"{SPARK_WORKDIR}/lakehouse", type="bind", read_only=True), Mount(source=project_path + r"\warehouse", target=f"{SPARK_WORKDIR}/warehouse", type="bind", read_only=True)], retries=0)


def create_dbt_build_operator(*, project_path):
    """Create the dbt build task used after a successful Warehouse load."""
    return DockerOperator(
        task_id="dbt_build",
        image=DBT_RUNNER_IMAGE,
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
