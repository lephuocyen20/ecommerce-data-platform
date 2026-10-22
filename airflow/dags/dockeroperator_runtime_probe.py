from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator


with DAG(
    dag_id="dockeroperator_runtime_probe",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["test", "dockeroperator", "runtime"],
) as dag:

    spark_container_probe = DockerOperator(
        task_id="spark_container_probe",
        image="ecommerce-spark:3.5.6",
        docker_url="unix:///var/run/docker.sock",
        network_mode="ecommerce-data-platform_default",
        command=["python3", "-c", "print(12345)"],
        mount_tmp_dir=False,
        auto_remove="force",
        do_xcom_push=False,
        retries=0,
    )
