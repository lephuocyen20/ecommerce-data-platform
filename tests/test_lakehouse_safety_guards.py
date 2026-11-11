"""Mock-only regressions for Lakehouse migration safety guards."""

from types import SimpleNamespace
import importlib.util
from pathlib import Path
import sys
import types

import pytest

from lakehouse.models import engine


pytestmark = pytest.mark.unit


class _Prepared:
    def __init__(self):
        self.view = None

    def createOrReplaceTempView(self, view):
        self.view = view


class _Result:
    def __init__(self, value):
        self.value = value

    def first(self):
        return SimpleNamespace(n=self.value)


class _Spark:
    def __init__(self, conflicts=0):
        self.conflicts = conflicts
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        if "SELECT count(*) n" in query:
            return _Result(self.conflicts)
        return _Result(None)


def _spec():
    return SimpleNamespace(name="customers", key="customer_id", immutable=False, audit_column=None)


def test_equal_watermark_with_different_payload_fails_before_merge(monkeypatch):
    monkeypatch.setattr(engine, "ensure_table", lambda *_args: None)
    spark, prepared = _Spark(conflicts=1), _Prepared()

    with pytest.raises(ValueError, match="equal-watermark conflict"):
        engine.merge(spark, prepared, _spec())

    assert prepared.view == "lakehouse_staged_customers"
    assert not any("MERGE INTO lakehouse.silver.customers t" in query for query in spark.queries)


def test_equal_watermark_with_same_payload_keeps_strict_newer_merge(monkeypatch):
    monkeypatch.setattr(engine, "ensure_table", lambda *_args: None)
    spark = _Spark(conflicts=0)

    engine.merge(spark, _Prepared(), _spec())

    assert any("s._source_updated_at=t._source_updated_at" in query.replace(" ", "") for query in spark.queries)
    assert any("WHEN MATCHED AND s._source_updated_at > t._source_updated_at" in query for query in spark.queries)


def test_partial_dry_run_unselected_table_uses_existing_silver_skip_contract():
    from airflow.dags.pipeline_tasks.ingestion import (
        build_unselected_ingestion_result,
        validate_unselected_ingestion_result,
    )
    result = build_unselected_ingestion_result("customers", "DRY_RUN")
    assert validate_unselected_ingestion_result("customers", result) == {
        "table_name": "customers", "mode": "DRY_RUN", "status": "SKIPPED", "reason": "DRY_RUN"
    }


def test_partial_write_unselected_table_uses_existing_silver_skip_contract():
    from airflow.dags.pipeline_tasks.ingestion import (
        build_unselected_ingestion_result,
        validate_unselected_ingestion_result,
    )
    result = build_unselected_ingestion_result("customers", "WRITE")
    assert validate_unselected_ingestion_result("customers", result) == {
        "table_name": "customers", "mode": "WRITE", "status": "SKIPPED", "reason": "NO_DATA"
    }


def test_spark_factory_uses_bounded_retry_and_timeout(monkeypatch):
    class _DockerOperator:
        def __init__(self, **kwargs): self.kwargs = kwargs
    class _Mount:
        def __init__(self, **kwargs): self.kwargs = kwargs
    docker_operator = types.ModuleType("airflow.providers.docker.operators.docker"); docker_operator.DockerOperator = _DockerOperator
    empty_operator = types.ModuleType("airflow.providers.standard.operators.empty"); empty_operator.EmptyOperator = object
    airflow_exceptions = types.ModuleType("airflow.exceptions")
    airflow_exceptions.AirflowSkipException = type("AirflowSkipException", (Exception,), {})
    docker_types = types.ModuleType("docker.types"); docker_types.Mount = _Mount
    for name, module in {"airflow": types.ModuleType("airflow"), "airflow.exceptions": airflow_exceptions, "airflow.providers": types.ModuleType("airflow.providers"), "airflow.providers.docker": types.ModuleType("airflow.providers.docker"), "airflow.providers.docker.operators": types.ModuleType("airflow.providers.docker.operators"), "airflow.providers.docker.operators.docker": docker_operator, "airflow.providers.standard": types.ModuleType("airflow.providers.standard"), "airflow.providers.standard.operators": types.ModuleType("airflow.providers.standard.operators"), "airflow.providers.standard.operators.empty": empty_operator, "docker": types.ModuleType("docker"), "docker.types": docker_types}.items(): monkeypatch.setitem(sys.modules, name, module)
    path = Path(__file__).parents[1] / "airflow/dags/pipeline_operators.py"; spec = importlib.util.spec_from_file_location("pipeline_operators_retry_test", path); operators = importlib.util.module_from_spec(spec); spec.loader.exec_module(operators)
    monkeypatch.setenv("MINIO_ENDPOINT", "minio:9000"); monkeypatch.setenv("MINIO_ACCESS_KEY", "key"); monkeypatch.setenv("MINIO_SECRET_KEY", "secret"); monkeypatch.setenv("BRONZE_BUCKET", "bronze")
    task = operators.create_spark_submit_operator(task_id="safe", project_path="C:\\project", script_path="/job.py")
    assert task.kwargs["retries"] == 2
    assert task.kwargs["retry_exponential_backoff"] is True
    assert task.kwargs["execution_timeout"].total_seconds() == 7200
    assert task.kwargs["timeout"] == 60
    assert task.kwargs["private_environment"]["PYTHONPATH"] == (
        "/opt/spark/work-dir:/opt/spark/python:"
        "/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
    )


def test_quality_cli_uses_absolute_imports_for_spark_submit():
    from pathlib import Path

    source = (Path(__file__).parents[1] / "lakehouse/quality/cli.py").read_text()
    assert "from lakehouse.quality.contracts import" in source
    assert "from lakehouse.quality.runner import" in source
