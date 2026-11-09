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


def _dependency_policy():
    path = Path(__file__).parents[1] / "airflow/dags/lakehouse_dependencies.py"
    spec = importlib.util.spec_from_file_location("lakehouse_dependencies_for_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parent_completion_contract_allows_noop_but_never_a_failed_parent():
    policy = _dependency_policy()

    assert policy.PARENT_COMPLETION_TRIGGER_RULE == "none_failed_min_one_success"
    assert policy.parent_completion_permits_child(("success", "skipped"))
    assert policy.parent_completion_permits_child(("skipped", "success"))
    assert not policy.parent_completion_permits_child(("skipped", "skipped"))
    assert not policy.parent_completion_permits_child(("failed", "skipped"))
    assert not policy.parent_completion_permits_child(("upstream_failed", "success"))


def test_unselected_parent_is_a_successful_noop_for_fk_completion():
    # ingest_incremental now returns NO_DATA instead of task-level SKIPPED;
    # the branch skip task therefore executes successfully.
    policy = _dependency_policy()
    assert policy.parent_completion_permits_child(("success", "skipped"))


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


def test_declared_fk_parent_graph_is_complete_and_deterministic():
    graph = {
        "orders": ("customers",),
        "order_items": ("orders", "products"),
        "payments": ("orders",),
        "shipments": ("orders",),
    }
    assert _dependency_policy().foreign_key_parents() == graph

    from lakehouse.models.spec import SPECS

    spec_graph = {
        name: tuple(sorted(set((spec.foreign_keys or {}).values())))
        for name, spec in SPECS.items()
        if spec.foreign_keys
    }
    assert graph == spec_graph


def test_completion_operator_uses_the_noop_safe_trigger_rule(monkeypatch):
    policy = _dependency_policy()
    monkeypatch.setitem(sys.modules, "lakehouse_dependencies", policy)

    class _EmptyOperator:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    docker_operator = types.ModuleType("airflow.providers.docker.operators.docker")
    docker_operator.DockerOperator = object
    empty_operator = types.ModuleType("airflow.providers.standard.operators.empty")
    empty_operator.EmptyOperator = _EmptyOperator
    docker_types = types.ModuleType("docker.types")
    docker_types.Mount = object
    for name, module in {
        "airflow": types.ModuleType("airflow"),
        "airflow.providers": types.ModuleType("airflow.providers"),
        "airflow.providers.docker": types.ModuleType("airflow.providers.docker"),
        "airflow.providers.docker.operators": types.ModuleType("airflow.providers.docker.operators"),
        "airflow.providers.docker.operators.docker": docker_operator,
        "airflow.providers.standard": types.ModuleType("airflow.providers.standard"),
        "airflow.providers.standard.operators": types.ModuleType("airflow.providers.standard.operators"),
        "airflow.providers.standard.operators.empty": empty_operator,
        "docker": types.ModuleType("docker"),
        "docker.types": docker_types,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    path = Path(__file__).parents[1] / "airflow/dags/pipeline_operators.py"
    spec = importlib.util.spec_from_file_location("pipeline_operators_for_test", path)
    operators = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(operators)

    task = operators.create_lakehouse_completion_operator(table_name="orders")
    assert task.kwargs == {
        "task_id": "iceberg_orders_completion",
        "trigger_rule": "none_failed_min_one_success",
        "retries": 0,
    }


def test_spark_factory_uses_bounded_retry_and_timeout(monkeypatch):
    policy = _dependency_policy()
    monkeypatch.setitem(sys.modules, "lakehouse_dependencies", policy)

    class _DockerOperator:
        def __init__(self, **kwargs): self.kwargs = kwargs
    class _Mount:
        def __init__(self, **kwargs): self.kwargs = kwargs
    docker_operator = types.ModuleType("airflow.providers.docker.operators.docker"); docker_operator.DockerOperator = _DockerOperator
    empty_operator = types.ModuleType("airflow.providers.standard.operators.empty"); empty_operator.EmptyOperator = object
    docker_types = types.ModuleType("docker.types"); docker_types.Mount = _Mount
    for name, module in {"airflow": types.ModuleType("airflow"), "airflow.providers": types.ModuleType("airflow.providers"), "airflow.providers.docker": types.ModuleType("airflow.providers.docker"), "airflow.providers.docker.operators": types.ModuleType("airflow.providers.docker.operators"), "airflow.providers.docker.operators.docker": docker_operator, "airflow.providers.standard": types.ModuleType("airflow.providers.standard"), "airflow.providers.standard.operators": types.ModuleType("airflow.providers.standard.operators"), "airflow.providers.standard.operators.empty": empty_operator, "docker": types.ModuleType("docker"), "docker.types": docker_types}.items(): monkeypatch.setitem(sys.modules, name, module)
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


def test_dbt_factory_explicitly_keeps_single_attempt_policy():
    source = (Path(__file__).parents[1] / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    start = source.index("def create_dbt_build_operator")
    section = source[start:source.index("def create_silver_snapshot_operator", start)]
    assert "retries=0" in section


def test_quality_cli_uses_absolute_imports_for_spark_submit():
    from pathlib import Path

    source = (Path(__file__).parents[1] / "lakehouse/quality/cli.py").read_text()
    assert "from lakehouse.quality.contracts import" in source
    assert "from lakehouse.quality.runner import" in source
