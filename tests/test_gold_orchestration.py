"""Regression tests for the native dimensional Gold orchestration."""

import importlib.util
import importlib
import copy
from types import SimpleNamespace
from types import ModuleType
from pathlib import Path
import sys

import pytest

pytestmark = pytest.mark.unit


def _dag_module(name):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        path = dags_path / f"{name}.py"
        spec = importlib.util.spec_from_file_location(f"{name}_for_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(dags_path))


def _runner_module():
    path = Path(__file__).parents[1] / "gold/implementation/dimensional/run_pipeline.py"
    spec = importlib.util.spec_from_file_location("gold_dimensional_runner_for_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pipeline_operators_module(monkeypatch):
    """Load the preflight helper without requiring the optional Docker provider."""
    docker_operator = ModuleType("airflow.providers.docker.operators.docker")
    docker_operator.DockerOperator = type("DockerOperator", (), {})
    empty_operator = ModuleType("airflow.providers.standard.operators.empty")
    empty_operator.EmptyOperator = type("EmptyOperator", (), {})
    docker_types = ModuleType("docker.types")
    docker_types.Mount = type("Mount", (), {})
    airflow_exceptions = ModuleType("airflow.exceptions")
    airflow_exceptions.AirflowSkipException = type("AirflowSkipException", (Exception,), {})
    for name, module in {
        "airflow.providers": ModuleType("airflow.providers"),
        "airflow.providers.docker": ModuleType("airflow.providers.docker"),
        "airflow.providers.docker.operators": ModuleType("airflow.providers.docker.operators"),
        "airflow.providers.docker.operators.docker": docker_operator,
        "airflow.providers.standard": ModuleType("airflow.providers.standard"),
        "airflow.providers.standard.operators": ModuleType("airflow.providers.standard.operators"),
        "airflow.providers.standard.operators.empty": empty_operator,
        "airflow.exceptions": airflow_exceptions,
        "docker.types": docker_types,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    return importlib.import_module("pipeline_operators")


def test_dimensional_gate_is_fail_closed_and_requires_verified_write_summary():
    policy = _dag_module("gold_gate_policy")
    verified_write = {
        "status": "PASS",
        "mode": "WRITE",
        "results": {"orders": {"status": "PASS"}},
    }
    assert policy.decide_gold_dimensional_gate(verified_write, enabled=True) == "resolve_gold_dimensional_lineage"
    assert policy.decide_gold_dimensional_gate(verified_write, enabled=False) == "skip_gold_dimensional"

    with pytest.raises(ValueError, match="successful Silver"):
        policy.decide_gold_dimensional_gate({**verified_write, "status": "FAILED"}, enabled=True)
    with pytest.raises(ValueError, match="WRITE"):
        policy.decide_gold_dimensional_gate({**verified_write, "mode": "DRY_RUN"}, enabled=True)


def test_disabled_gold_path_is_an_explicit_skip():
    policy = _dag_module("gold_gate_policy")
    assert policy.decide_gold_dimensional_gate({}, enabled=False) == "skip_gold_dimensional"


def test_controlled_dry_run_orchestration_propagates_one_real_merge_plan():
    from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
    from gold.implementation.dimensional.snapshot_reuse import (
        GOLD_SNAPSHOT_REUSE,
        SnapshotReuseReader,
        resolve_dimensional_inputs,
    )
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        from pipeline_tasks.gold import run_controlled_incremental_dry_run
    finally:
        sys.path.remove(str(dags_path))

    artifacts = {
        table: {"status": "PASS", "table_name": table}
        for table in DIMENSIONAL_SOURCE_TABLES
    }
    artifacts["customers"] = {"status": "NO_DATA", "table_name": "customers"}
    from test_gold_snapshot_reuse_contract import _reuse
    reuse = _reuse("customers")
    resolved = resolve_dimensional_inputs(
        artifacts, reuse_artifacts={"customers": reuse}, feature_enabled=True
    )
    deltas = {table: object() for table in DIMENSIONAL_SOURCE_TABLES if table != "customers"}
    captured = {}

    class VerifiedReader(SnapshotReuseReader):
        def read_verified_snapshot(self, _resolved):
            return {
                "status": "PASS", "table": "dim_customer", "snapshot_id": 41,
                "dataframe": object(),
            }

    result = run_controlled_incremental_dry_run(
        resolved,
        deltas,
        snapshot_reader=VerifiedReader(),
        writer=lambda **kwargs: captured.update(kwargs) or {"status": "DRY_RUN"},
    )
    assert result["status"] == "DRY_RUN"
    assert captured["mode"] == "DRY_RUN"
    assert captured["merge_plan"] is result["merge_plan"]


def test_controlled_task_disabled_is_noop_without_clients(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        from pipeline_tasks.gold import _execute_controlled_incremental_snapshot_reuse_dry_run
    finally:
        sys.path.remove(str(dags_path))

    sentinel = object()
    result = _execute_controlled_incremental_snapshot_reuse_dry_run(
        enabled=False,
        resolved_inputs=sentinel,
        silver_deltas=sentinel,
        snapshot_reader=lambda: pytest.fail("disabled task must not create/read clients"),
        writer=lambda **_: pytest.fail("disabled task must not write"),
    )
    assert result == {
        "status": "SKIPPED",
        "reason": "GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED=false",
    }


def test_controlled_task_enabled_passes_same_merge_plan_to_writer():
    from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
    from gold.implementation.dimensional.snapshot_reuse import (
        SnapshotReuseReader,
        resolve_dimensional_inputs,
    )
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        from pipeline_tasks.gold import _execute_controlled_incremental_snapshot_reuse_dry_run
    finally:
        sys.path.remove(str(dags_path))

    artifacts = {
        table: {"status": "PASS", "table_name": table}
        for table in DIMENSIONAL_SOURCE_TABLES
    }
    artifacts["customers"] = {"status": "NO_DATA", "table_name": "customers"}
    from test_gold_snapshot_reuse_contract import _reuse
    resolved = resolve_dimensional_inputs(
        artifacts,
        reuse_artifacts={"customers": _reuse("customers")},
        feature_enabled=True,
    )
    deltas = {table: object() for table in DIMENSIONAL_SOURCE_TABLES if table != "customers"}
    captured = {}

    class VerifiedReader(SnapshotReuseReader):
        def read_verified_snapshot(self, _resolved):
            return {
                "status": "PASS", "table": "dim_customer", "snapshot_id": 41,
                "dataframe": object(),
            }

    result = _execute_controlled_incremental_snapshot_reuse_dry_run(
        enabled=True,
        resolved_inputs=resolved,
        silver_deltas=deltas,
        snapshot_reader=VerifiedReader(),
        writer=lambda **kwargs: captured.update(kwargs) or {"status": "DRY_RUN"},
    )
    assert result["status"] == "DRY_RUN"
    assert captured["merge_plan"] is result["merge_plan"]
    assert captured["mode"] == "DRY_RUN"


def test_actual_dag_contains_disabled_snapshot_reuse_dry_run_gate():
    source = (Path(__file__).parents[1] / "airflow" / "dags" / "ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")
    assert "controlled_incremental_snapshot_reuse_dry_run_task" in source
    assert "controlled_snapshot_reuse_gate >> dimensional_writer" in source
    assert "GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED=false" in (
        Path(__file__).parents[1] / "airflow" / "dags" / "pipeline_tasks" / "gold.py"
    ).read_text(encoding="utf-8")


def test_actual_controlled_task_wrapper_uses_serializable_spec_and_same_plan(monkeypatch):
    from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
    from gold.implementation.dimensional.snapshot_reuse import resolve_dimensional_inputs

    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        import pipeline_tasks.gold as gold_tasks
    finally:
        sys.path.remove(str(dags_path))

    artifacts = {table: {"status": "PASS", "table_name": table} for table in DIMENSIONAL_SOURCE_TABLES}
    artifacts["customers"] = {"status": "NO_DATA", "table_name": "customers"}
    from test_gold_snapshot_reuse_contract import _reuse

    resolved = resolve_dimensional_inputs(
        artifacts, reuse_artifacts={"customers": _reuse("customers")}, feature_enabled=True
    )
    spec = {
        "version": 1,
        "resolved_inputs": resolved,
        "silver_delta_refs": {table: f"silver://{table}/manifest.json" for table in DIMENSIONAL_SOURCE_TABLES if table != "customers"},
        "snapshot_refs": {"customers": "gold://dim_customer/snapshot-41"},
    }
    monkeypatch.setenv("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", "true")
    task_wrapper = getattr(
        gold_tasks.controlled_incremental_snapshot_reuse_dry_run_task,
        "python_callable",
        gold_tasks.controlled_incremental_snapshot_reuse_dry_run_task,
    )
    result = task_wrapper(
        controlled_spec={"mode": "DRY_RUN", "tables": list(DIMENSIONAL_SOURCE_TABLES), "controlled_dry_run_spec": spec}
    )
    assert result["status"] == "DRY_RUN"
    assert result["writer_result"]["merge_plan_identity"] == result["merge_plan"]["identity"]
    assert result["writer_result"]["snapshot_refs"] == spec["snapshot_refs"]
    assert all(isinstance(value, str) for value in spec["silver_delta_refs"].values())


def test_actual_dag_passes_only_serializable_config_to_controlled_task():
    source = (Path(__file__).parents[1] / "airflow" / "dags" / "ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")
    assert "controlled_spec=config" in source
    assert "snapshot_reader=" not in source
    assert "writer=" not in source


def test_controlled_spec_absent_or_disabled_does_not_create_clients(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        from pipeline_tasks.gold import _execute_controlled_incremental_snapshot_reuse_dry_run
    finally:
        sys.path.remove(str(dags_path))

    monkeypatch.delenv("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", raising=False)
    assert _execute_controlled_incremental_snapshot_reuse_dry_run(enabled=False, controlled_spec={"bad": object()})["status"] == "SKIPPED"
    monkeypatch.setenv("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", "true")
    assert _execute_controlled_incremental_snapshot_reuse_dry_run(enabled=True, controlled_spec=None)["status"] == "SKIPPED"


def test_source_artifact_parser_rejects_malformed_duplicate_and_non_object_inputs():
    runner = _runner_module()
    with pytest.raises(ValueError, match="table=JSON"):
        runner.load_artifacts(["not-an-assignment"])
    with pytest.raises(ValueError, match="Invalid JSON"):
        runner.load_artifacts(["orders={bad-json"])
    with pytest.raises(ValueError, match="Duplicate"):
        runner.load_artifacts(["orders={}", "orders={}"])
    with pytest.raises(ValueError, match="Invalid object"):
        runner.load_artifacts(["orders=[]"])


def test_artifact_lineage_is_fail_closed_for_missing_or_wrong_source_keys():
    runner = _runner_module()
    lineage = {table: {"status": "PASS"} for table in ("customers", "products", "orders", "order_items", "payments", "shipments")}
    from gold.implementation.dimensional.lineage import build_dimensional_lineage_from_artifacts

    with pytest.raises(ValueError, match="six Silver"):
        build_dimensional_lineage_from_artifacts({**lineage, "unexpected": lineage["orders"]})
    with pytest.raises(ValueError, match="six Silver"):
        build_dimensional_lineage_from_artifacts({key: value for key, value in lineage.items() if key != "orders"})
    with pytest.raises(ValueError, match="Missing verified"):
        build_dimensional_lineage_from_artifacts(lineage)
    assert runner.load_artifacts(["orders={\"status\": \"PASS\"}"]) == {"orders": {"status": "PASS"}}


def test_legacy_source_lineage_is_still_validated_and_accepted():
    runner = _runner_module()
    digest = "a" * 64
    lineage = {
        "version": 1,
        "source_type": "iceberg_snapshot",
        "sources": {
            table: {
                "source_type": "iceberg_snapshot", "table": table,
                "catalog": "lakehouse", "namespace": "silver", "ref": "main",
                "snapshot_id": 1, "snapshot_attestation_identity": digest,
                "reconciliation_evidence": {"identity": digest, "status": "PASS", "snapshot_id": 1, "table": table},
                "quality_evidence": {"identity": digest, "status": "PASS", "snapshot_id": 1, "table": table},
            }
            for table in ("customers", "products", "orders", "order_items", "payments", "shipments")
        },
    }
    assert runner.load_lineage(__import__("json").dumps(lineage)) == lineage
    assert runner.validate_dimensional_lineage(lineage) is True


def test_write_requires_completed_graph_verification():
    runner = _runner_module()
    assert runner.require_verified_graph({"status": "VERIFIED"})["status"] == "VERIFIED"
    with pytest.raises(RuntimeError, match="VERIFIED graph"):
        runner.require_verified_graph({"status": "FAILED"})


def _patch_runner_runtime(monkeypatch, runner, *, events, verification=None):
    spark = SimpleNamespace(stop=lambda: events.append("spark.stop"))

    class Builder:
        def appName(self, _name):
            return self

        def config(self, _key, _value):
            return self

        def getOrCreate(self):
            events.append("spark.create")
            return spark

    monkeypatch.setattr(runner, "SparkSession", SimpleNamespace(builder=Builder()))
    monkeypatch.setattr(runner, "configure_s3a", lambda _spark: events.append("configure_s3a"))
    monkeypatch.setattr(runner, "create_minio_client", lambda: events.append("minio.create") or object())
    monkeypatch.setattr(runner, "validate_dimensional_lineage", lambda _lineage, scope=None: True)
    monkeypatch.setenv("BRONZE_BUCKET", "ecommerce-lakehouse-quality-it")
    monkeypatch.setenv("LAKEHOUSE_STORAGE_ENV", "quality-it")
    monkeypatch.setenv("QUALITY_IT_RUN_ID", "runner-test")
    monkeypatch.setenv("LAKEHOUSE_E2E_RUN_ID", "runner-test")
    monkeypatch.setenv("QUALITY_IT_PREFIX", "quality-it/runner-test/")
    monkeypatch.setattr(runner, "dimensional_lineage_identity", lambda _lineage, scope=None: "l" * 64)
    output_tables = [
        {"table": table, "manifest": {"snapshot_id": index, "quality": {"readback": {
            "status": "PASS", "snapshot_id": index, "file_count": 1, "file_bytes": 1, "row_count": 1,
        }}}}
        for index, table in enumerate(("dim_date", "dim_customer", "dim_product", "fact_orders",
                                       "fact_order_items", "fact_payments", "fact_shipments"), start=1)
    ]
    monkeypatch.setattr(
        runner,
        "run_dimensional_graph",
        lambda *_args, **_kwargs: events.append("run_dimensional_graph") or {
            "status": "WRITE", "graph": {"identity": "g" * 64}, "tables": output_tables,
        },
    )
    if verification is not None:
        verification_module = __import__(
            "gold.implementation.dimensional.verification", fromlist=["verify_completed_graph"]
        )
        monkeypatch.setattr(
            verification_module,
            "verify_completed_graph",
            lambda *_args, **_kwargs: events.append("verify_completed_graph") or verification,
        )


def _runner_args(runner, mode):
    return [
        "--source-lineage", '{"version": 1, "source_type": "parquet_manifest", "sources": {}}',
        "--dim-date-start", "2026-01-01",
        "--dim-date-end", "2026-01-02",
        "--mode", mode,
        "--bucket", "ecommerce-lakehouse-quality-it",
    ]


def test_main_dry_run_does_not_verify_completed_graph(monkeypatch):
    runner = _runner_module()
    events = []
    _patch_runner_runtime(monkeypatch, runner, events=events)

    runner.main(_runner_args(runner, runner.DRY_RUN))

    assert "run_dimensional_graph" in events
    assert "verify_completed_graph" not in events


def test_production_write_is_denied_without_separate_approval(monkeypatch):
    runner = _runner_module()
    monkeypatch.setenv("BRONZE_BUCKET", "ecommerce-lakehouse-prod")
    monkeypatch.setenv("LAKEHOUSE_STORAGE_ENV", "production")
    monkeypatch.setenv("NESSIE_REF", "gold_prod_current")
    monkeypatch.delenv("PRODUCTION_LAKEHOUSE_WRITE_APPROVED", raising=False)
    for name in ("LAKEHOUSE_E2E_APPROVED", "AIRFLOW_LOCAL_E2E_MODE", "QUALITY_IT_ENABLED", "QUALITY_IT_GATE2_APPROVED"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(runner, "SparkSession", SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: pytest.fail("Spark must not start"))))
    with pytest.raises(SystemExit, match="PRODUCTION_LAKEHOUSE_WRITE_APPROVED"):
        runner.main([
            "--source-lineage", '{"version": 1, "source_type": "parquet_manifest", "sources": {}}',
            "--dim-date-start", "2026-01-01", "--dim-date-end", "2026-01-02",
            "--mode", runner.WRITE, "--bucket", "ecommerce-lakehouse-prod",
        ])


def test_main_dry_run_never_persists_postgres_metadata_even_when_enabled(monkeypatch):
    runner = _runner_module()
    events = []
    _patch_runner_runtime(monkeypatch, runner, events=events)
    monkeypatch.setenv("GOLD_RUN_METADATA_PERSIST_ENABLED", "true")
    monkeypatch.setattr(runner, "build_certification_report", lambda **_kwargs: {"certified": False})
    monkeypatch.setattr(runner, "persist_if_enabled", lambda *args, **kwargs: pytest.fail("DRY_RUN must not persist metadata"))

    result = runner.main(_runner_args(runner, runner.DRY_RUN))

    assert result["run_metadata"] == {"persisted": False, "reason": "GOLD_RUN_METADATA_PERSIST_ENABLED=false"}


def test_main_write_verifies_only_after_dimensional_graph(monkeypatch):
    runner = _runner_module()
    events = []
    _patch_runner_runtime(monkeypatch, runner, events=events, verification={"status": "VERIFIED"})

    result = runner.main(_runner_args(runner, runner.WRITE))

    assert events.index("run_dimensional_graph") < events.index("verify_completed_graph")
    assert result["verification"] == {"status": "VERIFIED"}


def test_main_rejects_malformed_lineage_before_creating_runtime_clients(monkeypatch):
    runner = _runner_module()
    monkeypatch.setenv("BRONZE_BUCKET", "ecommerce-lakehouse-quality-it")
    monkeypatch.setenv("LAKEHOUSE_STORAGE_ENV", "quality-it")
    monkeypatch.setenv("QUALITY_IT_RUN_ID", "runner-test")
    monkeypatch.setenv("QUALITY_IT_PREFIX", "quality-it/runner-test/")
    calls = []
    monkeypatch.setattr(runner, "SparkSession", SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: calls.append("spark"))))
    monkeypatch.setattr(runner, "create_minio_client", lambda: calls.append("minio"))

    with pytest.raises(ValueError, match="Invalid dimensional lineage envelope"):
        runner.main([
            "--source-lineage", '{"version": 1}',
            "--dim-date-start", "2026-01-01",
            "--dim-date-end", "2026-01-02",
        "--bucket", "ecommerce-lakehouse-quality-it",
        ])

    assert calls == []


def test_main_fails_when_write_verification_fails(monkeypatch):
    runner = _runner_module()
    events = []
    _patch_runner_runtime(monkeypatch, runner, events=events, verification={"status": "FAILED"})

    with pytest.raises(RuntimeError, match="VERIFIED graph"):
        runner.main(_runner_args(runner, runner.WRITE))

    assert events.index("run_dimensional_graph") < events.index("verify_completed_graph")
    assert events[-1] == "spark.stop"


def test_native_dimensional_operator_uses_pinned_lineage_and_lakehouse_mount():
    source = (Path(__file__).parents[1] / "airflow" / "dags" / "pipeline_operators.py").read_text(encoding="utf-8")
    start = source.index("def create_gold_dimensional_operator")
    section = source[start:source.index("def create_gold_dimensional_confirmation_operator", start)]
    assert '"--source-artifact"' in section
    assert "GoldDimensionalDockerOperator" in section
    assert 'gold/implementation/dimensional/run_pipeline.py' in section
    # create_spark_submit_operator already supplies the single read-only
    # lakehouse mount; the Gold wrapper must not add a duplicate mount point.
    assert "extra_mounts" not in section


def test_gold_dimensional_operator_propagates_production_approval_without_implicit_approval(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        monkeypatch.setattr(pipeline_operators, "Mount", lambda **kwargs: kwargs)
        monkeypatch.setenv("BRONZE_BUCKET", "ecommerce-lakehouse-prod")
        monkeypatch.setenv("LAKEHOUSE_STORAGE_ENV", "production")
        monkeypatch.setenv("NESSIE_REF", "gold_prod_current")
        for name in ("LAKEHOUSE_E2E_APPROVED", "AIRFLOW_LOCAL_E2E_MODE", "QUALITY_IT_ENABLED", "QUALITY_IT_GATE2_APPROVED"):
            monkeypatch.delenv(name, raising=False)
        for name in ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"):
            monkeypatch.setenv(name, "test")

        captured = {}
        monkeypatch.setattr(
            pipeline_operators,
            "create_spark_submit_operator",
            lambda **kwargs: captured.update(kwargs) or object(),
        )

        monkeypatch.delenv("PRODUCTION_LAKEHOUSE_WRITE_APPROVED", raising=False)
        pipeline_operators.create_gold_dimensional_operator(project_path=str(Path.cwd()), mode="WRITE")
        assert captured["extra_environment"]["PRODUCTION_LAKEHOUSE_WRITE_APPROVED"] == ""

        monkeypatch.setenv("PRODUCTION_LAKEHOUSE_WRITE_APPROVED", "1")
        captured.clear()
        pipeline_operators.create_gold_dimensional_operator(project_path=str(Path.cwd()), mode="WRITE")
        assert captured["extra_environment"]["PRODUCTION_LAKEHOUSE_WRITE_APPROVED"] == "1"
    finally:
        sys.modules.pop("pipeline_operators", None)
        sys.path.remove(str(dags_path))


def test_dimensional_artifact_is_downstream_of_completed_silver_result():
    source = (Path(__file__).parents[1] / "airflow" / "dags" / "ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")
    assert "results[table] >> dimensional_artifacts[table]" in source
    assert "verify[table] >> dimensional_artifacts[table]" not in source


@pytest.mark.parametrize("reason", ["NO_DATA", "DRY_RUN"], ids=["no-data", "dry-run"])
def test_skipped_silver_result_emits_explicit_artifact_without_lineage(reason):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        from pipeline_tasks.gold import build_dimensional_parquet_artifact_task
        result = build_dimensional_parquet_artifact_task(
            "orders", {"table_name": "orders", "status": "SKIPPED", "reason": reason}
        )
    finally:
        sys.path.remove(str(dags_path))

    assert result == {"table_name": "orders", "status": "NO_DATA", "reason": reason}
    assert not {"batch_id", "manifest_key", "manifest_keys", "data_prefix", "parquet_lineage"} & set(result)


@pytest.mark.parametrize(
    "silver_result",
    [
        {"table_name": "orders", "status": "NO_DATA", "reason": "NO_DATA"},
        {"table_name": "orders", "status": "FAILED", "reason": "NO_DATA"},
    ],
)
def test_malformed_or_unexpected_silver_status_fails_closed(silver_result):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        from pipeline_tasks.gold import build_dimensional_parquet_artifact_task
        with pytest.raises(RuntimeError, match="Silver Parquet verification did not PASS"):
            build_dimensional_parquet_artifact_task("orders", silver_result)
    finally:
        sys.path.remove(str(dags_path))


def test_gold_writer_preflight_skips_no_data_before_lineage_or_runtime(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        artifacts = {
            "orders": {"table_name": "orders", "status": "NO_DATA", "reason": "NO_DATA"}
        }
        with pytest.raises(ValueError, match="facts require current Silver PASS"):
            pipeline_operators._preflight_dimensional_artifacts(artifacts)
    finally:
        sys.path.remove(str(dags_path))


def test_gold_writer_preflight_returns_no_change_for_all_six_no_data(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        artifacts = {
            table: {"table_name": table, "status": "NO_DATA", "reason": "NO_DATA"}
            for table in pipeline_operators.TABLES
        }
        result = pipeline_operators._preflight_dimensional_artifacts(artifacts)
        assert result["status"] == "GOLD_NO_CHANGE"
        assert result["reason"] == "NO_CHANGE"
    finally:
        sys.path.remove(str(dags_path))


def test_gold_writer_preflight_returns_no_change_for_all_six_dry_run_no_data(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        artifacts = {
            table: {"table_name": table, "status": "NO_DATA", "reason": "DRY_RUN"}
            for table in pipeline_operators.TABLES
        }

        result = pipeline_operators._preflight_dimensional_artifacts(artifacts)

        assert result["status"] == "GOLD_NO_CHANGE"
        assert result["reason"] == "NO_CHANGE"
    finally:
        sys.path.remove(str(dags_path))


def test_gold_writer_preflight_uses_validated_registry_for_registry_only_dimensions(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        from test_gold_snapshot_reuse_registry import _registry
        from test_gold_snapshot_reuse_contract import _reuse

        artifacts = {
            table: {"table_name": table, "status": "PASS"}
            for table in pipeline_operators.TABLES
        }
        artifacts["customers"] = {
            "table_name": "customers", "status": "NO_DATA", "reason": "NO_DATA",
            # This valid inline artifact must not override the registry-bound one.
            "snapshot_reuse": _reuse("customers"),
        }
        artifacts["products"] = {
            "table_name": "products", "status": "NO_DATA", "reason": "NO_DATA",
        }
        registry = _registry()
        monkeypatch.setenv("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", "true")

        result = pipeline_operators._preflight_dimensional_artifacts(
            artifacts, registry=registry
        )

        assert result["status"] == "GOLD_SNAPSHOT_REUSE_REQUIRED"
        assert result["effective_inputs"]["customers"]["artifact"]["snapshot"] == registry["customers"]["snapshot"]
        assert result["effective_inputs"]["products"]["artifact"]["snapshot"] == registry["products"]["snapshot"]
    finally:
        sys.path.remove(str(dags_path))


def test_gold_writer_preflight_resolves_both_no_data_dimensions_from_registry_only(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        from test_gold_snapshot_reuse_registry import _registry

        artifacts = {
            table: {"table_name": table, "status": "PASS"}
            for table in pipeline_operators.TABLES
        }
        artifacts["customers"] = {
            "table_name": "customers", "status": "NO_DATA", "reason": "NO_DATA",
        }
        artifacts["products"] = {
            "table_name": "products", "status": "NO_DATA", "reason": "NO_DATA",
        }
        assert "snapshot_reuse" not in artifacts["customers"]
        assert "snapshot_reuse" not in artifacts["products"]

        registry = _registry()
        external_calls = []
        monkeypatch.setenv("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", "true")
        monkeypatch.setattr(
            pipeline_operators.DockerOperator,
            "execute",
            lambda *_args, **_kwargs: external_calls.append("docker"),
            raising=False,
        )

        result = pipeline_operators._preflight_dimensional_artifacts(
            artifacts, registry=registry
        )

        assert result["status"] == "GOLD_SNAPSHOT_REUSE_REQUIRED"
        effective = result["effective_inputs"]
        for table in ("customers", "products"):
            assert effective[table]["resolution"] == "GOLD_SNAPSHOT_REUSE"
            assert effective[table]["artifact"] == registry[table]
            assert effective[table]["artifact"]["snapshot"] == registry[table]["snapshot"]
        assert external_calls == []
    finally:
        sys.path.remove(str(dags_path))


def test_gold_writer_preflight_rejects_invalid_registry_before_docker(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        from test_gold_snapshot_reuse_registry import _registry

        artifacts = {
            table: {"table_name": table, "status": "PASS"}
            for table in pipeline_operators.TABLES
        }
        artifacts["customers"] = {
            "table_name": "customers", "status": "NO_DATA", "reason": "NO_DATA",
        }
        invalid_registry = copy.deepcopy(_registry())
        invalid_registry["registry_identity"] = "0" * 64
        executed = []

        class _TaskInstance:
            def xcom_pull(self, *, task_ids):
                if task_ids.startswith("silver_result_"):
                    return {"status": "PASS"}
                if task_ids.startswith("dimensional_parquet_artifact_"):
                    table = task_ids.removeprefix("dimensional_parquet_artifact_")
                    return artifacts[table]
                if task_ids == "validate_config":
                    return {"gold_snapshot_reuse_registry": invalid_registry}
                return None

        monkeypatch.setenv("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", "true")
        monkeypatch.setitem(sys.modules, "common.runtime_policy", SimpleNamespace(
            is_feature_enabled=lambda _name: True,
        ))
        monkeypatch.setitem(sys.modules, "pipeline_tasks.silver", SimpleNamespace(
            build_pipeline_summary=lambda *_args, **_kwargs: None,
        ))
        monkeypatch.setattr(
            pipeline_operators.DockerOperator,
            "execute",
            lambda *_args: executed.append("docker"),
            raising=False,
        )
        operator = pipeline_operators.GoldDimensionalDockerOperator.__new__(
            pipeline_operators.GoldDimensionalDockerOperator
        )

        with pytest.raises(ValueError, match="Registry identity mismatch"):
            operator.execute({"ti": _TaskInstance()})
        assert executed == []
    finally:
        sys.path.remove(str(dags_path))


def test_gold_writer_no_change_returns_before_lineage_or_spark(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        events = []

        class _TaskInstance:
            def xcom_pull(self, *, task_ids):
                if task_ids.startswith("dimensional_parquet_artifact_"):
                    table = task_ids.removeprefix("dimensional_parquet_artifact_")
                    return {"table_name": table, "status": "NO_DATA", "reason": "NO_DATA"}
                if task_ids.startswith("silver_result_"):
                    table = task_ids.removeprefix("silver_result_")
                    return {"table_name": table, "status": "SKIPPED", "reason": "NO_DATA"}
                raise AssertionError(f"unexpected XCom read: {task_ids}")

        monkeypatch.setattr(pipeline_operators.DockerOperator, "execute", lambda *_: events.append("super"), raising=False)
        monkeypatch.setitem(sys.modules, "pipeline_tasks.silver", SimpleNamespace(
            build_pipeline_summary=lambda *_args, **_kwargs: events.append("summary"),
        ))
        monkeypatch.setitem(sys.modules, "common.runtime_policy", SimpleNamespace(
            is_feature_enabled=lambda _name: True,
        ))
        operator = pipeline_operators.GoldDimensionalDockerOperator.__new__(pipeline_operators.GoldDimensionalDockerOperator)
        result = operator.execute({"ti": _TaskInstance()})

        assert result["status"] == "GOLD_NO_CHANGE"
        assert events == []
    finally:
        sys.path.remove(str(dags_path))


def test_gold_no_change_skips_publication(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_tasks = importlib.import_module("pipeline_tasks.gold")
        with pytest.raises(Exception, match="NO_CHANGE"):
            pipeline_tasks.publish_gold_to_active_task(
                {"status": "GOLD_NO_CHANGE", "reason": "NO_CHANGE"}
            )
    finally:
        sys.path.remove(str(dags_path))


def test_runner_no_change_preflight_does_not_touch_clients(monkeypatch):
    runner = _runner_module()
    calls = []
    monkeypatch.setattr(runner, "create_minio_client", lambda: calls.append("minio"))
    monkeypatch.setattr(runner.SparkSession, "builder", property(lambda _self: calls.append("spark") or None), raising=False)
    artifacts = {
        table: {"table_name": table, "status": "NO_DATA", "reason": "NO_DATA"}
        for table in ("customers", "products", "orders", "order_items", "payments", "shipments")
    }
    result = runner._preflight_reuse_artifacts(artifacts)
    assert result["status"] == "GOLD_NO_CHANGE"
    assert calls == []


def test_gold_writer_preflight_skips_dry_run_artifact_before_lineage_or_runtime(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        artifacts = {
            "orders": {"table_name": "orders", "status": "NO_DATA", "reason": "DRY_RUN"}
        }
        with pytest.raises(ValueError, match="facts require current Silver PASS"):
            pipeline_operators._preflight_dimensional_artifacts(artifacts)
    finally:
        sys.path.remove(str(dags_path))


def test_gold_writer_preflight_rejects_malformed_no_data_envelope(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        with pytest.raises(ValueError, match="Malformed NO_DATA"):
            pipeline_operators._preflight_dimensional_artifacts({
                "orders": {
                    "table_name": "orders", "status": "NO_DATA", "reason": "NO_DATA",
                    "manifest_key": "historical.json",
                }
            })
    finally:
        sys.path.remove(str(dags_path))


def test_gold_writer_preflight_validates_later_artifacts_before_skipping(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        with pytest.raises(ValueError, match="Unknown dimensional artifact status: payments"):
            pipeline_operators._preflight_dimensional_artifacts({
                "orders": {"table_name": "orders", "status": "NO_DATA", "reason": "NO_DATA"},
                "payments": {"table_name": "payments", "status": "UNKNOWN", "reason": "NO_DATA"},
            })
    finally:
        sys.path.remove(str(dags_path))


def test_dimension_no_data_with_pinned_reuse_is_not_skipped(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        from test_gold_snapshot_reuse_contract import _reuse

        monkeypatch.setenv("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", "true")
        pipeline_operators._preflight_dimensional_artifacts({
            "customers": {
                "table_name": "customers", "status": "NO_DATA", "reason": "NO_DATA",
                "snapshot_reuse": _reuse("customers"),
            }
        })
    finally:
        sys.path.remove(str(dags_path))


def test_runner_preflight_rejects_fact_no_data_before_runtime_clients(monkeypatch):
    runner = _runner_module()
    monkeypatch.setenv("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", "true")
    with pytest.raises(ValueError, match="facts require current Silver PASS"):
        runner._preflight_reuse_artifacts({
            "orders": {"table_name": "orders", "status": "NO_DATA", "reason": "NO_DATA"},
        })


def test_runtime_merge_plan_does_not_bypass_explicit_reuse_flag(monkeypatch):
    runner = _runner_module()
    from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
    from test_gold_snapshot_reuse_contract import _reuse

    artifacts = {
        table: {"status": "PASS", "table_name": table}
        for table in DIMENSIONAL_SOURCE_TABLES
    }
    artifacts["customers"] = {
        "status": "NO_DATA", "table_name": "customers",
        "snapshot_reuse": _reuse("customers"),
    }
    monkeypatch.delenv("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", raising=False)

    with pytest.raises(ValueError, match="Snapshot reuse disabled"):
        runner._build_runtime_merge_plan(
            object(), object(), "unused", artifacts, {}, None
        )


def test_gold_dimensional_execute_preflights_before_super_on_no_data(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)
        events = []

        class _TaskInstance:
            def xcom_pull(self, *, task_ids):
                events.append(f"xcom:{task_ids}")
                if task_ids.startswith("dimensional_parquet_artifact_"):
                    table = task_ids.removeprefix("dimensional_parquet_artifact_")
                    if table == "orders":
                        return {"table_name": table, "status": "NO_DATA", "reason": "NO_DATA"}
                    return {"status": "PASS"}
                return None

        monkeypatch.setitem(sys.modules, "common.runtime_policy", SimpleNamespace(
            is_feature_enabled=lambda _name: True,
        ))
        monkeypatch.setitem(sys.modules, "gold.implementation.dimensional.lineage", SimpleNamespace(
            build_dimensional_lineage_from_artifacts=lambda *_args, **_kwargs: events.append("lineage"),
        ))
        monkeypatch.setitem(sys.modules, "pipeline_tasks.silver", SimpleNamespace(
            build_pipeline_summary=lambda *_args, **_kwargs: events.append("summary"),
        ))
        monkeypatch.setattr(
            pipeline_operators.DockerOperator,
            "execute",
            lambda _self, _context: events.append("super.execute"),
            raising=False,
        )

        operator = pipeline_operators.GoldDimensionalDockerOperator.__new__(
            pipeline_operators.GoldDimensionalDockerOperator
        )
        with pytest.raises(ValueError, match="facts require current Silver PASS"):
            operator.execute({"ti": _TaskInstance()})

        assert "super.execute" not in events
        assert "lineage" not in events
        assert "summary" not in events
    finally:
        sys.path.remove(str(dags_path))


def test_gold_dimensional_execute_normalizes_docker_stdout_for_xcom(monkeypatch):
    dags_path = Path(__file__).parents[1] / "airflow" / "dags"
    sys.path.insert(0, str(dags_path))
    try:
        pipeline_operators = _pipeline_operators_module(monkeypatch)

        class _TaskInstance:
            def xcom_pull(self, *, task_ids):
                if task_ids.startswith("silver_result_"):
                    return {"status": "PASS"}
                if task_ids.startswith("dimensional_parquet_artifact_"):
                    return {"status": "PASS"}
                if task_ids == "validate_config":
                    return {}
                return None

        monkeypatch.setitem(sys.modules, "common.runtime_policy", SimpleNamespace(
            is_feature_enabled=lambda _name: True,
        ))
        monkeypatch.setitem(sys.modules, "gold.implementation.dimensional.lineage", SimpleNamespace(
            build_dimensional_lineage_from_artifacts=lambda *_args, **_kwargs: None,
        ))
        monkeypatch.setitem(sys.modules, "pipeline_tasks.silver", SimpleNamespace(
            build_pipeline_summary=lambda *_args, **_kwargs: None,
        ))
        monkeypatch.setattr(
            pipeline_operators.DockerOperator,
            "execute",
            lambda _self, _context: [
                "INFO Spark output",
                'GOLD_DIMENSIONAL_RESULT={"mode":"WRITE","certified":true,"can_write":true}',
                "INFO Spark shutdown",
            ],
            raising=False,
        )

        operator = pipeline_operators.GoldDimensionalDockerOperator.__new__(
            pipeline_operators.GoldDimensionalDockerOperator
        )
        result = operator.execute({"ti": _TaskInstance()})

        assert result == {"mode": "WRITE", "certified": True, "can_write": True}
    finally:
        sys.path.remove(str(dags_path))
