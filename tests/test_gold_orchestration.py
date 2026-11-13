"""Regression tests for the native dimensional Gold orchestration."""

import importlib.util
from types import SimpleNamespace
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
        "--bucket", "ecommerce-quality-it",
    ]


def test_main_dry_run_does_not_verify_completed_graph(monkeypatch):
    runner = _runner_module()
    events = []
    _patch_runner_runtime(monkeypatch, runner, events=events)

    runner.main(_runner_args(runner, runner.DRY_RUN))

    assert "run_dimensional_graph" in events
    assert "verify_completed_graph" not in events


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
    calls = []
    monkeypatch.setattr(runner, "SparkSession", SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: calls.append("spark"))))
    monkeypatch.setattr(runner, "create_minio_client", lambda: calls.append("minio"))

    with pytest.raises(ValueError, match="Invalid dimensional lineage envelope"):
        runner.main([
            "--source-lineage", '{"version": 1}',
            "--dim-date-start", "2026-01-01",
            "--dim-date-end", "2026-01-02",
            "--bucket", "ecommerce-quality-it",
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
    assert 'source=project_path + r"\\lakehouse"' in section
    assert 'target=f"{SPARK_WORKDIR}/lakehouse"' in section
    assert "read_only=True" in section
