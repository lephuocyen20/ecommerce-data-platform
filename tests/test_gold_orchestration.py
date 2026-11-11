"""Regression tests for the native dimensional Gold orchestration."""

import importlib.util
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


def test_native_dimensional_operator_uses_pinned_lineage_and_lakehouse_mount():
    source = (Path(__file__).parents[1] / "airflow" / "dags" / "pipeline_operators.py").read_text(encoding="utf-8")
    start = source.index("def create_gold_dimensional_operator")
    section = source[start:source.index("def create_gold_dimensional_confirmation_operator", start)]
    assert '"--source-lineage"' in section
    assert 'gold/implementation/dimensional/run_pipeline.py' in section
    assert 'source=project_path + r"\\lakehouse"' in section
    assert 'target=f"{SPARK_WORKDIR}/lakehouse"' in section
    assert "read_only=True" in section
