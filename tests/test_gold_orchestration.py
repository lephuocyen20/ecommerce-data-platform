"""Regression tests for Gold orchestration independent of Warehouse/dbt."""

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


def test_gold_gate_requires_verified_write_summary_and_feature_flag():
    policy = _dag_module("gold_gate_policy")
    verified_write = {
        "status": "PASS",
        "mode": "WRITE",
        "results": {"orders": {"status": "PASS"}},
    }

    assert policy.decide_gold_snapshot_gate(verified_write, write_enabled=True) == (
        "resolve_gold_snapshot_lineage"
    )
    assert policy.decide_gold_snapshot_gate(verified_write, write_enabled=False) == (
        "skip_gold_layer"
    )
    assert policy.decide_gold_snapshot_gate(
        {**verified_write, "mode": "DRY_RUN"}, write_enabled=True
    ) == "skip_gold_layer"

    with pytest.raises(ValueError, match="successful Silver"):
        policy.decide_gold_snapshot_gate(
            {**verified_write, "status": "FAILED"}, write_enabled=True
        )
    with pytest.raises(ValueError, match="result details"):
        policy.decide_gold_snapshot_gate(
            {"status": "PASS", "mode": "WRITE"}, write_enabled=True
        )


class _Task:
    def __init__(self, task_id):
        self.task_id = task_id
        self.downstream = []

    def __rshift__(self, other):
        self.downstream.append(other)
        return other


def test_gold_graph_is_anchored_to_silver_and_has_no_warehouse_or_dbt_edges():
    graph = _dag_module("pipeline_gold")
    snapshots = {table: _Task(f"snapshot_{table}") for table in graph.TABLES}
    sources = {
        dataset: _Task(f"gold_sources_{dataset}")
        for dataset in graph.GOLD_DATASET_SOURCES
    }
    writers = {
        dataset: _Task(f"gold_{dataset}")
        for dataset in graph.GOLD_DATASET_SOURCES
    }
    summary, gate, skip, lineage = (
        _Task("pipeline_summary"),
        _Task("gold_snapshot_gate"),
        _Task("skip_gold_layer"),
        _Task("resolve_gold_snapshot_lineage"),
    )

    graph.link_gold_orchestration(
        pipeline_summary_task=summary,
        gold_gate_task=gate,
        gold_skip_task=skip,
        gold_lineage_task=lineage,
        snapshot_tasks=snapshots,
        dataset_source_tasks=sources,
        gold_write_tasks=writers,
    )

    assert summary.downstream == [gate]
    assert {task.task_id for task in gate.downstream} == {
        "skip_gold_layer", "resolve_gold_snapshot_lineage"
    }
    assert {task.task_id for task in lineage.downstream} == {
        f"snapshot_{table}" for table in graph.TABLES
    }
    assert all(
        "warehouse" not in task.task_id and "dbt" not in task.task_id
        for task in [summary, gate, skip, lineage, *snapshots.values(), *sources.values(), *writers.values()]
    )
    for dataset, required_sources in graph.GOLD_DATASET_SOURCES.items():
        assert sources[dataset] in [
            downstream
            for table in required_sources
            for downstream in snapshots[table].downstream
        ]
        assert sources[dataset].downstream == [writers[dataset]]


def test_gold_graph_rejects_incomplete_snapshot_wiring():
    graph = _dag_module("pipeline_gold")
    with pytest.raises(ValueError, match="snapshot task"):
        graph.link_gold_orchestration(
            pipeline_summary_task=_Task("summary"),
            gold_gate_task=_Task("gate"),
            gold_skip_task=_Task("skip"),
            gold_lineage_task=_Task("lineage"),
            snapshot_tasks={},
            dataset_source_tasks={dataset: _Task(dataset) for dataset in graph.GOLD_DATASET_SOURCES},
            gold_write_tasks={dataset: _Task(dataset) for dataset in graph.GOLD_DATASET_SOURCES},
        )


def test_iceberg_gold_graph_requires_all_evidence_and_no_warehouse():
    graph = _dag_module("pipeline_gold")
    evidence = {table: _Task(f"quality_iceberg_{table}") for table in graph.TABLES}
    sources = {dataset: _Task(f"gold_sources_{dataset}") for dataset in graph.GOLD_DATASET_SOURCES}
    writers = {dataset: _Task(f"gold_{dataset}") for dataset in graph.GOLD_DATASET_SOURCES}
    summary, gate, skip, lineage = _Task("pipeline_summary"), _Task("gold_gate"), _Task("skip"), _Task("resolve_gold_iceberg_lineage")
    graph.link_gold_iceberg_orchestration(
        pipeline_summary_task=summary, gold_gate_task=gate, gold_skip_task=skip,
        gold_lineage_task=lineage, evidence_tasks=evidence,
        dataset_source_tasks=sources, gold_write_tasks=writers,
    )
    assert summary.downstream == [gate]
    assert lineage in [task for item in evidence.values() for task in item.downstream]
    assert all("warehouse" not in task.task_id and "dbt" not in task.task_id
               for task in [summary, gate, skip, lineage, *evidence.values(), *sources.values(), *writers.values()])
    with pytest.raises(ValueError, match="evidence completion"):
        graph.link_gold_iceberg_orchestration(
            pipeline_summary_task=summary, gold_gate_task=gate, gold_skip_task=skip,
            gold_lineage_task=lineage, evidence_tasks={}, dataset_source_tasks=sources,
            gold_write_tasks=writers,
        )


def test_gold_iceberg_operator_mounts_lakehouse_and_uses_lineage_payload():
    source = (Path(__file__).parents[1] / "airflow" / "dags" / "pipeline_operators.py").read_text(encoding="utf-8")
    start = source.index("def create_gold_write_operator")
    section = source[start:source.index("def create_spark_submit_operator", start)]
    assert '"--source-lineage"' in section
    assert 'source=project_path + r"\\lakehouse"' in section
    assert 'target=f"{SPARK_WORKDIR}/lakehouse"' in section
    assert "read_only=True" in section
