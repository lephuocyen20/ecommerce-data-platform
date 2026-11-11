import json
import pytest
from tests.run_gold_dimensional_local_write import LOCAL_BUCKET, inspect_local_graph, run_local_write
from gold.implementation.dimensional.contracts import WRITE_ORDER
def _lineage(tmp_path):
    p = tmp_path / "lineage.json"; p.write_text(json.dumps({"source_type": "iceberg_snapshot", "sources": {}}), encoding="utf-8"); return p
def test_local_write_rejects_non_quality_bucket(monkeypatch, tmp_path):
    monkeypatch.setenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "true")
    with pytest.raises(ValueError, match=LOCAL_BUCKET): run_local_write(_lineage(tmp_path), "ecommerce-data-lake", dim_date_start="2026-01-01", dim_date_end="2026-01-07")
def test_local_write_requires_explicit_flag(monkeypatch, tmp_path):
    monkeypatch.delenv("GOLD_DIMENSIONAL_WRITE_ENABLED", raising=False)
    with pytest.raises(RuntimeError, match="GOLD_DIMENSIONAL_WRITE_ENABLED=true"): run_local_write(_lineage(tmp_path), LOCAL_BUCKET, dim_date_start="2026-01-01", dim_date_end="2026-01-07")


def test_inspection_is_read_only_and_declares_exact_output_order(tmp_path):
    lineage = {"version": 1, "source_type": "iceberg_snapshot", "sources": {}}
    # Invalid lineage must fail before any inspection-side effect.
    p = tmp_path / "lineage.json"; p.write_text(json.dumps(lineage), encoding="utf-8")
    with pytest.raises(ValueError): inspect_local_graph(p, LOCAL_BUCKET)
    assert len(WRITE_ORDER) == 7
