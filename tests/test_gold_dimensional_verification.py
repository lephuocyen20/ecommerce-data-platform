import json

import pytest

from gold.implementation.dimensional.contracts import WRITE_ORDER
from gold.implementation.dimensional.verification import (
    DimensionalVerificationError,
    verify_completed_graph,
)


def _result():
    return {"graph": {"identity": "g"}}


class Client:
    pass


def test_verification_requires_completed_graph_and_all_table_manifests(monkeypatch):
    completed = {"outputs": [{"table": table, "identity": table} for table in WRITE_ORDER]}
    tables = {table: {"identity": table} for table in WRITE_ORDER}
    monkeypatch.setattr("gold.implementation.dimensional.verification.read_completed_graph", lambda *_: completed)
    monkeypatch.setattr("gold.implementation.dimensional.verification.read_table_manifests", lambda *_: tables)
    result = verify_completed_graph("GOLD_DIMENSIONAL_RESULT=" + json.dumps(_result()), Client(), "ecommerce-quality-it")
    assert result["status"] == "VERIFIED"
    assert result["tables"] == list(WRITE_ORDER)


def test_verification_forwards_bounded_scope(monkeypatch):
    completed = {"outputs": [{"table": table, "identity": table} for table in WRITE_ORDER]}
    tables = {table: {"identity": table} for table in WRITE_ORDER}
    seen = []
    monkeypatch.setattr(
        "gold.implementation.dimensional.verification.read_completed_graph",
        lambda *args: (seen.append(args[-1]) or completed),
    )
    monkeypatch.setattr(
        "gold.implementation.dimensional.verification.read_table_manifests",
        lambda *args: (seen.append(args[-1]) or tables),
    )
    scope = object()
    verify_completed_graph(_result(), Client(), "ecommerce-data-lake", scope=scope)
    assert seen == [scope, scope]


def test_verification_rejects_incomplete_graph(monkeypatch):
    monkeypatch.setattr("gold.implementation.dimensional.verification.read_completed_graph", lambda *_: None)
    with pytest.raises(DimensionalVerificationError, match="missing"):
        verify_completed_graph(_result(), Client(), "ecommerce-quality-it")


def test_verification_rejects_duplicate_output_entries(monkeypatch):
    completed = {"outputs": [{"table": table, "identity": table} for table in WRITE_ORDER[:-1]] + [{"table": WRITE_ORDER[0], "identity": "duplicate"}]}
    monkeypatch.setattr("gold.implementation.dimensional.verification.read_completed_graph", lambda *_: completed)
    with pytest.raises(DimensionalVerificationError, match="seven outputs"):
        verify_completed_graph(_result(), Client(), "ecommerce-quality-it")
