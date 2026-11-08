"""MOCKED unit tests for the Phase 4 read-only runner."""

from types import SimpleNamespace

import pytest

from lakehouse.models.spec import SPECS
from lakehouse.quality.contracts import ERROR, PASS, QualityResult
from lakehouse.quality import runner


pytestmark = pytest.mark.unit


class _Catalog:
    def __init__(self, exists=True):
        self.exists = exists
        self.calls = []

    def tableExists(self, name):
        self.calls.append(name)
        return self.exists


class _Frame:
    def __init__(self, rows=0):
        self.rows = rows

    def count(self):
        return self.rows


class _Spark:
    def __init__(self, *, configured=True, exists=True, snapshot=7, rows=0):
        self.conf = SimpleNamespace(get=lambda key: "configured" if configured else "")
        self.catalog = _Catalog(exists)
        self.snapshot = snapshot
        self.frame = _Frame(rows)
        self.sql_calls = []
        self.tables = []

    def sql(self, query):
        self.sql_calls.append(query)
        return SimpleNamespace(first=lambda: None if self.snapshot is None else SimpleNamespace(snapshot_id=self.snapshot))

    def table(self, name):
        self.tables.append(name)
        return self.frame


def test_missing_catalog_configuration_is_error_never_pass():
    reports = runner.run_quality(_Spark(configured=False), specs={"customers": SPECS["customers"]})

    assert reports[0].status == ERROR
    assert "catalog" in reports[0].error.message.lower()


def test_empty_readable_table_is_distinct_from_no_valid_snapshot(monkeypatch):
    spec = {"customers": SPECS["customers"]}
    monkeypatch.setattr(runner, "evaluate_check", lambda *_args, **_kwargs:
                        QualityResult("check", "SCHEMA", PASS, "lakehouse.silver.customers"))

    empty = runner.run_quality(_Spark(snapshot=9, rows=0), specs=spec)[0]
    no_snapshot = runner.run_quality(_Spark(snapshot=None, rows=0), specs=spec)[0]

    assert (empty.status, empty.snapshot_id, empty.rows) == (PASS, 9, 0)
    assert no_snapshot.status == ERROR
    assert no_snapshot.snapshot_id is None
    assert "snapshot" in no_snapshot.error.message.lower()


def test_table_runtime_error_is_isolated_and_other_table_runs(monkeypatch):
    specs = {"customers": SPECS["customers"], "products": SPECS["products"]}
    spark = _Spark()
    original = runner.read_table_snapshot

    def read(spark, table):
        if table.endswith("customers"):
            raise RuntimeError("catalog temporarily unavailable")
        return original(spark, table)

    monkeypatch.setattr(runner, "read_table_snapshot", read)
    monkeypatch.setattr(runner, "evaluate_check", lambda *_args, **_kwargs:
                        QualityResult("check", "SCHEMA", PASS, _args[2]))
    reports = runner.run_quality(spark, specs=specs)

    assert [report.table for report in reports] == ["lakehouse.silver.customers", "lakehouse.silver.products"]
    assert [report.status for report in reports] == [ERROR, PASS]


def test_fk_resolver_uses_explicit_configured_parent_table():
    spark = _Spark()
    parent = runner.make_parent_resolver(spark)("customers")

    assert parent is spark.frame
    assert spark.catalog.calls == ["lakehouse.silver.customers"]
    assert spark.tables == ["lakehouse.silver.customers"]


@pytest.mark.parametrize("kwargs", [{"specs": {}}, {"table_name": "missing"}, {"sample_limit": -1}])
def test_invalid_pre_execution_configuration_raises_value_error(kwargs):
    complete = {"customers": SPECS["customers"]}
    kwargs.setdefault("specs", complete)
    with pytest.raises(ValueError):
        runner.run_quality(_Spark(), **kwargs)
