import pytest

from gold.implementation.dimensional.manifest import DimensionalManifestError, build_graph_manifest, build_graph_run_manifest, graph_manifest_key, build_table_manifest
from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES, WRITE_ORDER
from gold.implementation.dimensional.writer import (
    DRY_RUN, WRITE, DimensionalWriteError, assert_write_allowed, create_target_table,
    merge_table, recovery_state, run_dimensional_graph, validate_target_table,
)


def dimensional_lineage():
    tables = DIMENSIONAL_SOURCE_TABLES
    sources = {}
    for offset, table in enumerate(tables, start=1):
        sources[table] = {
            "source_type": "iceberg_snapshot", "table": table, "catalog": "lakehouse", "namespace": "silver", "ref": "main", "snapshot_id": offset,
            "snapshot_attestation_identity": f"{offset:064x}",
            "reconciliation_evidence": {"identity": f"{offset + 10:064x}", "status": "PASS", "snapshot_id": offset, "table": table},
            "quality_evidence": {"identity": f"{offset + 20:064x}", "status": "PASS", "snapshot_id": offset, "table": table},
        }
    return {"version": 1, "source_type": "iceberg_snapshot", "sources": sources}


def test_graph_identity_includes_explicit_date_range_and_table_manifest_is_stable():
    lineage = dimensional_lineage()
    graph = build_graph_manifest(lineage, dim_date_start="2026-01-01", dim_date_end="2026-12-31")
    assert graph["source_contract"]["delete"]["physical_delete"] is False
    assert graph["source_contract"]["late_data"]["mode"] == "overlap_reprocess"
    changed = build_graph_manifest(lineage, dim_date_start="2026-01-02", dim_date_end="2026-12-31")
    assert graph["identity"] != changed["identity"]
    manifest = build_table_manifest(graph, table="fact_orders", snapshot_id=123, quality={"status": "PASS", "row_count": 2})
    assert manifest["graph_identity"] == graph["identity"]
    assert len(manifest["identity"]) == 64
    manifests = [build_table_manifest(graph, table=table, snapshot_id=index, quality={"status": "PASS", "row_count": index})
                 for index, table in enumerate(WRITE_ORDER, start=1)]
    assert build_graph_run_manifest(graph, manifests)["outputs"][-1]["table"] == "fact_shipments"
    assert graph_manifest_key(graph["identity"]).endswith(f"/{graph['identity']}.json")


def test_graph_run_manifest_rejects_duplicate_table_entries():
    lineage = dimensional_lineage()
    graph = build_graph_manifest(lineage, dim_date_start="2026-01-01", dim_date_end="2026-12-31")
    manifests = [build_table_manifest(graph, table=table, snapshot_id=index, quality={"status": "PASS"}) for index, table in enumerate(WRITE_ORDER, start=1)]
    manifests[-1] = manifests[0]
    with pytest.raises(DimensionalManifestError, match="every table manifest"):
        build_graph_run_manifest(graph, manifests)


def test_write_feature_flag_defaults_to_off(monkeypatch):
    monkeypatch.delenv("GOLD_DIMENSIONAL_WRITE_ENABLED", raising=False)
    assert_write_allowed(DRY_RUN)
    with pytest.raises(DimensionalWriteError, match="WRITE requires"):
        assert_write_allowed(WRITE)
    monkeypatch.setenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "true")
    assert_write_allowed(WRITE)


def test_shared_bucket_requires_explicit_bounded_scope(monkeypatch):
    monkeypatch.setenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "true")
    with pytest.raises(DimensionalWriteError, match="bounded E2E scope"):
        run_dimensional_graph(None, object(), "ecommerce-data-lake", {},
                              dim_date_start="2026-01-01", dim_date_end="2026-01-01", mode=WRITE)


def test_recovery_never_treats_partial_table_publication_as_complete():
    assert recovery_state(True, {}) == "ALREADY_PUBLISHED"
    assert recovery_state(False, {}) == "NEW"
    assert recovery_state(False, {"dim_customer": "PUBLISHED"}) == "PARTIAL_REQUIRES_RECONCILIATION"


def test_date_range_is_strict_calendar_iso_and_not_lexical_only():
    with pytest.raises(Exception, match="calendar date"):
        build_graph_manifest(dimensional_lineage(), dim_date_start="2026-02-30", dim_date_end="2026-03-01")
    with pytest.raises(Exception, match="Invalid dim_date range"):
        build_graph_manifest(dimensional_lineage(), dim_date_start="2026-03-02", dim_date_end="2026-03-01")
    with pytest.raises(Exception, match="YYYY-MM-DD"):
        build_graph_manifest(dimensional_lineage(), dim_date_start="20260301", dim_date_end="2026-03-02")


def test_partial_preflight_blocks_before_source_read_or_merge(monkeypatch):
    import gold.implementation.dimensional.writer as writer
    monkeypatch.setattr(writer, "read_completed_graph", lambda *args: None)
    monkeypatch.setattr(writer, "read_table_manifests", lambda *args: {"dim_customer": {"table": "dim_customer"}})
    monkeypatch.setattr(writer, "read_dimensional_sources", lambda *args: pytest.fail("must not read sources"))
    result = run_dimensional_graph(None, object(), "bucket", dimensional_lineage(),
                                   dim_date_start="2026-01-01", dim_date_end="2026-01-02")
    assert result["status"] == "PARTIAL_REQUIRES_RECONCILIATION"


def test_write_requires_exclusive_fence_before_any_source_read(monkeypatch):
    import gold.implementation.dimensional.writer as writer
    monkeypatch.setenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "true")
    monkeypatch.setattr(writer, "read_completed_graph", lambda *args: None)
    monkeypatch.setattr(writer, "read_table_manifests", lambda *args: {})
    monkeypatch.setattr(writer, "acquire_global_fence", lambda *args: {"kind": "global"})
    monkeypatch.setattr(writer, "release_global_fence", lambda *args: None)
    monkeypatch.setattr(writer, "acquire_graph_fence", lambda *args: (_ for _ in ()).throw(DimensionalManifestError("Graph fence is already held")))
    monkeypatch.setattr(writer, "read_dimensional_sources", lambda *args: pytest.fail("must not read sources"))
    with pytest.raises(DimensionalManifestError, match="already held"):
        run_dimensional_graph(None, object(), "bucket", dimensional_lineage(),
                              dim_date_start="2026-01-01", dim_date_end="2026-01-02", mode=WRITE)


def test_table_write_failure_includes_table_exception_and_published_tables(monkeypatch):
    import gold.implementation.dimensional.writer as writer

    monkeypatch.setenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "true")
    monkeypatch.setattr(writer, "read_completed_graph", lambda *args: None)
    monkeypatch.setattr(writer, "read_table_manifests", lambda *args: {})
    monkeypatch.setattr(writer, "acquire_global_fence", lambda *args: {"kind": "global"})
    monkeypatch.setattr(writer, "assert_global_fence", lambda *args: None)
    monkeypatch.setattr(writer, "release_global_fence", lambda *args: None)
    monkeypatch.setattr(writer, "acquire_graph_fence", lambda *args: {"kind": "graph"})
    monkeypatch.setattr(writer, "assert_graph_fence", lambda *args: None)
    monkeypatch.setattr(writer, "read_dimensional_sources", lambda *args: {})
    monkeypatch.setattr(
        writer,
        "build_graph_frames",
        lambda *args, **kwargs: (
            {table: object() for table in WRITE_ORDER},
            {table: {"status": "PASS"} for table in WRITE_ORDER},
        ),
    )
    monkeypatch.setattr(writer, "create_target_table", lambda _spark, table, _scope=None: (
        (_ for _ in ()).throw(RuntimeError("simulated target failure"))
        if table == "dim_customer"
        else None
    ))
    monkeypatch.setattr(writer, "merge_table", lambda *args: None)
    monkeypatch.setattr(writer, "current_snapshot_id", lambda *args: 1)
    monkeypatch.setattr(writer, "publish_manifest", lambda *args: "PUBLISHED")

    with pytest.raises(DimensionalWriteError) as caught:
        run_dimensional_graph(
            None,
            object(),
            "bucket",
            dimensional_lineage(),
            dim_date_start="2026-01-01",
            dim_date_end="2026-01-02",
            mode=WRITE,
        )

    error = caught.value
    assert "'dim_customer'" in str(error)
    assert "RuntimeError: simulated target failure" in str(error)
    assert "published tables: ['dim_date']" in str(error)
    assert isinstance(error.__cause__, RuntimeError)
    assert str(error.__cause__) == "simulated target failure"


def test_distinct_graph_identity_is_blocked_by_global_fence_before_merge(monkeypatch):
    import gold.implementation.dimensional.writer as writer
    monkeypatch.setenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "true")
    monkeypatch.setattr(writer, "read_completed_graph", lambda *args: None)
    monkeypatch.setattr(writer, "read_table_manifests", lambda *args: {})
    monkeypatch.setattr(writer, "acquire_global_fence", lambda *args: (_ for _ in ()).throw(DimensionalManifestError("Another dimensional Gold writer holds the global fence")))
    monkeypatch.setattr(writer, "read_dimensional_sources", lambda *args: pytest.fail("must not read sources"))
    lineage = dimensional_lineage()
    with pytest.raises(DimensionalManifestError, match="global fence"):
        run_dimensional_graph(None, object(), "bucket", lineage,
                              dim_date_start="2026-02-01", dim_date_end="2026-02-02", mode=WRITE)


def test_create_and_merge_emit_only_allowlisted_ordered_sql():
    class Spark:
        def __init__(self): self.sqls = []
        def sql(self, statement):
            self.sqls.append(statement)
            if statement.startswith("SHOW TBLPROPERTIES"):
                return type("Rows", (), {"collect": lambda self: [("format-version", "2"), ("write.format.default", "parquet")]})()
        def table(self, identifier):
            fields = [type("Field", (), {"name": name, "dataType": type("Type", (), {"simpleString": lambda self, value=value: value})(), "nullable": nullable})()
                      for name, value, nullable in (("date_key", "bigint", True), ("calendar_date", "date", True), ("year", "int", True), ("month", "int", True), ("iso_week", "int", True))]
            return type("Table", (), {"schema": type("Schema", (), {"fields": fields})()})()
    class Frame:
        columns = ("date_key", "calendar_date", "year", "month", "iso_week")
        def createOrReplaceTempView(self, name): self.view = name
    spark = Spark()
    create_target_table(spark, "dim_date")
    merge_table(spark, "dim_date", Frame())
    assert "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold" in spark.sqls[0]
    assert "format-version'='2" in spark.sqls[1]
    assert "write.format.default'='parquet" in spark.sqls[1]
    assert "MERGE INTO lakehouse.gold.dim_date" in spark.sqls[-1]


def test_create_target_table_refreshes_catalog_before_validation(monkeypatch):
    import gold.implementation.dimensional.writer as writer

    events = []
    scope = type("Scope", (), {"gold_namespace": "gold_run_20261111_gold_03"})()

    class Catalog:
        def refreshTable(self, identifier):
            events.append(("refresh", identifier))

    class Spark:
        catalog = Catalog()

        def sql(self, _statement):
            events.append(("sql",))

    monkeypatch.setattr(
        writer,
        "validate_target_table",
        lambda _spark, _table, _scope=None: events.append(("validate", _scope)),
    )

    writer.create_target_table(Spark(), "dim_date", scope)

    assert [event[0] for event in events] == ["sql", "sql", "refresh", "validate"]
    assert events[2] == ("refresh", "lakehouse.gold_run_20261111_gold_03.dim_date")
    assert events[3] == ("validate", scope)


def test_existing_target_schema_or_format_drift_blocks_merge():
    class Spark:
        def table(self, identifier):
            field = type("Field", (), {"name": "wrong", "dataType": type("Type", (), {"simpleString": lambda self: "string"})(), "nullable": True})()
            return type("Table", (), {"schema": type("Schema", (), {"fields": [field]})()})()
        def sql(self, statement):
            return type("Rows", (), {"collect": lambda self: [("format-version", "1")]})()
    with pytest.raises(DimensionalWriteError, match="schema drift"):
        validate_target_table(Spark(), "dim_date")

    class FormatDriftSpark:
        def table(self, identifier):
            fields = [type("Field", (), {"name": name, "dataType": type("Type", (), {"simpleString": lambda self, value=value: value})(), "nullable": nullable})()
                      for name, value, nullable in (("date_key", "bigint", True), ("calendar_date", "date", True), ("year", "int", True), ("month", "int", True), ("iso_week", "int", True))]
            return type("Table", (), {"schema": type("Schema", (), {"fields": fields})()})()
        def sql(self, statement):
            return type("Rows", (), {"collect": lambda self: [("format-version", "1")]})()
    with pytest.raises(DimensionalWriteError, match="format-version"):
        validate_target_table(FormatDriftSpark(), "dim_date")

    class ParquetPropertyDriftSpark:
        def table(self, identifier):
            fields = [type("Field", (), {"name": name, "dataType": type("Type", (), {"simpleString": lambda self, value=value: value})(), "nullable": nullable})()
                      for name, value, nullable in (("date_key", "bigint", True), ("calendar_date", "date", True), ("year", "int", True), ("month", "int", True), ("iso_week", "int", True))]
            return type("Table", (), {"schema": type("Schema", (), {"fields": fields})()})()
        def sql(self, statement):
            return type("Rows", (), {"collect": lambda self: [("format-version", "2")]})()
    with pytest.raises(DimensionalWriteError, match="write.format.default"):
        validate_target_table(ParquetPropertyDriftSpark(), "dim_date")
