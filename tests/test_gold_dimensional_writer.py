import pytest

from gold.implementation.dimensional.manifest import (
    DimensionalManifestError,
    acquire_graph_fence,
    build_graph_manifest,
    build_graph_run_manifest,
    build_table_manifest,
    graph_lock_key,
    graph_manifest_key,
    global_lock_key,
)
from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES, WRITE_ORDER
from gold.implementation.dimensional.snapshot_reuse import build_incremental_merge_plan, resolve_dimensional_inputs
from gold.implementation.dimensional.writer import (
    DRY_RUN, WRITE, DimensionalWriteError, assert_write_allowed, create_target_table,
    build_graph_frames_from_merge_plan, merge_table, recovery_state, run_dimensional_graph, validate_target_table,
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
    return {
        "version": 1,
        "source_type": "iceberg_snapshot",
        "sources": sources,
        "legacy_coverage_compatibility": True,
    }


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


def test_incremental_writer_write_boundary_remains_default_off(monkeypatch):
    plan = build_incremental_merge_plan(
        resolve_dimensional_inputs(
            {table: {"status": "PASS", "table_name": table} for table in DIMENSIONAL_SOURCE_TABLES},
            feature_enabled=False,
        ),
        {table: object() for table in DIMENSIONAL_SOURCE_TABLES},
        {},
    )
    monkeypatch.delenv("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", raising=False)
    monkeypatch.setenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "true")
    with pytest.raises(DimensionalWriteError, match="requires GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED"):
        run_dimensional_graph(
            None, object(), "bucket", dimensional_lineage(),
            dim_date_start="2026-01-01", dim_date_end="2026-01-01", mode=WRITE, merge_plan=plan,
        )


def test_writer_consumes_plan_data_without_implicit_snapshot_read(monkeypatch):
    plan = build_incremental_merge_plan(
        resolve_dimensional_inputs(
            {table: {"status": "PASS", "table_name": table} for table in DIMENSIONAL_SOURCE_TABLES},
            feature_enabled=False,
        ),
        {table: object() for table in DIMENSIONAL_SOURCE_TABLES},
        {},
    )
    calls = []
    monkeypatch.setattr("gold.implementation.dimensional.writer.build_dim_date", lambda *args: calls.append("date") or object())
    monkeypatch.setattr("gold.implementation.dimensional.writer.build_dim_customer", lambda value: calls.append(("customer", value)) or object())
    monkeypatch.setattr("gold.implementation.dimensional.writer.build_dim_product", lambda value: calls.append(("product", value)) or object())
    monkeypatch.setattr("gold.implementation.dimensional.writer.build_fact_orders", lambda *args: calls.append("orders") or object())
    monkeypatch.setattr("gold.implementation.dimensional.writer.build_fact_order_items", lambda *args: calls.append("items") or object())
    monkeypatch.setattr("gold.implementation.dimensional.writer.build_fact_payments", lambda value: calls.append("payments") or object())
    monkeypatch.setattr("gold.implementation.dimensional.writer.build_fact_shipments", lambda value: calls.append("shipments") or object())
    monkeypatch.setattr("gold.implementation.dimensional.writer.validate_table_frame", lambda table, frame, frames: {"table": table, "status": "PASS"})
    frames, quality = build_graph_frames_from_merge_plan(None, plan, dim_date_start="2026-01-01", dim_date_end="2026-01-01")
    assert set(frames) == set(WRITE_ORDER)
    assert set(quality) == set(WRITE_ORDER)
    assert calls[0] == "date"
    assert "orders" in calls and "items" in calls


def test_read_table_readback_uses_pinned_snapshot_for_rows_not_current_head():
    from types import SimpleNamespace
    import gold.implementation.dimensional.writer as writer

    class Reader:
        def __init__(self):
            self.calls = []

        def format(self, value):
            self.calls.append(("format", value))
            return self

        def option(self, key, value):
            self.calls.append(("option", key, value))
            return self

        def load(self, identifier):
            self.calls.append(("load", identifier))
            if identifier.endswith(".files"):
                return SimpleNamespace(selectExpr=lambda *_: SimpleNamespace(first=lambda: SimpleNamespace(file_count=1, file_bytes=10)))
            return SimpleNamespace(count=lambda: 3)

    class Spark:
        read = Reader()

    spark = Spark()
    evidence = writer.read_table_readback(spark, "fact_orders", 7)
    assert evidence["snapshot_id"] == 7
    assert evidence["row_count"] == 3
    assert len(evidence["identity"]) == 64
    assert evidence["identity"] != ""
    assert ("load", "lakehouse.gold.fact_orders.files") in spark.read.calls
    assert ("load", "lakehouse.gold.fact_orders") in spark.read.calls
    assert ("option", "snapshot-id", 7) in spark.read.calls


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


@pytest.mark.parametrize("misaligned_table", ["payments", "shipments"])
def test_run_graph_rejects_misaligned_child_before_any_target_write(monkeypatch, misaligned_table):
    import gold.implementation.dimensional.writer as writer
    from gold.implementation.dimensional.quality import DimensionalQualityError

    class Frame:
        def __init__(self, order_ids):
            self.order_ids = set(order_ids)

        def select(self, _column):
            return self

        def where(self, _predicate):
            return self

        def distinct(self):
            return self

        def join(self, other, _key, how):
            assert how == "left_anti"
            return Frame(self.order_ids - other.order_ids)

        def count(self):
            return len(self.order_ids)

    sources = {
        table: Frame({5001})
        for table in DIMENSIONAL_SOURCE_TABLES
    }
    sources["orders"] = Frame({5001})
    sources[misaligned_table] = Frame({4020})
    create_calls = []
    merge_calls = []

    monkeypatch.setenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "true")
    monkeypatch.setattr(writer, "read_completed_graph", lambda *args: None)
    monkeypatch.setattr(writer, "read_table_manifests", lambda *args: {})
    monkeypatch.setattr(writer, "acquire_global_fence", lambda *args: {"kind": "global"})
    monkeypatch.setattr(writer, "assert_global_fence", lambda *args: None)
    monkeypatch.setattr(writer, "release_global_fence", lambda *args: None)
    monkeypatch.setattr(writer, "acquire_graph_fence", lambda *args: {"kind": "graph"})
    monkeypatch.setattr(writer, "assert_graph_fence", lambda *args: None)
    monkeypatch.setattr(writer, "read_dimensional_sources", lambda *args: sources)
    monkeypatch.setattr(writer, "build_dim_date", lambda *args: object())
    monkeypatch.setattr(writer, "build_dim_customer", lambda *args: object())
    monkeypatch.setattr(writer, "build_dim_product", lambda *args: object())
    monkeypatch.setattr(writer, "build_fact_orders", lambda *args: object())
    monkeypatch.setattr(writer, "build_fact_order_items", lambda *args: object())
    monkeypatch.setattr(writer, "build_fact_payments", lambda *args: object())
    monkeypatch.setattr(writer, "build_fact_shipments", lambda *args: object())
    monkeypatch.setattr(writer, "validate_table_frame", lambda *args: {"status": "PASS"})
    monkeypatch.setattr(writer, "create_target_table", lambda *args: create_calls.append(args))
    monkeypatch.setattr(writer, "merge_table", lambda *args: merge_calls.append(args))

    with pytest.raises(DimensionalQualityError, match="Silver order_id parity preflight failed"):
        run_dimensional_graph(
            None,
            object(),
            "bucket",
            dimensional_lineage(),
            dim_date_start="2026-01-01",
            dim_date_end="2026-01-02",
            mode=WRITE,
        )

    assert create_calls == []
    assert merge_calls == []


def test_coverage_mismatch_blocks_before_fences_or_gold_writes(monkeypatch):
    import gold.implementation.dimensional.writer as writer
    lineage = dimensional_lineage()
    lineage["sources"]["orders"].update({
        "batch_id": "a" * 32,
        "manifest_key": "silver/orders/manifest.json",
        "manifest_identity": "b" * 64,
    })
    for table in ("orders", "payments", "shipments"):
        lineage["sources"][table]["coverage"] = {
            "runtime_window_id": "window-1",
            "coverage_identity": "orders-coverage" if table == "orders" else f"{table}-coverage",
        }
    for table in ("payments", "shipments"):
        lineage["sources"][table]["coverage"]["parent_coverage_identity"] = "orders-coverage"
        lineage["sources"][table]["coverage"]["parent_orders_reference"] = {
            "table": "orders",
            "batch_id": lineage["sources"]["orders"]["batch_id"],
            "manifest_key": lineage["sources"]["orders"]["manifest_key"],
            "manifest_identity": lineage["sources"]["orders"]["manifest_identity"],
        }
    lineage["sources"]["payments"]["coverage"]["runtime_window_id"] = "window-2"
    fence_calls, write_calls = [], []
    monkeypatch.setenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "true")
    monkeypatch.setattr(writer, "build_graph_manifest", lambda *args, **kwargs: {"identity": "g" * 64})
    monkeypatch.setattr(writer, "read_completed_graph", lambda *args: None)
    monkeypatch.setattr(writer, "read_table_manifests", lambda *args: {})
    monkeypatch.setattr(writer, "acquire_global_fence", lambda *args: fence_calls.append("global"))
    monkeypatch.setattr(writer, "acquire_graph_fence", lambda *args: fence_calls.append("graph"))
    monkeypatch.setattr(writer, "create_target_table", lambda *args: write_calls.append("create"))
    monkeypatch.setattr(writer, "merge_table", lambda *args: write_calls.append("merge"))

    from gold.implementation.dimensional.coverage import CoverageContractError
    with pytest.raises(CoverageContractError, match="runtime_window_mismatch"):
        run_dimensional_graph(None, object(), "bucket", lineage,
                              dim_date_start="2026-01-01", dim_date_end="2026-01-02", mode=WRITE)
    assert fence_calls == []
    assert write_calls == []


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


def test_matching_graph_fence_is_deterministic_operator_recovery_error(monkeypatch):
    import gold.implementation.dimensional.manifest as manifest

    graph = build_graph_manifest(dimensional_lineage(), dim_date_start="2026-01-01", dim_date_end="2026-01-02")
    lock_key = graph_lock_key(graph["identity"])
    lock = {"version": 1, "kind": "gold_dimensional_graph_fence", "graph_identity": graph["identity"]}

    class PreconditionFailed(Exception):
        code = "PreconditionFailed"

    monkeypatch.setattr(manifest, "put_json_object_if_absent", lambda *args: (_ for _ in ()).throw(PreconditionFailed()))
    monkeypatch.setattr(manifest, "read_json_object", lambda *args: lock)

    with pytest.raises(DimensionalManifestError) as caught:
        acquire_graph_fence(object(), "bucket", graph)

    error = caught.value
    assert str(error) == (
        "Graph fence is already held; operator recovery required; "
        f"lock_key={lock_key}; graph_identity={graph['identity']}"
    )
    assert error.code == "GRAPH_FENCE_ALREADY_HELD"
    assert error.recovery_state == "GRAPH_FENCE_RECOVERY_REQUIRED"
    assert error.lock_key == lock_key
    assert error.graph_identity == graph["identity"]


def test_mismatched_graph_fence_remains_hard_conflict(monkeypatch):
    import gold.implementation.dimensional.manifest as manifest

    graph = build_graph_manifest(dimensional_lineage(), dim_date_start="2026-01-01", dim_date_end="2026-01-02")
    lock_key = graph_lock_key(graph["identity"])

    class ConditionalConflict(Exception):
        code = "ConditionalRequestConflict"

    monkeypatch.setattr(manifest, "put_json_object_if_absent", lambda *args: (_ for _ in ()).throw(ConditionalConflict()))
    monkeypatch.setattr(
        manifest,
        "read_json_object",
        lambda *args: {"version": 1, "kind": "gold_dimensional_graph_fence", "graph_identity": "different"},
    )

    with pytest.raises(DimensionalManifestError, match=f"lock_key={lock_key}") as caught:
        acquire_graph_fence(object(), "bucket", graph)

    assert caught.value.code == "GRAPH_FENCE_CONFLICT"
    assert caught.value.recovery_state is None


def test_matching_graph_fence_writer_does_not_read_sources(monkeypatch):
    import gold.implementation.dimensional.writer as writer

    monkeypatch.setenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "true")
    monkeypatch.setattr(writer, "read_completed_graph", lambda *args: None)
    monkeypatch.setattr(writer, "read_table_manifests", lambda *args: {})
    monkeypatch.setattr(writer, "acquire_global_fence", lambda *args: {"kind": "global"})
    monkeypatch.setattr(writer, "release_global_fence", lambda *args: None)
    error = DimensionalManifestError(
        "Graph fence is already held; operator recovery required; lock_key=lock; graph_identity=graph",
        code="GRAPH_FENCE_ALREADY_HELD",
        recovery_state="GRAPH_FENCE_RECOVERY_REQUIRED",
        lock_key="lock",
        graph_identity="graph",
    )
    monkeypatch.setattr(writer, "acquire_graph_fence", lambda *args: (_ for _ in ()).throw(error))
    monkeypatch.setattr(writer, "read_dimensional_sources", lambda *args: pytest.fail("must not read sources"))

    with pytest.raises(DimensionalManifestError) as caught:
        run_dimensional_graph(None, object(), "bucket", dimensional_lineage(),
                              dim_date_start="2026-01-01", dim_date_end="2026-01-02", mode=WRITE)

    assert caught.value.code == "GRAPH_FENCE_ALREADY_HELD"
    assert caught.value.recovery_state == "GRAPH_FENCE_RECOVERY_REQUIRED"


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
    monkeypatch.setattr(writer, "read_table_readback", lambda *args: {"status": "PASS", "snapshot_id": 1, "file_count": 1, "file_bytes": 1, "row_count": 1})
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


def test_distinct_graph_global_conflict_preserves_recovery_state_and_lock_metadata(monkeypatch):
    import gold.implementation.dimensional.manifest as manifest
    import gold.implementation.dimensional.writer as writer

    requested = build_graph_manifest(dimensional_lineage(), dim_date_start="2026-02-01", dim_date_end="2026-02-02")
    holder = {"version": 1, "kind": "gold_dimensional_global_fence", "graph_identity": "h" * 64}

    class PreconditionFailed(Exception):
        code = "PreconditionFailed"

    removed = []
    monkeypatch.setattr(manifest, "put_json_object_if_absent",
                        lambda *args: (_ for _ in ()).throw(PreconditionFailed()))
    monkeypatch.setattr(manifest, "read_json_object", lambda *_: holder)
    client = type("Client", (), {"remove_object": lambda *args: removed.append(args)})()

    with pytest.raises(DimensionalManifestError) as caught:
        writer.acquire_global_fence(client, "bucket", requested)

    error = caught.value
    assert error.code == "GLOBAL_FENCE_CONFLICT"
    assert error.recovery_state == "GLOBAL_FENCE_RECOVERY_REQUIRED"
    assert error.lock_key == global_lock_key()
    assert error.lock_metadata == holder
    assert removed == []


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
