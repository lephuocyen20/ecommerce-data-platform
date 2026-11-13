import pytest

from gold.implementation.dimensional.contracts import DIMENSIONAL_TABLES, FACTS, TABLE_COLUMNS
from gold.implementation.dimensional.reconciliation import (
    reconcile_fact_order_items,
    reconcile_fact_payments,
    reconcile_fact_shipments,
)
from gold.implementation.semantic_reconciliation import GoldSemanticMismatch, reconcile_rows
from gold.implementation.dimensional.verification import build_certification_report


def test_all_dimensional_outputs_declare_grain_merge_key_and_measure_semantics():
    assert set(DIMENSIONAL_TABLES) == {
        "dim_date", "dim_customer", "dim_product", "fact_orders",
        "fact_order_items", "fact_payments", "fact_shipments",
    }
    for table, contract in DIMENSIONAL_TABLES.items():
        assert tuple(contract["grain"]) == tuple(contract["merge_key"]), table
    assert FACTS["fact_orders"]["lifecycle"]["signed_revenue_measure"] is None
    assert "signed_revenue" not in TABLE_COLUMNS["fact_orders"]
    assert FACTS["fact_payments"]["measures_metadata"]["signed_revenue"]["additivity"] == "additive"
    assert FACTS["fact_payments"]["foreign_keys"] == {"order_id": "fact_orders"}
    assert FACTS["fact_shipments"]["foreign_keys"] == {"order_id": "fact_orders"}


def test_strict_reconciliation_rejects_missing_and_extra_columns():
    contract = {"semantic": {"version": "test"}, "primary_key": ["id"], "columns": {"id": "long", "value": "string"}}
    with pytest.raises(GoldSemanticMismatch, match="schema"):
        reconcile_rows("strict", [{"id": 1}], [{"id": 1, "value": "x"}], contract, strict_columns=True)
    with pytest.raises(GoldSemanticMismatch, match="schema"):
        reconcile_rows("strict", [{"id": 1, "value": "x", "extra": 1}], [{"id": 1, "value": "x"}], contract, strict_columns=True)


def test_fact_parity_helpers_cover_order_items_payments_and_shipments():
    items = [{
        "order_item_id": 1, "order_id": 10, "product_id": 7, "quantity": 2,
        "unit_price": "5.00", "gross_amount": "10.00", "discount_amount": "1.00", "net_amount": "9.00",
    }]
    assert reconcile_fact_order_items(items, [{**items[0], "product_key": 7}])["status"] == "PASS"

    payment = {"payment_id": 2, "order_id": 10, "status": "REFUNDED", "payment_method": "card", "amount": "10.00", "signed_revenue": "-10.00"}
    assert reconcile_fact_payments([payment], [{**payment, "payment_status": "REFUNDED", "payment_amount": "10.000"}])["status"] == "PASS"

    shipment = {"shipment_id": 3, "order_id": 10, "shipment_status": "DELIVERED", "shipped_at": "2026-01-01T00:00:00Z", "delivered_at": "2026-01-02T00:00:00Z"}
    assert reconcile_fact_shipments([shipment], [shipment])["status"] == "PASS"


def test_payment_parity_rejects_wrong_lifecycle_sign():
    payment = {"payment_id": 2, "order_id": 10, "payment_status": "REFUNDED", "payment_method": "card", "payment_amount": "10.00", "signed_revenue": "10.00"}
    with pytest.raises(ValueError, match="lifecycle sign"):
        reconcile_fact_payments([payment], [payment])


def _writer_result(status="DRY_RUN"):
    graph = {"identity": "a" * 64}
    quality = {table: {"status": "PASS", "row_count": index} for index, table in enumerate((
        "dim_date", "dim_customer", "dim_product", "fact_orders",
        "fact_order_items", "fact_payments", "fact_shipments",
    ), start=1)}
    return {"status": status, "mode": "DRY_RUN", "graph": graph, "quality": quality, "tables": list(quality)}


def test_certification_report_is_complete_and_dry_run_cannot_write():
    report = build_certification_report(
        run_id="run-1", code_identity="git-1", config_identity="cfg-1", lineage_identity="lineage-1",
        bucket="ecommerce-quality-it", prefix=None, ref="main", writer_result=_writer_result(),
    )
    assert report["graph_manifest_complete"] is False
    assert report["can_write"] is False
    assert report["certified"] is False
    assert report["readback_complete"] is False
    assert len(report["outputs"]) == 7
    assert report["recovery_classification"] == "NEW"


def test_certification_report_classifies_partial_without_cleanup_or_rollback():
    result = _writer_result("PARTIAL_REQUIRES_RECONCILIATION")
    result["tables"] = [{"table": "dim_customer"}]
    result["quality"] = {}
    report = build_certification_report(
        run_id="run-1", code_identity="git-1", config_identity="cfg-1", lineage_identity="lineage-1",
        bucket="ecommerce-quality-it", prefix=None, ref="main", writer_result=result,
    )
    assert report["recovery_classification"] == "PARTIAL_REQUIRES_RECONCILIATION"
    assert report["graph_manifest_complete"] is False
    assert report["outputs"] == [{"table": "dim_customer", "snapshot_id": None, "file_count": None, "row_count": None, "readback_status": "NOT_AVAILABLE"}]


def test_already_published_reads_completed_graph_and_table_manifests(monkeypatch):
    graph = {"identity": "b" * 64}
    manifests = {
        table: {"table": table, "identity": table, "snapshot_id": index,
                "quality": {"readback": {"status": "PASS", "snapshot_id": index,
                                           "file_count": 1, "file_bytes": 10, "row_count": index}}}
        for index, table in enumerate(("dim_date", "dim_customer", "dim_product", "fact_orders",
                                       "fact_order_items", "fact_payments", "fact_shipments"), start=1)
    }
    monkeypatch.setattr("gold.implementation.dimensional.verification.read_completed_graph", lambda *_: {"outputs": [{"table": table, "identity": manifests[table]["identity"]} for table in manifests]})
    monkeypatch.setattr("gold.implementation.dimensional.verification.read_table_manifests", lambda *_: manifests)
    report = build_certification_report(
        run_id="run-1", code_identity="git-1", config_identity="cfg-1", lineage_identity="lineage-1",
        bucket="ecommerce-quality-it", prefix=None, ref="main",
        writer_result={"status": "ALREADY_PUBLISHED", "mode": "WRITE", "graph": graph, "tables": []},
        client=object(),
    )
    assert report["certified"] is True
    assert report["readback_complete"] is True


def test_already_published_rejects_graph_table_identity_mismatch(monkeypatch):
    graph = {"identity": "c" * 64}
    tables = {table: {"identity": table, "snapshot_id": 1, "quality": {"readback": {
        "status": "PASS", "snapshot_id": 1, "file_count": 1, "file_bytes": 1, "row_count": 1,
    }}} for table in ("dim_date", "dim_customer", "dim_product", "fact_orders", "fact_order_items", "fact_payments", "fact_shipments")}
    monkeypatch.setattr("gold.implementation.dimensional.verification.read_completed_graph", lambda *_: {"outputs": [{"table": table, "identity": "wrong" if table == "dim_date" else table} for table in tables]})
    monkeypatch.setattr("gold.implementation.dimensional.verification.read_table_manifests", lambda *_: tables)
    with pytest.raises(ValueError, match="manifest mismatch"):
        build_certification_report(
            run_id="run-1", code_identity="git-1", config_identity="cfg-1", lineage_identity="lineage-1",
            bucket="ecommerce-quality-it", prefix=None, ref="main",
            writer_result={"status": "ALREADY_PUBLISHED", "mode": "WRITE", "graph": graph, "tables": []},
            client=object(),
        )


def test_published_without_snapshot_file_row_readback_is_not_certified():
    result = _writer_result("PUBLISHED")
    result["mode"] = "WRITE"
    report = build_certification_report(
        run_id="run-1", code_identity="git-1", config_identity="cfg-1", lineage_identity="lineage-1",
        bucket="ecommerce-quality-it", prefix=None, ref="main", writer_result=result,
    )
    assert report["graph_manifest_complete"] is True
    assert report["certified"] is False
    assert report["can_write"] is False
