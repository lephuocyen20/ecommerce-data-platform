from decimal import Decimal
from datetime import date

import pytest

from gold.implementation.run_metadata import build_gold_run_metadata, canonical_run_metadata
from gold.implementation.semantic_reconciliation import GoldSemanticMismatch, require_semantic_parity, reconcile_gold_rows


def test_daily_sales_semantic_parity_accepts_decimal_equivalence():
    rows = [{"order_date": "2026-01-01", "order_count": 2, "delivered_order_count": 1, "cancelled_order_count": 0, "gross_merchandise_value": "12.00", "discount_amount": "1.00", "net_order_value": "11.00", "delivered_order_value": "6.00"}]
    report = reconcile_gold_rows("daily_sales", rows, [dict(rows[0], gross_merchandise_value=Decimal("12.000"))])
    assert report["status"] == "PASS"


def test_semantic_reconciliation_is_fail_closed_for_metric_difference():
    left = [{"payment_method": "card", "payment_status": "paid", "payment_count": 1, "distinct_order_count": 1, "total_payment_amount": "10.00"}]
    right = [dict(left[0], total_payment_amount="11.00")]
    with pytest.raises(GoldSemanticMismatch):
        require_semantic_parity("payment_summary", left, right)


def test_semantic_reconciliation_rejects_missing_columns_and_bad_keys():
    with pytest.raises(GoldSemanticMismatch):
        reconcile_gold_rows("daily_sales", [{"order_date": "2026-01-01"}], [])
    row = {"payment_method": "card", "payment_status": "paid", "payment_count": 1, "distinct_order_count": 1, "total_payment_amount": "1"}
    with pytest.raises(GoldSemanticMismatch):
        reconcile_gold_rows("payment_summary", [row, row], [row])
    with pytest.raises(GoldSemanticMismatch):
        reconcile_gold_rows("payment_summary", [dict(row, payment_method=None)], [row])


def test_semantic_hash_normalises_decimal_formatting():
    row = {"payment_method": "card", "payment_status": "paid", "payment_count": 1, "distinct_order_count": 1, "total_payment_amount": "1.0"}
    report = reconcile_gold_rows("payment_summary", [row], [dict(row, total_payment_amount=Decimal("1.000"))])
    assert report["status"] == "PASS"


def test_semantic_primary_keys_normalise_date_and_numeric_values():
    sales = {"order_date": date(2026, 1, 1), "order_count": 1, "delivered_order_count": 1, "cancelled_order_count": 0, "gross_merchandise_value": "1", "discount_amount": "0", "net_order_value": "1", "delivered_order_value": "1"}
    assert reconcile_gold_rows("daily_sales", [sales], [dict(sales, order_date="2026-01-01")])["status"] == "PASS"
    payment = {"payment_method": "card", "payment_status": "paid", "payment_count": 1, "distinct_order_count": 1, "total_payment_amount": "1"}
    assert reconcile_gold_rows("payment_summary", [payment], [dict(payment, payment_count="1")])["status"] == "PASS"


def test_run_metadata_is_canonical_and_requires_complete_lineage():
    metadata = build_gold_run_metadata(run_id="run-1", bronze_batch_ids={"orders": "b1"}, silver_snapshots={"orders": "snap_1"}, iceberg_snapshots={"orders": 10}, attestations={"orders": "a"}, evidence={"orders": "e"}, gold_snapshot_id="gold_snap_1", status="PASS", code_sha="abc1234", started_at="2026-01-01T00:00:00Z", completed_at="2026-01-01T00:01:00Z", duration_seconds=60, rows_read=1, rows_written=1, rows_rejected=0)
    assert canonical_run_metadata(metadata) == canonical_run_metadata(dict(reversed(list(metadata.items()))))


def test_run_metadata_rejects_misaligned_lineage():
    with pytest.raises(ValueError):
        build_gold_run_metadata(run_id="r", bronze_batch_ids={"orders": "b"}, silver_snapshots={"orders": "s"}, iceberg_snapshots={}, attestations={"orders": "a"}, evidence={"orders": "e"}, gold_snapshot_id="gold_snap_1", status="PASS", code_sha="abc1234", started_at="2026-01-01T00:00:00Z", completed_at="2026-01-01T00:00:00Z", duration_seconds=0, rows_read=0, rows_written=0, rows_rejected=0)
