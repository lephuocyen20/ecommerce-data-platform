import pytest

from gold.implementation.dimensional.contracts import DIMENSIONS, FACTS, TABLE_COLUMNS, UNKNOWN_KEY, DIMENSIONAL_SEMANTIC_CONTRACT
from gold.implementation.dimensional.reconciliation import reconcile_fact_orders
from lakehouse.models.spec import SPECS

def test_dimensional_contracts_declare_grain_and_sources():
    assert UNKNOWN_KEY == -1
    assert DIMENSIONS["dim_customer"]["type"] == "type_1"
    assert FACTS["fact_orders"]["grain"] == ("order_id",)
    assert set(FACTS["fact_orders"]["sources"]) == {"orders", "order_items", "customers"}
    assert FACTS["fact_orders"]["foreign_keys"] == {"customer_key": "dim_customer", "order_date_key": "dim_date"}


def test_dimensional_contracts_declare_utc_semantics_and_measure_rules():
    assert DIMENSIONAL_SEMANTIC_CONTRACT["timezone"] == "UTC"


def test_payment_fact_maps_silver_status_to_canonical_payment_status():
    assert "status" in SPECS["payments"].columns
    assert "payment_status" in TABLE_COLUMNS["fact_payments"]
    assert FACTS["fact_orders"]["foreign_keys"]["order_date_key"] == "dim_date"
    assert FACTS["fact_orders"]["measure_rules"]["net_amount"] == "gross_amount - discount_amount"


def test_fact_orders_reconciler_imports_and_maps_customer_natural_key():
    row = {
        "order_id": 1, "customer_id": 7, "order_date_key": 20260101,
        "order_status": "delivered", "line_count": 1, "item_count": 2,
        "gross_amount": "12.00", "discount_amount": "1.00", "net_amount": "11.00",
    }
    iceberg = {**row, "customer_natural_key": row["customer_id"]}
    iceberg.pop("customer_id")
    assert reconcile_fact_orders([row], [iceberg])["status"] == "PASS"


def test_fact_orders_reconciliation_rejects_invalid_measure_and_date():
    row = {"order_id": 1, "customer_id": 7, "order_date_key": 20260101,
           "order_status": "delivered", "line_count": 1, "item_count": 2,
           "gross_amount": "12.00", "discount_amount": "1.00", "net_amount": "11.00"}
    with pytest.raises(ValueError, match="net_amount"):
        reconcile_fact_orders([dict(row, net_amount="-1.00")], [row])
    with pytest.raises(ValueError, match="YYYYMMDD"):
        reconcile_fact_orders([dict(row, order_date_key=1)], [row])
    with pytest.raises(ValueError, match="net_amount"):
        reconcile_fact_orders([dict(row, net_amount="10.00")], [row])

    with pytest.raises(ValueError, match="YYYYMMDD"):
        reconcile_fact_orders([dict(row, order_date_key=20260230)], [row])
