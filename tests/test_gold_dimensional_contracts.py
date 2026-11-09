from gold.implementation.dimensional.contracts import DIMENSIONS, FACTS, UNKNOWN_KEY
from gold.implementation.dimensional.reconciliation import reconcile_fact_orders

def test_dimensional_contracts_declare_grain_and_sources():
    assert UNKNOWN_KEY == -1
    assert DIMENSIONS["dim_customer"]["type"] == "type_1"
    assert FACTS["fact_orders"]["grain"] == ("order_id",)
    assert set(FACTS["fact_orders"]["sources"]) == {"orders", "order_items", "customers"}
    assert FACTS["fact_orders"]["foreign_keys"] == {"customer_key": "dim_customer"}


def test_fact_orders_reconciler_imports_and_maps_customer_natural_key():
    row = {
        "order_id": 1, "customer_id": 7, "order_date_key": 20260101,
        "order_status": "delivered", "line_count": 1, "item_count": 2,
        "gross_amount": "12.00", "discount_amount": "1.00", "net_amount": "11.00",
    }
    iceberg = {**row, "customer_natural_key": row["customer_id"]}
    iceberg.pop("customer_id")
    assert reconcile_fact_orders([row], [iceberg])["status"] == "PASS"
