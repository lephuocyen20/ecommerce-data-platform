"""Fail-closed parity checks for order-grain dimensional Gold."""
from gold.implementation.semantic_reconciliation import reconcile_rows, GoldSemanticMismatch

def reconcile_fact_orders(legacy_rows, iceberg_rows):
    contract = {"semantic": {"version": "1.0"}, "primary_key": ["order_id"], "columns": {"order_id":"long", "customer_id":"long", "order_date_key":"long", "order_status":"string", "line_count":"long", "item_count":"long", "gross_amount":"decimal(20,2)", "discount_amount":"decimal(20,2)", "net_amount":"decimal(20,2)"}}
    # Legacy dbt calls the business key customer_id; dimensional Gold retains it
    # as customer_natural_key alongside the surrogate customer_key.
    def canonical(rows):
        return [{**row, "customer_id": row.get("customer_natural_key", row.get("customer_id"))} for row in rows]
    return reconcile_rows("fact_orders", canonical(legacy_rows), canonical(iceberg_rows), contract)
