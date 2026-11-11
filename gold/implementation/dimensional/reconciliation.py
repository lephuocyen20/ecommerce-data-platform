"""Fail-closed parity checks for order-grain dimensional Gold."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from gold.implementation.semantic_reconciliation import reconcile_rows, GoldSemanticMismatch

def reconcile_fact_orders(legacy_rows, iceberg_rows):
    contract = {"semantic": {"version": "1.0"}, "primary_key": ["order_id"], "columns": {"order_id":"long", "customer_id":"long", "order_date_key":"long", "order_status":"string", "line_count":"long", "item_count":"long", "gross_amount":"decimal(20,2)", "discount_amount":"decimal(20,2)", "net_amount":"decimal(20,2)"}}
    # Legacy dbt calls the business key customer_id; dimensional Gold retains it
    # as customer_natural_key alongside the surrogate customer_key.
    def canonical(rows):
        result = []
        for row in rows:
            customer_id = row.get("customer_natural_key", row.get("customer_id"))
            if customer_id is None or row.get("order_id") is None:
                raise GoldSemanticMismatch("fact_orders parity requires non-null order_id and customer_natural_key")
            order_date_key = row.get("order_date_key")
            try:
                date_text = str(int(order_date_key))
                datetime.strptime(date_text, "%Y%m%d").replace(tzinfo=timezone.utc)
            except (TypeError, ValueError, OverflowError):
                raise GoldSemanticMismatch("fact_orders parity requires a UTC YYYYMMDD order_date_key")
            if len(date_text) != 8:
                raise GoldSemanticMismatch("fact_orders parity requires a UTC YYYYMMDD order_date_key")
            try:
                gross = Decimal(str(row["gross_amount"]))
                discount = Decimal(str(row["discount_amount"]))
                net = Decimal(str(row["net_amount"]))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise GoldSemanticMismatch("fact_orders parity measures must be numeric") from exc
            expected_net = (gross - discount).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            if net.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) != expected_net:
                raise GoldSemanticMismatch("fact_orders parity net_amount must equal gross_amount - discount_amount")
            for measure in ("line_count", "item_count", "gross_amount", "discount_amount", "net_amount"):
                if row.get(measure) is None or Decimal(str(row[measure])) < 0:
                    raise GoldSemanticMismatch(f"fact_orders parity measure {measure} is invalid")
            result.append({**row, "customer_id": customer_id})
        return result
    return reconcile_rows("fact_orders", canonical(legacy_rows), canonical(iceberg_rows), contract)
