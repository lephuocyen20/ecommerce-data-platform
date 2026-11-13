"""Fail-closed parity checks for order-grain dimensional Gold."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from gold.implementation.semantic_reconciliation import reconcile_rows, GoldSemanticMismatch

def reconcile_fact_orders(legacy_rows, iceberg_rows):
    contract = {"semantic": {"version": "1.0"}, "primary_key": ["order_id"], "columns": {"order_id":"long", "customer_id":"long", "order_date_key":"long", "order_status":"string", "line_count":"long", "item_count":"long", "gross_amount":"decimal(20,2)", "discount_amount":"decimal(20,2)", "net_amount":"decimal(20,2)"}}
    # Legacy dbt calls the business key customer_id; dimensional Gold retains it
    # as customer_natural_key alongside the surrogate customer_key.
    def canonical(rows, label):
        expected = set(contract["columns"])
        iceberg_shapes = (
            expected | {"customer_natural_key"},
            (expected - {"customer_id"}) | {"customer_natural_key"},
            (expected - {"customer_id"}) | {"customer_natural_key", "customer_key"},
        )
        result = []
        for row in rows:
            if not isinstance(row, dict):
                raise GoldSemanticMismatch(f"fact_orders {label} schema does not match parity contract")
            valid_shape = set(row) == expected if label == "legacy" else set(row) in iceberg_shapes
            if not valid_shape:
                raise GoldSemanticMismatch(f"fact_orders {label} schema does not match parity contract")
            if "customer_id" in row and "customer_natural_key" in row:
                try:
                    identifiers_match = Decimal(str(row["customer_id"])) == Decimal(str(row["customer_natural_key"]))
                except (InvalidOperation, TypeError, ValueError):
                    identifiers_match = row["customer_id"] == row["customer_natural_key"]
                if not identifiers_match:
                    raise GoldSemanticMismatch("fact_orders customer_id and customer_natural_key conflict")
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
            result.append({
                "order_id": row["order_id"], "customer_id": customer_id,
                "order_date_key": order_date_key, "order_status": row.get("order_status"),
                "line_count": row.get("line_count"), "item_count": row.get("item_count"),
                "gross_amount": row.get("gross_amount"), "discount_amount": row.get("discount_amount"),
                "net_amount": row.get("net_amount"),
            })
        return result
    return reconcile_rows("fact_orders", canonical(legacy_rows, "legacy"), canonical(iceberg_rows, "iceberg"), contract, strict_columns=True)


def _strict_fact_reconcile(dataset, legacy_rows, iceberg_rows, columns, aliases=None):
    aliases = aliases or {}
    names = [name for name, _ in columns]
    def canonical(rows):
        result = []
        for row in rows:
            if not isinstance(row, dict):
                raise GoldSemanticMismatch(f"{dataset} parity rows must be objects")
            item = {name: row.get(aliases.get(name, name)) for name in names}
            if item[names[0]] is None:
                raise GoldSemanticMismatch(f"{dataset} parity requires a non-null grain key")
            result.append(item)
        return result
    contract = {"semantic": {"version": "dimensional-1.0"}, "primary_key": [names[0]], "columns": dict(columns)}
    return reconcile_rows(dataset, canonical(legacy_rows), canonical(iceberg_rows), contract, strict_columns=True)


def reconcile_fact_order_items(legacy_rows, iceberg_rows):
    return _strict_fact_reconcile("fact_order_items", legacy_rows, iceberg_rows, (
        ("order_item_id", "long"), ("order_id", "long"), ("product_key", "long"),
        ("quantity", "long"), ("unit_price", "decimal(20,2)"), ("gross_amount", "decimal(20,2)"),
        ("discount_amount", "decimal(20,2)"), ("net_amount", "decimal(20,2)")), {"product_key": "product_id"})


def reconcile_fact_payments(legacy_rows, iceberg_rows):
    report = _strict_fact_reconcile("fact_payments", legacy_rows, iceberg_rows, (
        ("payment_id", "long"), ("order_id", "long"), ("payment_status", "string"),
        ("payment_method", "string"), ("payment_amount", "decimal(20,2)"), ("signed_revenue", "decimal(20,2)")),
        {"payment_status": "status", "payment_amount": "amount"})
    for row in iceberg_rows:
        amount = Decimal(str(row["payment_amount"]))
        expected = -amount if str(row["payment_status"]).upper() == "REFUNDED" else amount
        if Decimal(str(row["signed_revenue"])).quantize(Decimal("0.01")) != expected.quantize(Decimal("0.01")):
            raise GoldSemanticMismatch("fact_payments signed_revenue violates lifecycle sign")
    return report


def reconcile_fact_shipments(legacy_rows, iceberg_rows):
    return _strict_fact_reconcile("fact_shipments", legacy_rows, iceberg_rows, (
        ("shipment_id", "long"), ("order_id", "long"), ("shipment_status", "string"),
        ("shipped_at", "string"), ("delivered_at", "string")))
