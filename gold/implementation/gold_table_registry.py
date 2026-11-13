"""Canonical certified Gold table identity used by local BI contracts."""

from __future__ import annotations

RUN06_ID = "run-20261112-gold-06"
RUN06_SCHEMA = "gold_run_20261112_gold_06"
RUN06_PREFIX = "s3://ecommerce-data-lake/quality-it/run-20261112-gold-06/"
RUN06_NESSIE_REF = "quality_it_run_20261112_gold_06"

GOLD_TABLES = (
    "dim_date", "dim_customer", "dim_product", "fact_orders",
    "fact_order_items", "fact_payments", "fact_shipments",
)
RUN06_TABLES = GOLD_TABLES

RUN06_SNAPSHOT_IDS = {
    "dim_date": 2340245983998241199,
    "dim_customer": 4957293444030653664,
    "dim_product": 5087015407247836870,
    "fact_orders": 5274292672790927131,
    "fact_order_items": 3223103126187800136,
    "fact_payments": 9142204852940102980,
    "fact_shipments": 43752055579177804,
}
RUN06_EXPECTED_ROW_COUNTS = {
    "dim_date": 7, "dim_customer": 2, "dim_product": 2,
    "fact_orders": 1, "fact_order_items": 1, "fact_payments": 1,
    "fact_shipments": 1,
}


def require_exact_gold_tables(values) -> tuple[str, ...]:
    """Return canonical order only when the supplied table set is exact."""

    actual = tuple(values)
    if len(actual) != len(GOLD_TABLES) or set(actual) != set(GOLD_TABLES):
        raise ValueError("Gold table set must contain exactly the certified seven tables")
    return GOLD_TABLES
