"""Fail-closed in-memory quality gates before an Iceberg Gold write."""

import json

from pyspark.sql import functions as F

from .contracts import TABLE_COLUMNS, TABLE_MERGE_KEYS, UNKNOWN_KEY


class DimensionalQualityError(ValueError):
    pass


def _coverage_diagnostics(source):
    """Return immutable source coverage identifiers for error diagnostics."""
    return {
        "manifest_keys": list(source.get("manifest_keys", [])),
        "batch_id": source.get("batch_id"),
        "snapshot_id": source.get("snapshot_id"),
    }


def validate_order_id_parity(sources, lineage):
    """Fail closed when child source order IDs are outside the orders source.

    This runs after verified Silver reads and before any Gold target table is
    created or merged.  The returned diagnostics are deliberately lineage
    aware so an operator can identify the exact source coverage mismatch.
    """
    orders = sources["orders"].select("order_id").where("order_id IS NOT NULL").distinct()
    diagnostics = []
    for child_table in ("payments", "shipments"):
        child = sources[child_table].select("order_id").where("order_id IS NOT NULL").distinct()
        orphan_count = child.join(orders, "order_id", "left_anti").count()
        diagnostics.append({
            "child_table": child_table,
            "parent_table": "orders",
            "child_coverage": _coverage_diagnostics(lineage["sources"][child_table]),
            "parent_coverage": _coverage_diagnostics(lineage["sources"]["orders"]),
            "orphan_count": int(orphan_count),
        })
    failures = [item for item in diagnostics if item["orphan_count"]]
    if failures:
        raise DimensionalQualityError(
            "Silver order_id parity preflight failed; diagnostics_table="
            + json.dumps(failures, sort_keys=True, separators=(",", ":"))
        )
    return diagnostics


def validate_table_frame(name, dataframe, dimensions=None):
    expected = TABLE_COLUMNS[name]
    if tuple(dataframe.columns) != expected:
        raise DimensionalQualityError(f"Schema drift for {name}: expected {expected}, got {tuple(dataframe.columns)}")
    key = TABLE_MERGE_KEYS[name]
    if dataframe.where(" OR ".join(f"{column} IS NULL" for column in key)).limit(1).count():
        raise DimensionalQualityError(f"Null grain key in {name}")
    if dataframe.groupBy(*key).count().where("count > 1").limit(1).count():
        raise DimensionalQualityError(f"Duplicate grain in {name}")
    if name == "dim_date" and dataframe.limit(1).count() == 0:
        raise DimensionalQualityError("dim_date cannot be empty")
    if name == "dim_date" and dataframe.where(
        F.col("date_key") != F.date_format("calendar_date", "yyyyMMdd").cast("bigint")
    ).limit(1).count():
        raise DimensionalQualityError("dim_date date_key must match the UTC calendar_date")
    if name.startswith("dim_") and name != "dim_date":
        surrogate = key[0]
        if dataframe.where(F.col(surrogate) == UNKNOWN_KEY).count() != 1:
            raise DimensionalQualityError(f"{name} requires exactly one unknown member")
    if name in {"fact_orders", "fact_order_items", "fact_payments", "fact_shipments"} and dimensions is not None:
        dimension_columns = {"customer_key": "customer_key", "order_date_key": "date_key", "product_key": "product_key", "order_id": "order_id"}
        from .contracts import FACTS
        for column, dimension in FACTS[name]["foreign_keys"].items():
            valid = dimensions[dimension].select(F.col(dimension_columns[column]).alias(column))
            if dataframe.join(valid, column, "left_anti").limit(1).count():
                raise DimensionalQualityError(f"{name} has an unresolved {column} foreign key")
    if name == "fact_orders":
        if dataframe.where(F.col("customer_key").isNull()).limit(1).count():
            raise DimensionalQualityError("fact_orders customer_key cannot be null")
        for column in ("line_count", "item_count", "gross_amount", "discount_amount", "net_amount"):
            if dataframe.where(F.col(column) < 0).limit(1).count():
                raise DimensionalQualityError(f"fact_orders measure {column} cannot be negative")
        if dataframe.where(
            F.round(F.col("net_amount"), 2) != F.round(F.col("gross_amount") - F.col("discount_amount"), 2)
        ).limit(1).count():
            raise DimensionalQualityError("fact_orders net_amount must equal gross_amount - discount_amount")
    if name == "fact_order_items":
        for column in ("quantity", "gross_amount", "discount_amount", "net_amount"):
            if dataframe.where(F.col(column) < 0).limit(1).count():
                raise DimensionalQualityError(f"fact_order_items measure {column} cannot be negative")
        if dataframe.where(F.round(F.col("net_amount"), 2) != F.round(F.col("gross_amount") - F.col("discount_amount"), 2)).limit(1).count():
            raise DimensionalQualityError("fact_order_items net_amount must equal gross_amount - discount_amount")
    if name == "fact_payments" and dataframe.where(F.col("payment_amount") < 0).limit(1).count():
        raise DimensionalQualityError("fact_payments payment_amount cannot be negative")
    if name == "fact_payments" and dataframe.where(
        F.when(F.upper(F.col("payment_status")) == "REFUNDED", -F.col("payment_amount"))
        .otherwise(F.col("payment_amount")) != F.col("signed_revenue")
    ).limit(1).count():
        raise DimensionalQualityError("fact_payments signed_revenue violates REFUNDED lifecycle sign")
    return {"table": name, "status": "PASS", "row_count": dataframe.count()}
