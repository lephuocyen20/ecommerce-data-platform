"""Fail-closed in-memory quality gates before an Iceberg Gold write."""

from pyspark.sql import functions as F

from .contracts import TABLE_COLUMNS, TABLE_MERGE_KEYS, UNKNOWN_KEY


class DimensionalQualityError(ValueError):
    pass


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
