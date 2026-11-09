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
    if name.startswith("dim_") and name != "dim_date":
        surrogate = key[0]
        if dataframe.where(F.col(surrogate) == UNKNOWN_KEY).count() != 1:
            raise DimensionalQualityError(f"{name} requires exactly one unknown member")
    if name == "fact_orders":
        if dataframe.where(F.col("customer_key").isNull()).limit(1).count():
            raise DimensionalQualityError("fact_orders customer_key cannot be null")
        if dimensions is not None:
            valid = dimensions["dim_customer"].select("customer_key")
            if dataframe.join(valid, "customer_key", "left_anti").limit(1).count():
                raise DimensionalQualityError("fact_orders has an unresolved customer foreign key")
    return {"table": name, "status": "PASS", "row_count": dataframe.count()}
