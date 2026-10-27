
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    DecimalType,
)

from gold.implementation.contracts.datasets import (
    get_gold_contract,
)


MONEY_TYPE = "decimal(20,2)"


class GoldDataQualityError(ValueError):
    """Raised when a Gold data quality guard fails."""


def require(condition, message):
    if not condition:
        raise GoldDataQualityError(message)


def has_rows(df):
    return df.limit(1).count() > 0


def check_primary_key(df, columns, name):
    """
    Check primary key completeness and uniqueness.
    """

    require(
        all(column in df.columns for column in columns),
        f"{name}: missing primary key columns",
    )

    null_condition = None

    for column in columns:
        condition = F.col(column).isNull()
        null_condition = (
            condition
            if null_condition is None
            else null_condition | condition
        )

    require(
        not has_rows(df.filter(null_condition)),
        f"{name}: NULL primary key",
    )

    duplicates = (
        df.groupBy(*columns)
        .count()
        .filter(F.col("count") > 1)
    )

    require(
        not has_rows(duplicates),
        f"{name}: duplicate primary key",
    )


def validate_daily_sales_sources(orders_df, items_df):
    """
    Validate Silver source data before transformation.
    """

    required_orders = {
        "order_id",
        "order_date",
        "status",
    }

    required_items = {
        "order_item_id",
        "order_id",
        "quantity",
        "unit_price",
        "discount_amount",
    }

    require(
        required_orders.issubset(set(orders_df.columns)),
        "orders: missing required columns",
    )

    require(
        required_items.issubset(set(items_df.columns)),
        "order_items: missing required columns",
    )

    check_primary_key(
        orders_df,
        ["order_id"],
        "orders",
    )

    check_primary_key(
        items_df,
        ["order_item_id"],
        "order_items",
    )

    require(
        not has_rows(
            items_df.filter(F.col("order_id").isNull())
        ),
        "order_items: NULL order_id",
    )

    orphan_items = (
        items_df
        .select("order_id")
        .join(
            orders_df.select("order_id"),
            on="order_id",
            how="left_anti",
        )
    )

    require(
        not has_rows(orphan_items),
        "order_items: orphan order_id",
    )

    invalid_money = items_df.filter(
        F.col("quantity").isNull()
        | F.col("unit_price").isNull()
        | F.col("discount_amount").isNull()
        | (F.col("quantity") <= 0)
        | (F.col("unit_price") < 0)
        | (F.col("discount_amount") < 0)
        | (
            F.col("discount_amount")
            > F.col("quantity") * F.col("unit_price")
        )
    )

    require(
        not has_rows(invalid_money),
        "order_items: invalid monetary values",
    )

    invalid_dates = orders_df.filter(
        F.col("order_date").isNull()
        | F.to_date("order_date").isNull()
    )

    require(
        not has_rows(invalid_dates),
        "orders: invalid order_date",
    )

    require(
        not has_rows(
            orders_df.filter(
                F.col("status").isNull()
                | ~F.col("status").isin(
                    "PENDING",
                    "CONFIRMED",
                    "PROCESSING",
                    "SHIPPED",
                    "DELIVERED",
                    "CANCELLED",
                )
            )
        ),
        "orders: invalid status",
    )

    return {
        "status": "PASS",
        "checks": [
            "orders_primary_key",
            "items_primary_key",
            "items_foreign_key",
            "monetary_values",
            "order_dates",
            "order_status",
        ],
    }


def validate_daily_sales_output(gold_df):
    """
    Validate Gold schema, grain and metrics.
    """

    contract = get_gold_contract("daily_sales")

    require(
        gold_df.columns == list(contract["columns"]),
        "daily_sales: column mismatch",
    )

    expected_types = {
        "date": DateType(),
        "long": LongType(),
        "string": StringType(),
        "decimal(20,2)": DecimalType(20, 2),
    }

    for field in gold_df.schema.fields:
        expected_type = expected_types[
            contract["columns"][field.name]
        ]

        require(
            field.dataType == expected_type,
            f"daily_sales: invalid type for {field.name}",
        )

    check_primary_key(
        gold_df,
        contract["primary_key"],
        "daily_sales",
    )

    metrics = [
        "order_count",
        "delivered_order_count",
        "cancelled_order_count",
        "gross_merchandise_value",
        "discount_amount",
        "net_order_value",
        "delivered_order_value",
    ]

    invalid_metrics = None

    for column in metrics:
        condition = F.col(column).isNull()

        invalid_metrics = (
            condition
            if invalid_metrics is None
            else invalid_metrics | condition
        )

    require(
        not has_rows(gold_df.filter(invalid_metrics)),
        "daily_sales: NULL metrics",
    )

    invalid_values = gold_df.filter(
        (F.col("order_count") <= 0)
        | (F.col("delivered_order_count") < 0)
        | (F.col("cancelled_order_count") < 0)
        | (
            F.col("delivered_order_count")
            + F.col("cancelled_order_count")
            > F.col("order_count")
        )
        | (F.col("gross_merchandise_value") < 0)
        | (F.col("discount_amount") < 0)
        | (F.col("net_order_value") < 0)
        | (F.col("delivered_order_value") < 0)
        | (
            F.col("delivered_order_value")
            > F.col("net_order_value")
        )
        | (
            F.col("net_order_value")
            != F.col("gross_merchandise_value")
            - F.col("discount_amount")
        )
    )

    require(
        not has_rows(invalid_values),
        "daily_sales: invalid metric values",
    )

    return {
        "status": "PASS",
        "checks": [
            "contract_schema",
            "unique_grain",
            "non_null_metrics",
            "metric_consistency",
        ],
    }
