
from pyspark.sql import functions as F


MONEY_TYPE = "decimal(20,2)"


def build_daily_sales(orders_df, order_items_df):
    """
    Build Gold daily_sales using Order Value Analytics.

    Grain: one row per order_date.

    CANCELLED orders remain in net_order_value.
    Only DELIVERED orders contribute to delivered_order_value.
    """

    item_amounts = (
        order_items_df
        .select(
            "order_id",
            "quantity",
            "unit_price",
            "discount_amount",
        )
        .withColumn(
            "gross_amount",
            (
                F.col("quantity") * F.col("unit_price")
            ).cast(MONEY_TYPE),
        )
    )

    order_totals = (
        item_amounts
        .groupBy("order_id")
        .agg(
            F.sum("gross_amount")
            .cast(MONEY_TYPE)
            .alias("gross_amount"),
            F.sum("discount_amount")
            .cast(MONEY_TYPE)
            .alias("discount_amount"),
        )
        .withColumn(
            "net_amount",
            (
                F.col("gross_amount")
                - F.col("discount_amount")
            ).cast(MONEY_TYPE),
        )
    )

    orders_with_totals = (
        orders_df
        .select("order_id", "order_date", "status")
        .join(order_totals, on="order_id", how="left")
        .withColumn(
            "gross_amount",
            F.coalesce(
                F.col("gross_amount"),
                F.lit(0).cast(MONEY_TYPE),
            ),
        )
        .withColumn(
            "discount_amount",
            F.coalesce(
                F.col("discount_amount"),
                F.lit(0).cast(MONEY_TYPE),
            ),
        )
        .withColumn(
            "net_amount",
            F.coalesce(
                F.col("net_amount"),
                F.lit(0).cast(MONEY_TYPE),
            ),
        )
        .withColumn(
            "order_date",
            F.to_date("order_date"),
        )
    )

    result = (
        orders_with_totals
        .groupBy("order_date")
        .agg(
            F.count("*").alias("order_count"),
            F.sum(
                F.when(
                    F.col("status") == "DELIVERED", 1
                ).otherwise(0)
            ).cast("long").alias("delivered_order_count"),
            F.sum(
                F.when(
                    F.col("status") == "CANCELLED", 1
                ).otherwise(0)
            ).cast("long").alias("cancelled_order_count"),
            F.sum("gross_amount")
            .cast(MONEY_TYPE)
            .alias("gross_merchandise_value"),
            F.sum("discount_amount")
            .cast(MONEY_TYPE)
            .alias("discount_amount"),
            F.sum("net_amount")
            .cast(MONEY_TYPE)
            .alias("net_order_value"),
            F.sum(
                F.when(
                    F.col("status") == "DELIVERED",
                    F.col("net_amount"),
                ).otherwise(F.lit(0).cast(MONEY_TYPE))
            )
            .cast(MONEY_TYPE)
            .alias("delivered_order_value"),
        )
        .select(
            "order_date",
            "order_count",
            "delivered_order_count",
            "cancelled_order_count",
            "gross_merchandise_value",
            "discount_amount",
            "net_order_value",
            "delivered_order_value",
        )
    )

    return result
