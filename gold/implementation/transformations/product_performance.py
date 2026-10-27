from pyspark.sql import functions as F


MONEY_TYPE = "decimal(20,2)"


def build_product_performance(products_df, orders_df, order_items_df):
    """
    Build Gold product_performance.

    Grain: one row per product_id.

    Business rules:
    - Preserve products without orders.
    - Count distinct orders per product.
    - CANCELLED orders remain in total order value.
    - Only DELIVERED orders contribute to delivered metrics.
    - Values represent order value, not recognized revenue.
    """

    products = products_df.select(
        "product_id",
        "product_name",
    )

    orders = orders_df.select(
        "order_id",
        "status",
    )

    items = (
        order_items_df
        .select(
            "order_id",
            "product_id",
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
        .withColumn(
            "net_amount",
            (
                F.col("quantity") * F.col("unit_price")
                - F.col("discount_amount")
            ).cast(MONEY_TYPE),
        )
    )

    items_with_status = items.join(
        orders,
        on="order_id",
        how="inner",
    )

    product_totals = (
        items_with_status
        .groupBy("product_id")
        .agg(
            F.countDistinct("order_id")
            .cast("long")
            .alias("order_count"),

            F.sum("quantity")
            .cast("long")
            .alias("units_ordered"),

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
                    F.col("quantity"),
                ).otherwise(0)
            )
            .cast("long")
            .alias("delivered_units"),

            F.sum(
                F.when(
                    F.col("status") == "DELIVERED",
                    F.col("net_amount"),
                ).otherwise(F.lit(0).cast(MONEY_TYPE))
            )
            .cast(MONEY_TYPE)
            .alias("delivered_order_value"),
        )
    )

    result = products.join(
        product_totals,
        on="product_id",
        how="left",
    )

    numeric_columns = {
        "order_count": "long",
        "units_ordered": "long",
        "gross_merchandise_value": MONEY_TYPE,
        "discount_amount": MONEY_TYPE,
        "net_order_value": MONEY_TYPE,
        "delivered_units": "long",
        "delivered_order_value": MONEY_TYPE,
    }

    for column, data_type in numeric_columns.items():
        result = result.withColumn(
            column,
            F.coalesce(
                F.col(column),
                F.lit(0).cast(data_type),
            ).cast(data_type),
        )

    return result.select(
        F.col("product_id").cast("long").alias("product_id"),
        "product_name",
        "order_count",
        "units_ordered",
        "gross_merchandise_value",
        "discount_amount",
        "net_order_value",
        "delivered_units",
        "delivered_order_value",
    )