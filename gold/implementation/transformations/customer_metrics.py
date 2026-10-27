from pyspark.sql import functions as F


MONEY_TYPE = "decimal(20,2)"


def build_customer_metrics(
    customers_df,
    orders_df,
    order_items_df,
):
    """
    Build Gold customer_metrics.

    Grain:
        One row per customer_id.

    Business rules:
        - Preserve customers without orders.
        - Count each order exactly once.
        - Orders without items have zero value.
        - CANCELLED orders remain in total order value.
        - Delivered value includes DELIVERED orders only.
        - Customers without orders have NULL order dates.
    """

    # ========================================================
    # 1. Customers
    # ========================================================

    customers = customers_df.select(
        F.col("customer_id").cast("long"),
        F.col("full_name"),
    )

    # ========================================================
    # 2. Aggregate order items at order grain
    # ========================================================

    items = (
        order_items_df
        .select(
            "order_id",
            "quantity",
            "unit_price",
            "discount_amount",
        )
        .withColumn(
            "net_amount",
            (
                F.col("quantity") * F.col("unit_price")
                - F.col("discount_amount")
            ).cast(MONEY_TYPE),
        )
    )

    order_values = (
        items
        .groupBy("order_id")
        .agg(
            F.sum("net_amount")
            .cast(MONEY_TYPE)
            .alias("order_value")
        )
    )

    # ========================================================
    # 3. Join orders with aggregated order values
    # ========================================================

    orders = orders_df.select(
        "order_id",
        "customer_id",
        "status",
        "order_date",
    )

    orders_with_values = (
        orders
        .join(
            order_values,
            on="order_id",
            how="left",
        )
        .withColumn(
            "order_value",
            F.coalesce(
                F.col("order_value"),
                F.lit(0).cast(MONEY_TYPE),
            ).cast(MONEY_TYPE),
        )
        .withColumn(
            "order_date",
            F.to_date("order_date"),
        )
    )

    # ========================================================
    # 4. Aggregate metrics by customer
    # ========================================================

    customer_aggregates = (
        orders_with_values
        .groupBy("customer_id")
        .agg(
            F.count("order_id")
            .cast("long")
            .alias("order_count"),

            F.sum(
                F.when(
                    F.col("status") == "DELIVERED",
                    1,
                ).otherwise(0)
            )
            .cast("long")
            .alias("delivered_order_count"),

            F.sum(
                F.when(
                    F.col("status") == "CANCELLED",
                    1,
                ).otherwise(0)
            )
            .cast("long")
            .alias("cancelled_order_count"),

            F.sum("order_value")
            .cast(MONEY_TYPE)
            .alias("total_order_value"),

            F.sum(
                F.when(
                    F.col("status") == "DELIVERED",
                    F.col("order_value"),
                ).otherwise(
                    F.lit(0).cast(MONEY_TYPE)
                )
            )
            .cast(MONEY_TYPE)
            .alias("delivered_order_value"),

            F.min("order_date")
            .cast("date")
            .alias("first_order_date"),

            F.max("order_date")
            .cast("date")
            .alias("last_order_date"),
        )
    )

    # ========================================================
    # 5. Preserve customers without orders
    # ========================================================

    result = customers.join(
        customer_aggregates,
        on="customer_id",
        how="left",
    )

    numeric_columns = {
        "order_count": "long",
        "delivered_order_count": "long",
        "cancelled_order_count": "long",
        "total_order_value": MONEY_TYPE,
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

    # ========================================================
    # 6. Enforce Gold Contract column order
    # ========================================================

    return result.select(
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("full_name").cast("string").alias("full_name"),
        "order_count",
        "delivered_order_count",
        "cancelled_order_count",
        "total_order_value",
        "delivered_order_value",
        F.col("first_order_date").cast("date"),
        F.col("last_order_date").cast("date"),
    )