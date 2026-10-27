
"""Build Gold payment_summary from verified Silver payments."""

from pyspark.sql import functions as F


def build_payment_summary(payments_df):
    """
    Aggregate payments by payment method and status.

    Grain:
        payment_method + payment_status

    Rules:
        - Include all valid payment statuses.
        - Count payment records independently of distinct orders.
        - Sum payment amounts without interpreting them as revenue.
        - Preserve the Gold Contract column types and order.
    """

    gold_df = (
        payments_df
        .groupBy(
            "payment_method",
            "status",
        )
        .agg(
            F.count(F.lit(1))
            .cast("long")
            .alias("payment_count"),

            F.countDistinct("order_id")
            .cast("long")
            .alias("distinct_order_count"),

            F.sum(
                F.col("amount").cast("decimal(20,2)")
            )
            .cast("decimal(20,2)")
            .alias("total_payment_amount"),
        )
        .select(
            F.col("payment_method"),
            F.col("status").alias("payment_status"),
            F.col("payment_count"),
            F.col("distinct_order_count"),
            F.col("total_payment_amount"),
        )
    )

    return gold_df
