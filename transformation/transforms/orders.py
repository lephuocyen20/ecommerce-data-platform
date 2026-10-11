from pyspark.sql.functions import col
from pyspark.sql.types import LongType, TimestampType

valid_statuses = [
    "PENDING",
    "CONFIRMED",
    "PROCESSING",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
]


def transform_orders(df):
    """Cast order columns and remove incomplete, invalid, or duplicate records."""
    orders_clean_df = (
        df.withColumn("order_id", col("order_id").cast(LongType()))
        .withColumn("customer_id", col("customer_id").cast(LongType()))
        .withColumn("order_date", col("order_date").cast(TimestampType()))
        .withColumn("updated_at", col("updated_at").cast(TimestampType()))
    )
    return (
        orders_clean_df.dropna(
            subset=[
                "order_id",
                "customer_id",
                "status",
                "order_date",
                "updated_at",
            ]
        )
        .filter(col("status").isin(valid_statuses))
        .dropDuplicates(["order_id"])
    )
