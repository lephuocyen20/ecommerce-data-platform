from pyspark.sql.functions import col
from pyspark.sql.types import LongType, DecimalType, TimestampType

VALID_STATUSES = [
    "PENDING",
    "SUCCESS",
    "FAILED",
    "REFUNDED",
]


def transform_payments(df):
    """Cast payment columns and remove incomplete, invalid, or duplicate records."""
    payments_clean_df = (
        df.withColumn("payment_id", col("payment_id").cast(LongType()))
        .withColumn("order_id", col("order_id").cast(LongType()))
        .withColumn("amount", col("amount").cast(DecimalType(12, 2)))
        .withColumn("created_at", col("created_at").cast(TimestampType()))
    )
    return (
        payments_clean_df.dropna(
            subset=[
                "payment_id",
                "order_id",
                "amount",
                "payment_method",
                "status",
                "created_at",
            ]
        )
        .filter(col("amount") > 0)
        .filter(col("status").isin(VALID_STATUSES))
        .dropDuplicates(["payment_id"])
    )
