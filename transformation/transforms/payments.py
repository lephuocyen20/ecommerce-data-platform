from pyspark.sql.functions import col
from pyspark.sql.types import LongType, DecimalType, TimestampType, BooleanType
from ingestion.incremental.tombstones import validate_tombstone_columns, validate_tombstone_dataframe

VALID_STATUSES = [
    "PENDING",
    "SUCCESS",
    "FAILED",
    "REFUNDED",
]


def transform_payments(df):
    """Cast payment columns and remove incomplete, invalid, or duplicate records."""
    validate_tombstone_columns("payments", df.columns)
    payments_clean_df = (
        df.withColumn("payment_id", col("payment_id").cast(LongType()))
        .withColumn("order_id", col("order_id").cast(LongType()))
        .withColumn("amount", col("amount").cast(DecimalType(12, 2)))
        .withColumn("created_at", col("created_at").cast(TimestampType()))
    )
    payments_clean_df = (payments_clean_df
        .withColumn("updated_at", col("updated_at").cast(TimestampType()))
        .withColumn("_is_deleted", col("_is_deleted").cast(BooleanType()))
        .withColumn("deleted_at", col("deleted_at").cast(TimestampType())))
    validate_tombstone_dataframe(payments_clean_df)
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
