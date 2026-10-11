from pyspark.sql.functions import col
from pyspark.sql.types import LongType, DecimalType, TimestampType

VALID_STATUSES = [
    "PENDING",
    "PICKED_UP",
    "IN_TRANSIT",
    "OUT_FOR_DELIVERY",
    "DELIVERED",
    "FAILED",
    "RETURNED",
]


def transform_shipments(df):
    """Cast shipment columns and remove incomplete, invalid, or duplicate records."""
    shipments_clean_df = (
        df.withColumn("shipment_id", col("shipment_id").cast(LongType()))
        .withColumn("order_id", col("order_id").cast(LongType()))
        .withColumn("shipping_fee", col("shipping_fee").cast(DecimalType(12, 2)))
        .withColumn("shipped_at", col("shipped_at").cast(TimestampType()))
        .withColumn("delivered_at", col("delivered_at").cast(TimestampType()))
        .withColumn(
            "estimated_delivery_at", col("estimated_delivery_at").cast(TimestampType())
        )
        .withColumn("created_at", col("created_at").cast(TimestampType()))
        .withColumn("updated_at", col("updated_at").cast(TimestampType()))
    )
    return (
        shipments_clean_df.dropna(
            subset=[
                "shipment_id",
                "order_id",
                "shipping_address",
                "shipping_fee",
                "status",
                "created_at",
                "updated_at",
            ]
        )
        .filter(col("shipping_fee") >= 0)
        .filter(col("status").isin(VALID_STATUSES))
        .dropDuplicates(["shipment_id"])
    )
