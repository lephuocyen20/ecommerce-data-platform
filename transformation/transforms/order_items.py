from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, LongType, DecimalType, TimestampType, BooleanType
from ingestion.incremental.tombstones import validate_tombstone_columns, validate_tombstone_dataframe


def transform_order_items(df):
    """Cast order item columns and remove incomplete, invalid, or duplicate records."""
    validate_tombstone_columns("order_items", df.columns)
    order_items_clean_df = (
        df.withColumn("order_item_id", col("order_item_id").cast(LongType()))
        .withColumn("order_id", col("order_id").cast(LongType()))
        .withColumn("product_id", col("product_id").cast(LongType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(12, 2)))
        .withColumn("discount_amount", col("discount_amount").cast(DecimalType(12, 2)))
    )
    order_items_clean_df = (order_items_clean_df
        .withColumn("updated_at", col("updated_at").cast(TimestampType()))
        .withColumn("_is_deleted", col("_is_deleted").cast(BooleanType()))
        .withColumn("deleted_at", col("deleted_at").cast(TimestampType())))
    validate_tombstone_dataframe(order_items_clean_df)
    return (
        order_items_clean_df.dropna(
            subset=[
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
                "unit_price",
                "discount_amount",
            ]
        )
        .filter(col("quantity") > 0)
        .filter(col("unit_price") > 0)
        .filter(col("discount_amount") >= 0)
        .dropDuplicates(["order_item_id"])
    )
