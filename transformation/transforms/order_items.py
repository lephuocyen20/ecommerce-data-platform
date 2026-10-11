from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, LongType, DecimalType


def transform_order_items(df):
    """Cast order item columns and remove incomplete, invalid, or duplicate records."""
    order_items_clean_df = (
        df.withColumn("order_item_id", col("order_item_id").cast(LongType()))
        .withColumn("order_id", col("order_id").cast(LongType()))
        .withColumn("product_id", col("product_id").cast(LongType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(12, 2)))
        .withColumn("discount_amount", col("discount_amount").cast(DecimalType(12, 2)))
    )
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
