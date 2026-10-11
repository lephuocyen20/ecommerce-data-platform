from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, IntegerType, LongType, TimestampType


def transform_products(df):
    """Cast product columns and remove incomplete, invalid, or duplicate records."""
    products_clean_df = (
        df.withColumn("product_id", col("product_id").cast(LongType()))
        .withColumn("stock_quantity", col("stock_quantity").cast(IntegerType()))
        .withColumn("price", col("price").cast(DecimalType(12, 2)))
        .withColumn("created_at", col("created_at").cast(TimestampType()))
        .withColumn("updated_at", col("updated_at").cast(TimestampType()))
    )
    return (
        products_clean_df.dropna(
            subset=[
                "product_id",
                "product_name",
                "stock_quantity",
                "price",
                "created_at",
                "updated_at",
            ]
        )
        .filter(col("stock_quantity") >= 0)
        .filter(col("price") > 0)
        .dropDuplicates(["product_id"])
    )
