from pyspark.sql.functions import col
from pyspark.sql.types import LongType, TimestampType


def transform_customers(df):
    """Cast customer columns and remove incomplete or duplicate records."""
    customers_clean_df = (
        df.withColumn("customer_id", col("customer_id").cast(LongType()))
        .withColumn("created_at", col("created_at").cast(TimestampType()))
        .withColumn("updated_at", col("updated_at").cast(TimestampType()))
    )
    return customers_clean_df.dropna(
        subset=["customer_id", "full_name", "email", "created_at", "updated_at"]
    ).dropDuplicates(["customer_id"])
