"""Canonical Iceberg Silver model for products."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


TABLE = "lakehouse.silver.products"
KEY = "product_id"
SOURCE_COLUMNS = ("product_id", "product_name", "stock_quantity", "price", "created_at", "updated_at")


def prepare_products(source_df, batch_id, ingested_at=None):
    missing = set(SOURCE_COLUMNS) - set(source_df.columns)
    if missing:
        raise ValueError(f"products source missing columns: {sorted(missing)}")
    if not batch_id:
        raise ValueError("batch_id is required")
    typed = source_df.select(
        F.col("product_id").cast("bigint").alias("product_id"),
        F.col("product_name").cast("string").alias("product_name"),
        F.col("stock_quantity").cast("int").alias("stock_quantity"),
        F.col("price").cast("decimal(12,2)").alias("price"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
    ).filter(F.col(KEY).isNotNull() & F.col("updated_at").isNotNull() & (F.col("stock_quantity") >= 0) & (F.col("price") > 0))
    record_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SOURCE_COLUMNS]), 256)
    staged = typed.withColumn("_batch_id", F.lit(batch_id)).withColumn("_ingested_at", F.current_timestamp() if ingested_at is None else F.lit(ingested_at).cast("timestamp")).withColumn("_source_updated_at", F.col("updated_at")).withColumn("_record_hash", record_hash)
    return staged.withColumn("_rank", F.row_number().over(Window.partitionBy(KEY).orderBy(F.col("_source_updated_at").desc(), F.col("_record_hash").desc()))).filter("_rank = 1").drop("_rank")


def ensure_table(spark, table=TABLE):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {'.'.join(table.split('.')[:-1])}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        product_id BIGINT, product_name STRING, stock_quantity INT, price DECIMAL(12,2), created_at TIMESTAMP, updated_at TIMESTAMP,
        _batch_id STRING, _ingested_at TIMESTAMP, _source_updated_at TIMESTAMP, _record_hash STRING
    ) USING iceberg""")


def merge_products(spark, prepared_df, table=TABLE):
    ensure_table(spark, table); prepared_df.createOrReplaceTempView("lakehouse_staged_products")
    spark.sql(f"""MERGE INTO {table} target USING lakehouse_staged_products source ON target.product_id = source.product_id
    WHEN MATCHED AND source._source_updated_at > target._source_updated_at THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
