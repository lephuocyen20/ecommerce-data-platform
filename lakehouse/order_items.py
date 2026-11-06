"""Canonical Iceberg Silver model for order items.

Order items are immutable line facts in the source contract; an incoming row
with the same primary key is accepted only when its deterministic business hash
matches. Conflicting replays are rejected instead of silently overwriting a
financial line amount.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TABLE = "lakehouse.silver.order_items"
KEY = "order_item_id"
SOURCE_COLUMNS = ("order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount")

def prepare_order_items(source_df, batch_id, ingested_at=None):
    missing = set(SOURCE_COLUMNS) - set(source_df.columns)
    if missing: raise ValueError(f"order_items source missing columns: {sorted(missing)}")
    typed = source_df.select(F.col("order_item_id").cast("bigint").alias("order_item_id"), F.col("order_id").cast("bigint").alias("order_id"), F.col("product_id").cast("bigint").alias("product_id"), F.col("quantity").cast("int").alias("quantity"), F.col("unit_price").cast("decimal(12,2)").alias("unit_price"), F.col("discount_amount").cast("decimal(12,2)").alias("discount_amount"))
    typed = typed.filter(F.col(KEY).isNotNull() & F.col("order_id").isNotNull() & F.col("product_id").isNotNull() & (F.col("quantity") > 0) & (F.col("unit_price") >= 0) & (F.col("discount_amount") >= 0))
    record_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SOURCE_COLUMNS]), 256)
    staged = typed.withColumn("_batch_id", F.lit(batch_id)).withColumn("_ingested_at", F.current_timestamp() if ingested_at is None else F.lit(ingested_at).cast("timestamp")).withColumn("_record_hash", record_hash)
    return staged.withColumn("_rank", F.row_number().over(Window.partitionBy(KEY).orderBy(F.col("_record_hash").desc()))).filter("_rank=1").drop("_rank")

def ensure_table(spark, table=TABLE):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {'.'.join(table.split('.')[:-1])}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
      order_item_id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT,
      unit_price DECIMAL(12,2), discount_amount DECIMAL(12,2), _batch_id STRING,
      _ingested_at TIMESTAMP, _record_hash STRING
    ) USING iceberg""")

def merge_order_items(spark, prepared_df, table=TABLE):
    ensure_table(spark, table); prepared_df.createOrReplaceTempView("lakehouse_staged_order_items")
    conflict = spark.sql(f"SELECT count(*) AS n FROM lakehouse_staged_order_items s JOIN {table} t ON s.order_item_id=t.order_item_id WHERE s._record_hash <> t._record_hash").first().n
    if conflict: raise ValueError(f"Order-item replay conflicts with immutable business state: {conflict}")
    spark.sql(f"MERGE INTO {table} t USING lakehouse_staged_order_items s ON t.order_item_id=s.order_item_id WHEN NOT MATCHED THEN INSERT *")
