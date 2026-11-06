"""Canonical Iceberg Silver model for orders."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

TABLE = "lakehouse.silver.orders"
KEY = "order_id"
SOURCE_COLUMNS = ("order_id", "customer_id", "status", "order_date", "updated_at")
VALID_STATUSES = ("PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED")


def prepare_orders(source_df, batch_id, ingested_at=None):
    missing = set(SOURCE_COLUMNS) - set(source_df.columns)
    if missing:
        raise ValueError(f"orders source missing columns: {sorted(missing)}")
    if not batch_id:
        raise ValueError("batch_id is required")
    typed = source_df.select(
        F.col("order_id").cast("bigint").alias("order_id"), F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("status").cast("string").alias("status"), F.col("order_date").cast("timestamp").alias("order_date"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
    ).filter(F.col(KEY).isNotNull() & F.col("customer_id").isNotNull() & F.col("order_date").isNotNull() & F.col("updated_at").isNotNull() & F.col("status").isin(*VALID_STATUSES))
    record_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SOURCE_COLUMNS]), 256)
    staged = typed.withColumn("_batch_id", F.lit(batch_id)).withColumn("_ingested_at", F.current_timestamp() if ingested_at is None else F.lit(ingested_at).cast("timestamp")).withColumn("_source_updated_at", F.col("updated_at")).withColumn("_record_hash", record_hash)
    return staged.withColumn("_rank", F.row_number().over(Window.partitionBy(KEY).orderBy(F.col("_source_updated_at").desc(), F.col("_record_hash").desc()))).filter("_rank = 1").drop("_rank")


def ensure_table(spark, table=TABLE):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {'.'.join(table.split('.')[:-1])}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        order_id BIGINT, customer_id BIGINT, status STRING, order_date TIMESTAMP, updated_at TIMESTAMP,
        _batch_id STRING, _ingested_at TIMESTAMP, _source_updated_at TIMESTAMP, _record_hash STRING
    ) USING iceberg""")


def merge_orders(spark, prepared_df, table=TABLE):
    ensure_table(spark, table); prepared_df.createOrReplaceTempView("lakehouse_staged_orders")
    spark.sql(f"""MERGE INTO {table} target USING lakehouse_staged_orders source ON target.order_id = source.order_id
    WHEN MATCHED AND source._source_updated_at > target._source_updated_at THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")


def validate_orders(spark, table=TABLE):
    frame = spark.table(table); rows = frame.count(); nulls = frame.filter("order_id IS NULL OR customer_id IS NULL").count(); duplicate = frame.groupBy("order_id").count().filter("count > 1").count(); invalid = frame.filter(~F.col("status").isin(*VALID_STATUSES)).count()
    if nulls or duplicate or invalid:
        raise ValueError(f"orders quality gate failed: null_keys={nulls}, duplicate_keys={duplicate}, invalid_statuses={invalid}")
    return {"row_count": rows, "null_keys": nulls, "duplicate_keys": duplicate, "invalid_statuses": invalid}


def latest_snapshot_id(spark, table=TABLE):
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    if not rows: raise ValueError("Orders Iceberg table has no snapshot")
    return int(rows[0].snapshot_id)
