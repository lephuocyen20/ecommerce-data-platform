"""Canonical Iceberg Silver model for payments."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

TABLE = "lakehouse.silver.payments"
KEY = "payment_id"
SOURCE_COLUMNS = ("payment_id", "order_id", "amount", "payment_method", "status", "transaction_id", "created_at")
VALID_STATUSES = ("PENDING", "SUCCESS", "FAILED", "REFUNDED")


def prepare_payments(source_df, batch_id, ingested_at=None):
    missing = set(SOURCE_COLUMNS) - set(source_df.columns)
    if missing:
        raise ValueError(f"payments source missing columns: {sorted(missing)}")
    if not batch_id:
        raise ValueError("batch_id is required")
    typed = source_df.select(*[F.col(c).cast("bigint" if c in ("payment_id", "order_id") else "decimal(12,2)" if c == "amount" else "timestamp" if c == "created_at" else "string").alias(c) for c in SOURCE_COLUMNS])
    typed = typed.filter(F.col(KEY).isNotNull() & F.col("order_id").isNotNull() & F.col("amount").isNotNull() & (F.col("amount") > 0) & F.col("created_at").isNotNull() & F.col("status").isin(*VALID_STATUSES))
    record_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SOURCE_COLUMNS]), 256)
    staged = typed.withColumn("_batch_id", F.lit(batch_id)).withColumn("_ingested_at", F.current_timestamp() if ingested_at is None else F.lit(ingested_at).cast("timestamp")).withColumn("_source_created_at", F.col("created_at")).withColumn("_record_hash", record_hash)
    return staged.withColumn("_rank", F.row_number().over(Window.partitionBy(KEY).orderBy(F.col("_source_created_at").desc(), F.col("_record_hash").desc()))).filter("_rank = 1").drop("_rank")


def ensure_table(spark, table=TABLE):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        payment_id BIGINT, order_id BIGINT, amount DECIMAL(12,2), payment_method STRING, status STRING, transaction_id STRING, created_at TIMESTAMP,
        _batch_id STRING, _ingested_at TIMESTAMP, _source_created_at TIMESTAMP, _record_hash STRING
    ) USING iceberg""")


def merge_payments(spark, prepared_df, table=TABLE):
    ensure_table(spark, table); prepared_df.createOrReplaceTempView("lakehouse_staged_payments")
    spark.sql(f"""MERGE INTO {table} target USING lakehouse_staged_payments source ON target.payment_id = source.payment_id
    WHEN MATCHED AND source._source_created_at > target._source_created_at THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")


def validate_payments(spark, table=TABLE):
    frame = spark.table(table); rows = frame.count(); nulls = frame.filter("payment_id IS NULL OR order_id IS NULL").count(); duplicates = frame.groupBy("payment_id").count().filter("count > 1").count(); invalid = frame.filter((F.col("amount") <= 0) | ~F.col("status").isin(*VALID_STATUSES)).count()
    if nulls or duplicates or invalid: raise ValueError(f"payments quality gate failed: null_keys={nulls}, duplicate_keys={duplicates}, invalid={invalid}")
    return {"row_count": rows, "null_keys": nulls, "duplicate_keys": duplicates, "invalid_records": invalid}
