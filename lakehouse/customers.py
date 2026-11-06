"""Canonical Iceberg Silver helpers for the customers entity."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


TABLE = "lakehouse.silver.customers"
KEY = "customer_id"
SOURCE_COLUMNS = ("customer_id", "full_name", "email", "created_at", "updated_at")


def prepare_customers(source_df, batch_id, ingested_at=None):
    """Add audit fields and retain the newest valid record for every key."""
    required = set(SOURCE_COLUMNS)
    missing = required - set(source_df.columns)
    if missing:
        raise ValueError(f"customers source missing columns: {sorted(missing)}")
    if not batch_id:
        raise ValueError("batch_id is required")
    loaded_at = F.current_timestamp() if ingested_at is None else F.lit(ingested_at).cast("timestamp")
    typed = source_df.select(
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("full_name").cast("string").alias("full_name"),
        F.col("email").cast("string").alias("email"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
    )
    record_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SOURCE_COLUMNS]), 256)
    staged = (typed.filter(F.col(KEY).isNotNull() & F.col("updated_at").isNotNull())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", loaded_at)
        .withColumn("_source_updated_at", F.col("updated_at").cast("timestamp"))
        .withColumn("_record_hash", record_hash))
    window = Window.partitionBy(KEY).orderBy(F.col("_source_updated_at").desc(), F.col("_record_hash").desc())
    return staged.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")


def ensure_table(spark, table=TABLE):
    namespace = ".".join(table.split(".")[:-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        customer_id BIGINT, full_name STRING, email STRING, created_at TIMESTAMP,
        updated_at TIMESTAMP, _batch_id STRING, _ingested_at TIMESTAMP,
        _source_updated_at TIMESTAMP, _record_hash STRING
    ) USING iceberg""")


def merge_customers(spark, prepared_df, table=TABLE):
    """Idempotently insert new keys and update only newer source records."""
    ensure_table(spark, table)
    prepared_df.createOrReplaceTempView("staged_customers")
    spark.sql(f"""MERGE INTO {table} target USING staged_customers source
      ON target.customer_id = source.customer_id
      WHEN MATCHED AND source._source_updated_at > target._source_updated_at THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")


def validate_customers(spark, table=TABLE):
    """Verify cardinality and business-key integrity after every merge."""
    frame = spark.table(table)
    row_count = frame.count()
    null_keys = frame.filter(F.col(KEY).isNull()).count()
    duplicate_keys = frame.groupBy(KEY).count().filter("count > 1").count()
    if null_keys or duplicate_keys:
        raise ValueError(f"customers quality gate failed: null_keys={null_keys}, duplicate_keys={duplicate_keys}")
    return {"row_count": row_count, "null_keys": null_keys, "duplicate_keys": duplicate_keys}


def latest_snapshot_id(spark, table=TABLE):
    """Return the latest Iceberg snapshot ID after a committed mutation."""
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    if not rows:
        raise ValueError("customers table has no Iceberg snapshot")
    return int(rows[0].snapshot_id)
