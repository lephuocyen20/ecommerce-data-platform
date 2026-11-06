"""Canonical Iceberg Silver model for shipments."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TABLE = "lakehouse.silver.shipments"
KEY = "shipment_id"
SOURCE_COLUMNS = ("shipment_id", "order_id", "carrier", "tracking_number", "shipping_address", "shipping_fee", "status", "shipped_at", "delivered_at", "estimated_delivery_at", "created_at", "updated_at")
VALID_STATUSES = ("PENDING", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "FAILED", "RETURNED")


def prepare_shipments(source_df, batch_id, ingested_at=None):
    missing = set(SOURCE_COLUMNS) - set(source_df.columns)
    if missing or not batch_id:
        raise ValueError(f"shipments source missing columns: {sorted(missing)}" if missing else "batch_id is required")
    timestamps = {"shipped_at", "delivered_at", "estimated_delivery_at", "created_at", "updated_at"}
    typed = source_df.select(*[F.col(c).cast("bigint" if c in ("shipment_id", "order_id") else "decimal(12,2)" if c == "shipping_fee" else "timestamp" if c in timestamps else "string").alias(c) for c in SOURCE_COLUMNS])
    typed = typed.filter(F.col(KEY).isNotNull() & F.col("order_id").isNotNull() & F.col("shipping_fee").isNotNull() & (F.col("shipping_fee") >= 0) & F.col("created_at").isNotNull() & F.col("updated_at").isNotNull() & F.col("status").isin(*VALID_STATUSES))
    record_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SOURCE_COLUMNS]), 256)
    staged = typed.withColumn("_batch_id", F.lit(batch_id)).withColumn("_ingested_at", F.current_timestamp() if ingested_at is None else F.lit(ingested_at).cast("timestamp")).withColumn("_source_updated_at", F.col("updated_at")).withColumn("_record_hash", record_hash)
    return staged.withColumn("_rank", F.row_number().over(Window.partitionBy(KEY).orderBy(F.col("_source_updated_at").desc(), F.col("_record_hash").desc()))).filter("_rank = 1").drop("_rank")


def ensure_table(spark, table=TABLE):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        shipment_id BIGINT, order_id BIGINT, carrier STRING, tracking_number STRING, shipping_address STRING, shipping_fee DECIMAL(12,2), status STRING, shipped_at TIMESTAMP, delivered_at TIMESTAMP, estimated_delivery_at TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP,
        _batch_id STRING, _ingested_at TIMESTAMP, _source_updated_at TIMESTAMP, _record_hash STRING
    ) USING iceberg""")


def merge_shipments(spark, prepared_df, table=TABLE):
    ensure_table(spark, table); prepared_df.createOrReplaceTempView("lakehouse_staged_shipments")
    spark.sql(f"""MERGE INTO {table} target USING lakehouse_staged_shipments source ON target.shipment_id = source.shipment_id
    WHEN MATCHED AND source._source_updated_at > target._source_updated_at THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")


def validate_shipments(spark, table=TABLE):
    frame = spark.table(table); rows = frame.count(); nulls = frame.filter("shipment_id IS NULL OR order_id IS NULL").count(); duplicates = frame.groupBy("shipment_id").count().filter("count > 1").count(); invalid = frame.filter((F.col("shipping_fee") < 0) | ~F.col("status").isin(*VALID_STATUSES)).count()
    if nulls or duplicates or invalid: raise ValueError(f"shipments quality gate failed: null_keys={nulls}, duplicate_keys={duplicates}, invalid={invalid}")
    return {"row_count": rows, "null_keys": nulls, "duplicate_keys": duplicates, "invalid_records": invalid}
