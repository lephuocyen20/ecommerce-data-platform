"""Cumulative business-state and customer-FK reconciliation for orders."""
import json, os, sys
from minio import Minio
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, get_minio_settings
from lakehouse.parquet_orders import discover_manifests

COLS = ["order_id", "customer_id", "status", "order_date", "updated_at"]
def hashes(frame): return frame.select("order_id", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in COLS]), 256).alias("business_hash"))
def main():
    settings = get_minio_settings(); bucket = os.environ["BRONZE_BUCKET"]; client = Minio(settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=False)
    manifests = discover_manifests(client, bucket)
    if not manifests: raise ValueError("No completed orders manifests found")
    spark = SparkSession.builder.appName("ReconcileIcebergOrders").getOrCreate()
    try:
        configure_s3a(spark); frames = [spark.read.parquet(f"s3a://{bucket}/{manifest['data_prefix']}") for _, manifest in manifests]; legacy = frames[0]
        for frame in frames[1:]: legacy = legacy.unionByName(frame)
        legacy = legacy.withColumn("_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in COLS]), 256)).withColumn("_rank", F.row_number().over(Window.partitionBy("order_id").orderBy(F.col("updated_at").cast("timestamp").desc(), F.col("_hash").desc()))).filter("_rank = 1").drop("_hash", "_rank")
        iceberg = spark.table("lakehouse.silver.orders"); customers = spark.table("lakehouse.silver.customers").select("customer_id").distinct()
        legacy_hashes, iceberg_hashes = hashes(legacy), hashes(iceberg); diff = legacy_hashes.exceptAll(iceberg_hashes).unionByName(iceberg_hashes.exceptAll(legacy_hashes)); mismatch = diff.count()
        legacy_fk = legacy.select("customer_id").distinct().join(customers, "customer_id", "left_anti").count(); iceberg_fk = iceberg.select("customer_id").distinct().join(customers, "customer_id", "left_anti").count()
        result = {"legacy_rows": legacy.count(), "iceberg_rows": iceberg.count(), "legacy_distinct_order_ids": legacy.select("order_id").distinct().count(), "iceberg_distinct_order_ids": iceberg.select("order_id").distinct().count(), "business_hash_mismatches": mismatch, "legacy_missing_customer_fks": legacy_fk, "iceberg_missing_customer_fks": iceberg_fk, "mismatch_samples": [row.asDict() for row in diff.limit(10).collect()], "manifest_count": len(manifests)}
        if result["legacy_rows"] != result["iceberg_rows"] or result["legacy_distinct_order_ids"] != result["iceberg_distinct_order_ids"] or mismatch or legacy_fk or iceberg_fk: raise RuntimeError(f"orders reconciliation failed: {result}")
        print(json.dumps(result, sort_keys=True))
    finally: spark.stop()
if __name__ == "__main__": main()
