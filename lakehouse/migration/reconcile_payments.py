"""Read-only business reconciliation for payments Parquet and Iceberg."""
import json, os, sys
from minio import Minio
from pyspark.sql import SparkSession, functions as F
sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, get_minio_settings
from lakehouse.migration.parquet_payments import discover_manifests

COLS = ("payment_id", "order_id", "amount", "payment_method", "status", "transaction_id", "created_at")
settings = get_minio_settings(); bucket = os.environ["BRONZE_BUCKET"]; client = Minio(settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=False); manifests = discover_manifests(client, bucket)
if not manifests: raise ValueError("No completed payments Silver manifests found")
spark = SparkSession.builder.appName("ReconcileIcebergPayments").getOrCreate()
try:
    configure_s3a(spark); frames = [spark.read.parquet(f"s3a://{bucket}/{manifest['data_prefix']}") for _, manifest in manifests]; legacy = frames[0]
    for frame in frames[1:]: legacy = legacy.unionByName(frame)
    iceberg = spark.table("lakehouse.silver.payments")
    hashed = lambda frame: frame.select("payment_id", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in COLS]), 256).alias("business_hash"))
    diff = hashed(legacy).exceptAll(hashed(iceberg)).unionByName(hashed(iceberg).exceptAll(hashed(legacy)))
    metrics = lambda frame: {"rows": frame.count(), "distinct_payment_ids": frame.select("payment_id").distinct().count(), "total_amount": str(frame.agg(F.sum("amount").alias("n")).first().n or 0), "status_totals": {row.status: str(row.total) for row in frame.groupBy("status").agg(F.sum("amount").alias("total")).collect()}}
    orders = spark.table("lakehouse.silver.orders").select("order_id").distinct(); result = {"legacy": metrics(legacy), "iceberg": metrics(iceberg), "business_hash_mismatches": diff.count(), "missing_order_fks": iceberg.select("order_id").distinct().join(orders, "order_id", "left_anti").count(), "manifest_count": len(manifests)}
    if result["legacy"] != result["iceberg"] or result["business_hash_mismatches"] or result["missing_order_fks"]: raise RuntimeError(f"payments reconciliation failed: {result}")
    print(json.dumps(result, sort_keys=True))
finally: spark.stop()
