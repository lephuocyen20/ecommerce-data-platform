"""Read-only business reconciliation for shipments Parquet and Iceberg."""
import json, os, sys
from minio import Minio
from pyspark.sql import SparkSession, functions as F
sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, get_minio_settings
from lakehouse.migration.manifest import discover_manifests

COLS = ("shipment_id", "order_id", "carrier", "tracking_number", "shipping_address", "shipping_fee", "status", "shipped_at", "delivered_at", "estimated_delivery_at", "created_at", "updated_at")
settings = get_minio_settings(); bucket = os.environ["BRONZE_BUCKET"]; client = Minio(settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=False); manifests = discover_manifests(client, bucket, "shipments")
if not manifests: raise ValueError("No completed shipments Silver manifests found")
spark = SparkSession.builder.appName("ReconcileIcebergShipments").getOrCreate()
try:
    configure_s3a(spark); frames = [spark.read.parquet(f"s3a://{bucket}/{manifest['data_prefix']}") for _, manifest in manifests]; legacy = frames[0]
    for frame in frames[1:]: legacy = legacy.unionByName(frame)
    iceberg = spark.table("lakehouse.silver.shipments")
    hashed = lambda frame: frame.select("shipment_id", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in COLS]), 256).alias("business_hash"))
    diff = hashed(legacy).exceptAll(hashed(iceberg)).unionByName(hashed(iceberg).exceptAll(hashed(legacy)))
    metrics = lambda frame: {"rows": frame.count(), "distinct_shipment_ids": frame.select("shipment_id").distinct().count(), "total_shipping_fee": str(frame.agg(F.sum("shipping_fee").alias("n")).first().n or 0), "status_totals": {row.status: str(row.total) for row in frame.groupBy("status").agg(F.sum("shipping_fee").alias("total")).collect()}}
    orders = spark.table("lakehouse.silver.orders").select("order_id").distinct(); result = {"legacy": metrics(legacy), "iceberg": metrics(iceberg), "business_hash_mismatches": diff.count(), "missing_order_fks": iceberg.select("order_id").distinct().join(orders, "order_id", "left_anti").count(), "manifest_count": len(manifests)}
    if result["legacy"] != result["iceberg"] or result["business_hash_mismatches"] or result["missing_order_fks"]: raise RuntimeError(f"shipments reconciliation failed: {result}")
    print(json.dumps(result, sort_keys=True))
finally: spark.stop()
