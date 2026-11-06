"""Read-only cumulative reconciliation for Parquet and Iceberg order items."""

import json
import os
import sys

from minio import Minio
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, get_minio_settings
from lakehouse.migration.parquet_order_items import discover

BUSINESS_COLUMNS = ("order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount")


def business_hashes(frame):
    return frame.select("order_item_id", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(column).cast("string"), F.lit("")) for column in BUSINESS_COLUMNS]), 256).alias("business_hash"))


def financial_metrics(frame):
    quantity = F.coalesce(F.col("quantity").cast("decimal(38, 8)"), F.lit(0))
    unit_price = F.coalesce(F.col("unit_price").cast("decimal(38, 8)"), F.lit(0))
    discount = F.coalesce(F.col("discount_amount").cast("decimal(38, 8)"), F.lit(0))
    row = frame.agg(
        F.count("*").alias("rows"), F.countDistinct("order_item_id").alias("distinct_order_item_ids"),
        F.sum(quantity).alias("total_quantity"), F.sum(quantity * unit_price).alias("gross_amount"),
        F.sum(discount).alias("discount_amount"), F.sum(quantity * unit_price - discount).alias("net_amount"),
    ).first()
    return {key: str(value) if value is not None else "0" for key, value in row.asDict().items()}


def main():
    settings = get_minio_settings()
    bucket = os.environ["BRONZE_BUCKET"]
    client = Minio(settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=False)
    manifests = discover(client, bucket)
    if not manifests:
        raise ValueError("No completed order_items Silver manifests found")
    spark = SparkSession.builder.appName("ReconcileIcebergOrderItems").getOrCreate()
    try:
        configure_s3a(spark)
        frames = [spark.read.parquet(f"s3a://{bucket}/{manifest['data_prefix']}") for _, manifest in manifests]
        legacy = frames[0]
        for frame in frames[1:]:
            legacy = legacy.unionByName(frame)
        iceberg = spark.table("lakehouse.silver.order_items")
        legacy_hashes, iceberg_hashes = business_hashes(legacy), business_hashes(iceberg)
        differences = legacy_hashes.exceptAll(iceberg_hashes).unionByName(iceberg_hashes.exceptAll(legacy_hashes))
        orders = spark.table("lakehouse.silver.orders").select("order_id").distinct()
        products = spark.table("lakehouse.silver.products").select("product_id").distinct()
        result = {
            "legacy": financial_metrics(legacy), "iceberg": financial_metrics(iceberg),
            "business_hash_mismatches": differences.count(),
            "missing_order_fks": iceberg.select("order_id").distinct().join(orders, "order_id", "left_anti").count(),
            "missing_product_fks": iceberg.select("product_id").distinct().join(products, "product_id", "left_anti").count(),
            "mismatch_samples": [row.asDict() for row in differences.limit(10).collect()], "manifest_count": len(manifests),
        }
        if result["legacy"] != result["iceberg"] or result["business_hash_mismatches"] or result["missing_order_fks"] or result["missing_product_fks"]:
            raise RuntimeError(f"order_items reconciliation failed: {result}")
        print(json.dumps(result, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
