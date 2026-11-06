"""Read-only cumulative reconciliation for Silver Parquet and Iceberg products."""
import json
import os
import sys

from minio import Minio
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, get_minio_settings
from lakehouse.migration.parquet_products import discover_manifests


def main():
    settings = get_minio_settings(); bucket = os.environ["BRONZE_BUCKET"]
    client = Minio(settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=False)
    manifests = discover_manifests(client, bucket)
    if not manifests: raise ValueError("No completed products manifests found")
    spark = SparkSession.builder.appName("ReconcileIcebergProducts").getOrCreate()
    try:
        configure_s3a(spark)
        frames = [spark.read.parquet(f"s3a://{bucket}/{manifest['data_prefix']}") for _, manifest in manifests]
        legacy = frames[0]
        for frame in frames[1:]: legacy = legacy.unionByName(frame)
        cols = ["product_id", "product_name", "stock_quantity", "price", "created_at", "updated_at"]
        hash_expr = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)
        legacy = legacy.withColumn("_hash", hash_expr).withColumn("_rank", F.row_number().over(Window.partitionBy("product_id").orderBy(F.col("updated_at").cast("timestamp").desc(), F.col("_hash").desc()))).filter("_rank=1").drop("_hash", "_rank")
        iceberg = spark.table("lakehouse.silver.products")
        def metrics(frame):
            hashes = frame.select("product_id", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256).alias("business_hash"))
            return frame.count(), frame.select("product_id").distinct().count(), hashes
        lrows, lids, lh = metrics(legacy); irows, iids, ih = metrics(iceberg)
        diff = lh.exceptAll(ih).unionByName(ih.exceptAll(lh)); mismatch = diff.count()
        result = {"legacy_rows": lrows, "iceberg_rows": irows, "legacy_distinct_product_ids": lids, "iceberg_distinct_product_ids": iids, "business_hash_mismatches": mismatch, "mismatch_samples": [row.asDict() for row in diff.limit(10).collect()], "manifest_count": len(manifests)}
        if lrows != irows or lids != iids or mismatch: raise RuntimeError(f"products reconciliation failed: {result}")
        print(json.dumps(result, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__": main()
