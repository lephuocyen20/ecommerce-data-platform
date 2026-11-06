"""Read-only reconciliation of legacy Silver Parquet and Iceberg customers."""

import argparse
import json
import sys

from minio import Minio
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, get_minio_settings
from lakehouse.migration.parquet_customers import discover_manifests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-key", required=False, help="validated current manifest; retained for task lineage")
    args = parser.parse_args()
    settings = get_minio_settings()
    import os
    bucket = os.environ["BRONZE_BUCKET"]
    client = Minio(settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=False)
    manifests = discover_manifests(client, bucket)
    if not manifests:
        raise ValueError("No completed customers Silver manifests found")
    spark = SparkSession.builder.appName("ReconcileIcebergCustomers").getOrCreate()
    try:
        configure_s3a(spark)
        legacy_batches = [spark.read.parquet(f"s3a://{bucket}/{manifest['data_prefix']}") for _, manifest in manifests]
        legacy = legacy_batches[0]
        for frame in legacy_batches[1:]:
            legacy = legacy.unionByName(frame)
        hash_expr = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in ["customer_id", "full_name", "email", "created_at", "updated_at"]]), 256)
        legacy = legacy.withColumn("_hash", hash_expr)
        legacy = legacy.withColumn("_rank", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.col("updated_at").cast("timestamp").desc(), F.col("_hash").desc()))).filter("_rank = 1").drop("_rank", "_hash")
        iceberg = spark.table("lakehouse.silver.customers")
        cols = ["customer_id", "full_name", "email", "created_at", "updated_at"]
        def metrics(frame):
            hashed = frame.select("customer_id", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256).alias("business_hash"))
            return {"rows": frame.count(), "distinct_customer_ids": frame.select("customer_id").distinct().count(), "hashes": hashed}
        legacy_metrics, iceberg_metrics = metrics(legacy), metrics(iceberg)
        mismatch = legacy_metrics["hashes"].exceptAll(iceberg_metrics["hashes"]).count() + iceberg_metrics["hashes"].exceptAll(legacy_metrics["hashes"]).count()
        samples = legacy_metrics["hashes"].exceptAll(iceberg_metrics["hashes"]).unionByName(iceberg_metrics["hashes"].exceptAll(legacy_metrics["hashes"])).limit(10).collect()
        result = {"legacy_rows": legacy_metrics["rows"], "iceberg_rows": iceberg_metrics["rows"], "legacy_distinct_customer_ids": legacy_metrics["distinct_customer_ids"], "iceberg_distinct_customer_ids": iceberg_metrics["distinct_customer_ids"], "business_hash_mismatches": mismatch, "mismatch_samples": [row.asDict() for row in samples], "manifest_count": len(manifests)}
        if result["legacy_rows"] != result["iceberg_rows"] or result["legacy_distinct_customer_ids"] != result["iceberg_distinct_customer_ids"] or mismatch:
            raise RuntimeError(f"customers dual-run reconciliation failed: {result}")
        print(json.dumps(result, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
