"""Load one verified Silver Parquet customers manifest into Iceberg."""

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a
from lakehouse.parquet_customers import migrate_all_manifests, migrate_manifest
from common.minio import create_minio_client


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--manifest-key")
    group.add_argument("--backfill", action="store_true")
    args = parser.parse_args()
    if os.getenv("LAKEHOUSE_WRITE_ENABLED", "false").lower() != "true":
        raise RuntimeError("Lakehouse write blocked: set LAKEHOUSE_WRITE_ENABLED=true for an explicit write")

    spark = SparkSession.builder.appName("BronzeToIcebergCustomers").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        configure_s3a(spark)
        client = create_minio_client()
        bucket = os.environ["BRONZE_BUCKET"]
        result = migrate_all_manifests(spark, client, bucket) if args.backfill else migrate_manifest(spark, client, bucket, args.manifest_key)
        print(json.dumps(result, sort_keys=True, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
