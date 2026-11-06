"""Migrate verified Silver Parquet orders manifests into Iceberg."""
import argparse, json, os, sys
from pyspark.sql import SparkSession
sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, create_minio_client
from lakehouse.migration.parquet_orders import migrate_all_manifests, migrate_manifest
def main():
    parser = argparse.ArgumentParser(); group = parser.add_mutually_exclusive_group(required=True); group.add_argument("--manifest-key"); group.add_argument("--backfill", action="store_true"); args = parser.parse_args()
    if os.getenv("LAKEHOUSE_WRITE_ENABLED", "false").lower() != "true": raise RuntimeError("Lakehouse write blocked: set LAKEHOUSE_WRITE_ENABLED=true")
    spark = SparkSession.builder.appName("SilverParquetToIcebergOrders").getOrCreate()
    try:
        configure_s3a(spark); client = create_minio_client(); result = migrate_all_manifests(spark, client, os.environ["BRONZE_BUCKET"]) if args.backfill else migrate_manifest(spark, client, os.environ["BRONZE_BUCKET"], args.manifest_key); print(json.dumps(result, sort_keys=True, default=str))
    finally: spark.stop()
if __name__ == "__main__": main()
