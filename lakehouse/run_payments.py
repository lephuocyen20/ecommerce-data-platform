import argparse, os, sys
from pyspark.sql import SparkSession
sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, create_minio_client
from lakehouse.parquet_payments import discover_manifests, migrate_manifest

parser = argparse.ArgumentParser(); parser.add_argument("--backfill", action="store_true"); parser.add_argument("--manifest-key"); args = parser.parse_args()
if os.getenv("LAKEHOUSE_WRITE_ENABLED", "false").lower() != "true": raise RuntimeError("Lakehouse write blocked")
spark = SparkSession.builder.appName("SilverParquetToIcebergPayments").getOrCreate()
try:
    configure_s3a(spark); client = create_minio_client(); bucket = os.environ["BRONZE_BUCKET"]
    print([migrate_manifest(spark, client, bucket, key, manifest) for key, manifest in discover_manifests(client, bucket)] if args.backfill else migrate_manifest(spark, client, bucket, args.manifest_key))
finally: spark.stop()
