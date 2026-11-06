import argparse, os, sys
from minio import Minio
from pyspark.sql import SparkSession
sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, get_minio_settings
from lakehouse.migration.parquet_shipments import discover_manifests, migrate_manifest

parser = argparse.ArgumentParser(); parser.add_argument("--manifest-key"); parser.add_argument("--backfill", action="store_true"); args = parser.parse_args()
if os.getenv("LAKEHOUSE_WRITE_ENABLED", "false").lower() != "true": raise RuntimeError("LAKEHOUSE_WRITE_ENABLED=true is required")
settings = get_minio_settings(); bucket = os.environ["BRONZE_BUCKET"]; client = Minio(settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=False); spark = SparkSession.builder.appName("ParquetToIcebergShipments").getOrCreate()
try:
    configure_s3a(spark); manifests = discover_manifests(client, bucket); selected = [(key, item) for key, item in manifests if args.backfill or key == args.manifest_key]
    if not selected: raise ValueError("No selected completed shipments manifests")
    for key, item in selected: print(migrate_manifest(spark, client, bucket, key, item))
finally: spark.stop()
