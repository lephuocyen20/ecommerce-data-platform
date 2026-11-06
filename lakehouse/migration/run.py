"""Generic guarded Parquet-to-Iceberg migration entrypoint."""

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, create_minio_client
from lakehouse.migration.registry import TABLES, migration_api


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-name", choices=TABLES, required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--manifest-key")
    group.add_argument("--backfill", action="store_true")
    args = parser.parse_args()
    if os.getenv("LAKEHOUSE_WRITE_ENABLED", "false").lower() != "true":
        raise RuntimeError("Lakehouse write blocked: set LAKEHOUSE_WRITE_ENABLED=true")

    spark = SparkSession.builder.appName(f"SilverParquetToIceberg{args.table_name.title().replace('_', '')}").getOrCreate()
    try:
        configure_s3a(spark)
        client, bucket = create_minio_client(), os.environ["BRONZE_BUCKET"]
        discover, migrate = migration_api(args.table_name)
        result = ([migrate(spark, client, bucket, key, manifest) for key, manifest in discover(client, bucket)]
                  if args.backfill else migrate(spark, client, bucket, args.manifest_key))
        print(json.dumps(result, sort_keys=True, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
