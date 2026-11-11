"""Generic guarded Parquet-to-Iceberg migration entrypoint."""

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, create_minio_client
from common.bounded_e2e_scope import TEST_BUCKET, build_bounded_e2e_scope
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

    bucket = os.environ["BRONZE_BUCKET"]
    scope = build_bounded_e2e_scope(os.environ) if bucket == TEST_BUCKET else None
    builder = SparkSession.builder.appName(f"SilverParquetToIceberg{args.table_name.title().replace('_', '')}")
    if scope is not None:
        builder = builder.config("spark.sql.catalog.lakehouse.warehouse", scope.warehouse).config("spark.sql.catalog.lakehouse.ref", scope.nessie_ref)
    spark = builder.getOrCreate()
    try:
        configure_s3a(spark)
        client = create_minio_client()
        discover, migrate = migration_api(args.table_name, scope)
        result = ([migrate(spark, client, bucket, key, manifest) for key, manifest in discover(client, bucket)]
                  if args.backfill else migrate(spark, client, bucket, args.manifest_key))
        print(json.dumps(result, sort_keys=True, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
