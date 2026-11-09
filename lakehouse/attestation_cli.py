"""Publish one cumulative Iceberg snapshot attestation for Airflow."""

import argparse
import json
import os

from pyspark.sql import SparkSession

from common.minio import configure_s3a, create_minio_client
from lakehouse.migration.manifest import _identity, publish_snapshot_attestation_for_progress, read_manifest
from lakehouse.models.spec import SPECS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-name", required=True, choices=tuple(SPECS))
    parser.add_argument("--manifest-key", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName(f"AttestIceberg{args.table_name.title().replace('_', '')}").getOrCreate()
    try:
        configure_s3a(spark)
        manifest = read_manifest(create_minio_client(), os.environ["BRONZE_BUCKET"], args.manifest_key, args.table_name)
        identity = _identity(manifest).replace("'", "''")
        progress = f"lakehouse.metadata.{args.table_name}_parquet_migration_progress"
        row = spark.sql(f"SELECT snapshot_id FROM {progress} WHERE manifest_identity = '{identity}'").first()
        if row is None:
            raise RuntimeError("Migration progress is missing for attestation")
        result = publish_snapshot_attestation_for_progress(
            spark, create_minio_client(), os.environ["BRONZE_BUCKET"],
            args.table_name, int(row.snapshot_id),
        )
        print(json.dumps(result, sort_keys=True, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
