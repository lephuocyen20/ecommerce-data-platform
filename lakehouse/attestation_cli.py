"""Publish one cumulative Iceberg snapshot attestation for Airflow."""

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession

from common.minio import configure_s3a, create_minio_client
from common.bounded_e2e_scope import TEST_BUCKET, build_bounded_e2e_scope
from lakehouse.migration.manifest import _identity, publish_snapshot_attestation_for_progress, read_manifest
from lakehouse.models.spec import SPECS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-name", required=True, choices=tuple(SPECS))
    parser.add_argument("--manifest-key", required=True)
    args = parser.parse_args()
    bucket = os.environ["BRONZE_BUCKET"]
    scope = build_bounded_e2e_scope(os.environ) if bucket == TEST_BUCKET else None
    builder = SparkSession.builder.appName(f"AttestIceberg{args.table_name.title().replace('_', '')}")
    if scope is not None:
        builder = builder.config("spark.sql.catalog.lakehouse.warehouse", scope.warehouse).config("spark.sql.catalog.lakehouse.ref", scope.nessie_ref)
    spark = builder.getOrCreate()
    result = None
    try:
        configure_s3a(spark)
        manifest = read_manifest(create_minio_client(), bucket, args.manifest_key, args.table_name, scope)
        identity = _identity(manifest).replace("'", "''")
        progress = f"lakehouse.metadata.{args.table_name}_parquet_migration_progress"
        row = spark.sql(f"SELECT snapshot_id FROM {progress} WHERE manifest_identity = '{identity}'").first()
        if row is None:
            raise RuntimeError("Migration progress is missing for attestation")
        result = publish_snapshot_attestation_for_progress(
            spark, create_minio_client(), os.environ["BRONZE_BUCKET"],
            args.table_name, int(row.snapshot_id),
            namespace=scope.silver_namespace if scope is not None else "silver",
            ref=scope.nessie_ref if scope is not None else "main",
            scope=scope,
        )
    finally:
        spark.stop()
    # Emit the machine-readable payload after Spark has stopped so DockerOperator
    # XCom captures the attestation as the final stdout line (shutdown logs must
    # not replace the contract payload).
    # ``spark-submit`` installs JVM shutdown hooks that can emit log lines
    # after Python returns from ``main``.  DockerOperator's XCom contract is
    # the final stdout payload, so a normal interpreter exit can overwrite
    # this JSON with shutdown noise.  Flush the attestation and terminate the
    # short-lived CLI before those hooks can write to the inherited stream.
    sys.stdout.write(json.dumps(result, sort_keys=True, default=str) + "\n")
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
