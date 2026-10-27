"""CLI entry point for building one immutable Silver snapshot."""

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession

from ingestion.incremental.database import get_db_connection
from silver.snapshots.pipeline import run_snapshot_pipeline
from common.minio import configure_s3a, create_minio_client


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-name", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = None
    conn = None

    try:
        conn = get_db_connection()
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName(f"SilverSnapshot_{args.table_name}")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        configure_s3a(spark)

        client = create_minio_client()
        result = run_snapshot_pipeline(
            conn=conn,
            spark=spark,
            client=client,
            bucket=os.environ["BRONZE_BUCKET"],
            table_name=args.table_name,
        )
        print("SILVER_SNAPSHOT_RESULT=" + json.dumps(result, sort_keys=True), flush=True)
    except Exception as exc:
        print(
            f"SILVER SNAPSHOT FAILED: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        raise
    finally:
        if conn is not None:
            conn.close()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
