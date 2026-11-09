"""Executable adapter for the dimensional writer; DRY_RUN remains default."""

import argparse
import json
import sys

from pyspark.sql import SparkSession

from common.minio import configure_s3a, create_minio_client

from .writer import DRY_RUN, WRITE, run_dimensional_graph


def build_parser():
    parser = argparse.ArgumentParser(description="Run native dimensional Iceberg Gold graph")
    parser.add_argument("--source-lineage", required=True, help="Path to strict dimensional lineage JSON")
    parser.add_argument("--dim-date-start", required=True)
    parser.add_argument("--dim-date-end", required=True)
    parser.add_argument("--mode", choices=(DRY_RUN, WRITE), default=DRY_RUN)
    parser.add_argument("--bucket", required=True, help="Explicit MinIO bucket for manifests/evidence")
    return parser


def load_lineage(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def main(argv=None):
    args = build_parser().parse_args(argv)
    lineage = load_lineage(args.source_lineage)
    spark = None
    try:
        spark = SparkSession.builder.appName("GoldDimensionalIcebergWriter").getOrCreate()
        configure_s3a(spark)
        result = run_dimensional_graph(
            spark, create_minio_client(), args.bucket, lineage,
            dim_date_start=args.dim_date_start, dim_date_end=args.dim_date_end, mode=args.mode,
        )
        print("GOLD_DIMENSIONAL_RESULT=" + json.dumps(result, sort_keys=True, default=str), flush=True)
        return result
    except Exception as exc:
        print(f"GOLD DIMENSIONAL FAILED: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
