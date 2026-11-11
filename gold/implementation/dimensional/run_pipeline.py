"""Executable adapter for the dimensional writer; DRY_RUN remains default."""

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession

from common.minio import configure_s3a, create_minio_client
from common.bounded_e2e_scope import BoundedE2EScopeError, build_bounded_e2e_scope
from gold.implementation.run_metadata import build_runtime_dimensional_metadata
from gold.implementation.run_metadata_repository import persist_if_enabled

try:
    from .writer import DRY_RUN, WRITE, run_dimensional_graph
except ImportError:  # executed by spark-submit as a standalone script
    sys.path.insert(0, "/opt/spark/work-dir")
    from gold.implementation.dimensional.writer import DRY_RUN, WRITE, run_dimensional_graph


def build_parser():
    parser = argparse.ArgumentParser(description="Run native dimensional Gold Iceberg graph from Silver Parquet")
    parser.add_argument("--source-lineage", required=True, help="Path to strict dimensional lineage JSON")
    parser.add_argument("--dim-date-start", required=True)
    parser.add_argument("--dim-date-end", required=True)
    parser.add_argument("--mode", choices=(DRY_RUN, WRITE), default=DRY_RUN)
    parser.add_argument("--bucket", required=True, help="Explicit MinIO bucket for manifests/evidence")
    parser.add_argument("--warehouse", help="Run-scoped Iceberg warehouse")
    parser.add_argument("--nessie-ref", help="Run-scoped Nessie ref")
    return parser


def load_lineage(path):
    try:
        if isinstance(path, str) and path.lstrip().startswith("{"):
            return json.loads(path)
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("--source-lineage must be a JSON document or readable path") from exc


def main(argv=None):
    args = build_parser().parse_args(argv)
    scope = None
    if args.bucket == "ecommerce-data-lake":
        try:
            scope = build_bounded_e2e_scope(os.environ)
        except BoundedE2EScopeError as exc:
            raise SystemExit(f"Refusing shared-bucket run: {exc}") from exc
        if args.warehouse and args.warehouse != scope.warehouse:
            raise SystemExit("--warehouse must match the immutable E2E scope")
        if args.nessie_ref and args.nessie_ref != scope.nessie_ref:
            raise SystemExit("--nessie-ref must match the immutable E2E scope")
    elif args.warehouse or args.nessie_ref:
        raise SystemExit("Scoped warehouse/ref require ecommerce-data-lake")
    lineage = load_lineage(args.source_lineage)
    spark = None
    started_at = datetime.now(timezone.utc)
    try:
        builder = SparkSession.builder.appName("GoldDimensionalIcebergWriter")
        if scope is not None:
            builder = (builder
                       .config("spark.sql.catalog.lakehouse.warehouse", scope.warehouse)
                       .config("spark.sql.catalog.lakehouse.ref", scope.nessie_ref))
        spark = builder.getOrCreate()
        configure_s3a(spark)
        result = run_dimensional_graph(
            spark, create_minio_client(), args.bucket, lineage,
            dim_date_start=args.dim_date_start, dim_date_end=args.dim_date_end, mode=args.mode, scope=scope,
        )
        if os.environ.get("GOLD_RUN_METADATA_PERSIST_ENABLED", "false").lower() == "true":
            metadata = build_runtime_dimensional_metadata(
                lineage=lineage, result=result, mode=args.mode, environment=os.environ, started_at=started_at,
            )
            result["run_metadata"] = persist_if_enabled(
                "dimensional_graph", metadata,
                connection_factory=lambda: __import__("ingestion.incremental.database", fromlist=["get_db_connection"]).get_db_connection(),
            )
        else:
            result["run_metadata"] = {"persisted": False, "reason": "GOLD_RUN_METADATA_PERSIST_ENABLED=false"}
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
