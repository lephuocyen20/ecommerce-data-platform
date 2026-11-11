"""Run local Gold dimensional WRITE against persisted local fixture lineage."""
from __future__ import annotations
import argparse, json, os, sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0, str(PROJECT_ROOT))
from pyspark.sql import SparkSession
from common.minio import configure_s3a, create_minio_client
from common.bounded_e2e_scope import BoundedE2EScopeError, build_bounded_e2e_scope
from gold.implementation.dimensional.writer import WRITE, run_dimensional_graph
from gold.implementation.dimensional.contracts import WRITE_ORDER
from gold.implementation.dimensional.lineage import validate_dimensional_lineage
LOCAL_BUCKET = "ecommerce-quality-it"

def inspect_local_graph(lineage_path: Path, bucket: str):
    """Read-only preflight for local lineage and expected seven outputs."""
    if bucket != LOCAL_BUCKET:
        raise ValueError(f"Refusing bucket {bucket!r}; only {LOCAL_BUCKET!r} is allowed")
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
    validate_dimensional_lineage(lineage)
    return {"status": "INSPECT_ONLY", "source_tables": sorted(lineage["sources"]), "target_tables": list(WRITE_ORDER)}
def run_local_write(lineage_path: Path, bucket: str, *, dim_date_start: str, dim_date_end: str):
    scope = None
    if bucket == "ecommerce-data-lake":
        # This check intentionally precedes Spark and MinIO client creation.
        try:
            scope = build_bounded_e2e_scope(os.environ)
        except BoundedE2EScopeError as exc:
            raise ValueError(f"Refusing bucket {bucket!r}; only {LOCAL_BUCKET!r} or an approved bounded scope is allowed: {exc}") from exc
    elif bucket != LOCAL_BUCKET:
        raise ValueError(f"Refusing bucket {bucket!r}; only {LOCAL_BUCKET!r} or a bounded E2E bucket is allowed")
    if os.getenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "false").lower() != "true": raise RuntimeError("Local WRITE requires GOLD_DIMENSIONAL_WRITE_ENABLED=true")
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
    if lineage.get("source_type") != "iceberg_snapshot": raise ValueError("Lineage must describe Iceberg snapshots")
    spark = SparkSession.builder.appName("GoldDimensionalLocalWrite").getOrCreate()
    try:
        configure_s3a(spark)
        return run_dimensional_graph(spark, create_minio_client(), bucket, lineage, dim_date_start=dim_date_start, dim_date_end=dim_date_end, mode=WRITE, scope=scope)
    finally: spark.stop()
def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("--bucket", default=LOCAL_BUCKET); p.add_argument("--lineage", type=Path, default=Path("/tmp/gold-local-lineage.json")); p.add_argument("--dim-date-start", default="2026-01-01"); p.add_argument("--dim-date-end", default="2026-01-07"); a = p.parse_args(argv)
    result = run_local_write(a.lineage, a.bucket, dim_date_start=a.dim_date_start, dim_date_end=a.dim_date_end); print("GOLD_LOCAL_WRITE_RESULT=" + json.dumps(result, sort_keys=True, default=str), flush=True); return result
if __name__ == "__main__": main()
