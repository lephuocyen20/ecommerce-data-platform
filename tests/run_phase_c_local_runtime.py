"""Local-only Phase C runtime proof planner (DRY_RUN only)."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
from gold.implementation.dimensional.manifest import build_graph_manifest

LOCAL_BUCKET = "ecommerce-quality-it"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "nessie", "quality-it-nessie", "host.docker.internal"}


class PhaseCLocalRuntimeError(ValueError):
    pass


def validate_local_scope(bucket, endpoint, ref):
    if bucket != LOCAL_BUCKET:
        raise PhaseCLocalRuntimeError("Phase C runner requires ecommerce-quality-it bucket")
    parsed = urlparse(endpoint or "")
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in LOCAL_HOSTS or not parsed.path.endswith("/api/v2"):
        raise PhaseCLocalRuntimeError("Phase C runner requires a local quality-it Nessie endpoint")
    if ref != "main":
        raise PhaseCLocalRuntimeError("Phase C runner requires the scoped main ref")


def run_phase_c_dry_run(*, bucket, endpoint, ref, schema_before, schema_after, physical_delete=False):
    validate_local_scope(bucket, endpoint, ref)
    if schema_before != schema_after:
        raise PhaseCLocalRuntimeError("Schema drift is rejected; explicit contract update is required")
    if physical_delete is not False:
        raise PhaseCLocalRuntimeError("Tombstone-only policy rejects physical delete intent")
    return {"status": "DRY_RUN", "bucket": bucket, "endpoint": endpoint, "ref": ref,
            "late_correction": "OVERLAP_REPROCESS_PLANNED", "schema": "UNCHANGED",
            "tombstone_policy": "TOMBSTONE_ONLY", "can_write": False}


def _lineage(snapshot_offset):
    sources = {}
    for index, table in enumerate(DIMENSIONAL_SOURCE_TABLES, start=1):
        snapshot_id = index + snapshot_offset
        sources[table] = {
            "source_type": "iceberg_snapshot", "table": table, "catalog": "lakehouse",
            "namespace": "silver", "ref": "main", "snapshot_id": snapshot_id,
            "snapshot_attestation_identity": f"{snapshot_id:064x}",
            "reconciliation_evidence": {"identity": f"{snapshot_id + 10:064x}", "status": "PASS", "snapshot_id": snapshot_id, "table": table},
            "quality_evidence": {"identity": f"{snapshot_id + 20:064x}", "status": "PASS", "snapshot_id": snapshot_id, "table": table},
        }
    return {"version": 1, "source_type": "iceberg_snapshot", "sources": sources}


def run_phase_c_spark_dry_run(spark, *, bucket, endpoint, ref, physical_delete=False):
    """Run a bounded late-correction scenario with Spark transformations only."""
    validate_local_scope(bucket, endpoint, ref)
    if physical_delete is not False:
        raise PhaseCLocalRuntimeError("Tombstone-only policy rejects physical delete intent")
    schema = "order_id long, status string, updated_at timestamp"
    before = spark.createDataFrame([(1, "PENDING", datetime(2026, 1, 1))], schema)
    after = spark.createDataFrame([(1, "DELIVERED", datetime(2026, 1, 3))], schema)
    if before.schema != after.schema:
        raise PhaseCLocalRuntimeError("Schema drift is rejected; explicit contract update is required")
    changed = before.alias("before").join(after.alias("after"), "order_id").where("before.status <> after.status")
    if changed.count() != 1:
        raise PhaseCLocalRuntimeError("Late correction was not detected")
    before_graph = build_graph_manifest(_lineage(0), dim_date_start="2026-01-01", dim_date_end="2026-01-03")
    after_graph = build_graph_manifest(_lineage(4), dim_date_start="2026-01-01", dim_date_end="2026-01-03")
    if before_graph["identity"] == after_graph["identity"]:
        raise PhaseCLocalRuntimeError("Late correction must use a distinct pinned lineage")
    return {"status": "DRY_RUN", "late_correction": "DETECTED", "before_lineage": before_graph["identity"],
            "after_lineage": after_graph["identity"], "after_snapshot_offset": 4, "can_write": False}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=LOCAL_BUCKET)
    parser.add_argument("--endpoint", default="http://localhost:19130/api/v2")
    parser.add_argument("--ref", default="main")
    parser.add_argument("--schema-before", required=True)
    parser.add_argument("--schema-after", required=True)
    parser.add_argument("--physical-delete", action="store_true")
    args = parser.parse_args(argv)
    print(json.dumps(run_phase_c_dry_run(**vars(args)), sort_keys=True))


if __name__ == "__main__":
    main()
