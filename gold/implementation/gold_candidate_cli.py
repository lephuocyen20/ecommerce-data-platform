"""Bounded production Gold candidate writer entrypoint.

All JSON contracts and target checks happen before Spark, MinIO or Nessie
clients are constructed.  This CLI never publishes or advances the stable ref.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.minio import configure_s3a, create_minio_client
from common.storage_layout import PRODUCTION_BUCKET, validate_storage_environment
from gold.implementation.bi_ref_promotion import NessieRestAdapter
from gold.implementation.gold_candidate_artifacts import (
    SILVER_TABLES, build_candidate_artifact_bundle,
)
from gold.implementation.gold_candidate_contract import (
    GoldCandidatePlan, validate_candidate_environment, validate_fact_and_dimension_bundle,
)
from gold.implementation.gold_candidate_orchestrator import run_gold_candidate
from gold.implementation.dimensional.lineage import build_dimensional_lineage_from_artifacts


@dataclass(frozen=True)
class CandidateScope:
    bucket: str
    candidate_ref: str
    target_namespace: str
    warehouse: str

    @property
    def nessie_ref(self):
        return self.candidate_ref

    @property
    def gold_namespace(self):
        return self.target_namespace

    @property
    def silver_namespace(self):
        return "silver"

    @property
    def prefix(self):
        return ""

    def key(self, suffix):
        """Return a validated object key for the candidate manifest namespace."""
        if not isinstance(suffix, str) or suffix.startswith("/") or ".." in suffix.split("/"):
            raise ValueError("candidate object key is outside the approved scope")
        return f"{self.prefix}{suffix}"


def _json_file(path: str, name: str):
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{name} must be a readable JSON file") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{name} must contain a JSON object")
    return value


def build_parser():
    parser = argparse.ArgumentParser(description="Run an isolated production Gold candidate")
    parser.add_argument("--plan-json", required=True)
    parser.add_argument("--bundle-json", required=True)
    parser.add_argument("--bucket", default="ecommerce-lakehouse-prod")
    parser.add_argument("--dim-date-start", default="2020-01-01")
    parser.add_argument("--dim-date-end", default="2035-12-31")
    return parser


def _candidate_envelope(raw_bundle):
    """Select the immutable input envelope without constructing any client."""
    silver_run = raw_bundle.get("silver_run") if isinstance(raw_bundle, dict) else None
    artifacts = silver_run.get("artifacts") if isinstance(silver_run, dict) else None
    full_silver = (
        isinstance(artifacts, dict)
        and set(artifacts) == set(SILVER_TABLES)
        and all(isinstance(item, dict) and item.get("status") == "PASS" for item in artifacts.values())
    )
    if full_silver:
        return {"silver_run": silver_run}
    if "gold_snapshots" not in raw_bundle:
        raise ValueError("gold_snapshots is required when the full-Silver bundle is absent")
    return {"silver_run": silver_run, "gold_snapshots": raw_bundle["gold_snapshots"]}


def _preflight_candidate(plan, environ, raw_bundle):
    """Validate candidate environment and lineage before Spark/MinIO/Nessie."""
    validate_candidate_environment(environ, plan)
    envelope = _candidate_envelope(raw_bundle)
    inputs = validate_fact_and_dimension_bundle(
        envelope, expected_dag_run_id=plan.silver_dag_run_id
    )
    return envelope, inputs


def main(argv=None):
    args = build_parser().parse_args(argv)
    plan_data = _json_file(args.plan_json, "plan-json")
    raw_bundle = _json_file(args.bundle_json, "bundle-json")
    plan = GoldCandidatePlan(
        run_id=plan_data.get("run_id"),
        candidate_ref=plan_data.get("candidate_ref"),
        target_namespace=plan_data.get("target_namespace"),
        source_ref=plan_data.get("source_ref", "gold_prod_current"),
        mode=plan_data.get("mode", "WRITE"),
        silver_dag_run_id=plan_data.get("silver_dag_run_id"),
    ).validate()
    environ = dict(os.environ)
    if args.bucket != PRODUCTION_BUCKET or environ.get("BRONZE_BUCKET") not in (None, PRODUCTION_BUCKET):
        raise ValueError("candidate CLI requires ecommerce-lakehouse-prod")
    environ.update({"BRONZE_BUCKET": PRODUCTION_BUCKET, "LAKEHOUSE_STORAGE_ENV": "production",
                    "NESSIE_REF": plan.candidate_ref,
                    "GOLD_TARGET_NAMESPACE": plan.target_namespace})
    layout = validate_storage_environment(environ)
    if layout.nessie_ref != plan.candidate_ref:
        raise ValueError("candidate storage layout lost the explicit candidate ref")
    envelope, _validated_inputs = _preflight_candidate(plan, environ, raw_bundle)
    bundle = build_candidate_artifact_bundle(
        envelope["silver_run"], envelope.get("gold_snapshots")
    )
    if "gold_snapshots" in envelope:
        facts = {table: bundle[table] for table in ("orders", "order_items", "payments", "shipments")}
        # Preserve the legacy mixed-input lineage shape for pinned Gold
        # dimension reuse. This branch is never used by the full-Silver path.
        artifacts = {
            **facts,
            "customers": {"status": "NO_DATA", "table_name": "customers", "reason": "NO_DATA",
                          "snapshot_reuse": bundle["customers"]},
            "products": {"status": "NO_DATA", "table_name": "products", "reason": "NO_DATA",
                         "snapshot_reuse": bundle["products"]},
        }
    else:
        # Full-Silver candidates pass all six verified artifacts unchanged.
        artifacts = bundle
    candidate_scope = CandidateScope(
        bucket=args.bucket, candidate_ref=plan.candidate_ref,
        target_namespace=plan.target_namespace,
        warehouse=f"s3a://{args.bucket}/lakehouse",
    )
    lineage = build_dimensional_lineage_from_artifacts(artifacts, scope=candidate_scope)

    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.appName("GoldProductionCandidate")
               .config("spark.sql.catalog.lakehouse.warehouse", candidate_scope.warehouse)
               .config("spark.sql.catalog.lakehouse.ref", candidate_scope.nessie_ref))
    spark = builder.getOrCreate()
    try:
        configure_s3a(spark)
        client = create_minio_client()
        nessie = NessieRestAdapter(environ["NESSIE_URI"])
        runtime = {"spark": spark, "client": client, "nessie": nessie,
                   "scope": candidate_scope, "bucket": args.bucket, "lineage": lineage,
                   # The dimensional manifest contract accepts ISO date strings
                   # and performs calendar validation at the writer boundary.
                   "dim_date_start": args.dim_date_start,
                   "dim_date_end": args.dim_date_end,
                   "captured_at": datetime.now(timezone.utc).isoformat(),
                   "environ": environ}
        report = run_gold_candidate(
            plan, envelope,
            environ, adapter=nessie, runtime_factory=lambda _plan, _inputs: runtime,
        )
        print("GOLD_CANDIDATE_RESULT=" + json.dumps(report, sort_keys=True, default=str), flush=True)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
