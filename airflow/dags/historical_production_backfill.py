"""Dedicated historical backfill DAG; it never imports incremental orchestration.

The task graph is intentionally complete even while the production Gold writer
adapter is unavailable: write mode stops at that boundary instead of claiming
candidate verification or promotion.
"""

import os
import sys
import json
from datetime import datetime, timedelta

from airflow.sdk import dag, task

PROJECT_ROOT = "/opt/airflow/project"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ingestion.historical.config import HistoricalBackfillConfig
from ingestion.historical.orchestrator import (
    assert_parent_first,
    build_execution_plan,
    resolve_execution_mode,
)
from pipeline_operators import create_historical_spark_operator


def _compact_bronze_metadata(snapshot):
    """Return only JSON-safe immutable Bronze metadata for XCom transport."""
    if not isinstance(snapshot, dict) or snapshot.get("status") not in {"DRY_RUN", "PASS"}:
        raise RuntimeError("historical Bronze extraction did not pass")
    compact = {"status": snapshot["status"], "run_id": snapshot.get("run_id")}
    if snapshot["status"] == "PASS":
        bronze = snapshot.get("bronze")
        if not isinstance(bronze, dict):
            raise RuntimeError("historical Bronze metadata is missing")
        compact["bronze"] = {}
        for table, envelope in bronze.items():
            if not isinstance(envelope, dict):
                raise RuntimeError(f"historical Bronze metadata is invalid: {table}")
            compact["bronze"][table] = {
                "table": table,
                **{
                    key: envelope[key]
                    for key in ("object_key", "manifest_key", "row_count", "checksum_sha256")
                    if key in envelope
                },
            }
        json.dumps(compact, sort_keys=True, separators=(",", ":"))
    return compact


@dag(
    dag_id="ecommerce_historical_production_backfill",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(hours=6)},
    tags=["ecommerce", "historical", "production-guarded", "dry-run"],
)
def ecommerce_historical_production_backfill():
    @task(task_id="validate_historical_scope")
    def validate_scope():
        run_id = os.environ.get("HISTORICAL_BACKFILL_RUN_ID", "historical-dry-run")
        capture_end = datetime.fromisoformat(os.environ.get("HISTORICAL_BACKFILL_CAPTURE_END", "2026-09-27T00:00:00"))
        config, write = resolve_execution_mode(HistoricalBackfillConfig(run_id=run_id, capture_end=capture_end), os.environ)
        return assert_parent_first(build_execution_plan(config, os.environ, write=write))

    @task(task_id="extract_historical_snapshot")
    def extract_snapshot(plan):
        if plan["snapshot_isolation"] != "REPEATABLE READ" or plan["lower_boundary"] != "updated_at >= window_start":
            raise ValueError("historical extraction snapshot/boundary contract is invalid")
        from ingestion.historical.pipeline import run_historical_bronze

        return _compact_bronze_metadata(run_historical_bronze(plan, os.environ))

    @task(task_id="bronze_validate_upload_metadata")
    def bronze(snapshot, plan):
        # Do not put extracted records in XCom.  Spark receives only the
        # immutable Bronze object/manifest identity envelopes.
        compact = _compact_bronze_metadata(snapshot)
        return compact

    @task(task_id="candidate_verification")
    def candidate(gold_result, plan):
        if gold_result.get("status") != "VERIFIED":
            raise RuntimeError("candidate verification requires a verified seven-table Gold readback")
        return {"status": "VERIFIED", "candidate_ref": plan["candidate_ref"]}

    plan = validate_scope()
    extract = extract_snapshot(plan)
    bronze_result = bronze(extract, plan)
    silver_result = create_historical_spark_operator(
        task_id="silver_transform_manifest",
        project_path=os.environ.get("AIRFLOW_DOCKER_PROJECT_PATH", PROJECT_ROOT),
        stage="silver",
        input_argument="--bronze-json",
        plan_value=plan,
        input_value=bronze_result,
    )
    gold_result = create_historical_spark_operator(
        task_id="gold_integration_verification_gate",
        project_path=os.environ.get("AIRFLOW_DOCKER_PROJECT_PATH", PROJECT_ROOT),
        stage="gold",
        input_argument="--silver-artifacts-json",
        plan_value=plan,
        input_value=silver_result.output,
    )
    bronze_result >> silver_result
    silver_result >> gold_result
    candidate(gold_result.output, plan)


ecommerce_historical_production_backfill()
