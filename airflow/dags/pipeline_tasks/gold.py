"""TaskFlow tasks for the native dimensional Gold Iceberg graph."""

import json
import logging
import os
import sys

try:
    from airflow.sdk import task
except ImportError:
    try:
        from airflow.decorators import task
    except ImportError:
        def task(*_args, **_kwargs):
            return lambda fn: fn

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from common.bounded_e2e_scope import TEST_BUCKET, build_bounded_e2e_scope
from common.minio import create_minio_client
from common.runtime_policy import is_feature_enabled
from gold_gate_policy import decide_gold_dimensional_gate


@task.branch(retries=0)
def gold_dimensional_gate(pipeline_result: dict):
    """Keep the native dimensional graph opt-in and fail closed by default."""
    return decide_gold_dimensional_gate(
        pipeline_result,
        enabled=is_feature_enabled("GOLD_DIMENSIONAL_ENABLED"),
    )


@task(task_id="skip_gold_dimensional", retries=0)
def skip_gold_dimensional():
    return {"status": "SKIPPED", "reason": "GOLD_DIMENSIONAL_DISABLED"}


@task(retries=0)
def build_dimensional_artifact_task(table_name, attestation_result, reconciliation_result, quality_result, silver_result=None):
    """Accept only a complete local Iceberg artifact from the quality task."""
    logging.getLogger("airflow.task").warning(
        "dimensional artifact transport table=%s attestation_type=%s quality_type=%s",
        table_name, type(attestation_result).__name__, type(quality_result).__name__,
    )
    if isinstance(quality_result, str) and "=" in quality_result:
        try:
            quality_result = json.loads(quality_result.rsplit("=", 1)[1].strip())
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid Iceberg quality payload: {table_name}") from exc
    elif isinstance(quality_result, list):
        for line in reversed(quality_result):
            if not isinstance(line, str) or not line.lstrip().startswith("["):
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, list):
                quality_result = parsed
                break
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    try:
        return normalize_dimensional_artifact(
            table_name, attestation_result, reconciliation_result, quality_result,
            bronze_batch_id=silver_result.get("batch_id") if isinstance(silver_result, dict) else None,
            row_count=silver_result.get("silver_row_count") if isinstance(silver_result, dict) else None,
        )
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc


@task(task_id="verify_gold_dimensional_graph_manifest", retries=0)
def verify_gold_dimensional_graph_manifest_task(writer_result):
    bucket = os.environ.get("BRONZE_BUCKET")
    if bucket not in {"ecommerce-quality-it", TEST_BUCKET}:
        raise RuntimeError("Native dimensional verification requires a local or approved bounded bucket")
    from gold.implementation.dimensional.verification import verify_completed_graph
    scope = build_bounded_e2e_scope(os.environ) if bucket == TEST_BUCKET else None
    return verify_completed_graph(writer_result, create_minio_client(), bucket, scope=scope)


@task(task_id="resolve_gold_dimensional_lineage", retries=0)
def resolve_gold_dimensional_lineage_task(silver_results: dict):
    """Validate caller-supplied Iceberg lineage for the native graph only."""
    if not isinstance(silver_results, dict) or any(
        not isinstance(result, dict) or result.get("status") != "PASS"
        for result in silver_results.values()
    ):
        raise RuntimeError("Native dimensional Gold requires all verified Silver results")
    from gold.implementation.dimensional.lineage import build_dimensional_lineage_from_artifacts
    bucket = os.environ.get("BRONZE_BUCKET")
    scope = build_bounded_e2e_scope(os.environ) if bucket == TEST_BUCKET else None
    artifacts = {}
    for table, result in silver_results.items():
        artifact = result.get("dimensional_artifact") if "dimensional_artifact" in result else result
        if not isinstance(artifact, dict):
            raise RuntimeError(f"Invalid dimensional artifact envelope: {table}")
        artifacts[table] = artifact
    return build_dimensional_lineage_from_artifacts(artifacts, scope=scope)
