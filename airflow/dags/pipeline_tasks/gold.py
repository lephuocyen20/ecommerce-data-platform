"""Gold TaskFlow tasks for the incremental DAG."""

import json
import sys
import os
import logging

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

from common.runtime_policy import is_feature_enabled
from common.bounded_e2e_scope import TEST_BUCKET, build_bounded_e2e_scope
from gold_gate_policy import decide_gold_dimensional_gate, decide_gold_snapshot_gate
from pipeline_gold import (
    build_gold_source_snapshot_payload,
    build_gold_source_lineage_payload,
    load_iceberg_gold_lineage,
    resolve_silver_snapshot_lineage,
)
from gold_source_mode import ICEBERG_SNAPSHOT, resolve_gold_source_mode
from common.minio import create_minio_client


@task.branch(retries=0)
def gold_snapshot_gate(pipeline_result: dict):
    return decide_gold_snapshot_gate(
        pipeline_result,
        write_enabled=is_feature_enabled("GOLD_SNAPSHOT_WRITE_ENABLED"),
        lineage_task_id=("resolve_gold_iceberg_lineage" if resolve_gold_source_mode(os.environ.get("GOLD_SOURCE_MODE")) == ICEBERG_SNAPSHOT else "resolve_gold_snapshot_lineage"),
    )


@task(retries=0)
def skip_gold_layer():
    return {
        "status": "SKIPPED",
        "reason": "GOLD_SNAPSHOT_NOT_ENABLED_OR_SILVER_NOT_WRITABLE",
    }


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
        "dimensional artifact attestation transport type=%s keys=%s list_item_types=%s",
        type(attestation_result).__name__,
        sorted(attestation_result.keys()) if isinstance(attestation_result, dict) else None,
        [type(item).__name__ for item in attestation_result] if isinstance(attestation_result, list) else None,
    )
    if isinstance(quality_result, str) and "=" in quality_result:
        try:
            quality_result = json.loads(quality_result.rsplit("=", 1)[1].strip())
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid Iceberg quality payload: {table_name}") from exc
    elif isinstance(quality_result, list):
        # DockerOperator ``xcom_all`` returns stdout lines.  The quality CLI
        # emits one JSON array line; unwrap only that documented terminal
        # shape so the dimensional normalizer receives quality report objects.
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
        # TaskFlow XCom contains the artifact envelope directly. Preserve
        # compatibility with the older wrapped shape without accepting any
        # arbitrary nested payload.
        artifact = result.get("dimensional_artifact") if "dimensional_artifact" in result else result
        if not isinstance(artifact, dict):
            raise RuntimeError(f"Invalid dimensional artifact envelope: {table}")
        artifacts[table] = artifact
    return build_dimensional_lineage_from_artifacts(artifacts, scope=scope)


@task(task_id="resolve_gold_snapshot_lineage", retries=0)
def resolve_gold_snapshot_lineage_task():
    return resolve_silver_snapshot_lineage()


@task(task_id="resolve_gold_iceberg_lineage", retries=0)
def resolve_gold_iceberg_lineage_task():
    """Return only caller-provided, immutable v2 lineage after object validation."""
    if resolve_gold_source_mode(os.environ.get("GOLD_SOURCE_MODE")) != ICEBERG_SNAPSHOT:
        raise RuntimeError("Iceberg lineage task invoked outside iceberg_snapshot mode")
    lineage = load_iceberg_gold_lineage(os.environ.get("GOLD_ICEBERG_SOURCE_LINEAGE"))
    # Validate each referenced attestation/evidence through the Gold reader's
    # strict artifact verification path; no Spark read is performed here.
    from lakehouse.attestation import read_snapshot_attestation
    from lakehouse.evidence import verify_evidence
    bucket = os.environ["BRONZE_BUCKET"]
    scope = build_bounded_e2e_scope(os.environ) if bucket == TEST_BUCKET else None
    client = create_minio_client()
    for table, source in lineage["sources"].items():
        read_snapshot_attestation(client, bucket, source["snapshot_attestation_identity"], table=table,
                                  catalog=source["catalog"], namespace=source["namespace"], ref=source["ref"], snapshot_id=source["snapshot_id"], scope=scope)
        for kind, field in (("reconciliation", "reconciliation_evidence"), ("quality", "quality_evidence")):
            ref = source[field]
            key = f"lakehouse/evidence/v2/{ref['identity']}.json"
            if scope is not None:
                key = scope.key(key)
            response = client.get_object(bucket, key)
            try:
                import json
                document = json.loads(response.read().decode("utf-8"))
            finally:
                response.close(); response.release_conn()
            verify_evidence(document, kind=kind, table=table, snapshot_id=source["snapshot_id"],
                            catalog=source["catalog"], namespace=source["namespace"], ref=source["ref"],
                            snapshot_attestation_identity=source["snapshot_attestation_identity"])
            if {key: document[key] for key in ref} != ref:
                raise RuntimeError(f"Invalid {kind} evidence for {table}")
    return lineage


@task(task_id="prepare_gold_dataset_sources", retries=0)
def prepare_gold_dataset_sources_task(dataset_name: str, silver_snapshots: dict):
    if isinstance(silver_snapshots, dict) and silver_snapshots.get("source_type") == ICEBERG_SNAPSHOT:
        return build_gold_source_lineage_payload(dataset_name, silver_snapshots)
    return build_gold_source_snapshot_payload(dataset_name, silver_snapshots)
