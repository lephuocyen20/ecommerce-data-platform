"""Gold TaskFlow tasks for the incremental DAG."""

import sys
import os

from airflow.sdk import task

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from common.runtime_policy import is_feature_enabled
from gold_gate_policy import decide_gold_snapshot_gate
from pipeline_gold import (
    build_gold_source_snapshot_payload,
    build_gold_source_lineage_payload,
    load_iceberg_gold_lineage,
    resolve_silver_snapshot_lineage,
)
from gold_source_mode import ICEBERG_SNAPSHOT, resolve_gold_source_mode
from common.minio import create_minio_client
from gold.implementation.readers import read_iceberg_gold_sources


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
    client, bucket = create_minio_client(), os.environ["BRONZE_BUCKET"]
    for table, source in lineage["sources"].items():
        read_snapshot_attestation(client, bucket, source["snapshot_attestation_identity"], table=table,
                                  catalog=source["catalog"], namespace=source["namespace"], ref=source["ref"], snapshot_id=source["snapshot_id"])
        for kind, field in (("reconciliation", "reconciliation_evidence"), ("quality", "quality_evidence")):
            ref = source[field]
            response = client.get_object(bucket, f"lakehouse/evidence/v2/{ref['identity']}.json")
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
