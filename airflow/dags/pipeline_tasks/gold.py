"""TaskFlow tasks for the native dimensional Gold Iceberg graph."""

import json
import hashlib
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

try:
    from airflow.exceptions import AirflowSkipException
except ImportError:
    class AirflowSkipException(Exception):
        pass

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from common.bounded_e2e_scope import TEST_BUCKET, build_bounded_e2e_scope
from common.minio import create_minio_client
from common.runtime_policy import is_feature_enabled
from gold_gate_policy import decide_gold_dimensional_gate
from gold.implementation.dimensional.lineage import build_parquet_lineage_artifact


@task(retries=0)
def gold_dimensional_gate(silver_results: dict, dimensional_artifacts: dict, config: dict):
    """Validate Silver inputs and skip the Gold branch when it is disabled."""
    from pipeline_tasks.silver import build_pipeline_summary

    if not isinstance(dimensional_artifacts, dict) or set(dimensional_artifacts) != set(silver_results or {}):
        raise RuntimeError("Dimensional Gold requires all verified Silver artifacts")
    pipeline_result = build_pipeline_summary(silver_results, config)
    if not is_feature_enabled("GOLD_DIMENSIONAL_ENABLED"):
        raise AirflowSkipException("GOLD_DIMENSIONAL_ENABLED is false")
    decision = decide_gold_dimensional_gate(pipeline_result, enabled=True)
    if decision != "resolve_gold_dimensional_lineage":
        raise RuntimeError(f"Unexpected Gold gate decision: {decision}")
    return pipeline_result


@task(retries=0)
def build_dimensional_parquet_artifact_task(table_name, silver_result):
    """Build cumulative Gold lineage from verified Silver Parquet manifests."""
    if not isinstance(silver_result, dict) or silver_result.get("table_name") != table_name:
        raise RuntimeError(f"Invalid Silver result for {table_name}")
    if silver_result.get("status") != "PASS":
        raise RuntimeError(f"Silver Parquet verification did not PASS: {table_name}")
    manifest_key = silver_result.get("manifest_key")
    if not isinstance(manifest_key, str) or not manifest_key:
        raise RuntimeError(f"Silver manifest key is missing: {table_name}")
    bucket = os.environ.get("BRONZE_BUCKET")
    if bucket == TEST_BUCKET:
        scope = build_bounded_e2e_scope(os.environ)
        if not manifest_key.startswith(scope.prefix):
            raise RuntimeError(f"Silver manifest is outside bounded scope: {table_name}")
    client = create_minio_client()
    def read_manifest(key):
        document = client.get_object(bucket, key)
        try:
            value = json.loads(document.read().decode("utf-8"))
        finally:
            document.close()
            document.release_conn()
        if value.get("table_name") != table_name or value.get("status") != "COMPLETED":
            raise RuntimeError(f"Invalid Silver completion manifest: {table_name}")
        return value

    current = read_manifest(manifest_key)
    prefix = manifest_key.rsplit("batch_id=", 1)[0]
    manifest_keys = sorted(
        item.object_name
        for item in client.list_objects(bucket, prefix=prefix, recursive=True)
        if item.object_name.endswith("/_BATCH_METADATA.json")
    )
    if manifest_key not in manifest_keys:
        raise RuntimeError(f"Current Silver manifest is not discoverable: {table_name}")
    manifests = [read_manifest(key) for key in manifest_keys]
    identities = [hashlib.sha256(json.dumps(item, sort_keys=True, separators=(",", ":")).encode()).hexdigest() for item in manifests]
    return build_parquet_lineage_artifact(
        table=table_name,
        bucket=bucket,
        manifest_key=manifest_key,
        data_prefix=current["data_prefix"],
        manifest_identity=identities[manifest_keys.index(manifest_key)],
        batch_id=current["batch_id"],
        row_count=sum(item["silver_row_count"] for item in manifests),
        bronze_batch_id=current["batch_id"],
        manifest_keys=manifest_keys,
        manifest_identities=identities,
        data_prefixes=[item["data_prefix"] for item in manifests],
    )


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
    """Validate caller-supplied Silver Parquet lineage for the native graph."""
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
        artifact = result
        if not isinstance(artifact, dict):
            raise RuntimeError(f"Invalid dimensional artifact envelope: {table}")
        artifacts[table] = artifact
    return build_dimensional_lineage_from_artifacts(artifacts, scope=scope)
