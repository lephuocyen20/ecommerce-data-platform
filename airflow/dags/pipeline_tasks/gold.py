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
from common.manifest import canonical_manifest_identity
from common.minio import create_minio_client
from common.runtime_policy import is_feature_enabled
from common.storage_layout import QUALITY_BUCKET
from common.storage_layout import validate_storage_environment
from gold_gate_policy import decide_gold_dimensional_gate
from gold.implementation.dimensional.lineage import build_parquet_lineage_artifact


def build_controlled_incremental_merge_plan(resolved_inputs, silver_deltas, *, snapshot_reader):
    """Build the real merge plan used by the controlled writer DRY_RUN seam.

    The reader is intentionally injected because Airflow/XCom must not carry
    Spark DataFrames and this boundary must not create a catalog client.  A
    missing reader or missing verified reuse result fails closed.
    """
    from gold.implementation.dimensional.snapshot_reuse import (
        build_controlled_dry_run_merge_plan,
    )

    return build_controlled_dry_run_merge_plan(
        resolved_inputs, silver_deltas, snapshot_reader=snapshot_reader
    )


def run_controlled_incremental_dry_run(
    resolved_inputs, silver_deltas, *, snapshot_reader, writer
):
    """Propagate one injected merge plan through the writer DRY_RUN boundary."""
    if not callable(writer):
        raise RuntimeError("Controlled DRY_RUN requires an injected writer seam")
    merge_plan = build_controlled_incremental_merge_plan(
        resolved_inputs, silver_deltas, snapshot_reader=snapshot_reader
    )
    writer_result = writer(merge_plan=merge_plan, mode="DRY_RUN")
    if not isinstance(writer_result, dict) or writer_result.get("status") != "DRY_RUN":
        raise RuntimeError("Controlled DRY_RUN writer did not return DRY_RUN evidence")
    return {"status": "DRY_RUN", "merge_plan": merge_plan, "writer_result": writer_result}


def _execute_controlled_incremental_snapshot_reuse_dry_run(
    *,
    enabled,
    controlled_spec=None,
    resolved_inputs=None,
    silver_deltas=None,
    snapshot_reader=None,
    writer=None,
):
    """Execute the controlled task boundary without putting DataFrames in XCom.

    ``resolved_inputs`` and ``silver_deltas`` are explicit inputs for the
    controlled seam.  ``snapshot_reader`` and ``writer`` are injected only by
    a test/runtime wrapper that owns those objects; the production DAG calls
    this task without them and therefore remains fail-closed.
    """
    if not enabled:
        return {"status": "SKIPPED", "reason": "GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED=false"}
    if controlled_spec is None and resolved_inputs is None and silver_deltas is None:
        return {"status": "SKIPPED", "reason": "controlled DRY_RUN spec absent"}
    if controlled_spec is not None:
        return _execute_serializable_controlled_dry_run(controlled_spec)
    if resolved_inputs is None or silver_deltas is None:
        raise RuntimeError(
            "Controlled DRY_RUN requires explicit resolved_inputs and silver_deltas"
        )
    if snapshot_reader is None or not callable(writer):
        raise RuntimeError(
            "Controlled DRY_RUN requires explicitly injected DRY_RUN reader and writer"
        )
    return run_controlled_incremental_dry_run(
        resolved_inputs,
        silver_deltas,
        snapshot_reader=snapshot_reader,
        writer=writer,
    )


def _execute_serializable_controlled_dry_run(controlled_spec):
    """Run the bounded seam from JSON-safe references only.

    This is the production DAG path.  Airflow/XCom carries no DataFrame,
    reader, writer, or callable.  The local adapters deliberately materialize
    opaque reference handles and only return DRY_RUN evidence; they never
    create a Spark/catalog client or perform storage/catalog mutation.
    """
    from gold.implementation.dimensional.snapshot_reuse import (
        GOLD_SNAPSHOT_REUSE,
        DIMENSIONAL_SOURCE_TABLES,
        SnapshotReuseReader,
        validate_incremental_merge_plan,
    )

    if isinstance(controlled_spec, dict) and "controlled_dry_run_spec" in controlled_spec:
        controlled_spec = controlled_spec["controlled_dry_run_spec"]
    if controlled_spec is None:
        return {"status": "SKIPPED", "reason": "controlled DRY_RUN spec absent"}
    if not isinstance(controlled_spec, dict):
        raise RuntimeError("Controlled DRY_RUN spec must be an object")
    try:
        json.dumps(controlled_spec, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Controlled DRY_RUN spec must be JSON-serializable") from exc
    if set(controlled_spec) != {"version", "resolved_inputs", "silver_delta_refs", "snapshot_refs"}:
        raise RuntimeError("Controlled DRY_RUN spec has an invalid shape")
    if controlled_spec["version"] != 1:
        raise RuntimeError("Unsupported controlled DRY_RUN spec version")
    resolved = controlled_spec["resolved_inputs"]
    if not isinstance(resolved, dict) or resolved.get("version") != 1:
        raise RuntimeError("Controlled DRY_RUN spec has invalid resolved_inputs")
    resolutions = resolved.get("resolutions")
    if not isinstance(resolutions, dict) or set(resolutions) != set(DIMENSIONAL_SOURCE_TABLES):
        raise RuntimeError("Controlled DRY_RUN spec has incomplete resolutions")

    delta_refs = controlled_spec["silver_delta_refs"]
    snapshot_refs = controlled_spec["snapshot_refs"]
    if not isinstance(delta_refs, dict) or not isinstance(snapshot_refs, dict):
        raise RuntimeError("Controlled DRY_RUN artifact references must be objects")
    expected_delta_tables = {
        table for table, item in resolutions.items()
        if isinstance(item, dict) and item.get("resolution") != GOLD_SNAPSHOT_REUSE
    }
    expected_snapshot_tables = {
        table for table, item in resolutions.items()
        if isinstance(item, dict) and item.get("resolution") == GOLD_SNAPSHOT_REUSE
    }
    if set(delta_refs) != expected_delta_tables or set(snapshot_refs) != expected_snapshot_tables:
        raise RuntimeError("Controlled DRY_RUN artifact references do not match resolutions")
    for refs in (delta_refs, snapshot_refs):
        if any(not isinstance(value, str) or not value for value in refs.values()):
            raise RuntimeError("Controlled DRY_RUN artifact references must be non-empty strings")

    class _ManifestSnapshotReuseReader(SnapshotReuseReader):
        def read_verified_snapshot(self, resolved_input):
            from gold.implementation.dimensional.snapshot_reuse import GoldSnapshotReuseAdapter

            # Resolve performs the complete pinned snapshot contract validation;
            # no catalog or object-store client is needed for this controlled seam.
            validated = GoldSnapshotReuseAdapter(now=self.now).resolve(
                resolved_input["table_name"], resolved_input
            )
            source = validated["table_name"]
            return {
                "status": "PASS",
                "table": validated["snapshot"]["table"],
                "snapshot_id": validated["snapshot"]["snapshot_id"],
                "dataframe": {"artifact_ref": snapshot_refs[source]},
            }

    class _DryRunWriterAdapter:
        def __call__(self, *, merge_plan, mode):
            if mode != "DRY_RUN":
                raise RuntimeError("Controlled writer adapter only permits DRY_RUN")
            validated = validate_incremental_merge_plan(merge_plan)
            return {
                "status": "DRY_RUN",
                "mode": "DRY_RUN",
                "merge_plan_identity": validated["identity"],
                "silver_delta_refs": dict(delta_refs),
                "snapshot_refs": dict(snapshot_refs),
            }

    silver_deltas = {
        table: {"artifact_ref": delta_refs[table]}
        for table in expected_delta_tables
    }
    return run_controlled_incremental_dry_run(
        resolved,
        silver_deltas,
        snapshot_reader=_ManifestSnapshotReuseReader(),
        writer=_DryRunWriterAdapter(),
    )


@task(task_id="gold_incremental_snapshot_reuse_dry_run", retries=0)
def controlled_incremental_snapshot_reuse_dry_run_task(
    controlled_spec=None,
):
    """Run the controlled seam from a JSON-safe spec only.

    The actual DAG passes only a serializable config/spec reference.  Spark
    DataFrames and arbitrary callable instances are never XCom inputs.
    """
    from gold.implementation.dimensional.snapshot_reuse import snapshot_reuse_enabled

    return _execute_controlled_incremental_snapshot_reuse_dry_run(
        enabled=snapshot_reuse_enabled(),
        controlled_spec=controlled_spec,
    )


def _structured_writer_result(value):
    """Normalize DockerOperator XCom output without weakening Gold evidence checks."""
    from gold.implementation.dimensional.verification import (
        DimensionalVerificationError,
        parse_writer_result,
    )

    candidates = value if isinstance(value, (list, tuple)) else (value,)
    for candidate in reversed(candidates):
        if isinstance(candidate, dict):
            return candidate
        if not isinstance(candidate, str) or "GOLD_DIMENSIONAL_RESULT=" not in candidate:
            continue
        try:
            parsed = parse_writer_result(candidate)
        except DimensionalVerificationError:
            raise RuntimeError("Gold writer emitted invalid canonical result") from None
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError("Gold publish requires the structured writer result")


@task(task_id="publish_gold_to_active", retries=0)
def publish_gold_to_active_task(writer_result):
    """CAS-publish only a verified WRITE result to the stable BI branch."""
    writer_result = _structured_writer_result(writer_result)
    if (
        writer_result.get("status") == "GOLD_NO_CHANGE"
        and writer_result.get("reason") == "NO_CHANGE"
    ):
        raise AirflowSkipException("Gold NO_CHANGE: publication and confirmations are skipped")
    if writer_result.get("mode") != "WRITE":
        return {"status": "SKIPPED", "reason": "Gold writer was not WRITE"}
    if os.environ.get("BRONZE_BUCKET") != TEST_BUCKET:
        return {"status": "SKIPPED", "reason": "Active publication requires the approved shared Gold bucket"}
    report = writer_result.get("certification_report")
    if not isinstance(report, dict) or report.get("certified") is not True or report.get("can_write") is not True:
        raise RuntimeError("Gold publish requires certified WRITE evidence")
    if os.environ.get("BI_ACTIVE_PUBLISH_APPLY") != "1":
        raise RuntimeError("BI_ACTIVE_PUBLISH_APPLY=1 is required for active Gold publication")
    expected_hash = os.environ.get("BI_GOLD_CURRENT_EXPECTED_HASH")
    if not expected_hash:
        raise RuntimeError("BI_GOLD_CURRENT_EXPECTED_HASH is required for active Gold publication")
    from gold.implementation.bi_ref_promotion import (
        NessieRestAdapter,
        advance_stable_branch,
        candidate_from_certification,
    )
    candidate_ref = report.get("ref")
    schema = os.environ.get("GOLD_TARGET_NAMESPACE", "gold_run_20261112_gold_06")
    scope = build_bounded_e2e_scope(os.environ)
    if not isinstance(candidate_ref, str) or candidate_ref != scope.nessie_ref or not schema:
        raise RuntimeError("Gold publish requires candidate ref and GOLD_TARGET_NAMESPACE")
    adapter = NessieRestAdapter(os.environ["NESSIE_URI"])
    candidate_hash = adapter.get_ref(candidate_ref).hash
    publish_report = {**report, "candidate_hash": candidate_hash}
    manifest = candidate_from_certification(
        publish_report,
        candidate_ref=candidate_ref,
        schema=schema,
        bucket=scope.bucket,
        prefix=scope.prefix,
        run_id=scope.run_id,
    )
    return advance_stable_branch(
        manifest,
        adapter=adapter,
        expected_current_hash=expected_hash,
        apply=True,
    )


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
def build_dimensional_parquet_artifact_task(table_name, silver_result, config=None):
    """Build cumulative Gold lineage from verified Silver Parquet manifests."""
    if not isinstance(silver_result, dict) or silver_result.get("table_name") != table_name:
        raise RuntimeError(f"Invalid Silver result for {table_name}")
    status = silver_result.get("status")
    if status == "SKIPPED" and silver_result.get("reason") in {"NO_DATA", "DRY_RUN"}:
        reason = silver_result["reason"]
        result = {"table_name": table_name, "status": "NO_DATA", "reason": reason}
        if table_name in {"customers", "products"} and isinstance(config, dict):
            reuse = (config.get("gold_snapshot_reuse_artifacts") or {}).get(table_name)
            registry = config.get("gold_snapshot_reuse_registry")
            if registry is not None:
                from gold.implementation.dimensional.snapshot_reuse_registry import registry_artifacts
                reuse = registry_artifacts(registry)[table_name]
            if reuse is not None:
                result["snapshot_reuse"] = reuse
        return result
    if status != "PASS":
        raise RuntimeError(f"Silver Parquet verification did not PASS: {table_name}")
    manifest_key = silver_result.get("manifest_key")
    if not isinstance(manifest_key, str) or not manifest_key:
        raise RuntimeError(f"Silver manifest key is missing: {table_name}")
    bucket = os.environ.get("BRONZE_BUCKET")
    storage_environment = dict(os.environ)
    storage_environment.setdefault("BRONZE_BUCKET", bucket or "")
    validate_storage_environment(storage_environment)
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
    manifest_keys = silver_result.get("manifest_keys")
    if manifest_keys != [manifest_key]:
        raise RuntimeError(f"Silver manifest selection is not explicit for {table_name}")
    # Match verify_silver_manifest and child parent references.  The manifest
    # embeds its own identity fields, so the canonical helper must exclude
    # those circular fields from the hash.
    identity = canonical_manifest_identity(current)
    return build_parquet_lineage_artifact(
        table=table_name,
        bucket=bucket,
        manifest_key=manifest_key,
        data_prefix=current["data_prefix"],
        manifest_identity=identity,
        batch_id=current["batch_id"],
        row_count=current["silver_row_count"],
        bronze_batch_id=current["batch_id"],
        manifest_keys=manifest_keys,
        manifest_identities=[identity],
        data_prefixes=[current["data_prefix"]],
        coverage=current.get("coverage"),
    )


@task(task_id="verify_gold_dimensional_graph_manifest", retries=0)
def verify_gold_dimensional_graph_manifest_task(writer_result):
    bucket = os.environ.get("BRONZE_BUCKET")
    validate_storage_environment(os.environ)
    if bucket not in {QUALITY_BUCKET, TEST_BUCKET}:
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
