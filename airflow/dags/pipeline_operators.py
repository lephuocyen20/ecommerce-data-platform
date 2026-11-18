"""Reusable Airflow DockerOperator factories for Spark pipeline stages."""

import json
import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from docker.types import Mount
from common.bounded_e2e_scope import BoundedE2EScopeError, build_bounded_e2e_scope
from common.storage_layout import PRODUCTION_BUCKET, PRODUCTION_REF, validate_storage_environment
from common.airflow_state_adapter import AirflowStateClient, make_runtime_task_failure_callback
try:
    from pipeline_contracts import TABLES
except ImportError:
    from airflow.dags.pipeline_contracts import TABLES


SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "ecommerce-spark:3.5.6")
DOCKER_NETWORK = os.environ.get(
    "AIRFLOW_DOCKER_NETWORK", "ecommerce-data-platform_default"
)
SPARK_WORKDIR = "/opt/spark/work-dir"
SPARK_PYTHONPATH = ":".join(
    (
        SPARK_WORKDIR,
        "/opt/spark/python",
        "/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
    )
)
TRANSFORMATION_TARGET = f"{SPARK_WORKDIR}/transformation"
IDEMPOTENT_RETRIES = 2
IDEMPOTENT_RETRY_DELAY = timedelta(minutes=1)
IDEMPOTENT_EXECUTION_TIMEOUT = timedelta(hours=2)
DOCKER_API_TIMEOUT_SECONDS = 60
PRODUCTION_WRITE_APPROVAL_ENV = "PRODUCTION_LAKEHOUSE_WRITE_APPROVED"


def _preflight_dimensional_artifacts(artifacts, *, registry=None):
    """Validate mixed Silver/Gold inputs before any Spark or lakehouse client work."""
    if not isinstance(artifacts, dict):
        return
    from gold.implementation.dimensional.snapshot_reuse import (
        SnapshotReuseContractError,
        resolve_effective_gold_inputs,
    )
    registry_reuse = None
    if registry is not None:
        from gold.implementation.dimensional.snapshot_reuse_registry import registry_artifacts
        registry_reuse = registry_artifacts(registry)
    effective = None
    has_explicit_table_names = all(
        isinstance(artifact, dict) and artifact.get("table_name") == table
        for table, artifact in artifacts.items()
    )
    all_no_data = all(
        isinstance(artifact, dict) and artifact.get("status") == "NO_DATA"
        for artifact in artifacts.values()
    )
    if set(artifacts) == set(TABLES) and (has_explicit_table_names or all_no_data):
        try:
            inline_reuse = {
                table: artifact["snapshot_reuse"]
                for table, artifact in artifacts.items()
                if isinstance(artifact, dict) and "snapshot_reuse" in artifact
            }
            # A validated registry is authoritative for its dimensions.  Keep
            # the legacy inline mapping when no registry is supplied, but do
            # not let an inline artifact bypass a registry-bound identity.
            reuse_artifacts = dict(inline_reuse)
            if registry_reuse is not None:
                reuse_artifacts.update(registry_reuse)
            effective = resolve_effective_gold_inputs(
                artifacts,
                reuse_artifacts=reuse_artifacts,
                feature_enabled=(
                    os.environ.get("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", "false").lower()
                    == "true"
                ),
            )
        except SnapshotReuseContractError as exc:
            raise ValueError(str(exc)) from exc
        if effective["status"] == "GOLD_NO_CHANGE":
            return effective
    no_data_tables = []
    reuse_enabled = os.environ.get("GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED", "false").lower() == "true"
    for table, artifact in artifacts.items():
        if not isinstance(artifact, dict):
            raise ValueError(f"Malformed dimensional artifact envelope: {table}")
        status = artifact.get("status")
        if status == "NO_DATA":
            expected = {"table_name", "status", "reason"}
            allowed_keys = {frozenset(expected)}
            if table in {"customers", "products"} and reuse_enabled:
                if registry_reuse is None and "snapshot_reuse" in artifact:
                    expected.add("snapshot_reuse")
                    allowed_keys = {frozenset(expected)}
                elif registry_reuse is not None:
                    # Inline reuse is accepted for compatibility, but the
                    # validated registry mapping remains authoritative above.
                    allowed_keys.add(frozenset(expected | {"snapshot_reuse"}))
            if (
                frozenset(artifact) not in allowed_keys
                or artifact.get("table_name") != table
                or artifact.get("reason") not in {"NO_DATA", "DRY_RUN"}
            ):
                raise ValueError(f"Malformed NO_DATA dimensional artifact: {table}")
            if table in {"customers", "products"} and reuse_enabled:
                from gold.implementation.dimensional.snapshot_reuse import GoldSnapshotReuseAdapter
                reuse = (registry_reuse or {}).get(table, artifact.get("snapshot_reuse"))
                GoldSnapshotReuseAdapter().resolve(table, reuse)
            else:
                no_data_tables.append(table)
        elif status != "PASS":
            raise ValueError(f"Unknown dimensional artifact status: {table}")
    if no_data_tables:
        if any(table in {"orders", "order_items", "payments", "shipments"} for table in no_data_tables):
            raise ValueError(f"Gold dimensional facts require current Silver PASS: {no_data_tables[0]}")
        table = no_data_tables[0]
        raise AirflowSkipException(f"Gold dimensional skipped: Silver artifact has NO_DATA: {table}")
    return effective


def _bounded_scope_environment():
    """Propagate an already-authorized shared-bucket scope to Spark tasks."""
    if os.environ.get("BRONZE_BUCKET") != "ecommerce-data-lake":
        return {}
    scope = build_bounded_e2e_scope(os.environ)
    return {
        "LAKEHOUSE_E2E_APPROVED": "1",
        "LAKEHOUSE_E2E_TEST_BUCKET": scope.bucket,
        "LAKEHOUSE_E2E_RUN_ID": scope.run_id,
        "LAKEHOUSE_E2E_PREFIX": scope.prefix,
    }


def create_incremental_silver_operator(*, table_name, project_path):
    """Create the Bronze-to-Silver operator for one validated table batch."""
    validation_task_id = f"validate_{table_name}"

    def xcom_value(field):
        return (
            "{{ ti.xcom_pull(task_ids='"
            + validation_task_id
            + "')['"
            + field
            + "'] }}"
        )

    arguments = [
            "--table-name", table_name,
            "--batch-id", xcom_value("batch_id"),
            "--object-key", xcom_value("object_key"),
            "--bronze-checksum", xcom_value("bronze_checksum_sha256"),
            "--runtime-window-id", "{{ dag_run.run_id }}",
        ]
    if table_name in ("payments", "shipments"):
        arguments.extend(["--parent-orders-batch-id", "{{ ti.xcom_pull(task_ids='validate_orders')['batch_id'] }}"])
    operator = create_spark_submit_operator(
        task_id=f"bronze_to_silver_{table_name}",
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/silver/run_incremental.py",
        arguments=arguments,
        retries=0,
        extra_environment=_bounded_scope_environment(),
    )
    return operator


class GoldDimensionalDockerOperator(DockerOperator):
    """Run the complete Gold gate, lineage, write and verification lifecycle."""

    def execute(self, context):
        from common.runtime_policy import is_feature_enabled
        from gold.implementation.dimensional.lineage import build_dimensional_lineage_from_artifacts
        from pipeline_tasks.silver import build_pipeline_summary

        if not is_feature_enabled("GOLD_DIMENSIONAL_ENABLED"):
            raise AirflowSkipException("GOLD_DIMENSIONAL_ENABLED is false")

        ti = context["ti"]
        silver_results = {
            table: ti.xcom_pull(task_ids=f"silver_result_{table}")
            for table in TABLES
        }
        artifacts = {
            table: ti.xcom_pull(task_ids=f"dimensional_parquet_artifact_{table}")
            for table in TABLES
        }
        all_no_data = (
            set(artifacts) == set(TABLES)
            and all(
                isinstance(artifact, dict) and artifact.get("status") == "NO_DATA"
                for artifact in artifacts.values()
            )
        )
        # NO_CHANGE is fully determined by the six artifact envelopes.  Do
        # not require a validate_config XCom in minimal/test contexts for this
        # terminal path; every path that can continue to Gold reads it below.
        config = None if all_no_data else ti.xcom_pull(task_ids="validate_config")
        registry = config.get("gold_snapshot_reuse_registry") if isinstance(config, dict) else None
        effective = _preflight_dimensional_artifacts(artifacts, registry=registry)
        if effective and effective.get("status") == "GOLD_NO_CHANGE":
            return effective
        build_pipeline_summary(silver_results, config)
        bucket = os.environ.get("BRONZE_BUCKET")
        scope = build_bounded_e2e_scope(os.environ) if bucket == "ecommerce-data-lake" else None
        build_dimensional_lineage_from_artifacts(artifacts, scope=scope)
        raw_result = super().execute(context)
        # DockerOperator returns the captured stdout when xcom_all=True.  Keep
        # only the canonical writer attestation in XCom; Spark logs are not a
        # stable or appropriate task contract.
        from pipeline_tasks.gold import _structured_writer_result

        return _structured_writer_result(raw_result)


class HistoricalSparkResultOperator(DockerOperator):
    """Normalize the single JSON result emitted by the historical Spark CLI."""

    @staticmethod
    def _resolve_json_value(value, context):
        resolver = getattr(value, "resolve", None)
        resolved = resolver(context) if callable(resolver) else value
        if not isinstance(resolved, (dict, list)):
            raise RuntimeError("historical Spark input did not resolve to structured JSON")
        return json.dumps(resolved, sort_keys=True, separators=(",", ":"))

    def execute(self, context):
        # Resolve XComArg/task outputs in the worker context.  This avoids
        # Jinja indexing of ti.xcom_pull and prevents StrictUndefined from
        # reaching json serialization during Airflow 3 template rendering.
        self.command = [
            "/opt/spark/bin/spark-submit",
            self._historical_script_path,
            "--stage", self._historical_stage,
            "--plan-json", self._resolve_json_value(self._historical_plan, context),
            self._historical_input_argument,
            self._resolve_json_value(self._historical_input, context),
        ]
        raw_result = super().execute(context)
        candidates = raw_result if isinstance(raw_result, (list, tuple)) else (raw_result,)
        marker = "HISTORICAL_GOLD_RESULT="
        for candidate in reversed(candidates):
            if isinstance(candidate, str) and marker in candidate:
                return json.loads(candidate.split(marker, 1)[1])
            if isinstance(candidate, dict) and "status" in candidate:
                return candidate
        raise RuntimeError("historical Spark CLI emitted no structured result")


def create_historical_spark_operator(*, task_id, project_path, stage, input_argument, plan_value, input_value):
    """Run a historical Silver/Gold stage only inside the pinned Spark image."""
    if stage not in {"silver", "gold"}:
        raise ValueError("historical stage must be silver or gold")
    operator = create_spark_submit_operator(
        task_id=task_id,
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/gold/implementation/historical_gold_cli.py",
        arguments=(),
        extra_environment={
            "HISTORICAL_BACKFILL_WRITE_APPROVED": os.environ.get("HISTORICAL_BACKFILL_WRITE_APPROVED", ""),
            "GOLD_DIMENSIONAL_MODE": os.environ.get("GOLD_DIMENSIONAL_MODE", "DRY_RUN"),
            "GOLD_DIMENSIONAL_ENABLED": os.environ.get("GOLD_DIMENSIONAL_ENABLED", "false"),
            "GOLD_DIMENSIONAL_WRITE_ENABLED": os.environ.get("GOLD_DIMENSIONAL_WRITE_ENABLED", "false"),
        },
        do_xcom_push=True,
        xcom_all=True,
        operator_class=HistoricalSparkResultOperator,
    )
    return _bind_historical_operator(operator, stage, input_argument, plan_value, input_value)


def _bind_historical_operator(operator, stage, input_argument, plan_value, input_value):
    operator._historical_stage = stage
    operator._historical_input_argument = input_argument
    operator._historical_plan = plan_value
    operator._historical_input = input_value
    operator._historical_script_path = f"{SPARK_WORKDIR}/gold/implementation/historical_gold_cli.py"
    return operator


def create_gold_dimensional_operator(*, project_path, mode="DRY_RUN"):
    """Create the single native dimensional graph writer.

    This operator has no PostgreSQL/Warehouse dependency; lineage and Silver
    reads are resolved by the Spark dimensional CLI from verified Parquet data.
    """
    if mode not in {"DRY_RUN", "WRITE"}:
        raise ValueError("Unsupported dimensional Gold mode")
    storage_environment = dict(os.environ)
    storage_environment.setdefault("LAKEHOUSE_STORAGE_ENV", "production")
    storage_environment.setdefault("BRONZE_BUCKET", PRODUCTION_BUCKET)
    validate_storage_environment(storage_environment)
    bucket = storage_environment["BRONZE_BUCKET"]
    scope = None
    if bucket == "ecommerce-data-lake":
        try:
            scope = build_bounded_e2e_scope(os.environ)
        except BoundedE2EScopeError as exc:
            raise ValueError(f"Refusing unscoped ecommerce-data-lake Gold operator: {exc}") from exc
    arguments = [
        "--dim-date-start", os.environ.get("GOLD_DIM_DATE_START", "2020-01-01"),
        "--dim-date-end", os.environ.get("GOLD_DIM_DATE_END", "2035-12-31"),
        "--mode", mode,
        "--bucket", bucket,
    ]
    for table in TABLES:
        arguments.extend([
            "--source-artifact",
            f"{table}={{{{ ti.xcom_pull(task_ids='dimensional_parquet_artifact_{table}') | tojson }}}}",
        ])
    extra_environment = {
        "GOLD_DIMENSIONAL_WRITE_ENABLED": os.environ.get("GOLD_DIMENSIONAL_WRITE_ENABLED", "false"),
        "GOLD_RUN_METADATA_PERSIST_ENABLED": os.environ.get("GOLD_RUN_METADATA_PERSIST_ENABLED", "false"),
        "LAKEHOUSE_STORAGE_ENV": os.environ.get("LAKEHOUSE_STORAGE_ENV", "production"),
        PRODUCTION_WRITE_APPROVAL_ENV: os.environ.get(PRODUCTION_WRITE_APPROVAL_ENV, ""),
    }
    if extra_environment["GOLD_RUN_METADATA_PERSIST_ENABLED"].lower() == "true":
        extra_environment.update({
            "DB_HOST": os.environ.get("DB_HOST", "postgres"),
            "DB_PORT": os.environ.get("DB_PORT", "5432"),
            "DB_NAME": os.environ.get("DB_NAME", "ecommerce_platform"),
            "DB_USER": os.environ.get("DB_USER", "postgres"),
            "DB_PASSWORD": os.environ.get("DB_PASSWORD", ""),
            "GOLD_RUN_ID": os.environ.get("GOLD_RUN_ID", ""),
            "GOLD_CODE_SHA": os.environ.get("GOLD_CODE_SHA", "local000"),
        })
    if scope is not None:
        arguments.extend(["--warehouse", scope.warehouse, "--nessie-ref", scope.nessie_ref])
        target_namespace = os.environ.get("GOLD_TARGET_NAMESPACE", "gold_run_20261112_gold_06")
        extra_environment.update({
            "LAKEHOUSE_E2E_APPROVED": "1",
            "LAKEHOUSE_E2E_TEST_BUCKET": scope.bucket,
            "LAKEHOUSE_E2E_RUN_ID": scope.run_id,
            "LAKEHOUSE_E2E_PREFIX": scope.prefix,
            "GOLD_TARGET_NAMESPACE": target_namespace,
        })
    return create_spark_submit_operator(
        task_id="gold_dimensional_write",
        project_path=project_path,
        script_path=f"{SPARK_WORKDIR}/gold/implementation/dimensional/run_pipeline.py",
        arguments=arguments,
        extra_environment=extra_environment,
        do_xcom_push=True,
        xcom_all=True,
        # The writer may have committed a partial external state before a
        # failure is observed. Recovery must be explicit and evidence-based;
        # never blindly replay a stateful Gold write.
        retries=0,
        operator_class=GoldDimensionalDockerOperator,
    )


def create_gold_dimensional_confirmation_operator(*, table_name):
    """Expose one published Gold Dim/Fact output in the Airflow graph.

    The atomic Iceberg write is performed by ``gold_dimensional_write``;
    this node is a lightweight output marker for table-level observability.
    """
    return EmptyOperator(
        task_id=f"confirm_gold_dimensional_{table_name}",
        task_display_name=f"gold_dimensional_write_{table_name}",
        doc_md=f"Gold output marker for `{table_name}`. The atomic Iceberg write is performed upstream.",
        trigger_rule="none_failed_min_one_success",
        retries=0,
    )


def create_gold_dimensional_graph_verification_operator():
    """Non-writing barrier: the native writer returns only after graph manifest verification."""
    return EmptyOperator(
        task_id="verify_gold_dimensional_graph_manifest",
        trigger_rule="all_success",
        retries=0,
    )


def create_spark_submit_operator(
    *,
    task_id,
    project_path,
    transformation_script=None,
    script_path=None,
    arguments=(),
    extra_environment=None,
    extra_mounts=(),
    retries=IDEMPOTENT_RETRIES,
    do_xcom_push=False,
    xcom_all=False,
    operator_class=DockerOperator,
    state_client: AirflowStateClient | None = None,
):
    """Create a consistently configured Spark DockerOperator.

    Every stage receives the same MinIO credentials and read-only source mounts.
    Pass a transformation-relative script or an explicit absolute ``script_path``.
    """
    environment = {
        "MINIO_ENDPOINT": os.environ["MINIO_ENDPOINT"],
        "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
        "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
        "BRONZE_BUCKET": os.environ["BRONZE_BUCKET"],
        "LAKEHOUSE_STORAGE_ENV": os.environ.get("LAKEHOUSE_STORAGE_ENV", "production"),
        "NESSIE_URI": os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2"),
        "NESSIE_REF": os.environ.get("NESSIE_REF", PRODUCTION_REF),
        "PYTHONPATH": SPARK_PYTHONPATH,
    }
    for name in ("QUALITY_IT_RUN_ID", "QUALITY_IT_PREFIX"):
        if name in os.environ:
            environment[name] = os.environ[name]
    environment.update(extra_environment or {})

    if (transformation_script is None) == (script_path is None):
        raise ValueError(
            "Provide exactly one of transformation_script or script_path"
        )
    submit_script = (
        script_path
        if script_path is not None
        else f"{TRANSFORMATION_TARGET}/{transformation_script}"
    )

    mounts = [
        Mount(
            source=project_path + r"\ingestion",
            target=f"{SPARK_WORKDIR}/ingestion",
            type="bind",
            read_only=True,
        ),
        Mount(
            source=project_path + r"\common",
            target=f"{SPARK_WORKDIR}/common",
            type="bind",
            read_only=True,
        ),
        Mount(
            source=project_path + r"\transformation",
            target=TRANSFORMATION_TARGET,
            type="bind",
            read_only=True,
        ),
        Mount(
            source=project_path + r"\silver",
            target=f"{SPARK_WORKDIR}/silver",
            type="bind",
            read_only=True,
        ),
        Mount(
            source=project_path + r"\gold",
            target=f"{SPARK_WORKDIR}/gold",
            type="bind",
            read_only=True,
        ),
        Mount(
            source=project_path + r"\lakehouse",
            target=f"{SPARK_WORKDIR}/lakehouse",
            type="bind",
            read_only=True,
        ),
    ]
    mounts.extend(extra_mounts)

    # The callback is always present on production operators. It resolves the
    # API transport only in the worker failure path; absent runtime config fails
    # closed instead of opening a metadata DB connection or silently doing nothing.
    operator_kwargs = {
        "on_failure_callback": make_runtime_task_failure_callback(state_client),
    }

    return operator_class(
        task_id=task_id,
        image=SPARK_IMAGE,
        network_mode=DOCKER_NETWORK,
        private_environment=environment,
        command=[
            "/opt/spark/bin/spark-submit",
            submit_script,
            *arguments,
        ],
        mounts=mounts,
        mount_tmp_dir=False,
        do_xcom_push=do_xcom_push,
        xcom_all=xcom_all,
        retries=retries,
        retry_delay=IDEMPOTENT_RETRY_DELAY,
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
        execution_timeout=IDEMPOTENT_EXECUTION_TIMEOUT,
        timeout=DOCKER_API_TIMEOUT_SECONDS,
        **operator_kwargs,
    )
