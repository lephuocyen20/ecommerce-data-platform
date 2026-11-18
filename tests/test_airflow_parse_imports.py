"""Static guards for the lightweight Airflow DAG parse import chain."""

from pathlib import Path
import importlib.util
import ast


ROOT = Path(__file__).parents[1]


def test_gold_task_module_does_not_import_spark_reader_at_parse_time():
    source = (ROOT / "airflow/dags/pipeline_tasks/gold.py").read_text(encoding="utf-8")
    assert "pyspark" not in source


def test_snapshot_reuse_resolver_is_not_an_airflow_task_boundary():
    path = ROOT / "airflow/dags/pipeline_tasks/gold.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))

    assert "resolve_gold_snapshot_reuse_task" not in source
    assert 'task_id="resolve_gold_snapshot_reuse"' not in source
    assert "controlled_incremental_snapshot_reuse_dry_run_task" in source
    assert "controlled_dry_run_spec" in source

    # Snapshot reuse is allowed only behind the function-local controlled seam.
    # Keep Spark and dimensional reader dependencies out of the module parse
    # import graph so importing the DAG remains lightweight and side-effect free.
    top_level_imports = [
        node
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    assert all("pyspark" not in ast.unparse(node) for node in top_level_imports)
    assert all(
        "gold.implementation.dimensional.reader" not in ast.unparse(node)
        for node in top_level_imports
    )
    assert all(
        imported_name not in ast.unparse(node)
        for node in top_level_imports
        for imported_name in ("SnapshotReuseReader", "GoldSnapshotReuseAdapter")
    )


def test_historical_dag_keeps_spark_and_dimensional_writer_out_of_airflow_parse():
    source = (ROOT / "airflow/dags/historical_production_backfill.py").read_text(encoding="utf-8")
    assert "historical_gold_writer" not in source
    assert "run_historical_gold_candidate" not in source
    assert "run_historical_silver" not in source
    assert "create_historical_spark_operator" in source
    cli = (ROOT / "gold/implementation/historical_gold_cli.py").read_text(encoding="utf-8")
    assert cli.index("from gold.implementation.historical_gold_writer") > cli.index("production_environment(values")


def test_generic_spark_operator_does_not_forward_production_write_approval():
    source = (ROOT / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    generic = source[source.index("def create_spark_submit_operator"):]
    assert '"PRODUCTION_LAKEHOUSE_WRITE_APPROVED"' not in generic


def test_incremental_silver_factory_returns_operator_and_dag_import_graph_is_complete():
    operators_path = ROOT / "airflow/dags/pipeline_operators.py"
    operators_source = operators_path.read_text(encoding="utf-8")
    operators_tree = ast.parse(operators_source, filename=str(operators_path))
    factory = next(
        node
        for node in operators_tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "create_incremental_silver_operator"
    )

    class SentinelOperator:
        task_id = "bronze_to_silver_orders"

    sentinel = SentinelOperator()
    namespace = {
        "SPARK_WORKDIR": "/opt/spark/work-dir",
        "_bounded_scope_environment": lambda: {},
        "create_spark_submit_operator": lambda **kwargs: sentinel,
    }
    exec(compile(ast.Module(body=[factory], type_ignores=[]), str(operators_path), "exec"), namespace)
    assert namespace["create_incremental_silver_operator"](
        table_name="orders", project_path=str(ROOT)
    ) is sentinel

    dag_path = ROOT / "airflow/dags/ecommerce_incremental_ingestion.py"
    dag_source = dag_path.read_text(encoding="utf-8")
    ast.parse(dag_source, filename=str(dag_path))
    assert "transforms[table] = create_incremental_silver_operator" in dag_source
    assert "eligibility[table] >> [skip, transforms[table]]" in dag_source


def test_gold_write_factory_parses_without_forwarding_production_approval(monkeypatch):
    operators_path = ROOT / "airflow/dags/pipeline_operators.py"
    operators_source = operators_path.read_text(encoding="utf-8")
    operators_tree = ast.parse(operators_source, filename=str(operators_path))
    factory = next(
        node
        for node in operators_tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "create_gold_dimensional_operator"
    )

    class SentinelOperator:
        pass

    sentinel = SentinelOperator()
    monkeypatch.delenv("PRODUCTION_LAKEHOUSE_WRITE_APPROVED", raising=False)
    namespace = {
        "os": __import__("os"),
        "PRODUCTION_BUCKET": "ecommerce-lakehouse-prod",
        "TABLES": (),
        "SPARK_WORKDIR": "/opt/spark/work-dir",
        "PRODUCTION_WRITE_APPROVAL_ENV": "PRODUCTION_LAKEHOUSE_WRITE_APPROVED",
        "Mount": lambda **kwargs: kwargs,
        "GoldDimensionalDockerOperator": object,
        "validate_storage_environment": lambda _environment: None,
        "create_spark_submit_operator": lambda **kwargs: sentinel,
        "build_bounded_e2e_scope": lambda _environment: None,
        "BoundedE2EScopeError": RuntimeError,
    }
    exec(compile(ast.Module(body=[factory], type_ignores=[]), str(operators_path), "exec"), namespace)

    assert namespace["create_gold_dimensional_operator"](
        project_path=str(ROOT), mode="WRITE"
    ) is sentinel
    assert '"PRODUCTION_LAKEHOUSE_WRITE_APPROVED"' not in operators_source[
        operators_source.index("def create_gold_dimensional_operator") : operators_source.index(
            "def create_gold_dimensional_confirmation_operator"
        )
    ]


def test_historical_operator_resolves_xcomarg_structures_without_jinja_indexing():
    dag_source = (ROOT / "airflow/dags/historical_production_backfill.py").read_text(encoding="utf-8")
    operators = (ROOT / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    assert "ti.xcom_pull(task_ids='silver_transform_manifest')['artifacts']" not in dag_source
    assert "ti.xcom_pull(task_ids='bronze_validate_upload_metadata')['source']" not in dag_source
    assert "input_value=bronze_result," in dag_source
    assert "input_value=bronze_result[\"source\"]" not in dag_source
    assert '"source": compact' not in dag_source
    assert "input_value=silver_result.output," in dag_source
    assert "silver_result.output[\"artifacts\"]" not in dag_source
    assert "candidate(gold_result.output, plan)" in dag_source
    assert ".output[\"source\"]" not in dag_source
    assert "ti.xcom_pull" not in dag_source
    assert "_resolve_json_value" in operators
    assert "json.dumps(resolved, sort_keys=True, separators=(\",\", \":\"))" in operators
    assert "StrictUndefined" in operators


def test_historical_extract_boundary_returns_json_safe_metadata_without_records():
    import json
    from datetime import datetime
    from decimal import Decimal

    source = (ROOT / "airflow/dags/historical_production_backfill.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    helper = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_compact_bronze_metadata")
    namespace = {"json": json}
    exec(compile(ast.Module(body=[helper], type_ignores=[]), "historical_dag_helper", "exec"), namespace)
    raw = {
        "status": "PASS",
        "run_id": "run-d",
        "bronze": {
            "orders": {
                "table": "orders",
                "object_key": "bronze/orders/run-d/data.csv",
                "manifest_key": "bronze/orders/run-d/manifest.json",
                "row_count": 1,
                "checksum_sha256": "a" * 64,
                "records": [{"created_at": datetime.now(), "amount": Decimal("1.25")}],
            }
        },
    }
    compact = namespace["_compact_bronze_metadata"](raw)
    json_payload = json.dumps(compact, sort_keys=True)
    assert "records" not in json_payload
    assert "created_at" not in json_payload
    assert "amount" not in json_payload
    assert compact["bronze"]["orders"]["table"] == "orders"


def test_gold_writer_uses_deterministic_task_id_for_xcom_pull():
    source = (ROOT / "airflow/dags/ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")
    assert 'source_task_id = f"prepare_gold_sources_{name}"' not in source
    assert "prepare_gold_sources_" not in source


def test_pipeline_summary_receives_all_silver_results_explicitly():
    dag_source = (ROOT / "airflow/dags/ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")
    silver_source = (ROOT / "airflow/dags/pipeline_tasks/silver.py").read_text(encoding="utf-8")
    gold_source = (ROOT / "airflow/dags/pipeline_tasks/gold.py").read_text(encoding="utf-8")
    assert "create_gold_dimensional_operator(" in dag_source
    assert "list(dimensional_artifacts.values()) >> dimensional_writer" in dag_source
    assert "def build_pipeline_summary(silver_results: dict, config: dict):" in silver_source
    assert "build_pipeline_summary(silver_results, config)" in gold_source
    assert 'set(silver_results) != set(config["tables"])' in silver_source
    assert 'ti.xcom_pull(task_ids=f"silver_result_{table_name}")' not in silver_source


def test_incremental_dag_uses_15_minute_schedule_and_serializes_runs():
    dag_source = (ROOT / "airflow/dags/ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")
    assert 'schedule="*/15 * * * *"' in dag_source
    assert "catchup=False" in dag_source
    assert "max_active_runs=1" in dag_source


def test_gold_writer_is_not_blindly_retried_after_external_state_failure():
    operators_source = (ROOT / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    writer_start = operators_source.index("def create_gold_dimensional_operator")
    confirmation_start = operators_source.index("def create_gold_dimensional_confirmation_operator", writer_start)
    writer_source = operators_source[writer_start:confirmation_start]
    assert "retries=0" in writer_source
    assert "execution_timeout=IDEMPOTENT_EXECUTION_TIMEOUT" in operators_source


def test_production_write_configuration_remains_fail_closed_and_explicitly_wired():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert 'GOLD_DIMENSIONAL_ENABLED: "${GOLD_DIMENSIONAL_ENABLED:-false}"' in compose
    assert 'GOLD_DIMENSIONAL_MODE: "${GOLD_DIMENSIONAL_MODE:-DRY_RUN}"' in compose
    assert 'GOLD_DIMENSIONAL_WRITE_ENABLED: "${GOLD_DIMENSIONAL_WRITE_ENABLED:-false}"' in compose
    assert "PRODUCTION_LAKEHOUSE_WRITE_APPROVED: ${PRODUCTION_LAKEHOUSE_WRITE_APPROVED:-}" in compose
    assert "GOLD_DIMENSIONAL_ENABLED=false" in env_example
    assert "GOLD_DIMENSIONAL_MODE=DRY_RUN" in env_example
    assert "GOLD_DIMENSIONAL_WRITE_ENABLED=false" in env_example
    assert "PRODUCTION_LAKEHOUSE_WRITE_APPROVED=" in env_example


def test_native_dimensional_group_has_one_writer_and_non_writing_confirmations():
    dag_source = (ROOT / "airflow/dags/ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")
    operators = (ROOT / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    assert "create_gold_dimensional_operator" in dag_source
    assert "create_gold_dimensional_confirmation_operator(table_name=table)" in dag_source
    assert '"gold_dimensional_write"' in operators
    assert 'task_id=f"confirm_gold_dimensional_{table_name}"' in operators
    assert 'task_display_name=f"gold_dimensional_write_{table_name}"' in operators
    assert "| tojson" in operators
    assert "for confirmation in confirmations:" in dag_source
    assert "active_gold_publish >> confirmation" in dag_source
    assert "publish_gold_to_active_task" in dag_source
    assert "dimensional_verify >> confirmation" not in dag_source
    assert "dimensional_gate" not in dag_source
    gold_tasks = (ROOT / "airflow/dags/pipeline_tasks/gold.py").read_text(encoding="utf-8")
    assert "build_dimensional_parquet_artifact_task" in dag_source
    assert "artifacts[table] = artifact" in gold_tasks
    assert "create_lakehouse_migration_operator" not in dag_source
    assert "create_lakehouse_attestation_operator" not in dag_source
    assert "create_lakehouse_reconcile_operator" not in dag_source
    assert "create_lakehouse_quality_operator" not in dag_source
    assert "build_dimensional_artifact_task" not in dag_source
    assert "list_objects" not in gold_tasks
    assert "GOLD_DIMENSIONAL_SOURCE_LINEAGE" not in dag_source
    assert 'os.environ.get("GOLD_DIM_DATE_START"' in operators
    assert 'os.environ.get("GOLD_DIM_DATE_END"' in operators
    assert 'os.environ.get("BRONZE_BUCKET"' in operators
    assert "env_var('GOLD_DIM_DATE_START')" not in operators
    assert "1970-01-01" not in operators and "2100-01-01" not in operators
    assert "GoldDimensionalDockerOperator" in operators
    assert "do_xcom_push=True" in operators
    assert "GOLD_RUN_METADATA_PERSIST_ENABLED" in operators
    assert '"DB_HOST"' in operators
    assert '"GOLD_RUN_ID"' in operators
    assert "xcom_all=True" in operators
    assert "[dimensional_writer, dimensional_skip] >> confirmations" not in dag_source


def test_gold_writer_has_explicit_publish_barrier():
    dag_source = (ROOT / "airflow/dags/ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")
    assert "active_gold_publish = publish_gold_to_active_task(dimensional_writer.output)" in dag_source
    assert "dimensional_writer >> active_gold_publish" in dag_source


def test_publish_task_receives_writer_output_xcomarg_and_gold_writer_is_upstream():
    dag_path = ROOT / "airflow/dags/ecommerce_incremental_ingestion.py"
    dag_source = dag_path.read_text(encoding="utf-8")
    tree = ast.parse(dag_source, filename=str(dag_path))

    publish_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "publish_gold_to_active_task"
    ]
    assert len(publish_calls) == 1
    publish_argument = publish_calls[0].args[0]
    assert isinstance(publish_argument, ast.Attribute)
    assert isinstance(publish_argument.value, ast.Name)
    assert publish_argument.value.id == "dimensional_writer"
    assert publish_argument.attr == "output"

    operators_source = (ROOT / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    writer_start = operators_source.index("def create_gold_dimensional_operator")
    confirmation_start = operators_source.index("def create_gold_dimensional_confirmation_operator", writer_start)
    writer_source = operators_source[writer_start:confirmation_start]
    assert 'task_id="gold_dimensional_write"' in writer_source
    assert "dimensional_writer >> active_gold_publish" in dag_source


def test_gold_operator_consumes_the_canonical_six_source_tables():
    contracts = (ROOT / "airflow/dags/pipeline_contracts.py").read_text(encoding="utf-8")
    operators = (ROOT / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    assert "TABLES = SUPPORTED_TABLES" in contracts
    assert "from pipeline_contracts import TABLES" in operators
    assert 'for table in ("customers", "order_items", "orders", "payments", "shipments", "products")' not in operators
    assert "for table in TABLES" in operators


def test_dag_groups_show_table_lineage_across_bronze_silver_and_gold():
    source = (ROOT / "airflow/dags/ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")
    for group_id in ("bronze", "silver_parquet", "gold_dimensions", "gold_facts"):
        assert f'group_id="{group_id}"' in source
    assert "silver_finalize" not in source
    assert "gold_control" not in source
    assert "silver_completion_barrier" not in source
    assert "skip_gold_dimensional" not in source
    for table in ("customers", "products", "orders", "order_items", "payments", "shipments"):
        assert f'group_id=f"bronze_{{table}}"' in source
        assert f'group_id=f"silver_{{table}}"' in source
    for table in ("dim_date", "dim_customer", "dim_product", "fact_orders", "fact_order_items", "fact_payments", "fact_shipments"):
        assert f'create_gold_dimensional_confirmation_operator(table_name=table)' in source
        assert table in source


def test_child_silver_transforms_wait_for_verified_orders_manifest():
    source = (ROOT / "airflow/dags/ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")

    assert 'verify["orders"] >> transforms[table]' in source
    assert 'validation["orders"] >> transforms[table]' not in source
    assert 'verify_silver_manifest.override(task_id=f"verify_silver_{table}")' in source
    assert source.index('verify["orders"] >> transforms[table]') < source.index("transforms[table] >> verify[table]")


def test_child_silver_transforms_keep_eligibility_and_validation_inputs():
    source = (ROOT / "airflow/dags/ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")

    assert "eligibility[table] = check_silver_eligibility" in source
    assert "eligibility[table] >> [skip, transforms[table]]" in source
    assert "verify[table] = verify_silver_manifest" in source
    assert "results[table] = silver_result" in source


def test_child_silver_manifest_verification_uses_verified_orders_xcom_identity():
    source = (ROOT / "airflow/dags/pipeline_tasks/silver.py").read_text(encoding="utf-8")

    assert 'xcom_pull(task_ids="verify_silver_orders")' in source
    assert 'parent_coverage.get("runtime_window_id") != runtime_window_id' in source
    assert 'coverage.get("parent_coverage_identity") != parent_coverage["coverage_identity"]' in source
    assert 'reference.get("manifest_key") != parent_manifest_key' in source
    assert 'reference.get("manifest_identity") != parent_manifest_identity' in source
    assert '"manifest_identity": canonical_manifest_identity(manifest)' in source


def test_gold_artifact_builder_uses_canonical_manifest_identity():
    source = (ROOT / "airflow/dags/pipeline_tasks/gold.py").read_text(encoding="utf-8")

    assert "from common.manifest import canonical_manifest_identity" in source
    assert "identity = canonical_manifest_identity(current)" in source


def test_independent_silver_tables_do_not_require_orders_xcom():
    source = (ROOT / "airflow/dags/pipeline_tasks/silver.py").read_text(encoding="utf-8")

    child_branch = source.split('elif table_name in PARENT_ORDERS_TABLES:', 1)[1]
    assert 'xcom_pull(task_ids="verify_silver_orders")' in child_branch
    assert 'table_name in PARENT_ORDERS_TABLES' in source


def test_dimensional_gate_is_independent_and_fail_closed():
    path = ROOT / "airflow/dags/gold_gate_policy.py"
    spec = importlib.util.spec_from_file_location("gold_gate_policy_test", path)
    policy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(policy)
    assert policy.decide_gold_dimensional_gate({}, enabled=False) == "skip_gold_dimensional"
    summary = {"status": "PASS", "mode": "WRITE", "results": {"orders": {}}}
    assert policy.decide_gold_dimensional_gate(summary, enabled=True) == "resolve_gold_dimensional_lineage"


def test_spark_operator_mounts_required_read_only_packages_for_silver_transforms():
    operators = (ROOT / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    assert 'source=project_path + r"\\ingestion"' in operators
    assert 'target=f"{SPARK_WORKDIR}/ingestion"' in operators
    assert 'PYTHONPATH": SPARK_PYTHONPATH' in operators
    lakehouse_start = operators.index('source=project_path + r"\\lakehouse"')
    lakehouse_end = operators.index("        ),", lakehouse_start) + len("        ),")
    lakehouse_mount = operators[lakehouse_start:lakehouse_end]
    assert 'target=f"{SPARK_WORKDIR}/lakehouse"' in lakehouse_mount
    assert 'type="bind"' in lakehouse_mount
    assert "read_only=True" in lakehouse_mount


def test_dimensional_verifier_allows_local_and_approved_bounded_buckets():
    source = (ROOT / "airflow/dags/pipeline_tasks/gold.py").read_text(encoding="utf-8")
    assert 'bucket not in {QUALITY_BUCKET, TEST_BUCKET}' in source
    assert 'scope = build_bounded_e2e_scope(os.environ) if bucket == TEST_BUCKET else None' in source
    assert 'verify_completed_graph(writer_result, create_minio_client(), bucket, scope=scope)' in source


def test_quality_scope_is_propagated_to_spark_operator_environment():
    operators = (ROOT / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    assert '"QUALITY_IT_RUN_ID"' in operators
    assert '"QUALITY_IT_PREFIX"' in operators


def test_client_paths_validate_storage_before_initialization():
    gold_cli = (ROOT / "gold/implementation/dimensional/run_pipeline.py").read_text(encoding="utf-8")
    silver_cli = (ROOT / "silver/incremental.py").read_text(encoding="utf-8")
    gold_tasks = (ROOT / "airflow/dags/pipeline_tasks/gold.py").read_text(encoding="utf-8")
    for source, client_call in (
        (gold_cli, "create_minio_client()"),
        (silver_cli, "get_minio_client()"),
        (gold_tasks, "create_minio_client()"),
    ):
        assert source.index("validate_storage_environment") < source.index(client_call)


def test_silver_manifest_verifier_validates_storage_before_minio_and_uses_path_helpers():
    source = (ROOT / "airflow/dags/pipeline_tasks/silver.py").read_text(encoding="utf-8")
    assert "validate_storage_environment" in source
    assert "silver_incremental_batch_paths" in source
    assert source.index("validate_storage_environment()") < source.index("Minio(")
    assert 'silver_paths["manifest_key"]' in source
    assert 'silver_paths["runs_prefix"]' in source
    assert 're.fullmatch(r"[0-9a-f]{32}/", data_prefix[len(prefix):])' in source
    assert "silver_incremental/{table_name}" not in source


def test_spark_child_env_defaults_nessie_ref_to_production_ref():
    source = (ROOT / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    assert 'PRODUCTION_REF, validate_storage_environment' in source
    assert '"NESSIE_REF": os.environ.get("NESSIE_REF", PRODUCTION_REF)' in source
    assert 'os.environ.get("NESSIE_REF", "main")' not in source
