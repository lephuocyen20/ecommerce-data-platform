"""Static guards for the lightweight Airflow DAG parse import chain."""

from pathlib import Path
import importlib.util


ROOT = Path(__file__).parents[1]


def test_gold_task_module_does_not_import_spark_reader_at_parse_time():
    source = (ROOT / "airflow/dags/pipeline_tasks/gold.py").read_text(encoding="utf-8")
    assert "pyspark" not in source


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
    assert "dimensional_writer >> confirmation" in dag_source
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
    assert "xcom_all=True" not in operators
    assert "[dimensional_writer, dimensional_skip] >> confirmations" not in dag_source


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


def test_dimensional_gate_is_independent_and_fail_closed():
    path = ROOT / "airflow/dags/gold_gate_policy.py"
    spec = importlib.util.spec_from_file_location("gold_gate_policy_test", path)
    policy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(policy)
    assert policy.decide_gold_dimensional_gate({}, enabled=False) == "skip_gold_dimensional"
    summary = {"status": "PASS", "mode": "WRITE", "results": {"orders": {}}}
    assert policy.decide_gold_dimensional_gate(summary, enabled=True) == "resolve_gold_dimensional_lineage"


def test_dimensional_verifier_allows_local_and_approved_bounded_buckets():
    source = (ROOT / "airflow/dags/pipeline_tasks/gold.py").read_text(encoding="utf-8")
    assert 'bucket not in {"ecommerce-quality-it", TEST_BUCKET}' in source
    assert 'scope = build_bounded_e2e_scope(os.environ) if bucket == TEST_BUCKET else None' in source
    assert 'verify_completed_graph(writer_result, create_minio_client(), bucket, scope=scope)' in source
