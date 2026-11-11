from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "docker-compose.quality-it.local.yml"


def _compose():
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


def test_local_quality_it_ports_bind_loopback_only():
    services = _compose()["services"]
    for binding in services["nessie"]["ports"]:
        assert str(binding).startswith("127.0.0.1:")


def test_local_quality_it_isolated_and_fail_closed():
    compose = _compose()
    assert compose["networks"]["quality-it-local"]["name"].startswith("quality-it-local-")
    assert "minio" not in compose["services"]
    spark_env = compose["services"]["spark"]["environment"]
    assert spark_env["MINIO_ENDPOINT"] == "${QUALITY_IT_MINIO_ENDPOINT:-host.docker.internal:9000}"
    assert spark_env["QUALITY_IT_ENABLED"] == "false"
    assert spark_env["QUALITY_IT_GATE2_APPROVED"] == "0"


def test_local_quality_it_has_health_gates():
    compose = _compose()
    assert compose["services"]["nessie"]["depends_on"]["minio-init"]["condition"] == "service_completed_successfully"
    assert compose["services"]["nessie-ready"]["depends_on"]["nessie"]["condition"] == "service_started"
    assert compose["services"]["spark"]["depends_on"]["nessie-ready"]["condition"] == "service_completed_successfully"


def test_local_quality_it_avoids_external_mc_image_dependency():
    compose = _compose()
    assert compose["services"]["minio-init"]["image"] == "ecommerce-spark:3.5.6"
    assert compose["services"]["minio-init"]["build"]["dockerfile"] == "spark/Dockerfile"


def test_local_quality_it_overrides_spark_defaults_for_quality_bucket():
    defaults = (ROOT / "spark" / "conf" / "spark-defaults.quality-it.local.conf").read_text(encoding="utf-8")
    assert "s3a://ecommerce-quality-it/lakehouse" in defaults
    assert "http://nessie:19120/api/v2" in defaults
    assert "host.docker.internal:9000" in defaults
    assert "./spark/conf/spark-defaults.quality-it.local.conf:/opt/spark/conf/spark-defaults.conf:ro" in _compose()["services"]["spark"]["volumes"]


def test_local_quality_it_targets_only_dedicated_bucket():
    compose_text = COMPOSE.read_text(encoding="utf-8")
    assert "ecommerce-quality-it" in compose_text
    assert "ecommerce-data-lake" not in compose_text


def test_root_compose_passes_native_gold_and_nessie_env_with_safe_defaults():
    root = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    for key in ("AIRFLOW_LOCAL_E2E_MODE", "GOLD_DIMENSIONAL_ENABLED", "GOLD_DIMENSIONAL_MODE", "GOLD_DIMENSIONAL_WRITE_ENABLED", "GOLD_DIM_DATE_START", "GOLD_DIM_DATE_END", "NESSIE_URI", "NESSIE_REF"):
        assert key in root
    assert 'AIRFLOW_LOCAL_E2E_MODE:-' in root
    assert 'GOLD_DIMENSIONAL_ENABLED:-false' in root
    assert 'GOLD_DIMENSIONAL_WRITE_ENABLED:-false' in root


def test_local_e2e_selects_parquet_for_all_silver_tables():
    config = (ROOT / ".env.quality-it.local.example").read_text(encoding="utf-8")
    assert "INGESTION_WRITE_ENABLED=true" in config
    assert "AIRFLOW_LOCAL_E2E_MODE=WRITE" in config
    for table in ("CUSTOMERS", "PRODUCTS", "ORDERS", "ORDER_ITEMS", "PAYMENTS", "SHIPMENTS"):
        assert f"{table}_SILVER_SOURCE=parquet" in config


def test_local_e2e_env_selects_write_mode_for_all_tables(monkeypatch):
    import sys

    dags_path = str(ROOT / "airflow" / "dags")
    if dags_path not in sys.path:
        sys.path.insert(0, dags_path)
    import pipeline_config

    monkeypatch.setenv("BRONZE_BUCKET", "ecommerce-quality-it")
    monkeypatch.setenv("AIRFLOW_LOCAL_E2E_MODE", "WRITE")
    monkeypatch.setenv("INGESTION_WRITE_ENABLED", "true")
    result = pipeline_config.validate_incremental_run_config({})
    assert result["mode"] == "WRITE"
    assert len(result["tables"]) == 6


def test_local_e2e_write_selection_drives_all_silver_branches():
    dag_source = (ROOT / "airflow" / "dags" / "ecommerce_incremental_ingestion.py").read_text(encoding="utf-8")
    assert "config = validate_config()" in dag_source
    assert "eligibility = {}" in dag_source
    assert "transforms = {}" in dag_source
    assert "create_incremental_silver_operator(table_name=table, project_path=PROJECT_PATH)" in dag_source
    assert "eligibility[table] >> [skip, transforms[table]]" in dag_source
    assert "list(dimensional_artifacts.values()) >> dimensional_writer" in dag_source
    assert "for table in TABLES" in dag_source


def test_write_selection_remains_fail_closed_outside_local_profile(monkeypatch):
    import sys

    dags_path = str(ROOT / "airflow" / "dags")
    if dags_path not in sys.path:
        sys.path.insert(0, dags_path)
    import pipeline_config

    monkeypatch.delenv("BRONZE_BUCKET", raising=False)
    monkeypatch.delenv("AIRFLOW_LOCAL_E2E_MODE", raising=False)
    monkeypatch.delenv("INGESTION_WRITE_ENABLED", raising=False)
    result = pipeline_config.validate_incremental_run_config({})
    assert result["mode"] == "DRY_RUN"


def test_local_write_selection_still_requires_ingestion_write_flag(monkeypatch):
    import sys

    dags_path = str(ROOT / "airflow" / "dags")
    if dags_path not in sys.path:
        sys.path.insert(0, dags_path)
    import pipeline_config

    monkeypatch.setenv("BRONZE_BUCKET", "ecommerce-quality-it")
    monkeypatch.setenv("AIRFLOW_LOCAL_E2E_MODE", "WRITE")
    monkeypatch.delenv("INGESTION_WRITE_ENABLED", raising=False)
    import pytest

    with pytest.raises(RuntimeError, match="INGESTION_WRITE_ENABLED"):
        pipeline_config.validate_incremental_run_config({})
