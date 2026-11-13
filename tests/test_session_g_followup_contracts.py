from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_mutable_checkpoint_migration_has_fail_safe_conversion():
    sql = (ROOT / "source/database/migrations/004_add_mutable_source_change_signals.sql").read_text(encoding="utf-8")
    assert "Cannot convert % ID watermark" in sql
    assert "Cannot convert % ID batches" in sql
    assert "watermark_type = ''TIMESTAMP''" in sql
    assert "last_timestamp" in sql and "updated_at" in sql


def test_native_dimensional_path_has_default_off_metadata_guard():
    source = (ROOT / "gold/implementation/dimensional/run_pipeline.py").read_text(encoding="utf-8")
    assert "GOLD_RUN_METADATA_PERSIST_ENABLED" in source
    assert "build_runtime_dimensional_metadata" in source
    assert "persist_if_enabled" in source
    # Runtime identity fallbacks are explicit non-secret sentinels; this test
    # protects the metadata write guard rather than banning that sentinel.
    assert 'os.environ.get("GIT_COMMIT_SHA", "unknown")' in source
    assert 'os.environ.get("GOLD_CONFIG_IDENTITY", "unknown")' in source


def test_runtime_metadata_does_not_require_parse_time_json():
    source = (ROOT / "airflow/dags/pipeline_operators.py").read_text(encoding="utf-8")
    assert "GOLD_RUN_METADATA_JSON" not in source
    assert '"DB_HOST"' in source


def test_tombstone_migration_enforces_database_consistency():
    sql = (ROOT / "source/database/migrations/004_add_mutable_source_change_signals.sql").read_text(encoding="utf-8")
    assert "tombstone_state_ck" in sql
    assert "deleted_at IS NOT NULL" in sql


def test_metadata_does_not_substitute_attestation_for_bronze_batch():
    source = (ROOT / "gold/implementation/run_metadata.py").read_text(encoding="utf-8")
    assert 'source.get("bronze_batch_id")' in source
    assert 'bronze_batch_ids={table: source["snapshot_attestation_identity"]' not in source
