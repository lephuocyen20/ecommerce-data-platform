import json
from pathlib import Path

ROOT = Path(__file__).parents[1]
COMPOSE = (ROOT / "docker-compose.metabase-rest-trial.local.yml").read_text(encoding="utf-8")
SCRIPT = (ROOT / "scripts/validate_metabase_rest_trial.py").read_text(encoding="utf-8")


def test_trial_isolated_and_pinned():
    assert "metabase/metabase:v0.55.3" in COMPOSE
    assert "postgres:16.4" in COMPOSE
    assert '127.0.0.1:${METABASE_TRIAL_PORT:-3001}:3000' in COMPOSE
    assert "metabase_rest_trial_metadata" in COMPOSE
    assert "internal: true" in COMPOSE
    assert "docker-compose.metabase.local.yml" not in COMPOSE
    assert "metabase_metadata" not in COMPOSE
    assert "active_cutover: false" in COMPOSE


def test_trial_targets_only_rest_postgres_trino():
    assert "trino-rest-postgres-validation:8080" in COMPOSE
    assert 'trino_catalog: "lakehouse_rest_postgres"' in COMPOSE
    assert 'trino_schema: "gold_run_20261112_gold_06"' in COMPOSE
    assert "NESSIE" not in COMPOSE.upper()
    assert "production: false" in COMPOSE


def test_trial_validation_is_session_bound_and_read_only():
    assert "METABASE_TRIAL_SESSION" in SCRIPT
    assert "METABASE_TRIAL_DATABASE_ID" in SCRIPT
    assert '"/api/database/{database_id}"' in SCRIPT
    assert '"/api/dataset"' in SCRIPT
    assert 'SELECT count(*) AS row_count' in SCRIPT
    assert 'rows != [[1]]' in SCRIPT
    for forbidden in ("INSERT", "UPDATE", "DELETE", "MERGE", "DROP", "ALTER", "TRUNCATE", "CREATE", "CALL"):
        assert forbidden in SCRIPT
    assert "127.0.0.1:3001" in SCRIPT
    assert "METABASE_INTERNAL_URL must target the approved local trial endpoint" in (ROOT / "scripts/run_metabase_rest_trial.py").read_text(encoding="utf-8")
