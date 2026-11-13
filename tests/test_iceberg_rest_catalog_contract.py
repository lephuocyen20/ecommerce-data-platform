import json
from pathlib import Path


ROOT = Path(__file__).parents[1]
COMPOSE = (ROOT / "docker-compose.iceberg-rest-catalog-postgres.local.yml").read_text(encoding="utf-8")
DOCKERFILE = (ROOT / "iceberg-rest-catalog/postgres/Dockerfile").read_text(encoding="utf-8")


def test_persistent_rest_catalog_is_pinned_isolated_and_not_active():
    assert "local/iceberg-rest-fixture-postgres:1.10.0-jdbc42.7.4" in COMPOSE
    assert "postgres:16.4" in COMPOSE
    assert 'runtime_status: "PERSISTENT_JDBC_RUNTIME_PASS"' in COMPOSE
    assert "iceberg_rest_postgres_metadata" in COMPOSE
    assert "internal: true" in COMPOSE
    assert "active: false" in COMPOSE
    assert "cutover: false" in COMPOSE
    assert "docker-compose.metabase.local.yml" not in COMPOSE
    assert "TRINO_NESSIE_URI" not in COMPOSE


def test_custom_image_pins_jdbc_and_explicit_classpath():
    assert "FROM busybox:1.36.1 AS jdbc-fetch" in DOCKERFILE
    assert "FROM apache/iceberg-rest-fixture:1.10.0" in DOCKERFILE
    assert "postgresql/42.7.4/postgresql-42.7.4.jar" in DOCKERFILE
    assert "ADD --checksum=sha256:188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e" in DOCKERFILE
    assert "org.apache.iceberg.rest.RESTCatalogServer" in DOCKERFILE
    assert "iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql-42.7.4.jar" in DOCKERFILE


def test_persistent_validation_is_select_only_and_separate():
    catalog = (ROOT / "trino/rest-catalog-postgres-validation/etc/catalog/lakehouse_rest_postgres.properties").read_text(encoding="utf-8")
    rules = (ROOT / "trino/rest-catalog-postgres-validation/etc/rules.json").read_text(encoding="utf-8")
    assert "iceberg.catalog.type=rest" in catalog
    assert "iceberg.security=read_only" in catalog
    assert '"catalog": "lakehouse_rest_postgres"' in rules
    policy = json.loads(rules)
    data_tables = [item for item in policy["tables"] if "$" not in item["table"]]
    assert len(data_tables) == 7
    assert all(item["privileges"] == ["SELECT"] for item in data_tables)


def test_retained_trial_is_not_active_metabase():
    trial = (ROOT / "docker-compose.metabase-rest-trial.local.yml").read_text(encoding="utf-8")
    assert "metabase/metabase:v0.55.3" in trial
    assert "active_cutover: false" in trial
    assert "lakehouse_rest_postgres" in trial
