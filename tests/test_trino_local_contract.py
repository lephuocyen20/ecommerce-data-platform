import json
from pathlib import Path

import yaml

from gold.implementation.gold_table_registry import GOLD_TABLES, RUN06_SCHEMA


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_PATH = ROOT / "docker-compose.trino.local.yml"
TRINO = ROOT / "trino" / "etc"
EXPECTED_TABLES = set(GOLD_TABLES)
CERTIFIED_SCHEMA = RUN06_SCHEMA


def _compose():
    return yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))


def test_trino_is_pinned_private_and_isolated():
    compose = _compose()
    service = compose["services"]["trino"]
    assert compose["x-trino-local-contract"]["nessie_ref"] == "bi_gold_current"
    assert service["image"] == "trinodb/trino:476"
    assert service["ports"] == ["127.0.0.1:${TRINO_PORT:-8081}:8080"]
    assert service["restart"] == "no"
    assert "TRINO_NESSIE_URI" in service["environment"]
    assert service["volumes"] == ["./trino/etc:/etc/trino:ro"]
    assert service["networks"] == ["quality-it-local", "metabase-local", "root-local"]
    assert "trino" not in yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8")).get("services", {})


def test_trino_uses_existing_named_local_networks_only():
    compose = _compose()
    assert compose["networks"]["quality-it-local"] == {
        "name": "quality-it-local-${QUALITY_IT_RUN_ID:-dev}",
        "external": True,
    }
    assert compose["networks"]["metabase-local"] == {
        "name": "ecommerce-metabase-local-${METABASE_RUN_ID:-dev}",
        "external": True,
    }
    assert compose["networks"]["root-local"] == {
        "name": "ecommerce-data-platform_default",
        "external": True,
    }


def test_catalog_is_nessie_v2_pinned_to_certified_run06_and_minio():
    catalog = (TRINO / "catalog" / "lakehouse.properties").read_text(encoding="utf-8")
    assert "connector.name=iceberg" in catalog
    assert "iceberg.catalog.type=nessie" in catalog
    assert "iceberg.nessie-catalog.uri=${ENV:TRINO_NESSIE_URI}" in catalog
    assert "iceberg.nessie-catalog.ref=bi_gold_current" in catalog
    assert "iceberg.nessie-catalog.default-warehouse-dir=s3://ecommerce-data-lake" in catalog
    assert "iceberg.nessie-catalog.client-api-version=V2" in catalog
    assert "s3.endpoint=http://minio:9000" in catalog
    assert "s3.path-style-access=true" in catalog
    assert "TRINO_S3_ACCESS_KEY" in catalog and "TRINO_S3_SECRET_KEY" in catalog
    assert "TRINO_NESSIE_REF" not in catalog
    assert "19132" not in catalog


def test_trino_accepts_metabase_presto_protocol_header():
    config = (TRINO / "config.properties").read_text(encoding="utf-8")
    assert "protocol.v1.alternate-header-name=Presto" in config


def test_node_data_dir_uses_container_writable_ephemeral_path():
    node_properties = (TRINO / "node.properties").read_text(encoding="utf-8")
    assert "node.environment=local_readonly" in node_properties
    assert "node.environment=local-readonly" not in node_properties
    assert "node.data-dir=/tmp/trino-data" in node_properties
    assert "/var/trino/data" not in node_properties


def test_access_policy_is_default_deny_and_select_only_for_seven_gold_tables():
    rules = json.loads((TRINO / "rules.json").read_text(encoding="utf-8"))
    assert rules["catalogs"][-1] == {"catalog": ".*", "allow": "none"}
    assert rules["catalogs"][0] == {
        "user": "metabase",
        "catalog": "lakehouse",
        "allow": "read-only",
    }
    table_rules = rules["tables"]
    assert {(rule["schema"], rule["table"]) for rule in table_rules} == {
        (CERTIFIED_SCHEMA, table) for table in EXPECTED_TABLES
    }
    assert {rule["user"] for rule in table_rules} == {"metabase"}
    assert all(rule["privileges"] == ["SELECT"] for rule in table_rules)
    assert not any(
        privilege in {"INSERT", "DELETE", "UPDATE", "OWNERSHIP", "GRANT_SELECT"}
        for rule in table_rules
        for privilege in rule["privileges"]
    )


def test_compose_has_no_write_credentials_or_maintenance_contract():
    compose_text = COMPOSE_PATH.read_text(encoding="utf-8")
    catalog_text = (TRINO / "catalog" / "lakehouse.properties").read_text(encoding="utf-8")
    for forbidden in ("MINIO_ROOT", "AWS_SECRET_ACCESS_KEY", "access-key=admin", "execute optimize", "expire_snapshots", "remove_orphan_files"):
        assert forbidden not in compose_text
        assert forbidden not in catalog_text
    service_env = _compose()["services"]["trino"]["environment"]
    assert "TRINO_S3_READONLY_ACCESS_KEY" in service_env["TRINO_S3_ACCESS_KEY"]
    assert "TRINO_S3_READONLY_SECRET_KEY" in service_env["TRINO_S3_SECRET_KEY"]


def test_native_metabase_connection_contract_is_documented():
    runbook = (ROOT / "docs" / "RUNBOOK.md").read_text(encoding="utf-8")
    assert "native Trino" in runbook
    assert "host `trino`" in runbook
    assert "catalog `lakehouse`" in runbook
    assert f"schema `{CERTIFIED_SCHEMA}`" in runbook
    assert "TRINO_NESSIE_URI=http://host.docker.internal:19132/api/v2" in runbook
    assert "user `metabase`" in runbook
