from tests.run_gold_dimensional_local_fixture import LOCAL_BUCKET, PROJECT_ROOT, TABLES, _manifest_identity
from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
from gold.implementation.dimensional.lineage import validate_dimensional_lineage
from lakehouse.models.spec import SPECS


def test_local_fixture_is_allowlisted_to_quality_bucket():
    assert LOCAL_BUCKET == "ecommerce-quality-it"
    assert "ecommerce-data-lake" not in LOCAL_BUCKET
    assert len(TABLES) == 6


def test_fixture_manifest_identity_is_deterministic_and_sha256():
    identity = _manifest_identity("orders", 123)
    assert len(identity) == 64
    assert int(identity, 16) >= 0


def test_runner_bootstraps_project_import_root():
    assert (PROJECT_ROOT / "common").is_dir()
    assert (PROJECT_ROOT / "gold").is_dir()


def test_fixture_uses_contract_casts_for_temporal_columns():
    assert SPECS["customers"].casts["created_at"] == "timestamp"
    assert SPECS["orders"].casts["order_date"] == "timestamp"
    assert SPECS["shipments"].casts["estimated_delivery_at"] == "timestamp"


def test_all_fixture_tables_have_supported_explicit_contract_types():
    supported = {"string", "bigint", "int", "timestamp", "date", "decimal(12,2)"}
    for table in TABLES:
        assert set(SPECS[table].casts.values()) <= supported


def test_dimensional_lineage_contract_uses_graph_source_set():
    assert set(DIMENSIONAL_SOURCE_TABLES) == {"customers", "order_items", "orders", "products", "payments", "shipments"}
    assert validate_dimensional_lineage is not None


def test_fixture_runner_never_uses_destructive_cleanup():
    source = (PROJECT_ROOT / "tests/run_gold_dimensional_local_fixture.py").read_text(encoding="utf-8")
    assert "DROP TABLE" not in source
    assert "CREATE TABLE IF NOT EXISTS" in source
    assert 'MERGE INTO {identifier} target USING _local_{table} source' in source
    assert 'WHEN NOT MATCHED THEN INSERT' in source


def test_fixture_seeds_all_six_sources_even_when_table_is_partially_populated():
    source = (PROJECT_ROOT / "tests/run_gold_dimensional_local_fixture.py").read_text(encoding="utf-8")
    assert "for table in TABLES" in source
    assert "partially seeded local stack" in source
    assert "if not spark.table(identifier).limit(1).count()" not in source
