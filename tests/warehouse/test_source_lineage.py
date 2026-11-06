import pytest

from warehouse.loader.source_lineage import validate_source_lineage


def test_parquet_lineage_is_legacy_compatible():
    assert validate_source_lineage("parquet") == {"source_type": "parquet", "iceberg_table": None, "iceberg_catalog_ref": None, "iceberg_snapshot_id": None}


def test_iceberg_lineage_requires_complete_snapshot_identity():
    assert validate_source_lineage("iceberg", iceberg_table="lakehouse.silver.customers", iceberg_catalog_ref="main", iceberg_snapshot_id=10)["iceberg_snapshot_id"] == 10
    with pytest.raises(ValueError, match="snapshot"):
        validate_source_lineage("iceberg", iceberg_table="x", iceberg_catalog_ref="main", iceberg_snapshot_id=None)
