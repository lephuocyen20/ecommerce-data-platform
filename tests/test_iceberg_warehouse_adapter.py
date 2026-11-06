import pytest

from lakehouse.warehouse.reader import BUSINESS_COLUMNS, IcebergBatch


@pytest.mark.unit
def test_iceberg_customers_batch_exposes_warehouse_contract():
    batch = IcebergBatch(
        table="lakehouse.silver.customers", catalog_ref="main", snapshot_id=123,
        records=[{"customer_id": 1, "full_name": "Ada", "email": "a@example.com", "created_at": None, "updated_at": None}], columns=BUSINESS_COLUMNS["customers"],
    )
    assert batch.column_names == BUSINESS_COLUMNS["customers"]
    assert batch.lineage["source_type"] == "iceberg"
    assert batch.lineage["iceberg_snapshot_id"] == 123


@pytest.mark.unit
def test_iceberg_customers_batch_requires_immutable_snapshot():
    with pytest.raises(TypeError):
        IcebergBatch(table="x", catalog_ref="main", records=[], columns=())
