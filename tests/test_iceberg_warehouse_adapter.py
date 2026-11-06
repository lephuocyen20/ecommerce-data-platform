import pytest

from lakehouse.warehouse.warehouse_customers_reader import BUSINESS_COLUMNS, IcebergCustomersBatch


@pytest.mark.unit
def test_iceberg_customers_batch_exposes_warehouse_contract():
    batch = IcebergCustomersBatch(
        table="lakehouse.silver.customers", catalog_ref="main", snapshot_id=123,
        records=[{"customer_id": 1, "full_name": "Ada", "email": "a@example.com", "created_at": None, "updated_at": None}],
    )
    assert batch.column_names == BUSINESS_COLUMNS
    assert batch.lineage["source_type"] == "iceberg"
    assert batch.lineage["iceberg_snapshot_id"] == 123


@pytest.mark.unit
def test_iceberg_customers_batch_requires_immutable_snapshot():
    with pytest.raises(TypeError):
        IcebergCustomersBatch(table="x", catalog_ref="main", records=[])
