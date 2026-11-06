import pytest
import sys
from pathlib import Path

from lakehouse.warehouse_order_items_reader import BUSINESS_COLUMNS, IcebergOrderItemsBatch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "warehouse" / "loader"))
import warehouse_batch_loader


@pytest.mark.unit
def test_iceberg_order_items_batch_exposes_warehouse_contract():
    batch = IcebergOrderItemsBatch(
        table="lakehouse.silver.order_items",
        catalog_ref="main",
        snapshot_id=456,
        records=[{
            "order_item_id": 1, "order_id": 2, "product_id": 3,
            "quantity": 4, "unit_price": 9.99, "discount_amount": 0,
        }],
    )

    assert batch.column_names == BUSINESS_COLUMNS
    assert batch.lineage == {
        "source_type": "iceberg", "iceberg_table": "lakehouse.silver.order_items",
        "iceberg_catalog_ref": "main", "iceberg_snapshot_id": 456,
    }
    assert batch.select(tuple(reversed(BUSINESS_COLUMNS))).records[0]["order_item_id"] == 1


@pytest.mark.unit
def test_iceberg_order_items_batch_requires_immutable_snapshot():
    with pytest.raises(TypeError):
        IcebergOrderItemsBatch(table="x", catalog_ref="main", records=[])


@pytest.mark.unit
def test_iceberg_order_items_batch_rejects_partial_selection():
    batch = IcebergOrderItemsBatch("x", "main", 1, [])
    with pytest.raises(ValueError, match="preserve"):
        batch.select(("order_item_id",))


@pytest.mark.unit
def test_order_items_loader_records_immutable_iceberg_lineage(monkeypatch):
    calls = {}
    source = IcebergOrderItemsBatch(
        "lakehouse.silver.order_items", "main", 456,
        [{"order_item_id": 1, "order_id": 2, "product_id": 3, "quantity": 4, "unit_price": 9.99, "discount_amount": 0}],
    )
    batch = {"table_name": "order_items", "batch_id": "a" * 32}

    monkeypatch.setattr(warehouse_batch_loader, "prepare_raw_table", lambda table, value: (calls.setdefault("prepared", (table, value)), value)[1])
    monkeypatch.setattr(warehouse_batch_loader, "acquire_table_lock", lambda conn, table: calls.setdefault("locked", table))
    monkeypatch.setattr(warehouse_batch_loader, "check_batch_loadable", lambda conn, value: "READY")
    monkeypatch.setattr(warehouse_batch_loader, "upsert_raw_batch", lambda conn, table, batch_id, value: calls.setdefault("upsert", (table, batch_id, value)) and 1)
    monkeypatch.setattr(warehouse_batch_loader, "insert_loaded_batch", lambda conn, value, key, manifest, loaded, source_lineage: calls.setdefault("checkpoint", (key, manifest, loaded, source_lineage)))

    result = warehouse_batch_loader.load_iceberg_order_items_batch(type("Conn", (), {"autocommit": False})(), batch, source)

    assert result["status"] == "LOADED"
    assert calls["locked"] == "order_items"
    key, manifest, loaded, lineage = calls["checkpoint"]
    assert key.endswith("snapshot_id=456")
    assert manifest["silver_row_count"] == loaded == 1
    assert lineage == source.lineage


@pytest.mark.unit
def test_order_items_loader_requires_transaction():
    source = IcebergOrderItemsBatch("lakehouse.silver.order_items", "main", 1, [])
    with pytest.raises(ValueError, match="Autocommit"):
        warehouse_batch_loader.load_iceberg_order_items_batch(type("Conn", (), {"autocommit": True})(), {"table_name": "order_items", "batch_id": "a" * 32}, source)
