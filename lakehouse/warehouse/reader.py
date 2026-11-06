"""Generic pinned Iceberg snapshot adapter for Warehouse Raw."""
from dataclasses import dataclass
from typing import Dict, List

BUSINESS_COLUMNS = {
    "customers": ("customer_id", "full_name", "email", "created_at", "updated_at"),
    "products": ("product_id", "product_name", "stock_quantity", "price", "created_at", "updated_at"),
    "orders": ("order_id", "customer_id", "status", "order_date", "updated_at"),
    "order_items": ("order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount"),
    "payments": ("payment_id", "order_id", "amount", "payment_method", "status", "transaction_id", "created_at"),
    "shipments": ("shipment_id", "order_id", "carrier", "tracking_number", "shipping_address", "shipping_fee", "status", "shipped_at", "delivered_at", "estimated_delivery_at", "created_at", "updated_at"),
}

@dataclass(frozen=True)
class IcebergBatch:
    table: str; catalog_ref: str; snapshot_id: int; records: List[Dict]; columns: tuple
    @property
    def column_names(self): return self.columns
    def to_pylist(self): return self.records
    def select(self, columns):
        selected = tuple(columns)
        if set(selected) != set(self.columns): raise ValueError("Warehouse selection must preserve all business columns")
        return IcebergBatch(self.table, self.catalog_ref, self.snapshot_id, [{column: row[column] for column in selected} for row in self.records], selected)
    @property
    def lineage(self): return {"source_type": "iceberg", "iceberg_table": self.table, "iceberg_catalog_ref": self.catalog_ref, "iceberg_snapshot_id": self.snapshot_id}

def read_snapshot(spark, table_name, snapshot_id=None, catalog_ref="main"):
    columns = BUSINESS_COLUMNS[table_name]; table = f"lakehouse.silver.{table_name}"
    snapshots = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    if not snapshots and snapshot_id is None: raise ValueError(f"Iceberg {table_name} table has no snapshot")
    selected = int(snapshot_id) if snapshot_id is not None else int(snapshots[0].snapshot_id)
    frame = spark.read.format("iceberg").option("snapshot-id", selected).load(table)
    missing = set(columns) - set(frame.columns)
    if missing: raise ValueError(f"Iceberg {table_name} schema missing: {sorted(missing)}")
    return IcebergBatch(table, catalog_ref, selected, [row.asDict(recursive=False) for row in frame.select(*columns).toLocalIterator()], columns)
