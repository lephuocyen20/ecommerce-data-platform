"""Read a pinned Iceberg products snapshot for the Warehouse adapter."""

from dataclasses import dataclass
from typing import Dict, List


TABLE = "lakehouse.silver.products"
BUSINESS_COLUMNS = ("product_id", "product_name", "stock_quantity", "price", "created_at", "updated_at")


@dataclass(frozen=True)
class IcebergProductsBatch:
    table: str
    catalog_ref: str
    snapshot_id: int
    records: List[Dict]

    @property
    def column_names(self): return BUSINESS_COLUMNS
    def to_pylist(self): return self.records
    def select(self, columns):
        selected = tuple(columns)
        if set(selected) != set(BUSINESS_COLUMNS): raise ValueError("Products selection must preserve all business columns")
        return IcebergProductsBatch(self.table, self.catalog_ref, self.snapshot_id, [{column: row[column] for column in selected} for row in self.records])
    @property
    def lineage(self): return {"source_type": "iceberg", "iceberg_table": self.table, "iceberg_catalog_ref": self.catalog_ref, "iceberg_snapshot_id": self.snapshot_id}


def read_products_snapshot(spark, snapshot_id=None, catalog_ref="main", table=TABLE):
    snapshots = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    selected = int(snapshot_id) if snapshot_id is not None else int(snapshots[0].snapshot_id)
    frame = spark.read.format("iceberg").option("snapshot-id", selected).load(table)
    if set(BUSINESS_COLUMNS) - set(frame.columns): raise ValueError("Iceberg products schema mismatch")
    return IcebergProductsBatch(table, catalog_ref, selected, [row.asDict(recursive=False) for row in frame.select(*BUSINESS_COLUMNS).toLocalIterator()])
