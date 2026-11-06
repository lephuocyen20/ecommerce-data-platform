"""Read a pinned Iceberg orders snapshot for the Warehouse adapter."""
from dataclasses import dataclass
from typing import Dict, List
TABLE = "lakehouse.silver.orders"
BUSINESS_COLUMNS = ("order_id", "customer_id", "status", "order_date", "updated_at")
@dataclass(frozen=True)
class IcebergOrdersBatch:
    table: str; catalog_ref: str; snapshot_id: int; records: List[Dict]
    @property
    def column_names(self): return BUSINESS_COLUMNS
    def to_pylist(self): return self.records
    def select(self, columns):
        selected = tuple(columns)
        if set(selected) != set(BUSINESS_COLUMNS): raise ValueError("Orders selection must preserve all business columns")
        return IcebergOrdersBatch(self.table, self.catalog_ref, self.snapshot_id, [{column: row[column] for column in selected} for row in self.records])
    @property
    def lineage(self): return {"source_type": "iceberg", "iceberg_table": self.table, "iceberg_catalog_ref": self.catalog_ref, "iceberg_snapshot_id": self.snapshot_id}
def read_orders_snapshot(spark, snapshot_id=None, catalog_ref="main", table=TABLE):
    snapshots = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    selected = int(snapshot_id) if snapshot_id is not None else int(snapshots[0].snapshot_id)
    frame = spark.read.format("iceberg").option("snapshot-id", selected).load(table)
    if set(BUSINESS_COLUMNS) - set(frame.columns): raise ValueError("Iceberg orders schema mismatch")
    return IcebergOrdersBatch(table, catalog_ref, selected, [row.asDict(recursive=False) for row in frame.select(*BUSINESS_COLUMNS).toLocalIterator()])
