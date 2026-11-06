from dataclasses import dataclass
from typing import Dict, List
TABLE="lakehouse.silver.order_items"
BUSINESS_COLUMNS=("order_item_id","order_id","product_id","quantity","unit_price","discount_amount")
@dataclass(frozen=True)
class IcebergOrderItemsBatch:
    table:str; catalog_ref:str; snapshot_id:int; records:List[Dict]
    @property
    def column_names(self): return BUSINESS_COLUMNS
    def to_pylist(self): return self.records
    def select(self, columns):
        if set(columns)!=set(BUSINESS_COLUMNS): raise ValueError("Order-items selection must preserve all business columns")
        return IcebergOrderItemsBatch(self.table,self.catalog_ref,self.snapshot_id,[{c:r[c] for c in columns} for r in self.records])
    @property
    def lineage(self): return {"source_type":"iceberg","iceberg_table":self.table,"iceberg_catalog_ref":self.catalog_ref,"iceberg_snapshot_id":self.snapshot_id}
def read_order_items_snapshot(spark,snapshot_id=None,catalog_ref="main",table=TABLE):
    snapshots=spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect(); selected=int(snapshot_id) if snapshot_id else int(snapshots[0].snapshot_id)
    frame=spark.read.format("iceberg").option("snapshot-id",selected).load(table)
    return IcebergOrderItemsBatch(table,catalog_ref,selected,[r.asDict(recursive=False) for r in frame.select(*BUSINESS_COLUMNS).toLocalIterator()])
