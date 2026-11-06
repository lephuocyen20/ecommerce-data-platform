"""Read a pinned Iceberg customers snapshot for the Warehouse adapter."""

from dataclasses import dataclass
from typing import Dict, List


TABLE = "lakehouse.silver.customers"
BUSINESS_COLUMNS = ("customer_id", "full_name", "email", "created_at", "updated_at")


@dataclass(frozen=True)
class IcebergCustomersBatch:
    table: str
    catalog_ref: str
    snapshot_id: int
    records: List[Dict]

    @property
    def column_names(self):
        return BUSINESS_COLUMNS

    def to_pylist(self):
        return self.records

    def select(self, columns):
        """Provide the small Arrow-table protocol used by warehouse_prepare."""
        selected = tuple(columns)
        if set(selected) != set(BUSINESS_COLUMNS):
            raise ValueError("Iceberg customers selection must preserve all business columns")
        return IcebergCustomersBatch(
            table=self.table,
            catalog_ref=self.catalog_ref,
            snapshot_id=self.snapshot_id,
            records=[{column: record[column] for column in selected} for record in self.records],
        )

    @property
    def lineage(self):
        return {
            "source_type": "iceberg",
            "iceberg_table": self.table,
            "iceberg_catalog_ref": self.catalog_ref,
            "iceberg_snapshot_id": self.snapshot_id,
        }


def latest_snapshot_id(spark, table=TABLE):
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    if not rows:
        raise ValueError("Iceberg customers table has no snapshot")
    return int(rows[0].snapshot_id)


def read_customers_snapshot(spark, snapshot_id=None, catalog_ref="main", table=TABLE):
    """Return Warehouse-compatible customer records pinned to one snapshot.

    This bounded dimension adapter deliberately materializes only the customers
    entity. Fact-table migrations should use a distributed JDBC staging path.
    """
    selected_snapshot = latest_snapshot_id(spark, table) if snapshot_id is None else int(snapshot_id)
    frame = spark.read.format("iceberg").option("snapshot-id", selected_snapshot).load(table)
    missing = set(BUSINESS_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"Iceberg customers schema missing: {sorted(missing)}")
    records = [row.asDict(recursive=False) for row in frame.select(*BUSINESS_COLUMNS).toLocalIterator()]
    return IcebergCustomersBatch(table=table, catalog_ref=catalog_ref, snapshot_id=selected_snapshot, records=records)
