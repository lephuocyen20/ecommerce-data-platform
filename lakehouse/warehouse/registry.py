"""Table registry for generic Iceberg-to-Warehouse loading."""

from lakehouse.migration.registry import TABLES
from lakehouse.warehouse.reader import read_snapshot


def warehouse_api(table_name):
    if table_name not in TABLES:
        raise ValueError(f"Unsupported Warehouse table: {table_name}")
    from importlib import import_module
    loader_module = import_module("warehouse_batch_loader")
    loader = getattr(loader_module, f"load_iceberg_{table_name}_batch")
    return lambda spark: read_snapshot(spark, table_name), loader
