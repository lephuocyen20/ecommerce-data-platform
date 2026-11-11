"""Fail-closed Gold input mode policy, independent from Warehouse/dbt."""

PARQUET_SNAPSHOT = "parquet_snapshot"
ICEBERG_SNAPSHOT = "iceberg_snapshot"


def resolve_gold_source_mode(value):
    mode = (value or PARQUET_SNAPSHOT).strip().lower()
    if mode not in {PARQUET_SNAPSHOT, ICEBERG_SNAPSHOT}:
        raise ValueError("GOLD_SOURCE_MODE must be parquet_snapshot or iceberg_snapshot")
    return mode


def select_gold_lineage_task(mode, parquet_task, iceberg_task):
    mode = resolve_gold_source_mode(mode)
    if mode == PARQUET_SNAPSHOT:
        return parquet_task
    if iceberg_task is None:
        raise RuntimeError("Iceberg Gold source mode requires verified evidence lineage task")
    return iceberg_task
