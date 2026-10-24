"""Prepare verified Silver tables for Raw Warehouse UPSERT."""

from raw_upsert import TABLE_COLUMNS


def prepare_raw_table(table_name, arrow_table):
    """
    Reorder verified Silver columns to match Raw Warehouse.

    Reject missing or unexpected columns.
    """

    if table_name not in TABLE_COLUMNS:
        raise ValueError(f"Unsupported table: {table_name}")

    expected = TABLE_COLUMNS[table_name]
    actual = arrow_table.column_names

    if len(actual) != len(set(actual)):
        raise ValueError("Duplicate Silver column names")

    if set(actual) != set(expected):
        raise ValueError(
            f"Column mismatch for {table_name}: "
            f"expected={expected}, actual={actual}"
        )

    return arrow_table.select(list(expected))
