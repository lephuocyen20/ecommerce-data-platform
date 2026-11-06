"""Source-lineage contracts shared by Parquet and Iceberg Warehouse loaders."""


def validate_source_lineage(source_type, *, iceberg_table=None, iceberg_catalog_ref=None, iceberg_snapshot_id=None):
    if source_type == "parquet":
        if any(value is not None for value in (iceberg_table, iceberg_catalog_ref, iceberg_snapshot_id)):
            raise ValueError("Parquet lineage must not contain Iceberg fields")
        return {"source_type": "parquet", "iceberg_table": None, "iceberg_catalog_ref": None, "iceberg_snapshot_id": None}
    if source_type == "iceberg":
        if not isinstance(iceberg_table, str) or not iceberg_table:
            raise ValueError("Iceberg lineage requires iceberg_table")
        if not isinstance(iceberg_catalog_ref, str) or not iceberg_catalog_ref:
            raise ValueError("Iceberg lineage requires iceberg_catalog_ref")
        if not isinstance(iceberg_snapshot_id, int) or iceberg_snapshot_id <= 0:
            raise ValueError("Iceberg lineage requires a positive snapshot ID")
        return {"source_type": source_type, "iceberg_table": iceberg_table, "iceberg_catalog_ref": iceberg_catalog_ref, "iceberg_snapshot_id": iceberg_snapshot_id}
    raise ValueError("Unsupported Warehouse source_type")
