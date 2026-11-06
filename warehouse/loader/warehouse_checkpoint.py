"""Transactional Warehouse batch checkpoint management."""


def insert_loaded_batch(
    conn,
    batch,
    manifest_key,
    manifest,
    loaded_row_count,
    source_lineage=None,
):
    """
    Insert a successfully loaded Silver batch checkpoint.

    The caller owns the transaction.
    This function never commits or rolls back.
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    if not isinstance(loaded_row_count, int) or loaded_row_count < 0:
        raise ValueError("Invalid loaded_row_count")

    if loaded_row_count != manifest["silver_row_count"]:
        raise ValueError(
            "Loaded row count does not match Silver manifest"
        )

    if (
        manifest["batch_id"] != batch["batch_id"]
        or manifest["table_name"] != batch["table_name"]
        or manifest["status"] != "COMPLETED"
    ):
        raise ValueError("Batch and Silver manifest mismatch")

    if source_lineage is None:
        source_lineage = {
            "source_type": "parquet",
            "iceberg_table": None,
            "iceberg_catalog_ref": None,
            "iceberg_snapshot_id": None,
        }
    try:
        from source_lineage import validate_source_lineage
    except ModuleNotFoundError:
        from warehouse.loader.source_lineage import validate_source_lineage
    source_lineage = validate_source_lineage(**source_lineage)

    query = """
        INSERT INTO warehouse.loaded_batches (
            batch_id,
            table_name,
            watermark_type,
            start_timestamp,
            start_id,
            end_timestamp,
            end_id,
            silver_manifest_key,
            silver_data_prefix,
            silver_row_count,
            loaded_row_count,
            source_type,
            iceberg_table,
            iceberg_catalog_ref,
            iceberg_snapshot_id
        )
        VALUES (
            %(batch_id)s,
            %(table_name)s,
            %(watermark_type)s,
            %(start_timestamp)s,
            %(start_id)s,
            %(end_timestamp)s,
            %(end_id)s,
            %(silver_manifest_key)s,
            %(silver_data_prefix)s,
            %(silver_row_count)s,
            %(loaded_row_count)s,
            %(source_type)s,
            %(iceberg_table)s,
            %(iceberg_catalog_ref)s,
            %(iceberg_snapshot_id)s
        )
    """

    values = {
        "batch_id": batch["batch_id"],
        "table_name": batch["table_name"],
        "watermark_type": batch["watermark_type"],
        "start_timestamp": batch["start_timestamp"],
        "start_id": batch["start_id"],
        "end_timestamp": batch["end_timestamp"],
        "end_id": batch["end_id"],
        "silver_manifest_key": manifest_key,
        "silver_data_prefix": manifest["data_prefix"],
        "silver_row_count": manifest["silver_row_count"],
        "loaded_row_count": loaded_row_count,
        **source_lineage,
    }

    with conn.cursor() as cursor:
        cursor.execute(query, values)
