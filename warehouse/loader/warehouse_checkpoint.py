"""Transactional Warehouse batch checkpoint management."""


def insert_loaded_batch(
    conn,
    batch,
    manifest_key,
    manifest,
    loaded_row_count,
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
            loaded_row_count
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
            %(loaded_row_count)s
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
    }

    with conn.cursor() as cursor:
        cursor.execute(query, values)
