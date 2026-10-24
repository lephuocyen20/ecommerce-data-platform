"""Load one verified Silver batch into PostgreSQL Raw Warehouse."""

from raw_upsert import upsert_raw_batch
from warehouse_checkpoint import insert_loaded_batch
from warehouse_guards import (
    acquire_table_lock,
    check_batch_loadable,
)
from warehouse_prepare import prepare_raw_table
from verify_silver_data import load_verified_silver_batch


def load_one_batch(conn, client, bucket, batch):
    """
    Load one Silver batch and insert its Warehouse checkpoint.

    The caller owns the transaction.

    This function NEVER commits or rolls back.
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    table_name = batch["table_name"]
    batch_id = batch["batch_id"]

    # Verify source data before performing any Warehouse writes.
    manifest_key, manifest, arrow_table = (
        load_verified_silver_batch(client, bucket, batch)
    )

    prepared_table = prepare_raw_table(
        table_name,
        arrow_table,
    )

    # Serialize loaders working on the same source table.
    acquire_table_lock(conn, table_name)

    # Recheck after acquiring the lock.
    # Another loader may have committed this batch meanwhile.
    decision = check_batch_loadable(conn, batch)

    if decision == "SKIP":
        return {
            "table_name": table_name,
            "batch_id": batch_id,
            "status": "SKIPPED",
            "loaded_row_count": 0,
        }

    if decision != "READY":
        raise ValueError(
            f"Unexpected guard decision: {decision}"
        )

    # Raw UPSERT and checkpoint belong to the same transaction.
    loaded_row_count = upsert_raw_batch(
        conn,
        table_name,
        batch_id,
        prepared_table,
    )

    insert_loaded_batch(
        conn,
        batch,
        manifest_key,
        manifest,
        loaded_row_count,
    )

    return {
        "table_name": table_name,
        "batch_id": batch_id,
        "status": "LOADED",
        "loaded_row_count": loaded_row_count,
    }
