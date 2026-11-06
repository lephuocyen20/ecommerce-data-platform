"""Load one verified Silver batch into PostgreSQL Raw Warehouse."""

from raw_upsert import upsert_raw_batch
from warehouse_checkpoint import insert_loaded_batch
from warehouse_guards import (
    acquire_table_lock,
    check_batch_loadable,
)
from warehouse_prepare import prepare_raw_table


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
    # Legacy Parquet verification requires pyarrow. Keep that dependency out
    # of the Iceberg adapter path, which supplies its own row contract.
    from verify_silver_data import load_verified_silver_batch
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


def load_iceberg_customers_batch(conn, batch, iceberg_batch):
    """Load one pinned Iceberg customers snapshot with existing Warehouse guards.

    `batch` remains the incremental Bronze checkpoint identity.  The synthetic
    manifest only satisfies the legacy non-null checkpoint fields; authoritative
    source lineage is recorded in the Iceberg columns added by migration 003.
    """
    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")
    if batch.get("table_name") != "customers":
        raise ValueError("Iceberg loader currently supports customers only")
    lineage = iceberg_batch.lineage
    prepared_table = prepare_raw_table("customers", iceberg_batch)
    acquire_table_lock(conn, "customers")
    decision = check_batch_loadable(conn, batch)
    if decision == "SKIP":
        return {"table_name": "customers", "batch_id": batch["batch_id"], "status": "SKIPPED", "loaded_row_count": 0}
    if decision != "READY":
        raise ValueError(f"Unexpected guard decision: {decision}")
    loaded_row_count = upsert_raw_batch(conn, "customers", batch["batch_id"], prepared_table)
    snapshot_id = lineage["iceberg_snapshot_id"]
    manifest_key = f"iceberg/{lineage['iceberg_table']}/ref={lineage['iceberg_catalog_ref']}/snapshot_id={snapshot_id}"
    manifest = {
        "status": "COMPLETED",
        "table_name": "customers",
        "batch_id": batch["batch_id"],
        "data_prefix": manifest_key,
        "silver_row_count": len(iceberg_batch.records),
    }
    insert_loaded_batch(conn, batch, manifest_key, manifest, loaded_row_count, source_lineage=lineage)
    return {"table_name": "customers", "batch_id": batch["batch_id"], "status": "LOADED", "loaded_row_count": loaded_row_count, "source_type": "iceberg", "iceberg_snapshot_id": snapshot_id}


def load_iceberg_products_batch(conn, batch, iceberg_batch):
    """Load a pinned Iceberg products snapshot with the existing safeguards."""
    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")
    if batch.get("table_name") != "products":
        raise ValueError("Iceberg products loader requires a products batch")
    lineage = iceberg_batch.lineage
    prepared_table = prepare_raw_table("products", iceberg_batch)
    acquire_table_lock(conn, "products")
    decision = check_batch_loadable(conn, batch)
    if decision == "SKIP":
        return {"table_name": "products", "batch_id": batch["batch_id"], "status": "SKIPPED", "loaded_row_count": 0}
    if decision != "READY":
        raise ValueError(f"Unexpected guard decision: {decision}")
    loaded = upsert_raw_batch(conn, "products", batch["batch_id"], prepared_table)
    snapshot = lineage["iceberg_snapshot_id"]
    key = f"iceberg/{lineage['iceberg_table']}/ref={lineage['iceberg_catalog_ref']}/snapshot_id={snapshot}"
    manifest = {"status": "COMPLETED", "table_name": "products", "batch_id": batch["batch_id"], "data_prefix": key, "silver_row_count": len(iceberg_batch.records)}
    insert_loaded_batch(conn, batch, key, manifest, loaded, source_lineage=lineage)
    return {"table_name": "products", "batch_id": batch["batch_id"], "status": "LOADED", "loaded_row_count": loaded, "source_type": "iceberg", "iceberg_snapshot_id": snapshot}


def load_iceberg_orders_batch(conn, batch, iceberg_batch):
    """Load a pinned Iceberg orders snapshot with transactional safeguards."""
    if conn.autocommit: raise ValueError("Autocommit must be disabled")
    if batch.get("table_name") != "orders": raise ValueError("Iceberg orders loader requires an orders batch")
    lineage = iceberg_batch.lineage; prepared = prepare_raw_table("orders", iceberg_batch); acquire_table_lock(conn, "orders"); decision = check_batch_loadable(conn, batch)
    if decision == "SKIP": return {"table_name": "orders", "batch_id": batch["batch_id"], "status": "SKIPPED", "loaded_row_count": 0}
    if decision != "READY": raise ValueError(f"Unexpected guard decision: {decision}")
    loaded = upsert_raw_batch(conn, "orders", batch["batch_id"], prepared); snapshot = lineage["iceberg_snapshot_id"]; key = f"iceberg/{lineage['iceberg_table']}/ref={lineage['iceberg_catalog_ref']}/snapshot_id={snapshot}"
    manifest = {"status": "COMPLETED", "table_name": "orders", "batch_id": batch["batch_id"], "data_prefix": key, "silver_row_count": len(iceberg_batch.records)}
    insert_loaded_batch(conn, batch, key, manifest, loaded, source_lineage=lineage)
    return {"table_name": "orders", "batch_id": batch["batch_id"], "status": "LOADED", "loaded_row_count": loaded, "source_type": "iceberg", "iceberg_snapshot_id": snapshot}
