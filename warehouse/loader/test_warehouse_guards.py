"""Rollback-only integration test for Warehouse guards."""

import sys

sys.path.insert(0, "warehouse/loader")

from incremental_silver_to_warehouse import (
    get_connection,
    get_committed_batches,
)
from warehouse_guards import (
    acquire_table_lock,
    check_batch_loadable,
)


with get_connection() as conn:
    batches = get_committed_batches(conn)

products = [
    batch for batch in batches
    if batch["table_name"] == "products"
]

products.sort(
    key=lambda batch: (
        batch["end_timestamp"],
        batch["end_id"],
    )
)

assert len(products) == 5, (
    f"Expected 5 product batches, got {len(products)}"
)

oldest = products[0]
newest = products[-1]

conn = get_connection()

try:
    conn.autocommit = False

    acquire_table_lock(conn, "products")
    print("ADVISORY LOCK: PASS")

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM warehouse.loaded_batches
            WHERE table_name = 'products'
            """
        )
        assert cursor.fetchone()[0] == 0, (
            "Product checkpoints already exist"
        )

    assert check_batch_loadable(conn, oldest) == "READY"
    print("NEW BATCH READY: PASS")

    # Insert a temporary checkpoint for the newest batch.
    # No actual Raw data is modified.
    with conn.cursor() as cursor:
        cursor.execute(
            """
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
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            """,
            (
                newest["batch_id"],
                newest["table_name"],
                newest["watermark_type"],
                newest["start_timestamp"],
                newest["start_id"],
                newest["end_timestamp"],
                newest["end_id"],
                "TEST_ONLY_MANIFEST",
                "TEST_ONLY_PREFIX",
                newest["row_count"],
                newest["row_count"],
            ),
        )

    assert check_batch_loadable(conn, newest) == "SKIP"
    print("LOADED BATCH SKIP: PASS")

    try:
        check_batch_loadable(conn, oldest)
    except ValueError as exc:
        assert "Out-of-order" in str(exc)
        print("OUT-OF-ORDER REJECTED: PASS")
    else:
        raise AssertionError(
            "Older batch was not rejected"
        )

finally:
    conn.rollback()
    conn.close()

with get_connection() as verify_conn:
    with verify_conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM warehouse.loaded_batches
            WHERE table_name = 'products'
            """
        )
        remaining = cursor.fetchone()[0]

assert remaining == 0, (
    "Test checkpoint remained after rollback"
)

print("CHECKPOINT ROLLBACK: PASS")
print("FINAL RESULT: PASS")
