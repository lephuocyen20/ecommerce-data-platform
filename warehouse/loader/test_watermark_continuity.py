"""Rollback-only integration test for watermark continuity."""

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


def insert_test_checkpoint(conn, batch):
    """Insert a temporary checkpoint without committing."""

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
                batch["batch_id"],
                batch["table_name"],
                batch["watermark_type"],
                batch["start_timestamp"],
                batch["start_id"],
                batch["end_timestamp"],
                batch["end_id"],
                "TEST_ONLY_MANIFEST",
                "TEST_ONLY_PREFIX",
                batch["row_count"],
                batch["row_count"],
            ),
        )


with get_connection() as conn:
    batches = get_committed_batches(conn)

products = sorted(
    (
        batch for batch in batches
        if batch["table_name"] == "products"
    ),
    key=lambda batch: (
        batch["end_timestamp"],
        batch["end_id"],
    ),
)

assert len(products) == 5, "Expected 5 product batches"

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

    # Batch 1 must be accepted as the initial batch.
    assert check_batch_loadable(conn, products[0]) == "READY"
    print("INITIAL BATCH: PASS")

    # Batch 2 cannot load before Batch 1 is checkpointed.
    try:
        check_batch_loadable(conn, products[1])
    except ValueError as exc:
        assert "Missing initial batch" in str(exc)
        print("MISSING INITIAL BATCH REJECTED: PASS")
    else:
        raise AssertionError("Batch 2 was accepted without Batch 1")

    # Simulate Batch 1 having been loaded.
    insert_test_checkpoint(conn, products[0])

    assert check_batch_loadable(conn, products[0]) == "SKIP"
    print("ALREADY LOADED BATCH: PASS")

    assert check_batch_loadable(conn, products[1]) == "READY"
    print("NEXT CONTIGUOUS BATCH: PASS")

    # Batch 3 must not skip Batch 2.
    try:
        check_batch_loadable(conn, products[2])
    except ValueError as exc:
        assert "Watermark continuity violation" in str(exc)
        print("SKIPPED BATCH REJECTED: PASS")
    else:
        raise AssertionError("Skipped batch was accepted")

    # Check the full valid chain.
    for index in range(1, len(products)):
        assert check_batch_loadable(conn, products[index]) == "READY"
        insert_test_checkpoint(conn, products[index])

    print("FULL FIVE-BATCH CHAIN: PASS")

    # Simulate a different batch ID with an older watermark.
    replay = dict(products[0])
    replay["batch_id"] = "ffffffffffffffffffffffffffffffff"

    try:
        check_batch_loadable(conn, replay)
    except ValueError as exc:
        assert "Watermark continuity violation" in str(exc)
        print("OUT-OF-ORDER REPLAY REJECTED: PASS")
    else:
        raise AssertionError("Old watermark was accepted")

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
    "Temporary checkpoints remained after rollback"
)

print("CHECKPOINT ROLLBACK: PASS")
print("FINAL RESULT: PASS")
