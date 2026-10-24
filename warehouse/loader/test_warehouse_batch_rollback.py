"""End-to-end Warehouse batch test with mandatory rollback."""

from incremental_silver_to_warehouse import (
    get_connection,
    get_committed_batches,
    get_minio_client,
)
from warehouse_batch_loader import load_one_batch

import os


def count_rows(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        )
        return cursor.fetchone()[0]


def main():
    bucket = os.environ.get(
        "MINIO_BUCKET",
        "ecommerce-data-lake",
    )

    # Fetch metadata and select the customers batch.
    with get_connection() as conn:
        batches = get_committed_batches(conn)

    customers = [
        batch
        for batch in batches
        if batch["table_name"] == "customers"
    ]

    assert len(customers) == 1, (
        f"Expected one customers batch, got {len(customers)}"
    )

    batch = customers[0]
    client = get_minio_client()

    conn = get_connection()

    try:
        conn.autocommit = False

        # Verify the Warehouse is empty before testing.
        assert count_rows(conn, "raw.customers") == 0
        assert count_rows(conn, "warehouse.loaded_batches") == 0

        print("INITIAL WAREHOUSE EMPTY: PASS")

        # Full pipeline, but no commit.
        result = load_one_batch(
            conn,
            client,
            bucket,
            batch,
        )

        assert result["status"] == "LOADED"
        assert result["loaded_row_count"] == 100

        print("SILVER TO RAW LOADER: PASS")

        raw_count = count_rows(conn, "raw.customers")

        assert raw_count == 100, (
            f"Expected 100 Raw rows, got {raw_count}"
        )

        print("RAW DATA INSIDE TRANSACTION: PASS")

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    table_name,
                    silver_row_count,
                    loaded_row_count
                FROM warehouse.loaded_batches
                WHERE batch_id = %s
                """,
                (batch["batch_id"],),
            )

            checkpoint = cursor.fetchone()

        assert checkpoint == ("customers", 100, 100), (
            f"Unexpected checkpoint: {checkpoint}"
        )

        print("CHECKPOINT INSIDE TRANSACTION: PASS")

    finally:
        # Always rollback, including when an assertion fails.
        conn.rollback()
        conn.close()

    # Independent connection verifies nothing was persisted.
    with get_connection() as verify_conn:
        raw_count = count_rows(
            verify_conn,
            "raw.customers",
        )

        checkpoint_count = count_rows(
            verify_conn,
            "warehouse.loaded_batches",
        )

    assert raw_count == 0, (
        f"Raw rows remained after rollback: {raw_count}"
    )

    print("RAW ROLLBACK VERIFIED: PASS")

    assert checkpoint_count == 0, (
        "Warehouse checkpoint remained after rollback"
    )

    print("CHECKPOINT ROLLBACK VERIFIED: PASS")
    print("FINAL RESULT: PASS 7/7")


if __name__ == "__main__":
    main()
