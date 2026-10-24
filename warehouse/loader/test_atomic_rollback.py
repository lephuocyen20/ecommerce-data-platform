"""Verify atomic rollback across Raw UPSERT and Warehouse checkpoint."""

import sys
from datetime import datetime
from decimal import Decimal

import pyarrow as pa

sys.path.insert(0, "warehouse/loader")

from incremental_silver_to_warehouse import get_connection
from raw_upsert import upsert_raw_batch
from warehouse_checkpoint import insert_loaded_batch


TEST_PRODUCT_ID = -999999998
TEST_BATCH_ID = "00000000000000000000000000000002"

TEST_ROW = pa.table({
    "product_id": pa.array([TEST_PRODUCT_ID], type=pa.int64()),
    "product_name": pa.array(
        ["ATOMIC_ROLLBACK_TEST"], type=pa.string()
    ),
    "stock_quantity": pa.array([20], type=pa.int32()),
    "price": pa.array(
        [Decimal("199.99")],
        type=pa.decimal128(12, 2),
    ),
    "created_at": pa.array(
        [datetime(2026, 1, 1)],
        type=pa.timestamp("ns"),
    ),
    "updated_at": pa.array(
        [datetime(2026, 1, 1)],
        type=pa.timestamp("ns"),
    ),
})

# Use valid metadata except for an intentionally invalid
# watermark_type, which violates a PostgreSQL CHECK constraint.
TEST_BATCH = {
    "batch_id": TEST_BATCH_ID,
    "table_name": "products",
    "watermark_type": "INVALID",
    "start_timestamp": None,
    "start_id": None,
    "end_timestamp": None,
    "end_id": None,
}

TEST_MANIFEST = {
    "batch_id": TEST_BATCH_ID,
    "table_name": "products",
    "status": "COMPLETED",
    "silver_row_count": 1,
    "data_prefix": "silver_incremental/products/test/",
}

conn = get_connection()

try:
    conn.autocommit = False

    # Guard against accidentally touching existing test records.
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM raw.products WHERE product_id = %s",
            (TEST_PRODUCT_ID,),
        )
        product_count = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM warehouse.loaded_batches
            WHERE batch_id = %s
            """,
            (TEST_BATCH_ID,),
        )
        checkpoint_count = cursor.fetchone()[0]

    if product_count != 0 or checkpoint_count != 0:
        raise RuntimeError(
            "Test records already exist; refusing to modify them"
        )

    loaded_count = upsert_raw_batch(
        conn,
        "products",
        TEST_BATCH_ID,
        TEST_ROW,
    )

    assert loaded_count == 1

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM raw.products
            WHERE product_id = %s
            """,
            (TEST_PRODUCT_ID,),
        )
        assert cursor.fetchone()[0] == 1

    print("RAW UPSERT INSIDE TRANSACTION: PASS")

    try:
        insert_loaded_batch(
            conn,
            TEST_BATCH,
            "silver_incremental/products/test/_BATCH_METADATA.json",
            TEST_MANIFEST,
            loaded_count,
        )
    except Exception as exc:
        # The expected failure is a PostgreSQL CHECK constraint.
        from psycopg2.errors import CheckViolation

        if not isinstance(exc, CheckViolation):
            raise

        print("CHECKPOINT CONSTRAINT FAILURE: PASS")
    else:
        raise AssertionError(
            "Expected PostgreSQL CHECK constraint failure"
        )

finally:
    # Even if the test fails, never commit test data.
    conn.rollback()
    conn.close()

with get_connection() as verify_conn:
    with verify_conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM raw.products WHERE product_id = %s",
            (TEST_PRODUCT_ID,),
        )
        remaining_product = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM warehouse.loaded_batches
            WHERE batch_id = %s
            """,
            (TEST_BATCH_ID,),
        )
        remaining_checkpoint = cursor.fetchone()[0]

assert remaining_product == 0, "Raw rollback verification failed"
assert remaining_checkpoint == 0, "Checkpoint rollback verification failed"

print("RAW DATA ROLLBACK: PASS")
print("CHECKPOINT ROLLBACK: PASS")
print("FINAL RESULT: PASS")
