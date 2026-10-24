"""Rollback-only test for PostgreSQL Raw UPSERT."""

import sys

import pyarrow as pa
from datetime import datetime
from decimal import Decimal
sys.path.insert(0, "warehouse/loader")

from incremental_silver_to_warehouse import get_connection
from raw_upsert import upsert_raw_batch


TEST_BATCH_ID = "00000000000000000000000000000001"

TEST_ROW = pa.table({
    "product_id": pa.array(
        [-999999999],
        type=pa.int64(),
    ),
    "product_name": pa.array(
        ["UPSERT_ROLLBACK_TEST"],
        type=pa.string(),
    ),
    "stock_quantity": pa.array(
        [10],
        type=pa.int32(),
    ),
    "price": pa.array(
        [Decimal("99.99")],
        type=pa.decimal128(12, 2),
    ),
    "created_at": pa.array(
        [datetime(2026, 1, 1, 0, 0, 0)],
        type=pa.timestamp("ns"),
    ),
    "updated_at": pa.array(
        [datetime(2026, 1, 1, 0, 0, 0)],
        type=pa.timestamp("ns"),
    ),
})

conn = get_connection()

try:
    conn.autocommit = False

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM raw.products WHERE product_id = %s",
            (-999999999,),
        )
        before_count = cursor.fetchone()[0]

    if before_count != 0:
        raise RuntimeError(
            "Test product_id already exists; refusing to modify it"
        )

    inserted = upsert_raw_batch(
        conn,
        "products",
        TEST_BATCH_ID,
        TEST_ROW,
    )

    assert inserted == 1

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT product_name, stock_quantity, price,
                   _silver_batch_id
            FROM raw.products
            WHERE product_id = %s
            """,
            (-999999999,),
        )
        row = cursor.fetchone()

    assert row is not None
    assert row[0] == "UPSERT_ROLLBACK_TEST"
    assert row[1] == 10
    assert str(row[2]) == "99.99"
    assert row[3] == TEST_BATCH_ID

    print("UPSERT SQL: PASS")
    print("ROW VISIBLE INSIDE TRANSACTION: PASS")

finally:
    conn.rollback()
    conn.close()

with get_connection() as verify_conn:
    with verify_conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM raw.products WHERE product_id = %s",
            (-999999999,),
        )
        remaining = cursor.fetchone()[0]

assert remaining == 0, "Rollback verification failed"

print("ROLLBACK VERIFIED: PASS")
print("FINAL RESULT: PASS")
