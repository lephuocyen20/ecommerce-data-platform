
"""Integration test: Raw UPSERT + checkpoint atomic rollback."""

import os
import sys
import uuid
from pathlib import Path

import pyarrow as pa
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "warehouse" / "loader"))

from raw_upsert import upsert_raw_batch
from warehouse_checkpoint import insert_loaded_batch


load_dotenv(ROOT / ".env")


def get_connection():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_DB_HOST") or os.getenv("DB_HOST"),
        port=os.getenv("WAREHOUSE_DB_PORT") or os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("WAREHOUSE_DB_NAME") or os.getenv("DB_NAME"),
        user=os.getenv("WAREHOUSE_DB_USER") or os.getenv("DB_USER"),
        password=os.getenv("WAREHOUSE_DB_PASSWORD") or os.getenv("DB_PASSWORD"),
        connect_timeout=10,
    )


def count_test_records(conn, order_id, batch_id):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM raw.orders WHERE order_id = %s",
            (order_id,),
        )
        raw_count = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM warehouse.loaded_batches
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        checkpoint_count = cursor.fetchone()[0]

    return raw_count, checkpoint_count


def main():
    batch_id = uuid.uuid4().hex

    # Negative BIGINT ID: outside the normal positive SERIAL range.
    # We still verify that it does not already exist.
    order_id = -int(uuid.uuid4().int % 9_000_000_000_000_000) - 1

    conn = None

    try:
        conn = get_connection()
        conn.autocommit = False

        before = count_test_records(conn, order_id, batch_id)

        if before != (0, 0):
            raise RuntimeError(
                f"Test identifiers already exist: {before}"
            )

        print("PRE-TEST IDENTIFIERS: PASS")

        arrow_table = pa.table({
            "order_id": pa.array([order_id], type=pa.int64()),
            "customer_id": pa.array([None], type=pa.int64()),
            "status": pa.array(["TEST_ROLLBACK"], type=pa.string()),
            "order_date": pa.array(
                [None], type=pa.timestamp("us")
            ),
            "updated_at": pa.array(
                [None], type=pa.timestamp("us")
            ),
        })

        loaded_rows = upsert_raw_batch(
            conn,
            "orders",
            batch_id,
            arrow_table,
        )

        if loaded_rows != 1:
            raise AssertionError(
                f"Expected 1 loaded row, got {loaded_rows}"
            )

        print("RAW UPSERT: PASS")

        batch = {
            "batch_id": batch_id,
            "table_name": "orders",
            "watermark_type": "ID",
            "start_timestamp": None,
            "start_id": order_id,
            "end_timestamp": None,
            "end_id": order_id,
        }

        manifest = {
            "batch_id": batch_id,
            "table_name": "orders",
            "status": "COMPLETED",
            "silver_row_count": 1,
            "data_prefix": "integration-test/rollback",
        }

        insert_loaded_batch(
            conn,
            batch,
            "integration-test/rollback/manifest.json",
            manifest,
            loaded_rows,
        )

        print("CHECKPOINT INSERT: PASS")

        during = count_test_records(conn, order_id, batch_id)

        if during != (1, 1):
            raise AssertionError(
                f"Expected (1, 1), got {during}"
            )

        print("SAME TRANSACTION VISIBILITY: PASS")

    finally:
        if conn is not None:
            conn.rollback()
            conn.close()

    print("TRANSACTION ROLLBACK: PASS")

    verify_conn = get_connection()

    try:
        after = count_test_records(
            verify_conn,
            order_id,
            batch_id,
        )

        if after != (0, 0):
            raise AssertionError(
                f"Rollback verification failed: {after}"
            )

        print("RAW DATA CLEAN: PASS")
        print("CHECKPOINT CLEAN: PASS")
        print("FINAL RESULT: PASS 6/6")

    finally:
        verify_conn.rollback()
        verify_conn.close()


if __name__ == "__main__":
    main()
