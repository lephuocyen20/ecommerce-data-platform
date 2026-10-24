
"""Integration test: rollback Raw UPSERT UPDATE and checkpoint INSERT."""

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

TABLE_NAME = "orders"
TEST_ORDER_ID = 1

RAW_COLUMNS = (
    "order_id",
    "customer_id",
    "status",
    "order_date",
    "updated_at",
    "_silver_batch_id",
    "_loaded_at",
)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_DB_HOST") or os.getenv("DB_HOST"),
        port=os.getenv("WAREHOUSE_DB_PORT") or os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("WAREHOUSE_DB_NAME") or os.getenv("DB_NAME"),
        user=os.getenv("WAREHOUSE_DB_USER") or os.getenv("DB_USER"),
        password=os.getenv("WAREHOUSE_DB_PASSWORD") or os.getenv("DB_PASSWORD"),
        connect_timeout=10,
    )


def get_order(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                order_id,
                customer_id,
                status,
                order_date,
                updated_at,
                _silver_batch_id,
                _loaded_at
            FROM raw.orders
            WHERE order_id = %s
            """,
            (TEST_ORDER_ID,),
        )

        row = cursor.fetchone()

    return dict(zip(RAW_COLUMNS, row)) if row else None


def checkpoint_exists(conn, batch_id):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM warehouse.loaded_batches
                WHERE batch_id = %s
            )
            """,
            (batch_id,),
        )

        return cursor.fetchone()[0]


def main():
    batch_id = uuid.uuid4().hex

    conn = None
    original = None
    rollback_completed = False

    try:
        conn = get_connection()
        conn.autocommit = False

        # Prevent concurrent writers from changing the test row.
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT order_id
                FROM raw.orders
                WHERE order_id = %s
                FOR UPDATE
                """,
                (TEST_ORDER_ID,),
            )

            if cursor.fetchone() is None:
                raise AssertionError(
                    f"Order {TEST_ORDER_ID} does not exist"
                )

        original = get_order(conn)

        if checkpoint_exists(conn, batch_id):
            raise AssertionError(
                "Test batch_id already exists"
            )

        print("PRE-TEST RECORD VERIFIED: PASS")

        # Build a real Arrow batch with the same source columns.
        arrow_table = pa.table({
            "order_id": pa.array(
                [original["order_id"]],
                type=pa.int64(),
            ),
            "customer_id": pa.array(
                [original["customer_id"]],
                type=pa.int64(),
            ),
            "status": pa.array(
                ["TEST_UPDATE_ROLLBACK"],
                type=pa.string(),
            ),
            "order_date": pa.array(
                [original["order_date"]],
                type=pa.timestamp("us"),
            ),
            "updated_at": pa.array(
                [original["updated_at"]],
                type=pa.timestamp("us"),
            ),
        })

        loaded_rows = upsert_raw_batch(
            conn,
            TABLE_NAME,
            batch_id,
            arrow_table,
        )

        if loaded_rows != 1:
            raise AssertionError(
                f"Expected 1 loaded row, got {loaded_rows}"
            )

        updated = get_order(conn)

        if updated["status"] != "TEST_UPDATE_ROLLBACK":
            raise AssertionError(
                "UPSERT UPDATE did not change status"
            )

        if updated["_silver_batch_id"] != batch_id:
            raise AssertionError(
                "UPSERT UPDATE did not change batch_id"
            )

        print("RAW UPSERT UPDATE: PASS")

        batch = {
            "batch_id": batch_id,
            "table_name": TABLE_NAME,
            "watermark_type": "ID",
            "start_timestamp": None,
            "start_id": TEST_ORDER_ID,
            "end_timestamp": None,
            "end_id": TEST_ORDER_ID,
        }

        manifest = {
            "batch_id": batch_id,
            "table_name": TABLE_NAME,
            "status": "COMPLETED",
            "silver_row_count": 1,
            "data_prefix": "integration-test/update-rollback",
        }

        insert_loaded_batch(
            conn,
            batch,
            "integration-test/update-rollback/manifest.json",
            manifest,
            loaded_rows,
        )

        if not checkpoint_exists(conn, batch_id):
            raise AssertionError(
                "Checkpoint not visible inside transaction"
            )

        print("CHECKPOINT INSERT: PASS")
        print("SAME TRANSACTION VISIBILITY: PASS")

    finally:
        if conn is not None:
            try:
                conn.rollback()
                rollback_completed = True
            finally:
                conn.close()

    if not rollback_completed:
        raise AssertionError("Rollback was not completed")

    print("TRANSACTION ROLLBACK: PASS")

    # Verify persisted state using a new connection.
    verify_conn = get_connection()

    try:
        restored = get_order(verify_conn)

        if restored != original:
            raise AssertionError(
                f"Raw record was not restored.\n"
                f"Before: {original}\n"
                f"After: {restored}"
            )

        print("RAW RECORD RESTORED: PASS")

        if checkpoint_exists(verify_conn, batch_id):
            raise AssertionError(
                "Test checkpoint still exists after rollback"
            )

        print("CHECKPOINT CLEAN: PASS")
        print("FINAL RESULT: PASS 7/7")

    finally:
        verify_conn.rollback()
        verify_conn.close()


if __name__ == "__main__":
    main()
