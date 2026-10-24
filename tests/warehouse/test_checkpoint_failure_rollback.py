
"""Integration test: rollback Raw UPSERT when checkpoint INSERT fails."""

import os
import sys
import uuid
from pathlib import Path

import pyarrow as pa
import psycopg2
from psycopg2 import errors
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
            """
            SELECT COUNT(*)
            FROM raw.orders
            WHERE order_id = %s
            """,
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

    # Use a negative BIGINT ID to avoid normal positive IDs.
    order_id = -(uuid.uuid4().int % 9_000_000_000_000_000) - 1

    conn = None
    checkpoint_failure_verified = False

    try:
        conn = get_connection()
        conn.autocommit = False

        # --------------------------------------------------
        # 1. Verify test identifiers do not already exist
        # --------------------------------------------------
        before = count_test_records(conn, order_id, batch_id)

        if before != (0, 0):
            raise AssertionError(
                f"Test identifiers already exist: {before}"
            )

        print("PRE-TEST IDENTIFIERS: PASS")

        # --------------------------------------------------
        # 2. Perform real Raw Warehouse UPSERT
        # --------------------------------------------------
        arrow_table = pa.table({
            "order_id": pa.array(
                [order_id],
                type=pa.int64(),
            ),
            "customer_id": pa.array(
                [None],
                type=pa.int64(),
            ),
            "status": pa.array(
                ["TEST_CHECKPOINT_FAILURE"],
                type=pa.string(),
            ),
            "order_date": pa.array(
                [None],
                type=pa.timestamp("us"),
            ),
            "updated_at": pa.array(
                [None],
                type=pa.timestamp("us"),
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

        # --------------------------------------------------
        # 3. Prepare intentionally invalid checkpoint
        # --------------------------------------------------
        batch = {
            "batch_id": batch_id,
            "table_name": "orders",
            "watermark_type": " INVALID",
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
            "data_prefix": "integration-test/checkpoint-failure",
        }

        # --------------------------------------------------
        # 4. Checkpoint must fail at PostgreSQL constraint
        # --------------------------------------------------
        try:
            insert_loaded_batch(
                conn,
                batch,
                "integration-test/checkpoint-failure/manifest.json",
                manifest,
                loaded_rows,
            )

        except errors.CheckViolation as exc:
            if (
                exc.diag.constraint_name
                != "loaded_batches_watermark_check"
            ):
                raise

            checkpoint_failure_verified = True
            print("CHECKPOINT CONSTRAINT FAILURE: PASS")

        else:
            raise AssertionError(
                "Expected checkpoint CHECK constraint violation"
            )

    finally:
        # --------------------------------------------------
        # 5. Always rollback, even if checkpoint fails
        # --------------------------------------------------
        if conn is not None:
            try:
                conn.rollback()
            finally:
                conn.close()

    if not checkpoint_failure_verified:
        raise AssertionError(
            "Checkpoint failure was not verified"
        )

    print("TRANSACTION ROLLBACK: PASS")

    # ------------------------------------------------------
    # 6. Verify state using a new PostgreSQL connection
    # ------------------------------------------------------
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
