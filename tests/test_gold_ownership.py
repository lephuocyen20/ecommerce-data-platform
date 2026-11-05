
"""Unit tests for Gold execution ownership."""

import unittest
from unittest.mock import MagicMock
from gold.implementation.ownership import (
    GoldOwnershipBusyError,
    GoldOwnershipError,
    acquire_gold_ownership,
    build_gold_lock_key,
)


DATASET = "daily_sales"
SNAPSHOT_ID = "gold_snap_" + "a" * 32



def make_connection(lock_acquired=True):
    """Build a mock PostgreSQL connection."""

    conn = MagicMock()
    conn.closed = 0

    cursor = MagicMock()

    cursor.fetchone.return_value = (
        lock_acquired,
    )

    conn.cursor.return_value.__enter__.return_value = cursor

    return conn, cursor



class TestGoldOwnership(unittest.TestCase):

    def test_deterministic_lock_key(self):
        first = build_gold_lock_key(DATASET, SNAPSHOT_ID)
        second = build_gold_lock_key(DATASET, SNAPSHOT_ID)

        self.assertEqual(first, second)
        self.assertIsInstance(first, int)

    def test_different_snapshot_keys(self):
        first = build_gold_lock_key(
            DATASET,
            "gold_snap_" + "a" * 32,
        )
        second = build_gold_lock_key(
            DATASET,
            "gold_snap_" + "b" * 32,
        )

        self.assertNotEqual(first, second)

    def test_different_dataset_keys(self):
        first = build_gold_lock_key(
            "daily_sales",
            SNAPSHOT_ID,
        )
        second = build_gold_lock_key(
            "payment_summary",
            SNAPSHOT_ID,
        )

        self.assertNotEqual(first, second)

    def test_successful_acquisition(self):
        conn, cursor = make_connection()

        with acquire_gold_ownership(
            DATASET,
            SNAPSHOT_ID,
            connection_factory=lambda: conn,
        ) as ownership:
            self.assertEqual(
                ownership.status,
                "ACQUIRED",
            )
            self.assertEqual(
                ownership.lock_key,
                build_gold_lock_key(DATASET, SNAPSHOT_ID),
            )

        self.assertEqual(cursor.execute.call_count, 2)
        conn.close.assert_called_once()

    def test_busy_lock_rejected(self):
        conn, cursor = make_connection(
            lock_acquired=False,
        )

        with self.assertRaises(GoldOwnershipBusyError):
            with acquire_gold_ownership(
                DATASET,
                SNAPSHOT_ID,
                connection_factory=lambda: conn,
            ):
                self.fail("Busy lock must not enter context")

        self.assertEqual(cursor.execute.call_count, 1)
        conn.close.assert_called_once()

    def test_connection_unavailable(self):
        conn, _ = make_connection()
        conn.closed = 1

        with self.assertRaises(GoldOwnershipError):
            with acquire_gold_ownership(
                DATASET,
                SNAPSHOT_ID,
                connection_factory=lambda: conn,
            ):
                self.fail("Closed connection must be rejected")

        conn.close.assert_called_once()

    def test_connection_factory_failure(self):
        def failing_factory():
            raise OSError("Database unavailable")

        with self.assertRaises(GoldOwnershipError):
            with acquire_gold_ownership(
                DATASET,
                SNAPSHOT_ID,
                connection_factory=failing_factory,
            ):
                self.fail("Connection failure must be rejected")

    def test_release_on_execution_error(self):
        conn, cursor = make_connection()

        with self.assertRaisesRegex(
            RuntimeError,
            "Simulated execution failure",
        ):
            with acquire_gold_ownership(
                DATASET,
                SNAPSHOT_ID,
                connection_factory=lambda: conn,
            ):
                raise RuntimeError(
                    "Simulated execution failure"
                )

        self.assertEqual(cursor.execute.call_count, 2)
        conn.close.assert_called_once()

    def test_unlock_failure_rejected(self):
        conn, cursor = make_connection()

        cursor.fetchone.side_effect = [
            (True,),
            (False,),
        ]

        with self.assertRaises(GoldOwnershipError):
            with acquire_gold_ownership(
                DATASET,
                SNAPSHOT_ID,
                connection_factory=lambda: conn,
            ):
                pass

        conn.close.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
