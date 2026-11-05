
"""Gold Ownership Handle lifecycle unit tests."""

import unittest
from unittest.mock import MagicMock, patch

from gold.implementation.ownership import (
    GoldOwnershipError,
    acquire_gold_ownership,
)


DATASET = "daily_sales"
SNAPSHOT_ID = "gold_snap_" + "a" * 32


def make_connection():
    conn = MagicMock()
    conn.closed = 0

    cursor = MagicMock()
    cursor.fetchone.return_value = (True,)

    conn.cursor.return_value.__enter__.return_value = cursor

    return conn


class TestGoldOwnershipLifecycle(unittest.TestCase):

    def test_handle_alive_inside_context(self):
        conn = make_connection()

        with patch(
            "gold.implementation.ownership."
            "assert_gold_ownership_alive",
            return_value={"status": "HEALTHY"},
        ) as health:
            with acquire_gold_ownership(
                DATASET,
                SNAPSHOT_ID,
                connection_factory=lambda: conn,
            ) as handle:
                result = handle.assert_alive()

                self.assertEqual(
                    result["status"],
                    "HEALTHY",
                )
                health.assert_called_once_with(
                    conn,
                    handle.lock_key,
                )

    def test_handle_rejected_after_context(self):
        conn = make_connection()

        with acquire_gold_ownership(
            DATASET,
            SNAPSHOT_ID,
            connection_factory=lambda: conn,
        ) as handle:
            self.assertEqual(handle.status, "ACQUIRED")

        self.assertEqual(handle.status, "RELEASED")

        with self.assertRaises(GoldOwnershipError):
            handle.assert_alive()

    def test_handle_detects_connection_loss(self):
        conn = make_connection()

        with acquire_gold_ownership(
            DATASET,
            SNAPSHOT_ID,
            connection_factory=lambda: conn,
        ) as handle:
            conn.closed = 1

            with self.assertRaises(GoldOwnershipError):
                handle.assert_alive()

    def test_handle_deactivated_after_execution_error(self):
        conn = make_connection()
        handle = None

        with self.assertRaisesRegex(
            RuntimeError,
            "Simulated execution failure",
        ):
            with acquire_gold_ownership(
                DATASET,
                SNAPSHOT_ID,
                connection_factory=lambda: conn,
            ) as handle:
                raise RuntimeError(
                    "Simulated execution failure"
                )

        self.assertIsNotNone(handle)
        self.assertEqual(handle.status, "RELEASED")

        with self.assertRaises(GoldOwnershipError):
            handle.assert_alive()


if __name__ == "__main__":
    unittest.main(verbosity=2)
