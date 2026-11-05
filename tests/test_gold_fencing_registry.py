
"""Gold fencing registry allocation unit tests.

Covers atomic registry + attempt history allocation
and transaction failure behavior.
"""

import unittest
from unittest.mock import MagicMock, patch

import psycopg2

from gold.implementation.fencing import (
    GoldExecutionAttempt,
    GoldFencingError,
)
from gold.implementation.fencing_registry import (
    ALLOCATION_SQL,
    HISTORY_EXISTS_SQL,
    INSERT_HISTORY_SQL,
    allocate_gold_fencing_token,
)
from gold.implementation.ownership import (
    GoldOwnershipError,
    GoldOwnershipHandle,
)


DATASET = "daily_sales"
SNAPSHOT = "gold_snap_" + "a" * 32
ATTEMPT = "attempt_" + "b" * 32


def make_handle(token=1):
    conn = MagicMock()
    conn.closed = 0
    conn.autocommit = True

    cursor = MagicMock()

    # Query 1: history lookup -> attempt not found
    # Query 2: registry allocation -> fencing token
    cursor.fetchone.side_effect = [
        None,
        (token,) if token is not None else None,
    ]

    conn.cursor.return_value.__enter__.return_value = cursor

    handle = GoldOwnershipHandle(
        conn=conn,
        dataset_name=DATASET,
        snapshot_id=SNAPSHOT,
        lock_key=123,
    )

    return handle, conn, cursor


class TestGoldFencingRegistry(unittest.TestCase):

    def test_successful_allocation(self):
        handle, conn, cursor = make_handle(token=42)

        with patch.object(
            handle,
            "assert_alive",
            return_value={"status": "HEALTHY"},
        ) as health:
            attempt = allocate_gold_fencing_token(
                handle,
                ATTEMPT,
            )

        self.assertIsInstance(
            attempt,
            GoldExecutionAttempt,
        )
        self.assertEqual(attempt.fencing_token, 42)
        self.assertEqual(health.call_count, 2)

        # History check, allocation, history insert.
        self.assertEqual(cursor.execute.call_count, 3)

        self.assertEqual(
            [call.args[0] for call in cursor.execute.call_args_list],
            [
                HISTORY_EXISTS_SQL,
                ALLOCATION_SQL,
                INSERT_HISTORY_SQL,
            ],
        )

        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
        self.assertTrue(conn.autocommit)

    def test_invalid_handle_rejected(self):
        with self.assertRaises(GoldFencingError):
            allocate_gold_fencing_token({}, ATTEMPT)

    def test_invalid_attempt_id_rejected(self):
        handle, _, _ = make_handle()

        with self.assertRaises(GoldFencingError):
            allocate_gold_fencing_token(
                handle,
                "../unsafe",
            )

    def test_released_handle_rejected(self):
        handle, _, _ = make_handle()
        handle._deactivate()

        with self.assertRaises(GoldOwnershipError):
            allocate_gold_fencing_token(
                handle,
                ATTEMPT,
            )

    def test_closed_connection_rejected(self):
        handle, conn, _ = make_handle()
        conn.closed = 1

        with self.assertRaises(GoldOwnershipError):
            allocate_gold_fencing_token(
                handle,
                ATTEMPT,
            )

    def test_autocommit_required(self):
        handle, conn, _ = make_handle()
        conn.autocommit = False

        with patch.object(
            handle,
            "assert_alive",
            return_value={"status": "HEALTHY"},
        ):
            with self.assertRaises(GoldOwnershipError):
                allocate_gold_fencing_token(
                    handle,
                    ATTEMPT,
                )

    def test_token_overflow_rejected(self):
        handle, conn, _ = make_handle(token=None)

        with patch.object(
            handle,
            "assert_alive",
            return_value={"status": "HEALTHY"},
        ):
            with self.assertRaises(GoldFencingError):
                allocate_gold_fencing_token(
                    handle,
                    ATTEMPT,
                )

        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()

    def test_invalid_returned_token_rejected(self):
        for token in (0, -1, True, 2**63):
            with self.subTest(token=token):
                handle, conn, _ = make_handle(token=token)

                with patch.object(
                    handle,
                    "assert_alive",
                    return_value={"status": "HEALTHY"},
                ):
                    with self.assertRaises(GoldFencingError):
                        allocate_gold_fencing_token(
                            handle,
                            ATTEMPT,
                        )

                conn.commit.assert_not_called()
                conn.rollback.assert_called_once()

    def test_database_failure_rejected(self):
        handle, conn, cursor = make_handle()

        database_error = psycopg2.OperationalError(
            "Simulated database failure"
        )
        cursor.execute.side_effect = database_error

        with patch.object(
            handle,
            "assert_alive",
            return_value={"status": "HEALTHY"},
        ):
            with self.assertRaises(
                psycopg2.OperationalError
            ) as context:
                allocate_gold_fencing_token(
                    handle,
                    ATTEMPT,
                )

        self.assertIs(context.exception, database_error)
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()

    def test_post_allocation_ownership_loss(self):
        handle, conn, _ = make_handle(token=42)

        with patch.object(
            handle,
            "assert_alive",
            side_effect=[
                {"status": "HEALTHY"},
                GoldOwnershipError(
                    "Ownership lost after allocation"
                ),
            ],
        ):
            with self.assertRaises(GoldOwnershipError):
                allocate_gold_fencing_token(
                    handle,
                    ATTEMPT,
                )

        # Commit may have succeeded, but the attempt
        # must not be returned when ownership is lost.
        conn.commit.assert_called_once()

    def test_duplicate_current_attempt_rejected(self):
        handle, conn, cursor = make_handle()

        # History already contains this attempt.
        cursor.fetchone.side_effect = [(1,)]

        with patch.object(
            handle,
            "assert_alive",
            return_value={"status": "HEALTHY"},
        ):
            with self.assertRaises(GoldFencingError):
                allocate_gold_fencing_token(
                    handle,
                    ATTEMPT,
                )

        self.assertEqual(cursor.execute.call_count, 1)
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
