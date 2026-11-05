
import unittest
from unittest.mock import MagicMock, patch

from gold.implementation.fencing import GoldFencingError
from gold.implementation.fencing_registry import (
    allocate_gold_fencing_token,
)
from gold.implementation.ownership import (
    GoldOwnershipHandle,
)

DATASET = "daily_sales"
SNAPSHOT_ID = "gold_snap_" + "a" * 32
ATTEMPT_ID = "attempt_" + "b" * 32


class TestGoldFencingTransactionSafety(unittest.TestCase):

    def setUp(self):
        self.conn = MagicMock()
        self.conn.closed = 0
        self.conn.autocommit = True

        self.cursor = MagicMock()
        self.conn.cursor.return_value.__enter__.return_value = (
            self.cursor
        )

        # History lookup -> not found
        # Registry allocation -> token 1
        self.cursor.fetchone.side_effect = [
            None,
            (1,),
        ]

        self.handle = GoldOwnershipHandle(
            conn=self.conn,
            dataset_name=DATASET,
            snapshot_id=SNAPSHOT_ID,
            lock_key=12345,
        )

        self.health_patch = patch.object(
            self.handle,
            "assert_alive",
            return_value=True,
        )
        self.health_patch.start()
        self.addCleanup(self.health_patch.stop)

    def test_success_commits_once(self):
        attempt = allocate_gold_fencing_token(
            self.handle,
            ATTEMPT_ID,
        )

        self.assertEqual(attempt.fencing_token, 1)
        self.conn.commit.assert_called_once()
        self.conn.rollback.assert_not_called()
        self.assertTrue(self.conn.autocommit)


    def test_commit_failure_never_returns_attempt(self):
        commit_error = RuntimeError(
            "Simulated COMMIT failure"
        )

        self.conn.commit.side_effect = commit_error

        with self.assertRaises(RuntimeError) as context:
            allocate_gold_fencing_token(
                self.handle,
                ATTEMPT_ID,
            )

        # Preserve the original COMMIT exception.
        self.assertIs(
            context.exception,
            commit_error,
        )

        # COMMIT was attempted exactly once.
        self.conn.commit.assert_called_once()

        # Do not rollback an uncertain COMMIT.
        self.conn.rollback.assert_not_called()

        # Invalidate the unsafe ownership session.
        self.conn.close.assert_called_once()


    def test_rollback_failure_does_not_return_attempt(self):
        self.cursor.execute.side_effect = RuntimeError(
            "Simulated SQL failure"
        )
        self.conn.rollback.side_effect = RuntimeError(
            "Simulated ROLLBACK failure"
        )

        with self.assertRaises(RuntimeError):
            allocate_gold_fencing_token(
                self.handle,
                ATTEMPT_ID,
            )

        self.conn.rollback.assert_called_once()

    def test_closed_connection_rejected(self):
        self.conn.closed = 1

        with self.assertRaises(Exception):
            allocate_gold_fencing_token(
                self.handle,
                ATTEMPT_ID,
            )

        self.conn.commit.assert_not_called()

    def test_invalid_attempt_does_not_start_transaction(self):
        with self.assertRaises(GoldFencingError):
            allocate_gold_fencing_token(
                self.handle,
                "invalid_attempt",
            )

        self.conn.commit.assert_not_called()
        self.conn.rollback.assert_not_called()




    def test_rollback_failure_preserves_original_error(self):
        original_error = RuntimeError(
            "Original SQL execution failure"
        )

        rollback_error = RuntimeError(
            "Rollback cleanup failure"
        )

        self.cursor.execute.side_effect = original_error
        self.conn.rollback.side_effect = rollback_error

        with self.assertRaises(RuntimeError) as context:
            allocate_gold_fencing_token(
                self.handle,
                ATTEMPT_ID,
            )

        # Original SQL exception must be preserved.
        self.assertIs(
            context.exception,
            original_error,
        )

        # Rollback was attempted.
        self.conn.rollback.assert_called_once()

        # Unsafe PostgreSQL session must be invalidated.
        self.conn.close.assert_called_once()

        # No fencing token may be returned.
        self.conn.commit.assert_not_called()

        # Rollback failure should be recorded as diagnostic.
        if hasattr(original_error, "__notes__"):
            self.assertTrue(
                any(
                    "rollback" in note.lower()
                    for note in original_error.__notes__
                )
            )



    def test_commit_failure_closes_connection(self):
        commit_error = RuntimeError("COMMIT failed")
        self.conn.commit.side_effect = commit_error

        with self.assertRaises(RuntimeError) as context:
            allocate_gold_fencing_token(
                self.handle,
                ATTEMPT_ID,
            )

        self.assertIs(context.exception, commit_error)
        self.conn.close.assert_called_once()
        self.conn.rollback.assert_not_called()

    def test_autocommit_restore_failure_after_commit(self):
        conn = self.conn
        internal_state = {"autocommit": True}

        type(conn).autocommit = property(
            lambda obj: internal_state["autocommit"],
            lambda obj, value: (
                (_ for _ in ()).throw(
                    RuntimeError("Restore failed")
                )
                if value is True
                else internal_state.__setitem__(
                    "autocommit", value
                )
            ),
        )

        with self.assertRaises(GoldFencingError):
            allocate_gold_fencing_token(
                self.handle,
                ATTEMPT_ID,
            )

        conn.commit.assert_called_once()
        conn.close.assert_called_once()

    def test_ownership_failure_after_commit(self):
        from gold.implementation.ownership import GoldOwnershipError

        self.handle.assert_alive.side_effect = [
            True,
            GoldOwnershipError("Ownership lost"),
        ]

        with self.assertRaises(GoldOwnershipError):
            allocate_gold_fencing_token(
                self.handle,
                ATTEMPT_ID,
            )

        self.conn.commit.assert_called_once()

    def test_rollback_failure_closes_connection(self):
        original_error = RuntimeError("SQL failed")

        self.cursor.execute.side_effect = original_error
        self.conn.rollback.side_effect = RuntimeError(
            "Rollback failed"
        )

        with self.assertRaises(RuntimeError) as context:
            allocate_gold_fencing_token(
                self.handle,
                ATTEMPT_ID,
            )

        self.assertIs(context.exception, original_error)
        self.conn.close.assert_called_once()




if __name__ == "__main__":
    unittest.main(verbosity=2)
