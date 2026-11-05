
"""PostgreSQL advisory-lock ownership E2E tests."""

import os
import unittest

from dotenv import load_dotenv

from gold.implementation.ownership import (
    GoldOwnershipBusyError,
    acquire_gold_ownership,
    connect_gold_lock_db,
)


load_dotenv()

DATASET = "daily_sales"
SNAPSHOT_ID = "gold_snap_" + "e" * 32


class TestGoldOwnershipE2E(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        required = [
            "DB_HOST",
            "DB_PORT",
            "DB_NAME",
            "DB_USER",
            "DB_PASSWORD",
        ]

        missing = [
            name for name in required
            if not os.getenv(name)
        ]

        if missing:
            raise RuntimeError(
                f"Missing database environment variables: {missing}"
            )

        conn = connect_gold_lock_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                assert cursor.fetchone()[0] == 1
        finally:
            conn.close()

    def test_concurrent_acquisition_rejected(self):
        with acquire_gold_ownership(
            DATASET,
            SNAPSHOT_ID,
        ) as first:
            self.assertEqual(first.status, "ACQUIRED")

            with self.assertRaises(GoldOwnershipBusyError):
                with acquire_gold_ownership(
                    DATASET,
                    SNAPSHOT_ID,
                ):
                    self.fail(
                        "Second session acquired an occupied lock"
                    )

    def test_reacquire_after_release(self):
        with acquire_gold_ownership(
            DATASET,
            SNAPSHOT_ID,
        ):
            pass

        with acquire_gold_ownership(
            DATASET,
            SNAPSHOT_ID,
        ) as second:
            self.assertEqual(second.status, "ACQUIRED")

    def test_release_after_exception(self):
        with self.assertRaisesRegex(
            RuntimeError,
            "Simulated Gold failure",
        ):
            with acquire_gold_ownership(
                DATASET,
                SNAPSHOT_ID,
            ):
                raise RuntimeError(
                    "Simulated Gold failure"
                )

        with acquire_gold_ownership(
            DATASET,
            SNAPSHOT_ID,
        ) as recovered:
            self.assertEqual(
                recovered.status,
                "ACQUIRED",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
