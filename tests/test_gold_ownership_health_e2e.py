
"""PostgreSQL Gold Ownership Health Guard E2E tests."""

import unittest

from dotenv import load_dotenv

from gold.implementation.ownership import (
    GoldOwnershipError,
    assert_gold_ownership_alive,
    build_gold_lock_key,
    connect_gold_lock_db,
)


load_dotenv()

DATASET = "daily_sales"
SNAPSHOT_ID = "gold_snap_" + "f" * 32

LOCK_KEY = build_gold_lock_key(
    DATASET,
    SNAPSHOT_ID,
)


class TestGoldOwnershipHealthE2E(unittest.TestCase):

    def setUp(self):
        self.conn = connect_gold_lock_db()
        self.conn.autocommit = True

    def tearDown(self):
        if self.conn is not None:
            self.conn.close()

    def acquire_lock(self):
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (LOCK_KEY,),
            )
            result = cursor.fetchone()

        self.assertEqual(result, (True,))

    def release_lock(self):
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_unlock(%s)",
                (LOCK_KEY,),
            )
            result = cursor.fetchone()

        self.assertEqual(result, (True,))

    def test_health_while_lock_held(self):
        self.acquire_lock()

        try:
            result = assert_gold_ownership_alive(
                self.conn,
                LOCK_KEY,
            )

            self.assertEqual(
                result["status"],
                "HEALTHY",
            )
        finally:
            self.release_lock()

    def test_health_after_unlock(self):
        self.acquire_lock()
        self.release_lock()

        with self.assertRaises(GoldOwnershipError):
            assert_gold_ownership_alive(
                self.conn,
                LOCK_KEY,
            )

    def test_health_after_connection_close(self):
        self.acquire_lock()
        self.conn.close()

        with self.assertRaises(GoldOwnershipError):
            assert_gold_ownership_alive(
                self.conn,
                LOCK_KEY,
            )

    def test_negative_lock_key(self):
        negative_key = -123456789

        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (negative_key,),
            )
            self.assertEqual(
                cursor.fetchone(),
                (True,),
            )

        try:
            result = assert_gold_ownership_alive(
                self.conn,
                negative_key,
            )
            self.assertEqual(
                result["status"],
                "HEALTHY",
            )
        finally:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (negative_key,),
                )
                self.assertEqual(
                    cursor.fetchone(),
                    (True,),
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
