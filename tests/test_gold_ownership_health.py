
"""Unit tests for Gold Ownership Health Guard."""

import unittest
from unittest.mock import MagicMock

import psycopg2

from gold.implementation.ownership import (
    GoldOwnershipError,
    assert_gold_ownership_alive,
)


LOCK_KEY = 512516753388452137


def make_connection(healthy=True):
    conn = MagicMock()
    conn.closed = 0

    cursor = MagicMock()
    cursor.fetchone.return_value = (healthy,)

    conn.cursor.return_value.__enter__.return_value = cursor

    return conn, cursor


class TestGoldOwnershipHealth(unittest.TestCase):

    def test_healthy_lock(self):
        conn, cursor = make_connection()

        result = assert_gold_ownership_alive(
            conn,
            LOCK_KEY,
        )

        self.assertEqual(result["status"], "HEALTHY")
        self.assertEqual(result["lock_key"], LOCK_KEY)
        cursor.execute.assert_called_once()

    def test_lock_not_owned(self):
        conn, _ = make_connection(healthy=False)

        with self.assertRaises(GoldOwnershipError):
            assert_gold_ownership_alive(conn, LOCK_KEY)

    def test_closed_connection(self):
        conn, cursor = make_connection()
        conn.closed = 1

        with self.assertRaises(GoldOwnershipError):
            assert_gold_ownership_alive(conn, LOCK_KEY)

        cursor.execute.assert_not_called()

    def test_missing_connection(self):
        with self.assertRaises(GoldOwnershipError):
            assert_gold_ownership_alive(None, LOCK_KEY)

    def test_invalid_lock_key(self):
        conn, cursor = make_connection()

        for key in ["invalid", None, 2**63, -(2**63) - 1]:
            with self.subTest(key=key):
                with self.assertRaises(GoldOwnershipError):
                    assert_gold_ownership_alive(conn, key)

        cursor.execute.assert_not_called()

    def test_missing_query_result(self):
        conn, cursor = make_connection()
        cursor.fetchone.return_value = None

        with self.assertRaises(GoldOwnershipError):
            assert_gold_ownership_alive(conn, LOCK_KEY)

    def test_database_failure(self):
        conn, cursor = make_connection()
        cursor.execute.side_effect = psycopg2.OperationalError(
            "Connection lost"
        )

        with self.assertRaises(GoldOwnershipError):
            assert_gold_ownership_alive(conn, LOCK_KEY)

    def test_negative_lock_key(self):
        conn, _ = make_connection()

        result = assert_gold_ownership_alive(
            conn,
            -123456789,
        )

        self.assertEqual(result["status"], "HEALTHY")


if __name__ == "__main__":
    unittest.main(verbosity=2)
