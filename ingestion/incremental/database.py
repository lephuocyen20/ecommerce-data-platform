"""PostgreSQL connection and transaction guards."""

import os

import psycopg2

try:
    from .config import EXPECTED_DB_NAME
except ImportError:
    from config import EXPECTED_DB_NAME


def require_transaction(conn):
    """Ensure a caller-owned PostgreSQL transaction is active."""
    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")


def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    with conn.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        actual_db_name = cursor.fetchone()[0]

    if actual_db_name != EXPECTED_DB_NAME:
        conn.close()
        raise RuntimeError(
            f"Unsafe database connection: {actual_db_name}. "
            f"Expected: {EXPECTED_DB_NAME}"
        )

    return conn


__all__ = ["get_db_connection", "require_transaction"]
