"""PostgreSQL connection and transaction guards."""

import os

from dotenv import load_dotenv
from common.database import (
    assert_current_database,
    connect_postgres,
    require_transaction,
)

load_dotenv()

try:
    from .config import EXPECTED_DB_NAME
except ImportError:
    from config import EXPECTED_DB_NAME


def get_db_connection():
    conn = connect_postgres(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    try:
        assert_current_database(conn, EXPECTED_DB_NAME)
    except Exception:
        conn.close()
        raise

    return conn


__all__ = ["get_db_connection", "require_transaction"]
