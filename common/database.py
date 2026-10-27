"""PostgreSQL connection and transaction primitives shared by pipeline layers."""

import psycopg2


def connect_postgres(*, host, port, dbname, user, password, **kwargs):
    values = {"host": host, "port": port, "dbname": dbname, "user": user, "password": password}
    missing = [name for name, value in values.items() if value in (None, "")]
    if missing:
        raise RuntimeError(f"Missing PostgreSQL connection settings: {missing}")
    return psycopg2.connect(**values, **kwargs)


def require_transaction(conn):
    if conn is None:
        raise ValueError("PostgreSQL connection is unavailable")
    if isinstance(getattr(conn, "closed", 0), int) and conn.closed != 0:
        raise ValueError("PostgreSQL connection is unavailable")
    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")


def assert_current_database(conn, expected_database):
    if not isinstance(expected_database, str) or not expected_database:
        raise ValueError("Expected database name is required")
    with conn.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        row = cursor.fetchone()
    actual_database = row[0] if row else None
    if actual_database != expected_database:
        raise RuntimeError(
            f"Unsafe database connection: {actual_database}. "
            f"Expected: {expected_database}"
        )
    return actual_database
