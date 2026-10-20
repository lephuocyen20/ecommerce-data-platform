"""Incremental PostgreSQL extraction queries."""

from psycopg2 import sql

try:
    from .config import get_table_config, validate_positive_integer
except ImportError:
    from config import get_table_config, validate_positive_integer


def extract_incremental(conn, table_name, watermark, batch_size=1000):
    config = get_table_config(table_name)
    validate_positive_integer(batch_size, "batch_size")
    primary_key = config["primary_key"]
    watermark_column = config["watermark_column"]
    watermark_type = config["watermark_type"]

    if watermark["watermark_type"] != watermark_type:
        raise ValueError("Watermark type mismatch")

    with conn.cursor() as cursor:
        if watermark_type == "TIMESTAMP":
            if watermark["last_timestamp"] is None:
                query = sql.SQL("""
                    SELECT *
                    FROM public.{}
                    ORDER BY {} ASC, {} ASC
                    LIMIT %s
                """).format(
                    sql.Identifier(table_name),
                    sql.Identifier(watermark_column),
                    sql.Identifier(primary_key),
                )
                params = (batch_size,)
            else:
                query = sql.SQL("""
                    SELECT *
                    FROM public.{}
                    WHERE ({}, {}) > (%s, %s)
                    ORDER BY {} ASC, {} ASC
                    LIMIT %s
                """).format(
                    sql.Identifier(table_name),
                    sql.Identifier(watermark_column),
                    sql.Identifier(primary_key),
                    sql.Identifier(watermark_column),
                    sql.Identifier(primary_key),
                )
                params = (
                    watermark["last_timestamp"],
                    watermark["last_id"],
                    batch_size,
                )

        else:
            query = sql.SQL("""
                SELECT *
                FROM public.{}
                WHERE {} > %s
                ORDER BY {} ASC
                LIMIT %s
            """).format(
                sql.Identifier(table_name),
                sql.Identifier(primary_key),
                sql.Identifier(primary_key),
            )
            params = (watermark["last_id"], batch_size)

        cursor.execute(query, params)

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

    return {
        "table_name": table_name,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
    }

__all__ = ["extract_incremental"]
