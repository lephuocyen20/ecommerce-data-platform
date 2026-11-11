"""Incremental PostgreSQL extraction queries."""

from datetime import timedelta
from psycopg2 import sql
try:
    from .tombstones import validate_tombstone_columns
except ImportError:
    from tombstones import validate_tombstone_columns

try:
    from .config import get_late_data_overlap_seconds, get_table_config, validate_extraction_contract, validate_positive_integer
except ImportError:
    from config import get_late_data_overlap_seconds, get_table_config, validate_extraction_contract, validate_positive_integer


def extract_incremental(conn, table_name, watermark, batch_size=1000):
    validate_extraction_contract()
    overlap_seconds = get_late_data_overlap_seconds()
    config = get_table_config(table_name)
    validate_positive_integer(batch_size, "batch_size")
    primary_key = config["primary_key"]
    watermark_column = config["watermark_column"]
    watermark_type = config["watermark_type"]

    if watermark["watermark_type"] != watermark_type:
        raise ValueError("Watermark type mismatch")

    replay_upper_timestamp = watermark.get("replay_upper_timestamp")
    replay_upper_id = watermark.get("replay_upper_id", 0)
    replay_mode = watermark_type == "TIMESTAMP" and overlap_seconds > 0 and watermark["last_timestamp"] is not None
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
            elif not replay_mode:
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
                replay_cursor_timestamp = watermark.get("replay_cursor_timestamp")
                replay_cursor_id = watermark.get("replay_cursor_id", 0)
                if replay_cursor_timestamp is None:
                    replay_cursor_timestamp = watermark["last_timestamp"] - timedelta(seconds=overlap_seconds)
                if replay_upper_timestamp is None:
                    cursor.execute(
                        sql.SQL("SELECT MAX({}), MAX({}) FILTER (WHERE {} = (SELECT MAX({}) FROM public.{})) FROM public.{}" ).format(
                            sql.Identifier(watermark_column), sql.Identifier(primary_key), sql.Identifier(watermark_column),
                            sql.Identifier(watermark_column), sql.Identifier(table_name),
                            sql.Identifier(table_name)
                        )
                    )
                    replay_upper_timestamp, replay_upper_id = cursor.fetchone()
                if replay_upper_timestamp is None:
                    return {"table_name": table_name, "columns": [], "rows": [], "row_count": 0}
                query = sql.SQL("""
                    SELECT * FROM public.{}
                    WHERE ({}, {}) > (%s, %s) AND ({}, {}) <= (%s, %s)
                    ORDER BY {} ASC, {} ASC LIMIT %s
                """).format(
                    sql.Identifier(table_name), sql.Identifier(watermark_column),
                    sql.Identifier(primary_key), sql.Identifier(watermark_column), sql.Identifier(primary_key),
                    sql.Identifier(watermark_column), sql.Identifier(primary_key),
                )
                params = (replay_cursor_timestamp, replay_cursor_id, replay_upper_timestamp, replay_upper_id, batch_size)

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
        validate_tombstone_columns(table_name, columns)
        rows = cursor.fetchall()

    result = {
        "table_name": table_name,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
    }
    if replay_mode and rows:
        row_map = [dict(zip(columns, row)) for row in rows]
        last = row_map[-1]
        row_cursor = (last[watermark_column], last[primary_key])
        checkpoint = max((watermark["last_timestamp"], watermark["last_id"]), row_cursor)
        result.update({
            "checkpoint_timestamp": checkpoint[0],
            "checkpoint_id": checkpoint[1],
            "replay_cursor_timestamp": row_cursor[0],
            "replay_cursor_id": row_cursor[1],
            "replay_upper_timestamp": (
                replay_upper_timestamp
                if row_cursor < (replay_upper_timestamp, replay_upper_id)
                else None
            ),
            "replay_upper_id": replay_upper_id if row_cursor < (replay_upper_timestamp, replay_upper_id) else 0,
        })
    return result

__all__ = ["extract_incremental"]
