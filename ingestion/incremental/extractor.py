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


def extract_incremental(conn, table_name, watermark, batch_size=1000, parent_order_ids=None):
    validate_extraction_contract()
    overlap_seconds = get_late_data_overlap_seconds()
    config = get_table_config(table_name)
    validate_positive_integer(batch_size, "batch_size")
    if parent_order_ids is not None:
        if table_name not in ("payments", "shipments"):
            raise ValueError("parent_order_ids are supported only for payments and shipments")
        if not isinstance(parent_order_ids, (list, tuple, set)):
            raise ValueError("parent_order_ids must be a sequence")
        # psycopg2 adapts lists to PostgreSQL arrays, while tuples are adapted
        # as row/composite expressions and render as ANY((...)).
        parent_order_ids = list(parent_order_ids)
        if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in parent_order_ids):
            raise ValueError("parent_order_ids must contain positive integers")
        if not parent_order_ids:
            return {"table_name": table_name, "columns": [], "rows": [], "row_count": 0}
    primary_key = config["primary_key"]
    watermark_column = config["watermark_column"]
    watermark_type = config["watermark_type"]

    if watermark["watermark_type"] != watermark_type:
        raise ValueError("Watermark type mismatch")

    replay_upper_timestamp = watermark.get("replay_upper_timestamp")
    replay_upper_id = watermark.get("replay_upper_id", 0)
    replay_mode = watermark_type == "TIMESTAMP" and overlap_seconds > 0 and watermark["last_timestamp"] is not None
    scope_sql = sql.SQL(" AND {} = ANY(%s)").format(sql.Identifier("order_id")) if parent_order_ids is not None else sql.SQL("")
    initial_scope_sql = sql.SQL(" WHERE {} = ANY(%s)").format(sql.Identifier("order_id")) if parent_order_ids is not None else sql.SQL("")
    upper_scope_sql = sql.SQL(" WHERE {} = ANY(%s)").format(sql.Identifier("order_id")) if parent_order_ids is not None else sql.SQL("")
    scope_params = (parent_order_ids,) if parent_order_ids is not None else ()
    with conn.cursor() as cursor:
        if watermark_type == "TIMESTAMP":
            if watermark["last_timestamp"] is None:
                query = sql.SQL("""
                    SELECT *
                    FROM public.{}
                    {} ORDER BY {} ASC, {} ASC
                    LIMIT %s
                """).format(
                    sql.Identifier(table_name),
                    initial_scope_sql,
                    sql.Identifier(watermark_column),
                    sql.Identifier(primary_key),
                )
                params = (*scope_params, batch_size)
            elif not replay_mode:
                query = sql.SQL("""
                    SELECT *
                    FROM public.{}
                    WHERE ({}, {}) > (%s, %s){}
                    ORDER BY {} ASC, {} ASC
                    LIMIT %s
                """).format(
                    sql.Identifier(table_name),
                    sql.Identifier(watermark_column),
                    sql.Identifier(primary_key),
                    scope_sql,
                    sql.Identifier(watermark_column),
                    sql.Identifier(primary_key),
                )
                params = (
                    watermark["last_timestamp"],
                    watermark["last_id"],
                    *scope_params,
                    batch_size,
                )
            else:
                replay_cursor_timestamp = watermark.get("replay_cursor_timestamp")
                replay_cursor_id = watermark.get("replay_cursor_id", 0)
                if replay_cursor_timestamp is None:
                    replay_cursor_timestamp = watermark["last_timestamp"] - timedelta(seconds=overlap_seconds)
                if replay_upper_timestamp is None:
                    cursor.execute(
                        sql.SQL("SELECT MAX({}), MAX({}) FILTER (WHERE {} = (SELECT MAX({}) FROM public.{}{})) FROM public.{}{}" ).format(
                            sql.Identifier(watermark_column), sql.Identifier(primary_key), sql.Identifier(watermark_column),
                            sql.Identifier(watermark_column), sql.Identifier(table_name), upper_scope_sql,
                            sql.Identifier(table_name), upper_scope_sql
                        ), (*scope_params, *scope_params)
                    )
                    replay_upper_timestamp, replay_upper_id = cursor.fetchone()
                if replay_upper_timestamp is None:
                    return {"table_name": table_name, "columns": [], "rows": [], "row_count": 0}
                query = sql.SQL("""
                    SELECT * FROM public.{}
                    WHERE ({}, {}) > (%s, %s) AND ({}, {}) <= (%s, %s){}
                    ORDER BY {} ASC, {} ASC LIMIT %s
                """).format(
                    sql.Identifier(table_name), sql.Identifier(watermark_column),
                    sql.Identifier(primary_key), sql.Identifier(watermark_column), sql.Identifier(primary_key),
                    sql.Identifier(watermark_column), sql.Identifier(primary_key), scope_sql,
                )
                params = (replay_cursor_timestamp, replay_cursor_id, replay_upper_timestamp, replay_upper_id, *scope_params, batch_size)

        else:
            query = sql.SQL("""
                SELECT *
                FROM public.{}
                WHERE {} > %s{}
                ORDER BY {} ASC
                LIMIT %s
            """).format(
                sql.Identifier(table_name),
                sql.Identifier(primary_key), scope_sql,
                sql.Identifier(primary_key),
            )
            params = (watermark["last_id"], *scope_params, batch_size)

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
