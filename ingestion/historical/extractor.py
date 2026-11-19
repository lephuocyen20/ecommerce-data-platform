"""Snapshot-consistent keyset extraction; never reads incremental metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .config import START_AT, TABLE_CONFIG, SUPPORTED_TABLES


@dataclass(frozen=True)
class ExtractionPage:
    table_name: str
    columns: tuple[str, ...]
    rows: tuple[tuple, ...]
    next_cursor: tuple[datetime, int] | None


@dataclass(frozen=True)
class SnapshotLifecycle:
    capture_end: datetime
    isolation_level: str = "REPEATABLE READ"
    read_only: bool = True


def begin_snapshot(conn, capture_end: datetime) -> SnapshotLifecycle:
    """Start one explicit source snapshot; callers must reuse this conn."""
    if not isinstance(capture_end, datetime) or capture_end <= START_AT:
        raise ValueError("capture_end must be after 2026-09-26")
    with conn.cursor() as cursor:
        cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", ())
    return SnapshotLifecycle(capture_end)


def build_historical_query(table_name: str) -> str:
    if table_name not in SUPPORTED_TABLES:
        raise ValueError(f"Unsupported historical table: {table_name}")
    primary_key = TABLE_CONFIG[table_name]["primary_key"]
    return (
        f"SELECT * FROM public.{table_name} WHERE (updated_at, {primary_key}) >= (%s, %s) "
        f"AND (updated_at, {primary_key}) <= (%s, %s) "
        f"ORDER BY updated_at ASC, {primary_key} ASC LIMIT %s"
    )


def extract_page(
    conn,
    table_name,
    capture_end,
    *,
    batch_size=1000,
    after=None,
    parent_closure=False,
) -> ExtractionPage:
    if table_name not in SUPPORTED_TABLES:
        raise ValueError(f"Unsupported historical table: {table_name}")
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if capture_end <= START_AT:
        raise ValueError("capture_end must be after 2026-09-26")
    primary_key = TABLE_CONFIG[table_name]["primary_key"]
    if parent_closure:
        # Parent rows must remain available to child rows in the same snapshot.
        # A mutable parent's updated_at can move past capture_end even though
        # the parent existed before the snapshot (for example, a product
        # touched by a later source write).  The parent-closure path therefore
        # keys the historical boundary on immutable creation time.
        if after is None:
            query = (f"SELECT * FROM public.{table_name} WHERE created_at <= %s "
                     f"ORDER BY created_at ASC, {primary_key} ASC LIMIT %s")
            params = (capture_end, batch_size)
        else:
            cursor_timestamp, cursor_id = after
            query = (f"SELECT * FROM public.{table_name} WHERE (created_at, {primary_key}) > (%s, %s) "
                     f"AND created_at <= %s ORDER BY created_at ASC, {primary_key} ASC LIMIT %s")
            params = (cursor_timestamp, cursor_id, capture_end, batch_size)
        cursor_field = "created_at"
    else:
        cursor_field = "updated_at"
    if not parent_closure and after is None:
        query = (f"SELECT * FROM public.{table_name} WHERE updated_at >= %s "
                 f"AND updated_at <= %s ORDER BY updated_at ASC, {primary_key} ASC LIMIT %s")
        params = (START_AT, capture_end, batch_size)
    elif not parent_closure:
        cursor_timestamp, cursor_id = after
        query = (f"SELECT * FROM public.{table_name} WHERE (updated_at, {primary_key}) > (%s, %s) "
                 f"AND updated_at <= %s ORDER BY updated_at ASC, {primary_key} ASC LIMIT %s")
        params = (cursor_timestamp, cursor_id, capture_end, batch_size)
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        columns = tuple(column[0] for column in cursor.description)
        rows = tuple(cursor.fetchall())
    next_cursor = None
    if rows:
        values = dict(zip(columns, rows[-1]))
        next_cursor = (values[cursor_field], values[primary_key])
    return ExtractionPage(table_name, columns, rows, next_cursor)


__all__ = ["ExtractionPage", "SnapshotLifecycle", "begin_snapshot", "build_historical_query", "extract_page"]
