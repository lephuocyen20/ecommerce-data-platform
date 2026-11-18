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


def extract_page(conn, table_name, capture_end, *, batch_size=1000, after=None) -> ExtractionPage:
    if table_name not in SUPPORTED_TABLES:
        raise ValueError(f"Unsupported historical table: {table_name}")
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if capture_end <= START_AT:
        raise ValueError("capture_end must be after 2026-09-26")
    primary_key = TABLE_CONFIG[table_name]["primary_key"]
    if after is None:
        query = (f"SELECT * FROM public.{table_name} WHERE updated_at >= %s "
                 f"AND updated_at <= %s ORDER BY updated_at ASC, {primary_key} ASC LIMIT %s")
        params = (START_AT, capture_end, batch_size)
    else:
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
        next_cursor = (values["updated_at"], values[primary_key])
    return ExtractionPage(table_name, columns, rows, next_cursor)


__all__ = ["ExtractionPage", "SnapshotLifecycle", "begin_snapshot", "build_historical_query", "extract_page"]
