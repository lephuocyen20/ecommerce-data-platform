"""Separate historical run metadata repository; never touches incremental tables."""

from __future__ import annotations

import re
from datetime import datetime

from .config import SUPPORTED_TABLES

_BATCH_ID = re.compile(r"^[0-9a-f]{32}$")
_STATUSES = ("PENDING", "UPLOADED", "VERIFIED", "FAILED")
_TRANSITIONS = {"PENDING": {"PENDING", "UPLOADED", "FAILED"}, "UPLOADED": {"UPLOADED", "VERIFIED", "FAILED"}, "VERIFIED": {"VERIFIED"}, "FAILED": {"FAILED", "PENDING"}}
_FIELDS = ("batch_id", "run_id", "table_name", "row_count", "object_key", "checksum_sha256", "window_start", "window_end", "attempt", "status")


def _decode(row):
    if row is None:
        return None
    if len(row) == 4:
        return dict(zip(("batch_id", "run_id", "table_name", "status"), row))
    if len(row) == 7:
        return dict(zip(("batch_id", "run_id", "table_name", "row_count", "object_key", "checksum_sha256", "status"), row))
    return dict(zip(_FIELDS, row))


def validate_batch_id(batch_id):
    if not isinstance(batch_id, str) or _BATCH_ID.fullmatch(batch_id) is None:
        raise ValueError("historical batch_id must be a 32-character hex value")


def register_batch(conn, metadata):
    required = ("batch_id", "run_id", "table_name", "row_count", "object_key", "checksum_sha256", "status")
    missing = [field for field in required if field not in metadata]
    if missing:
        raise ValueError(f"Missing historical metadata fields: {missing}")
    validate_batch_id(metadata["batch_id"])
    if metadata["table_name"] not in SUPPORTED_TABLES:
        raise ValueError("Unsupported historical table")
    if metadata["status"] != "PENDING":
        raise ValueError("Historical batches must start as PENDING")
    window_start = metadata.get("window_start", datetime(2026, 9, 26))
    window_end = metadata.get("window_end", datetime(2026, 9, 27))
    attempt = metadata.get("attempt", 1)
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt <= 0:
        raise ValueError("historical attempt must be a positive integer")
    with conn.cursor() as cursor:
        cursor.execute(
            """INSERT INTO public.historical_backfill_batches
               (batch_id, run_id, table_name, row_count, object_key, checksum_sha256, status, window_start, window_end, attempt)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (run_id, table_name, window_start, window_end) DO NOTHING
               RETURNING batch_id, run_id, table_name, row_count, object_key, checksum_sha256, window_start, window_end, attempt, status""",
            (*tuple(metadata[field] for field in required), window_start, window_end, attempt),
        )
        result = cursor.fetchone()
        if result is None:
            cursor.execute("SELECT batch_id, run_id, table_name, row_count, object_key, checksum_sha256, window_start, window_end, attempt, status FROM public.historical_backfill_batches WHERE run_id=%s AND table_name=%s AND window_start=%s AND window_end=%s", (metadata["run_id"], metadata["table_name"], window_start, window_end))
            result = cursor.fetchone()
        existing = _decode(result)
        if existing is not None and len(result) == 4:
            return existing
        if existing is not None and existing.get("batch_id") != metadata["batch_id"]:
            raise ValueError("historical run/table/window identity conflict")
        if existing is not None and any(existing.get(key) != metadata[key] for key in ("run_id", "table_name", "row_count", "object_key", "checksum_sha256")):
            raise ValueError("historical batch identity/checksum conflict")
        if existing is None:
            cursor.execute("SELECT batch_id FROM public.historical_backfill_batches WHERE batch_id=%s", (metadata["batch_id"],))
            if cursor.fetchone() is not None:
                raise ValueError("historical batch identity conflict")
    if result is None:
        raise RuntimeError("Historical batch insert did not return its identity")
    return existing


def transition_batch(conn, batch_id, status, *, expected_status=None, expected_attempt=None, checksum_sha256=None, row_count=None):
    validate_batch_id(batch_id)
    if status not in _STATUSES:
        raise ValueError("invalid historical batch status")
    current = get_batch(conn, batch_id)
    if current is None:
        raise ValueError("historical batch does not exist")
    expected_status = current["status"] if expected_status is None else expected_status
    expected_attempt = current.get("attempt", 1) if expected_attempt is None else expected_attempt
    if expected_status != current["status"]:
        raise ValueError("historical batch expected status is stale")
    if status not in _TRANSITIONS[expected_status]:
        raise ValueError(f"invalid historical batch transition {expected_status}->{status}")
    if checksum_sha256 is not None and checksum_sha256 != current["checksum_sha256"]:
        raise ValueError("checksum cannot change during retry")
    next_attempt = expected_attempt + 1 if expected_status == "FAILED" and status == "PENDING" else expected_attempt
    with conn.cursor() as cursor:
        cursor.execute("UPDATE public.historical_backfill_batches SET status=%s, row_count=COALESCE(%s, row_count), attempt=%s, updated_at=CURRENT_TIMESTAMP WHERE batch_id=%s AND status=%s AND attempt=%s RETURNING batch_id, run_id, table_name, row_count, object_key, checksum_sha256, window_start, window_end, attempt, status", (status, row_count, next_attempt, batch_id, expected_status, expected_attempt))
        updated = _decode(cursor.fetchone())
    if updated is None:
        raise ValueError("historical batch transition lost a state race")
    return updated


def get_batch(conn, batch_id):
    validate_batch_id(batch_id)
    with conn.cursor() as cursor:
        cursor.execute("SELECT batch_id, run_id, table_name, row_count, object_key, checksum_sha256, window_start, window_end, attempt, status FROM public.historical_backfill_batches WHERE batch_id = %s", (batch_id,))
        return _decode(cursor.fetchone())


__all__ = ["get_batch", "register_batch", "transition_batch", "validate_batch_id"]
