"""Idempotent PostgreSQL repository for Gold run metadata.

The repository is deliberately opt-in. Importing it or constructing SQL never
opens a database connection; callers must explicitly enable persistence.
"""

import json
import os

from .run_metadata import canonical_run_metadata, build_gold_run_metadata


UPSERT_GOLD_RUN_METADATA_SQL = """
INSERT INTO public.gold_run_metadata (
    run_id, dataset_name, status, code_sha, started_at, completed_at,
    duration_seconds, rows_read, rows_written, rows_rejected, metadata
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (run_id, dataset_name) DO UPDATE SET
    status = EXCLUDED.status,
    code_sha = EXCLUDED.code_sha,
    started_at = EXCLUDED.started_at,
    completed_at = EXCLUDED.completed_at,
    duration_seconds = EXCLUDED.duration_seconds,
    rows_read = EXCLUDED.rows_read,
    rows_written = EXCLUDED.rows_written,
    rows_rejected = EXCLUDED.rows_rejected,
    metadata = EXCLUDED.metadata,
    updated_at = CURRENT_TIMESTAMP
RETURNING run_id, dataset_name
"""


def persist_gold_run_metadata(conn, dataset_name, metadata):
    """Upsert one validated record in the caller-owned transaction."""
    build_gold_run_metadata(**metadata)
    if not isinstance(dataset_name, str) or not dataset_name:
        raise ValueError("dataset_name is required")
    values = (
        metadata["run_id"], dataset_name, metadata["status"], metadata["code_sha"],
        metadata["started_at"], metadata["completed_at"], metadata["duration_seconds"],
        metadata["rows_read"], metadata["rows_written"], metadata["rows_rejected"],
        canonical_run_metadata(metadata),
    )
    with conn.cursor() as cursor:
        cursor.execute(UPSERT_GOLD_RUN_METADATA_SQL, values)
        row = cursor.fetchone()
    if row != (metadata["run_id"], dataset_name):
        raise RuntimeError("Gold run metadata upsert returned an unexpected identity")
    return {"run_id": row[0], "dataset_name": row[1], "persisted": True}


def persist_if_enabled(dataset_name, metadata, connection_factory=None, environ=None):
    """Persist only when explicitly enabled; default is no external write."""
    values = os.environ if environ is None else environ
    if values.get("GOLD_RUN_METADATA_PERSIST_ENABLED", "false").lower() != "true":
        return {"persisted": False, "reason": "GOLD_RUN_METADATA_PERSIST_ENABLED=false"}
    if connection_factory is None:
        raise RuntimeError("Gold run metadata persistence requires a connection factory")
    conn = connection_factory()
    try:
        result = persist_gold_run_metadata(conn, dataset_name, metadata)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
