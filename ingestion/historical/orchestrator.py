"""Historical orchestration plan with parent-first ordering and explicit write guard."""

from __future__ import annotations

import uuid
import re

from .config import (
    HistoricalBackfillConfig,
    SUPPORTED_TABLES,
    candidate_ref_for_run,
    require_write_approval,
    production_environment,
)

PARENT_FIRST_ORDER = ("customers", "products", "orders", "order_items", "payments", "shipments")
WRITE_MODE = "WRITE"
DRY_RUN_MODE = "DRY_RUN"
_FORBIDDEN_QUALITY_FLAGS = (
    "LAKEHOUSE_E2E_APPROVED",
    "AIRFLOW_LOCAL_E2E_MODE",
    "QUALITY_IT_ENABLED",
    "QUALITY_IT_GATE2_APPROVED",
    "QUALITY_IT_RUN_ID",
    "QUALITY_IT_PREFIX",
    "QUALITY_IT_TEST_BUCKET",
)
_ENABLED_FLAG_VALUES = frozenset({"1", "true", "yes", "on"})


def _is_enabled_flag(value):
    """Interpret environment flags explicitly; ``"0"`` is not truthy here."""
    return isinstance(value, str) and value.strip().lower() in _ENABLED_FLAG_VALUES


def new_batch_id():
    return uuid.uuid4().hex


def resolve_execution_mode(config: HistoricalBackfillConfig, environ=None):
    """Resolve the explicit historical mode before any external client exists."""
    values = {} if environ is None else environ
    mode = values.get("HISTORICAL_BACKFILL_MODE", DRY_RUN_MODE).upper()
    config = config.validate()
    if mode == DRY_RUN_MODE:
        return config, False
    if mode != WRITE_MODE:
        raise ValueError("HISTORICAL_BACKFILL_MODE must be DRY_RUN or WRITE")
    expected_ref = candidate_ref_for_run(config.run_id)
    production_environment(values, candidate_ref=expected_ref)
    require_write_approval(values)
    if any(_is_enabled_flag(values.get(name)) for name in _FORBIDDEN_QUALITY_FLAGS):
        raise ValueError("historical backfill WRITE cannot run with E2E or quality flags")
    return HistoricalBackfillConfig(config.run_id, config.capture_end, config.batch_size, dry_run=False), True


def require_verified_silver_artifacts(artifacts):
    """Accept exactly six real, verified Silver artifact envelopes.

    This is a transport boundary, not a placeholder/planning contract.  Every
    envelope must carry enough independently checkable identity to locate the
    physical Silver result and reconcile it to its producing batch.
    """
    expected = set(SUPPORTED_TABLES)
    if not isinstance(artifacts, dict) or set(artifacts) != expected:
        raise RuntimeError("historical Silver must provide exactly six artifact envelopes")
    for table in SUPPORTED_TABLES:
        artifact = artifacts[table]
        if not isinstance(artifact, dict):
            raise RuntimeError(f"historical Silver artifact is not verified: {table}")
        values = _flatten_artifact_values(artifact)
        source_table = artifact.get("source_table", artifact.get("table_name"))
        if source_table != table or artifact.get("status") != "PASS":
            raise RuntimeError(f"historical Silver artifact is not verified: {table}")
        if _contains_placeholder(artifact):
            raise RuntimeError(f"historical Silver artifact contains a placeholder: {table}")
        if not _non_empty_string(values, "artifact_identity", "identity"):
            raise RuntimeError(f"historical Silver artifact is not verified: {table}; artifact identity is missing")
        if not (
            _non_empty_string(values, "object_key", "object_path", "path", "data_path", "parquet_path")
            or _positive_int(values.get("snapshot_id"))
        ):
            raise RuntimeError(f"historical Silver artifact is not verified: {table}; object/path or snapshot identity is missing")
        if not _non_empty_string(values, "batch_id", "backfill_id", "run_id", "backfill_run_id"):
            raise RuntimeError(f"historical Silver artifact is not verified: {table}; batch/backfill identity is missing")
        if not _non_empty_string(values, "checksum_sha256", "checksum", "manifest_identity", "manifest_id"):
            raise RuntimeError(f"historical Silver artifact is not verified: {table}; checksum/manifest identity is missing")
        row_count = values.get("row_count", values.get("silver_row_count"))
        if not _positive_int(row_count):
            raise RuntimeError(f"historical Silver artifact is not verified: {table}; row_count must be a positive integer")
    return artifacts


def _flatten_artifact_values(value):
    flattened = {}
    if isinstance(value, dict):
        for key, child in value.items():
            flattened.setdefault(key, child)
            flattened.update(_flatten_artifact_values(child))
    return flattened


def _non_empty_string(values, *keys):
    return any(isinstance(values.get(key), str) and values[key].strip() for key in keys)


def _positive_int(value):
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _contains_placeholder(value):
    markers = re.compile(r"(?:placeholder|planned|fake|synthetic|dummy|example|todo|not[_ -]?implemented)", re.I)
    if isinstance(value, dict):
        return any(_contains_placeholder(key) or _contains_placeholder(child) for key, child in value.items())
    if isinstance(value, (list, tuple)):
        return any(_contains_placeholder(child) for child in value)
    return isinstance(value, str) and bool(markers.search(value))


def build_execution_plan(config: HistoricalBackfillConfig, environ=None, *, write=False):
    config = config.validate()
    if write:
        production_environment(environ, candidate_ref=config.candidate_ref)
        require_write_approval(environ)
    mode = WRITE_MODE if write else DRY_RUN_MODE
    return {"run_id": config.run_id, "candidate_ref": config.candidate_ref, "capture_start": "2026-09-26T00:00:00", "capture_end": config.capture_end.isoformat(), "tables": list(PARENT_FIRST_ORDER), "mode": "WRITE" if write else "DRY_RUN", "uses_incremental_watermarks": False, "metadata_table": "public.historical_backfill_batches", "snapshot_isolation": "REPEATABLE READ", "lower_boundary": "updated_at >= window_start", "gold_writer": "UNAVAILABLE_FAIL_CLOSED"}


def assert_parent_first(plan):
    if tuple(plan.get("tables", ())) != PARENT_FIRST_ORDER:
        raise ValueError("Historical execution must use parent-first table order")
    if plan.get("uses_incremental_watermarks") is not False:
        raise ValueError("Historical execution cannot use incremental watermarks")
    return plan


__all__ = ["DRY_RUN_MODE", "PARENT_FIRST_ORDER", "WRITE_MODE", "assert_parent_first", "build_execution_plan", "new_batch_id", "require_verified_silver_artifacts", "resolve_execution_mode"]
