"""Canonical object-key conventions for Bronze, Silver and Silver snapshots."""

import os
import re

from common.bounded_e2e_scope import TEST_BUCKET, build_bounded_e2e_scope

_BATCH_ID = re.compile(r"^[0-9a-f]{32}$")
_NAME = re.compile(r"^[A-Za-z0-9_-]+$")


def _require_name(value, label):
    if not isinstance(value, str) or not _NAME.fullmatch(value):
        raise ValueError(f"Invalid {label}")
    return value


def _require_batch_id(batch_id):
    if not isinstance(batch_id, str) or not _BATCH_ID.fullmatch(batch_id):
        raise ValueError("Invalid batch ID")
    return batch_id


def bronze_incremental_object_key(table_name, batch_id, *, scope=None):
    _require_name(table_name, "table name")
    _require_batch_id(batch_id)
    suffix = f"bronze_incremental/{table_name}/batch_id={batch_id}/data.csv"
    if scope is None and os.environ.get("BRONZE_BUCKET") == TEST_BUCKET:
        scope = build_bounded_e2e_scope(os.environ)
    return scope.key(suffix) if scope is not None else suffix


def silver_incremental_batch_paths(table_name, batch_id, *, validate_batch_id=True, scope=None):
    _require_name(table_name, "table name")
    if validate_batch_id:
        _require_batch_id(batch_id)
    elif batch_id is None or isinstance(batch_id, bool):
        raise ValueError("Invalid batch ID")
    suffix = f"silver_incremental/{table_name}/batch_id={batch_id}/"
    batch_prefix = scope.key(suffix) if scope is not None else suffix
    return {"batch_prefix": batch_prefix, "runs_prefix": batch_prefix + "runs/", "manifest_key": batch_prefix + "_BATCH_METADATA.json"}


def silver_snapshot_paths(table_name, snapshot_id):
    _require_name(table_name, "snapshot table")
    _require_name(snapshot_id, "snapshot ID")
    snapshot_prefix = f"silver_snapshots/{table_name}/snapshots/snapshot_id={snapshot_id}/"
    return {"snapshot_prefix": snapshot_prefix, "data_prefix": snapshot_prefix + "data/", "manifest_key": snapshot_prefix + "_SNAPSHOT_METADATA.json"}
