"""Canonical object-key conventions for Bronze and incremental Silver."""

import os
import re

from common.storage_layout import PRODUCTION_BUCKET, QUALITY_BUCKET, bronze_table_key, silver_table_key

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


def bronze_incremental_object_key(table_name, batch_id, *, scope=None, environ=None):
    _require_name(table_name, "table name")
    _require_batch_id(batch_id)
    values = os.environ if environ is None else environ
    if scope is None and values.get("BRONZE_BUCKET") in (PRODUCTION_BUCKET, QUALITY_BUCKET):
        return bronze_table_key(table_name, f"batch_id={batch_id}/data.csv", environ=values)
    suffix = f"bronze_incremental/{table_name}/batch_id={batch_id}/data.csv"
    if scope is None and values.get("BRONZE_BUCKET") == TEST_BUCKET:
        scope = build_bounded_e2e_scope(values)
    return scope.key(suffix) if scope is not None else suffix


def silver_incremental_batch_paths(table_name, batch_id, *, validate_batch_id=True, scope=None, environ=None):
    _require_name(table_name, "table name")
    if validate_batch_id:
        _require_batch_id(batch_id)
    elif batch_id is None or isinstance(batch_id, bool):
        raise ValueError("Invalid batch ID")
    values = os.environ if environ is None else environ
    if scope is None and values.get("BRONZE_BUCKET") in (PRODUCTION_BUCKET, QUALITY_BUCKET):
        batch_prefix = silver_table_key(table_name, f"batch_id={batch_id}/", environ=values)
        return {"batch_prefix": batch_prefix, "runs_prefix": batch_prefix + "runs/", "manifest_key": batch_prefix + "_BATCH_METADATA.json"}
    suffix = f"silver_incremental/{table_name}/batch_id={batch_id}/"
    if scope is None and values.get("BRONZE_BUCKET") == TEST_BUCKET:
        scope = build_bounded_e2e_scope(values)
    batch_prefix = scope.key(suffix) if scope is not None else suffix
    return {"batch_prefix": batch_prefix, "runs_prefix": batch_prefix + "runs/", "manifest_key": batch_prefix + "_BATCH_METADATA.json"}
