"""Validated run-level lineage record shared by Gold certification tooling."""

import json
import re
from datetime import datetime


REQUIRED_FIELDS = {"run_id", "bronze_batch_ids", "silver_snapshots", "iceberg_snapshots", "attestations", "evidence", "gold_snapshot_id", "status", "code_sha", "started_at", "completed_at", "duration_seconds", "rows_read", "rows_written", "rows_rejected"}
_SHA = re.compile(r"^[a-f0-9]{7,64}$")


def build_gold_run_metadata(**values):
    if set(values) != REQUIRED_FIELDS:
        raise ValueError("Unexpected Gold run metadata fields")
    if not isinstance(values["run_id"], str) or not values["run_id"]:
        raise ValueError("run_id is required")
    if values["status"] not in {"PASS", "FAIL", "DRY_RUN"}:
        raise ValueError("Invalid run metadata status")
    if not isinstance(values["code_sha"], str) or not _SHA.fullmatch(values["code_sha"]):
        raise ValueError("Invalid code_sha")
    for field in ("started_at", "completed_at"):
        if not isinstance(values[field], str):
            raise ValueError(f"{field} must be ISO timestamp")
        datetime.fromisoformat(values[field].replace("Z", "+00:00"))
    if not isinstance(values["duration_seconds"], (int, float)) or values["duration_seconds"] < 0:
        raise ValueError("Invalid duration_seconds")
    for field in ("rows_read", "rows_written", "rows_rejected"):
        if not isinstance(values[field], int) or values[field] < 0:
            raise ValueError(f"Invalid {field}")
    for field in {"bronze_batch_ids", "silver_snapshots", "iceberg_snapshots", "attestations", "evidence"}:
        if not isinstance(values[field], dict):
            raise ValueError(f"{field} must be a mapping")
    tables = set(values["silver_snapshots"])
    if not tables or any(set(values[field]) != tables for field in ("bronze_batch_ids", "iceberg_snapshots", "attestations", "evidence")):
        raise ValueError("Lineage mappings must cover the same non-empty tables")
    if values["status"] == "PASS" and (not isinstance(values["gold_snapshot_id"], str) or not values["gold_snapshot_id"].startswith("gold_snap_")):
        raise ValueError("PASS requires a Gold snapshot ID")
    return dict(values)


def canonical_run_metadata(metadata):
    build_gold_run_metadata(**metadata)
    return json.dumps(metadata, sort_keys=True, separators=(",", ":"))
