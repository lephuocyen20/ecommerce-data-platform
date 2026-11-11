"""Validated run-level lineage record shared by Gold certification tooling."""

import json
import os
import re
from datetime import datetime, timezone


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


def build_runtime_dimensional_metadata(*, lineage, result, mode, environment=None, started_at=None):
    """Build one run-scoped document after the dimensional writer completes."""
    values = os.environ if environment is None else environment
    started = started_at or datetime.now(timezone.utc)
    completed = datetime.now(timezone.utc)
    sources = lineage["sources"]
    run_id = values.get("GOLD_RUN_ID") or values.get("AIRFLOW_CTX_DAG_RUN_ID") or values.get("LAKEHOUSE_E2E_RUN_ID")
    if not run_id:
        raise ValueError("GOLD_RUN_ID or Airflow run context is required for metadata persistence")
    code_sha = values.get("GOLD_CODE_SHA", "local000")
    status = "PASS" if mode == "WRITE" and result.get("status") in {"PUBLISHED", "ALREADY_PUBLISHED"} else "DRY_RUN"
    graph_identity = result.get("graph", {}).get("identity")
    gold_snapshot_id = f"gold_snap_{graph_identity}" if status == "PASS" and graph_identity else None
    if all(source.get("source_type") == "parquet_manifest" for source in sources.values()):
        silver_ids = {table: source["manifest_identity"] for table, source in sources.items()}
        gold_ids = {
            item["table"]: item["manifest"]["snapshot_id"]
            for item in result.get("tables", [])
            if isinstance(item, dict) and isinstance(item.get("manifest"), dict)
        }
        # Keep the persisted envelope backward-compatible while assigning the
        # fields their new meaning: Silver identifiers are manifest identities,
        # and Iceberg identifiers are Gold output snapshots only.
        return build_gold_run_metadata(
            run_id=run_id,
            bronze_batch_ids={table: source.get("bronze_batch_id") for table, source in sources.items()},
            silver_snapshots=silver_ids,
            iceberg_snapshots=gold_ids or {table: None for table in sources},
            attestations=silver_ids,
            evidence=silver_ids,
            gold_snapshot_id=gold_snapshot_id,
            status=status,
            code_sha=code_sha,
            started_at=started.isoformat().replace("+00:00", "Z"),
            completed_at=completed.isoformat().replace("+00:00", "Z"),
            duration_seconds=max(0, (completed - started).total_seconds()),
            rows_read=sum(source.get("row_count", 0) for source in sources.values()),
            rows_written=sum(item.get("row_count", 0) for item in result.get("quality", {}).values()),
            rows_rejected=0,
        )
    return build_gold_run_metadata(
        run_id=run_id,
        bronze_batch_ids={table: source.get("bronze_batch_id") for table, source in sources.items()},
        silver_snapshots={table: str(source["snapshot_id"]) for table, source in sources.items()},
        iceberg_snapshots={table: source["snapshot_id"] for table, source in sources.items()},
        attestations={table: source["snapshot_attestation_identity"] for table, source in sources.items()},
        evidence={table: source["quality_evidence"]["identity"] for table, source in sources.items()},
        gold_snapshot_id=gold_snapshot_id,
        status=status,
        code_sha=code_sha,
        started_at=started.isoformat().replace("+00:00", "Z"),
        completed_at=completed.isoformat().replace("+00:00", "Z"),
        duration_seconds=max(0, (completed - started).total_seconds()),
        rows_read=sum(source.get("row_count", 0) for source in sources.values()),
        rows_written=sum(item.get("row_count", 0) for item in result.get("quality", {}).values()),
        rows_rejected=0,
    )
