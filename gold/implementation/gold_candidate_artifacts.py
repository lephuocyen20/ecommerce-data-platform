"""Pure, read-only artifact boundaries for the production Gold candidate gate.

This module deliberately imports no Spark, MinIO, PostgreSQL or Nessie client.
It only canonicalizes JSON-safe evidence already supplied by an approved
Silver DagRun and existing Gold snapshot metadata.
"""

from __future__ import annotations

import json
import re
from typing import Mapping

from common.manifest import canonical_manifest_identity
from .dimensional.snapshot_reuse import GoldSnapshotReuseAdapter

FACT_TABLES = ("orders", "order_items", "payments", "shipments")
DIMENSION_TABLES = ("customers", "products")
SILVER_TABLES = FACT_TABLES + DIMENSION_TABLES
_HEX64 = re.compile(r"^[a-f0-9]{64}$")


class CandidateArtifactError(ValueError):
    """Raised when candidate evidence is absent, fabricated or inconsistent."""


def _identity(value, field):
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise CandidateArtifactError(f"{field} must be a lowercase SHA-256 identity")
    return value


def _require_run_id(value):
    if not isinstance(value, str) or not value:
        raise CandidateArtifactError("approved DagRun identity is required")
    return value


def _validate_silver(table, artifact, *, dag_run_id, source_run_identity):
    if not isinstance(artifact, Mapping) or artifact.get("status") != "PASS":
        raise CandidateArtifactError(f"Silver artifact is not PASS: {table}")
    if artifact.get("table_name", artifact.get("source_table")) != table:
        raise CandidateArtifactError(f"Silver artifact table mismatch: {table}")
    if artifact.get("dag_run_id") != dag_run_id:
        raise CandidateArtifactError(f"Silver fact DagRun mismatch: {table}")
    if artifact.get("source_run_identity") != source_run_identity:
        raise CandidateArtifactError(f"Silver source run identity mismatch: {table}")
    if artifact.get("resolution") == "GOLD_SNAPSHOT_REUSE" or "snapshot_reuse" in artifact:
        raise CandidateArtifactError(f"fact snapshot reuse is forbidden: {table}")

    manifest_key = artifact.get("manifest_key")
    if not isinstance(manifest_key, str) or not manifest_key:
        raise CandidateArtifactError(f"Silver manifest key is missing: {table}")
    if artifact.get("manifest_keys") != [manifest_key]:
        raise CandidateArtifactError(f"Silver manifest key selection is not exact: {table}")
    manifest_identity = _identity(artifact.get("manifest_identity"), f"{table} manifest_identity")
    manifest = artifact.get("manifest")
    if manifest is not None and (
        not isinstance(manifest, Mapping) or canonical_manifest_identity(dict(manifest)) != manifest_identity
    ):
        raise CandidateArtifactError(f"Silver manifest identity mismatch: {table}")

    checksum = artifact.get("checksum_sha256", artifact.get("bronze_checksum_sha256"))
    _identity(checksum, f"{table} checksum_sha256")
    if artifact.get("checksum_sha256") is not None and artifact.get("bronze_checksum_sha256") is not None:
        if artifact["checksum_sha256"] != artifact["bronze_checksum_sha256"]:
            raise CandidateArtifactError(f"Silver checksum mismatch: {table}")
    if isinstance(manifest, Mapping):
        embedded_checksum = manifest.get("bronze_checksum_sha256", manifest.get("checksum_sha256"))
        if embedded_checksum is not None:
            _identity(embedded_checksum, f"{table} manifest checksum")
            if embedded_checksum != checksum:
                raise CandidateArtifactError(f"Silver manifest checksum mismatch: {table}")

    coverage = artifact.get("coverage")
    if not isinstance(coverage, Mapping) or coverage.get("runtime_window_id") != dag_run_id:
        raise CandidateArtifactError(f"Silver coverage is not bound to approved DagRun: {table}")
    lineage = artifact.get("lineage") or artifact.get("lineage_evidence")
    if not isinstance(lineage, Mapping) or not lineage:
        raise CandidateArtifactError(f"Silver lineage evidence is missing: {table}")
    if lineage.get("manifest_key", manifest_key) != manifest_key:
        raise CandidateArtifactError(f"Silver lineage manifest key mismatch: {table}")
    if lineage.get("manifest_identity", manifest_identity) != manifest_identity:
        raise CandidateArtifactError(f"Silver lineage manifest identity mismatch: {table}")
    if lineage.get("source_run_identity", source_run_identity) != source_run_identity:
        raise CandidateArtifactError(f"Silver lineage source run mismatch: {table}")
    return {**dict(artifact), "source_run_identity": source_run_identity, "checksum_sha256": checksum}


def _validated_silver_bundle(approved_run: Mapping, expected_tables):
    if not isinstance(approved_run, Mapping):
        raise CandidateArtifactError("approved Silver run must be a mapping")
    dag_run_id = _require_run_id(approved_run.get("dag_run_id"))
    source_run_identity = _require_run_id(approved_run.get("source_run_identity"))
    artifacts = approved_run.get("artifacts")
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(expected_tables):
        raise CandidateArtifactError(
            f"approved Silver run must contain exactly {len(expected_tables)} artifacts"
        )
    return {
        table: _validate_silver(
            table, artifacts[table], dag_run_id=dag_run_id,
            source_run_identity=source_run_identity,
        )
        for table in expected_tables
    }


def build_silver_fact_bundle(approved_run: Mapping) -> dict:
    """Build and validate exactly four facts from one approved DagRun."""
    if not isinstance(approved_run, Mapping):
        raise CandidateArtifactError("approved Silver run must be a mapping")
    dag_run_id = _require_run_id(approved_run.get("dag_run_id"))
    source_run_identity = _require_run_id(approved_run.get("source_run_identity"))
    facts = approved_run.get("artifacts")
    if not isinstance(facts, Mapping) or set(facts) != set(FACT_TABLES):
        raise CandidateArtifactError("approved Silver run must contain exactly four fact artifacts")
    return {table: _validate_silver(table, facts[table], dag_run_id=dag_run_id,
                                    source_run_identity=source_run_identity)
            for table in FACT_TABLES}


def build_silver_source_bundle(approved_run: Mapping) -> dict:
    """Build a complete six-table bundle from one verified Silver DagRun."""
    return _validated_silver_bundle(approved_run, SILVER_TABLES)


def build_gold_snapshot_bundle(snapshot_artifacts: Mapping, *, now=None) -> dict:
    """Validate both pinned Gold dimension snapshot artifacts without I/O."""
    if not isinstance(snapshot_artifacts, Mapping) or set(snapshot_artifacts) != set(DIMENSION_TABLES):
        raise CandidateArtifactError("Gold snapshot bundle must contain exactly customers and products")
    try:
        adapter = GoldSnapshotReuseAdapter(now=now)
        return {
            table: adapter.resolve(table, snapshot_artifacts[table]) for table in DIMENSION_TABLES
        }
    except Exception as exc:
        raise CandidateArtifactError("Gold snapshot bundle is incomplete or inconsistent") from exc


def build_candidate_artifact_bundle(approved_run: Mapping, snapshot_artifacts: Mapping | None = None, *, now=None) -> dict:
    """Build the canonical preflight input; no client or external write is created."""
    artifact_set = set((approved_run or {}).get("artifacts", {})) if isinstance(approved_run, Mapping) else set()
    if artifact_set == set(SILVER_TABLES):
        return build_silver_source_bundle(approved_run)
    if snapshot_artifacts is None:
        raise CandidateArtifactError("Gold snapshot bundle is required for the legacy four-fact path")
    return {
        **build_silver_fact_bundle(approved_run),
        **build_gold_snapshot_bundle(snapshot_artifacts, now=now),
    }


def validate_candidate_artifact_bundle(bundle: Mapping, *, now=None, expected_dag_run_id=None) -> dict:
    """Validate a previously built flat bundle and return a defensive copy."""
    if not isinstance(bundle, Mapping) or set(bundle) != set(FACT_TABLES) | set(DIMENSION_TABLES):
        raise CandidateArtifactError("candidate artifact bundle has an unexpected table set")
    full_silver = set(bundle) == set(SILVER_TABLES) and all(
        isinstance(item, Mapping) and item.get("status") == "PASS"
        and item.get("dag_run_id") is not None
        and item.get("source_run_identity") is not None
        for item in bundle.values()
    )
    if full_silver:
        dag_ids = {item.get("dag_run_id") for item in bundle.values() if isinstance(item, Mapping)}
        source_ids = {item.get("source_run_identity") for item in bundle.values() if isinstance(item, Mapping)}
        if len(dag_ids) != 1 or len(source_ids) != 1 or None in dag_ids or None in source_ids:
            raise CandidateArtifactError("Silver artifacts must share one DagRun and source run identity")
        approved = {"dag_run_id": next(iter(dag_ids)), "source_run_identity": next(iter(source_ids)),
                    "artifacts": dict(bundle)}
        if expected_dag_run_id is not None and approved["dag_run_id"] != expected_dag_run_id:
            raise CandidateArtifactError("Silver artifacts do not belong to the approved current DagRun")
        return build_silver_source_bundle(approved)
    facts = {table: bundle[table] for table in FACT_TABLES}
    dag_ids = {item.get("dag_run_id") for item in facts.values() if isinstance(item, Mapping)}
    source_ids = {item.get("source_run_identity") for item in facts.values() if isinstance(item, Mapping)}
    if len(dag_ids) != 1 or len(source_ids) != 1 or None in dag_ids or None in source_ids:
        raise CandidateArtifactError("facts must share one DagRun and source run identity")
    dag_run_id = next(iter(dag_ids))
    if expected_dag_run_id is not None and dag_run_id != expected_dag_run_id:
        raise CandidateArtifactError("Silver facts do not belong to the approved current DagRun")
    approved = {"dag_run_id": dag_run_id, "source_run_identity": next(iter(source_ids)), "artifacts": facts}
    return build_candidate_artifact_bundle(approved, {table: bundle[table] for table in DIMENSION_TABLES}, now=now)


__all__ = [
    "CandidateArtifactError", "FACT_TABLES", "DIMENSION_TABLES", "SILVER_TABLES",
    "build_silver_fact_bundle", "build_gold_snapshot_bundle",
    "build_silver_source_bundle", "build_candidate_artifact_bundle", "validate_candidate_artifact_bundle",
]
