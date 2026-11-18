"""Pure contract for the production Gold candidate write boundary.

This module intentionally has no Spark, MinIO, PostgreSQL or Nessie imports.
It validates the immutable target and the bounded input envelope before an
orchestrator is allowed to construct a runtime client.
"""

from __future__ import annotations

import re
import hashlib
import json
from dataclasses import dataclass
from typing import Mapping

from common.manifest import canonical_manifest_identity
from .dimensional.contracts import WRITE_ORDER
from .dimensional.snapshot_reuse import GoldSnapshotReuseAdapter
from .gold_candidate_artifacts import (
    CandidateArtifactError,
    build_candidate_artifact_bundle,
    build_silver_source_bundle,
    validate_candidate_artifact_bundle,
    SILVER_TABLES,
)

STABLE_REF = "gold_prod_current"
FACT_SOURCES = ("orders", "order_items", "payments", "shipments")
REUSED_DIMENSIONS = ("customers", "products")
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")
_CANDIDATE_REF = re.compile(r"^gold_prod_candidate_[A-Za-z0-9][A-Za-z0-9_-]{0,47}$")
_RUN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_HEX64 = re.compile(r"^[a-f0-9]{64}$")


def _remove_prefix(value: str, prefix: str) -> str:
    """Return *value* without *prefix*, using Python 3.8-compatible syntax."""
    return value[len(prefix):] if value.startswith(prefix) else value

FORBIDDEN_FLAGS = (
    "PRODUCTION_LAKEHOUSE_WRITE_APPROVED", "GOLD_DIMENSIONAL_WRITE_ENABLED",
    "GOLD_PUBLICATION_ENABLED", "GOLD_CUTOVER_ENABLED", "GOLD_CLEANUP_ENABLED",
    "GOLD_CHECKPOINT_RESET_ENABLED", "GOLD_WATERMARK_MUTATION_ENABLED",
    "GOLD_COMPACTION_ENABLED", "GOLD_SNAPSHOT_EXPIRATION_ENABLED",
    "GOLD_RUN_METADATA_PERSIST_ENABLED", "GOLD_MANIFEST_MUTATION_ENABLED",
    "GOLD_ICEBERG_ROLLBACK_ENABLED", "GOLD_ORPHAN_CLEANUP_ENABLED",
    "BI_ACTIVE_PUBLISH_APPLY", "HISTORICAL_BACKFILL_WRITE_APPROVED",
)
CANDIDATE_WRITE_APPROVAL = "GOLD_PRODUCTION_CANDIDATE_WRITE_APPROVED"


class CandidateContractError(ValueError):
    pass


@dataclass(frozen=True)
class GoldCandidatePlan:
    run_id: str
    candidate_ref: str
    target_namespace: str
    source_ref: str = STABLE_REF
    mode: str = "WRITE"
    silver_dag_run_id: str | None = None

    def validate(self) -> "GoldCandidatePlan":
        if _RUN.fullmatch(self.run_id or "") is None:
            raise CandidateContractError("run_id must be an immutable safe identifier")
        if not isinstance(self.candidate_ref, str) or not self.candidate_ref:
            raise CandidateContractError("candidate_ref is required")
        if self.candidate_ref == STABLE_REF:
            raise CandidateContractError("candidate_ref cannot be gold_prod_current")
        if _CANDIDATE_REF.fullmatch(self.candidate_ref) is None:
            raise CandidateContractError("candidate_ref must be an explicit gold_prod_candidate ref")
        if _NAME.fullmatch(self.target_namespace or "") is None:
            raise CandidateContractError("target_namespace must be an explicit safe namespace")
        if self.target_namespace == "gold_prod_current":
            raise CandidateContractError("target_namespace cannot be gold_prod_current")
        expected_namespace = "gold_candidate_" + _remove_prefix(
            self.candidate_ref, "gold_prod_candidate_"
        )
        if self.target_namespace != expected_namespace:
            raise CandidateContractError("target_namespace must deterministically match candidate_ref")
        if self.source_ref != STABLE_REF:
            raise CandidateContractError("candidate source_ref must be gold_prod_current")
        if self.mode != "WRITE":
            raise CandidateContractError("candidate plan mode must be WRITE")
        if self.silver_dag_run_id is not None and _RUN.fullmatch(self.silver_dag_run_id) is None:
            raise CandidateContractError("silver_dag_run_id must be an immutable safe identifier")
        return self


def validate_candidate_environment(environ: Mapping[str, str], plan: GoldCandidatePlan):
    plan.validate()
    if environ.get("NESSIE_REF") not in (None, plan.candidate_ref):
        raise CandidateContractError("NESSIE_REF must match the explicit candidate ref")
    if environ.get("GOLD_TARGET_NAMESPACE") not in (None, plan.target_namespace):
        raise CandidateContractError("GOLD_TARGET_NAMESPACE must match the candidate namespace")
    if environ.get(CANDIDATE_WRITE_APPROVAL) != "1":
        raise CandidateContractError(f"{CANDIDATE_WRITE_APPROVAL}=1 is required for candidate WRITE")
    for flag in FORBIDDEN_FLAGS:
        if environ.get(flag, "").strip().lower() in {"1", "true", "yes", "on"}:
            raise CandidateContractError(f"candidate WRITE rejects active forbidden flag: {flag}")
    return {"NESSIE_REF": plan.candidate_ref, "GOLD_TARGET_NAMESPACE": plan.target_namespace}


def validate_fact_and_dimension_bundle(bundle: Mapping, *, now=None, expected_dag_run_id=None):
    """Validate exactly four current fact artifacts and two pinned dimensions."""
    if not isinstance(bundle, Mapping):
        raise CandidateContractError("Silver bundle must be a mapping")
    # Gate C's canonical boundary keeps the approved DagRun envelope separate
    # from the flat legacy writer input.  It remains pure and runs before the
    # candidate orchestrator can create a Nessie/runtime client.
    if set(bundle) in ({"silver_run", "gold_snapshots"}, {"silver_run"}):
        try:
            built = build_candidate_artifact_bundle(
                bundle["silver_run"], bundle.get("gold_snapshots"), now=now
            )
            return validate_candidate_artifact_bundle(
                built, now=now, expected_dag_run_id=expected_dag_run_id
            )
        except CandidateArtifactError as exc:
            raise CandidateContractError(str(exc)) from exc
    full_silver = set(bundle) == set(SILVER_TABLES) and all(
        isinstance(item, Mapping) and item.get("status") == "PASS"
        and item.get("dag_run_id") is not None
        and item.get("source_run_identity") is not None
        for item in bundle.values()
    )
    if full_silver:
        try:
            built = validate_candidate_artifact_bundle(
                bundle, now=now, expected_dag_run_id=expected_dag_run_id
            )
        except CandidateArtifactError as exc:
            raise CandidateContractError(str(exc)) from exc
        return {"facts": {table: built[table] for table in FACT_SOURCES},
                "dimensions": {table: built[table] for table in REUSED_DIMENSIONS}}
    expected = set(FACT_SOURCES) | set(REUSED_DIMENSIONS)
    if set(bundle) != expected:
        raise CandidateContractError("candidate input bundle must contain exactly four facts and two dimensions")
    for table in FACT_SOURCES:
        artifact = bundle[table]
        if not isinstance(artifact, Mapping) or artifact.get("status") != "PASS":
            raise CandidateContractError(f"current Silver fact is not PASS: {table}")
        if artifact.get("table_name", artifact.get("source_table", table)) != table:
            raise CandidateContractError(f"Silver fact table mismatch: {table}")
        if artifact.get("resolution") == "GOLD_SNAPSHOT_REUSE" or "snapshot_reuse" in artifact:
            raise CandidateContractError(f"fact snapshot reuse is forbidden: {table}")
        _validate_current_silver_artifact(table, artifact)
        # Preserve the security-specific diagnostic before validating the full
        # Silver envelope, so a fact can never masquerade as a reusable dim.
        if artifact.get("resolution") == "GOLD_SNAPSHOT_REUSE" or "snapshot_reuse" in artifact:
            raise CandidateContractError(f"fact snapshot reuse is forbidden: {table}")
    dag_runs = {
        artifact.get("dag_run_id") or artifact["coverage"].get("runtime_window_id")
        for artifact in (bundle[table] for table in FACT_SOURCES)
    }
    if len(dag_runs) != 1 or None in dag_runs:
        raise CandidateContractError("Silver facts must belong to one current DagRun")
    source_run_identities = {bundle[table].get("source_run_identity") for table in FACT_SOURCES}
    if (len(source_run_identities) != 1
            or None in source_run_identities
            or not isinstance(next(iter(source_run_identities)), str)
            or not next(iter(source_run_identities)).strip()):
        raise CandidateContractError("Silver facts must share one non-empty source run identity")
    if expected_dag_run_id is not None and dag_runs != {expected_dag_run_id}:
        raise CandidateContractError("Silver facts do not belong to the approved current DagRun")
    resolved = {}
    for source in REUSED_DIMENSIONS:
        try:
            resolved[source] = GoldSnapshotReuseAdapter(now=now).resolve(source, bundle[source])
        except Exception as exc:
            raise CandidateContractError(f"invalid pinned dimension snapshot: {source}") from exc
    return {"facts": {table: bundle[table] for table in FACT_SOURCES}, "dimensions": resolved}


def validate_exact_readback(report: Mapping):
    if not isinstance(report, Mapping) or report.get("status") != "VERIFIED":
        raise CandidateContractError("candidate result must be VERIFIED")
    if report.get("stable_ref") is not None or report.get("published") is not False:
        raise CandidateContractError("candidate result must have no publication/cutover edge")
    candidate_ref = report.get("candidate_ref")
    namespace = report.get("target_namespace", report.get("namespace"))
    if _CANDIDATE_REF.fullmatch(candidate_ref or "") is None or _NAME.fullmatch(namespace or "") is None:
        raise CandidateContractError("candidate readback must be bound to a safe candidate ref and namespace")
    manifests = report.get("table_manifests")
    if (not isinstance(manifests, Mapping) or set(manifests) != set(WRITE_ORDER)
            or any(not isinstance(manifests[t], Mapping) or not manifests[t] for t in WRITE_ORDER)):
        raise CandidateContractError("candidate result must contain exactly seven table manifests")
    graph_manifest = report.get("graph_manifest")
    if not isinstance(graph_manifest, Mapping) or not graph_manifest:
        raise CandidateContractError("candidate result must contain one graph manifest")
    graph_identity = graph_manifest.get("graph_identity") or graph_manifest.get("identity")
    if not isinstance(graph_identity, str) or not graph_identity:
        raise CandidateContractError("candidate graph manifest identity is required")
    graph_outputs = graph_manifest.get("outputs")
    if (not isinstance(graph_outputs, list) or len(graph_outputs) != 7
            or {item.get("table") for item in graph_outputs if isinstance(item, Mapping)} != set(WRITE_ORDER)):
        raise CandidateContractError("candidate graph manifest must reference all seven tables")
    tables = report.get("tables")
    if tuple(tables or ()) != tuple(WRITE_ORDER):
        raise CandidateContractError("candidate result must contain exactly seven Gold tables in contract order")
    outputs = report.get("outputs")
    if not isinstance(outputs, list) or len(outputs) != 7:
        raise CandidateContractError("candidate result must contain seven readbacks")
    seen = set()
    for item in outputs:
        if not isinstance(item, Mapping) or item.get("table") not in WRITE_ORDER:
            raise CandidateContractError("candidate readback contains an unexpected table")
        table = item["table"]
        if table in seen:
            raise CandidateContractError(f"duplicate candidate readback: {table}")
        seen.add(table)
        if item.get("status") != "PASS" or item.get("manifest_status") != "PASS":
            raise CandidateContractError(f"candidate readback is not PASS: {table}")
        if item.get("candidate_ref") != candidate_ref or item.get("namespace", item.get("target_namespace")) != namespace:
            raise CandidateContractError(f"candidate readback target mismatch: {table}")
        manifest = manifests[table]
        if (manifest.get("table") != table or manifest.get("graph_identity") != graph_identity
                or manifest.get("identity") != item.get("manifest_identity")
                or not isinstance(manifest.get("identity"), str)
                or not manifest.get("identity")):
            raise CandidateContractError(f"candidate manifest identity mismatch: {table}")
        if item.get("snapshot_id") != manifest.get("snapshot_id"):
            raise CandidateContractError(f"candidate snapshot mismatch: {table}")
        graph_item = next((entry for entry in graph_outputs if entry.get("table") == table), None)
        if (not isinstance(graph_item, Mapping)
                or graph_item.get("identity") != manifest.get("identity")
                or graph_item.get("snapshot_id") != item.get("snapshot_id")):
            raise CandidateContractError(f"candidate graph reference mismatch: {table}")
        for field in ("snapshot_id", "row_count", "file_count", "file_bytes"):
            value = item.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0 or (field != "row_count" and value == 0):
                raise CandidateContractError(f"candidate readback has invalid {field}: {table}")
    return dict(report)


def _validate_current_silver_artifact(table, artifact):
    if not isinstance(artifact.get("manifest_identity"), str) or _HEX64.fullmatch(artifact["manifest_identity"]) is None:
        raise CandidateContractError(f"Silver manifest identity is invalid: {table}")
    manifest_keys = artifact.get("manifest_keys")
    if (not isinstance(artifact.get("manifest_key"), str) or not artifact["manifest_key"]
            or manifest_keys != [artifact["manifest_key"]]):
        raise CandidateContractError(f"Silver manifest identity is incomplete: {table}")
    checksum = artifact.get("bronze_checksum_sha256", artifact.get("checksum_sha256"))
    if not isinstance(checksum, str) or _HEX64.fullmatch(checksum) is None:
        raise CandidateContractError(f"Silver checksum is invalid: {table}")
    coverage = artifact.get("coverage")
    if not isinstance(coverage, Mapping) or not isinstance(coverage.get("runtime_window_id"), str):
        raise CandidateContractError(f"Silver DagRun coverage is missing: {table}")
    lineage = artifact.get("lineage") or artifact.get("lineage_evidence")
    if not isinstance(lineage, Mapping) or not lineage:
        raise CandidateContractError(f"Silver lineage evidence is missing: {table}")
    if lineage.get("manifest_identity") is not None and lineage.get("manifest_identity") != artifact["manifest_identity"]:
        raise CandidateContractError(f"Silver lineage manifest identity mismatch: {table}")
    if lineage.get("source_run_identity") is not None and artifact.get("source_run_identity") is not None:
        if lineage["source_run_identity"] != artifact["source_run_identity"]:
            raise CandidateContractError(f"Silver lineage source run mismatch: {table}")
    if lineage.get("manifest_key") is not None and lineage.get("manifest_key") != artifact["manifest_key"]:
        raise CandidateContractError(f"Silver lineage manifest key mismatch: {table}")
    manifest = artifact.get("manifest")
    if isinstance(manifest, Mapping):
        if canonical_manifest_identity(dict(manifest)) != artifact["manifest_identity"]:
            raise CandidateContractError(f"Silver manifest identity mismatch: {table}")
        embedded_checksum = manifest.get("bronze_checksum_sha256", manifest.get("checksum_sha256"))
        if embedded_checksum is not None and embedded_checksum != checksum:
            raise CandidateContractError(f"Silver manifest checksum mismatch: {table}")


__all__ = ["CANDIDATE_WRITE_APPROVAL", "CandidateContractError", "FACT_SOURCES", "FORBIDDEN_FLAGS", "GoldCandidatePlan", "REUSED_DIMENSIONS", "STABLE_REF", "validate_candidate_environment", "validate_exact_readback", "validate_fact_and_dimension_bundle"]
