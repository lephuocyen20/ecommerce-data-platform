"""Versioned, immutable input lineage for Gold source reads.

Version 1 is the existing mapping of table names to published Parquet snapshot
IDs.  Version 2 is deliberately an envelope: an Iceberg snapshot ID is not a
Silver Parquet ``snap_*`` identifier and must never be coerced into one.
"""

import hashlib
import json
import re

from gold.implementation.contracts.datasets import get_gold_contract


PARQUET_SNAPSHOT = "parquet_snapshot"
ICEBERG_SNAPSHOT = "iceberg_snapshot"
LINEAGE_VERSION = 2
_SOURCE_TYPES = {PARQUET_SNAPSHOT, ICEBERG_SNAPSHOT}
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class GoldSourceLineageError(ValueError):
    pass


def canonical_lineage(lineage):
    """Return a stable JSON representation after fail-closed validation."""
    validate_source_lineage(lineage)
    return json.dumps(lineage, sort_keys=True, separators=(",", ":"))


def lineage_identity(lineage):
    return hashlib.sha256(canonical_lineage(lineage).encode("utf-8")).hexdigest()


def _nonempty(value, name):
    if not isinstance(value, str) or not value.strip():
        raise GoldSourceLineageError(f"Invalid {name}")
    return value


def _identifier(value, name):
    _nonempty(value, name)
    if _IDENTIFIER.fullmatch(value) is None:
        raise GoldSourceLineageError(f"Invalid SQL identifier: {name}")
    return value


def _validate_evidence(evidence, name, source):
    if not isinstance(evidence, dict):
        raise GoldSourceLineageError(f"Invalid {name} evidence")
    expected = {"identity", "status", "snapshot_id", "table"}
    if set(evidence) != expected:
        raise GoldSourceLineageError(f"Unexpected {name} evidence fields")
    _nonempty(evidence["identity"], f"{name} evidence identity")
    if evidence["status"] != "PASS":
        raise GoldSourceLineageError(f"{name} evidence is not PASS")
    if evidence["snapshot_id"] != source["snapshot_id"]:
        raise GoldSourceLineageError(f"{name} evidence snapshot mismatch")
    if evidence["table"] != source["table"]:
        raise GoldSourceLineageError(f"{name} evidence table mismatch")


def _validate_iceberg_source(source, table):
    required = {
        "source_type", "table", "catalog", "namespace", "ref", "snapshot_id",
        "snapshot_attestation_identity", "reconciliation_evidence", "quality_evidence",
    }
    if set(source) != required:
        raise GoldSourceLineageError("Unexpected Iceberg source lineage fields")
    if source["source_type"] != ICEBERG_SNAPSHOT or source["table"] != table:
        raise GoldSourceLineageError("Iceberg source type or table mismatch")
    for field in ("catalog", "namespace", "ref"):
        _identifier(source[field], field)
    _nonempty(source["snapshot_attestation_identity"], "snapshot_attestation_identity")
    if re.fullmatch(r"[a-f0-9]{64}", source["snapshot_attestation_identity"]) is None:
        raise GoldSourceLineageError("Invalid snapshot_attestation_identity")
    if not isinstance(source["snapshot_id"], int) or isinstance(source["snapshot_id"], bool) or source["snapshot_id"] <= 0:
        raise GoldSourceLineageError("Invalid Iceberg snapshot_id")
    _validate_evidence(source["reconciliation_evidence"], "reconciliation", source)
    _validate_evidence(source["quality_evidence"], "quality", source)


def validate_source_lineage(lineage, dataset_name=None):
    """Validate a v2 lineage envelope and reject mixed-source datasets."""
    if not isinstance(lineage, dict) or set(lineage) != {"version", "source_type", "sources"}:
        raise GoldSourceLineageError("Invalid Gold source lineage envelope")
    if lineage["version"] != LINEAGE_VERSION or lineage["source_type"] not in _SOURCE_TYPES:
        raise GoldSourceLineageError("Unsupported Gold source lineage version or type")
    if not isinstance(lineage["sources"], dict) or not lineage["sources"]:
        raise GoldSourceLineageError("Gold source lineage requires sources")
    expected = None
    if dataset_name is not None:
        expected = set(get_gold_contract(dataset_name)["sources"])
        if set(lineage["sources"]) != expected:
            raise GoldSourceLineageError("Gold source lineage does not match dataset contract")
    for table, source in lineage["sources"].items():
        _nonempty(table, "source table")
        if not isinstance(source, dict):
            raise GoldSourceLineageError("Invalid Gold source entry")
        if lineage["source_type"] == PARQUET_SNAPSHOT:
            if set(source) != {"source_type", "table", "snapshot_id"} or source.get("source_type") != PARQUET_SNAPSHOT or source.get("table") != table:
                raise GoldSourceLineageError("Invalid Parquet source lineage")
            snapshot_id = source.get("snapshot_id")
            if not isinstance(snapshot_id, str) or not snapshot_id.startswith("snap_"):
                raise GoldSourceLineageError("Invalid Parquet snapshot_id")
        else:
            _validate_iceberg_source(source, table)
    return True


def parquet_lineage(snapshot_ids, dataset_name=None):
    lineage = {
        "version": LINEAGE_VERSION,
        "source_type": PARQUET_SNAPSHOT,
        "sources": {
            table: {"source_type": PARQUET_SNAPSHOT, "table": table, "snapshot_id": snapshot_id}
            for table, snapshot_id in snapshot_ids.items()
        },
    }
    validate_source_lineage(lineage, dataset_name)
    return lineage
