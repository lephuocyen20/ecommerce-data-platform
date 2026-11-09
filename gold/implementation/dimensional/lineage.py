"""Strict, Iceberg-only Silver lineage for the dimensional Gold graph."""

import hashlib
import json
import re

from .contracts import DIMENSIONAL_SOURCE_TABLES

ICEBERG_SNAPSHOT = "iceberg_snapshot"
LINEAGE_VERSION = 1
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")


class DimensionalLineageError(ValueError):
    pass


def _identifier(value, field):
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise DimensionalLineageError(f"Invalid {field}")


def _evidence(value, field, source):
    expected = {"identity", "status", "snapshot_id", "table"}
    if not isinstance(value, dict) or set(value) != expected:
        raise DimensionalLineageError(f"Invalid {field} evidence")
    if value["status"] != "PASS" or value["snapshot_id"] != source["snapshot_id"] or value["table"] != source["table"]:
        raise DimensionalLineageError(f"Unauthorized {field} evidence")
    if not isinstance(value["identity"], str) or _SHA256.fullmatch(value["identity"]) is None:
        raise DimensionalLineageError(f"Invalid {field} evidence identity")


def validate_dimensional_lineage(lineage):
    if not isinstance(lineage, dict) or set(lineage) != {"version", "source_type", "sources"}:
        raise DimensionalLineageError("Invalid dimensional lineage envelope")
    if lineage["version"] != LINEAGE_VERSION or lineage["source_type"] != ICEBERG_SNAPSHOT:
        raise DimensionalLineageError("Dimensional Gold requires Iceberg snapshot lineage")
    if not isinstance(lineage["sources"], dict) or set(lineage["sources"]) != set(DIMENSIONAL_SOURCE_TABLES):
        raise DimensionalLineageError("Dimensional source set does not match graph contract")
    fields = {"source_type", "table", "catalog", "namespace", "ref", "snapshot_id", "snapshot_attestation_identity", "reconciliation_evidence", "quality_evidence"}
    for table in DIMENSIONAL_SOURCE_TABLES:
        source = lineage["sources"][table]
        if not isinstance(source, dict) or set(source) != fields:
            raise DimensionalLineageError(f"Unexpected lineage fields for {table}")
        if source["source_type"] != ICEBERG_SNAPSHOT or source["table"] != table:
            raise DimensionalLineageError(f"Source type/table mismatch for {table}")
        if (source["catalog"], source["namespace"], source["ref"]) != ("lakehouse", "silver", "main"):
            raise DimensionalLineageError("Only lakehouse.silver on main is permitted")
        for field in ("catalog", "namespace", "ref"):
            _identifier(source[field], field)
        if not isinstance(source["snapshot_id"], int) or isinstance(source["snapshot_id"], bool) or source["snapshot_id"] <= 0:
            raise DimensionalLineageError(f"Invalid snapshot_id for {table}")
        if not isinstance(source["snapshot_attestation_identity"], str) or _SHA256.fullmatch(source["snapshot_attestation_identity"]) is None:
            raise DimensionalLineageError(f"Invalid snapshot attestation for {table}")
        _evidence(source["reconciliation_evidence"], "reconciliation", source)
        _evidence(source["quality_evidence"], "quality", source)
    return True


def canonical_dimensional_lineage(lineage):
    validate_dimensional_lineage(lineage)
    return json.dumps(lineage, sort_keys=True, separators=(",", ":"))


def dimensional_lineage_identity(lineage):
    return hashlib.sha256(canonical_dimensional_lineage(lineage).encode("utf-8")).hexdigest()
