"""Pure cross-table Silver coverage contract for the dimensional Gold graph."""

import json
import re


CROSS_TABLES = ("orders", "payments", "shipments")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:-]{1,256}$")


class CoverageContractError(ValueError):
    """Raised when cross-table Silver coverage is absent or inconsistent."""


def _diagnostic(table, source, coverage, reason):
    source = source if isinstance(source, dict) else {}
    return {
        "table": table,
        "batch_id": source.get("batch_id"),
        "manifest_keys": list(source.get("manifest_keys", [])),
        "runtime_window_id": coverage.get("runtime_window_id") if isinstance(coverage, dict) else None,
        "coverage_identity": coverage.get("coverage_identity") if isinstance(coverage, dict) else None,
        "parent_coverage_identity": coverage.get("parent_coverage_identity") if isinstance(coverage, dict) else None,
        "parent_orders_reference": coverage.get("parent_orders_reference") if isinstance(coverage, dict) else None,
        "reason": reason,
    }


def _validate_metadata(table, source):
    coverage = source.get("coverage")
    if not isinstance(coverage, dict):
        return None, None
    allowed = {"runtime_window_id", "coverage_identity", "parent_coverage_identity", "parent_orders_reference"}
    if set(coverage) - allowed:
        return None, "malformed_coverage_metadata"
    required = ("runtime_window_id", "coverage_identity")
    if any(not isinstance(coverage.get(field), str) or not _IDENTIFIER.fullmatch(coverage[field]) for field in required):
        return None, "malformed_coverage_identity"
    parent_identity = coverage.get("parent_coverage_identity")
    parent_reference = coverage.get("parent_orders_reference")
    if table == "orders":
        if parent_identity is not None or parent_reference is not None:
            return None, "orders_has_parent_reference"
    else:
        if parent_identity is None:
            return None, "missing_parent_coverage_identity"
        if not isinstance(parent_identity, str) or not _IDENTIFIER.fullmatch(parent_identity):
            return None, "invalid_parent_coverage_identity"
        if parent_reference is None:
            return None, "missing_parent_orders_reference"
        allowed_reference = {"table", "batch_id", "manifest_key", "manifest_identity"}
        if not isinstance(parent_reference, dict) or set(parent_reference) != allowed_reference:
            return None, "parent_orders_reference_mismatch"
        if parent_reference.get("table") != "orders":
            return None, "parent_orders_reference_table_mismatch"
        if not isinstance(parent_reference.get("batch_id"), str) or not re.fullmatch(r"[0-9a-f]{32}", parent_reference["batch_id"]):
            return None, "invalid_parent_orders_reference"
        if not isinstance(parent_reference.get("manifest_key"), str) or not parent_reference["manifest_key"]:
            return None, "invalid_parent_orders_reference"
    return coverage, None


def validate_cross_table_coverage(lineage):
    """Validate one runtime window for orders, payments and shipments.

    Legacy lineage remains compatible only with an explicit legacy marker.
    """
    sources = lineage.get("sources") if isinstance(lineage, dict) else None
    if not isinstance(sources, dict):
        diagnostics = [_diagnostic(table, {}, None, "missing_source") for table in CROSS_TABLES]
        raise CoverageContractError(
            "Silver cross-table coverage preflight failed; diagnostics_table="
            + json.dumps(diagnostics, sort_keys=True, separators=(",", ":"))
        )
    diagnostics = []
    metadata = {}
    for table in CROSS_TABLES:
        source = sources.get(table)
        if not isinstance(source, dict):
            diagnostics.append(_diagnostic(table, {}, None, "missing_source"))
            continue
        coverage, malformed_reason = _validate_metadata(table, source)
        metadata[table] = coverage
        if malformed_reason:
            diagnostics.append(_diagnostic(table, source, source.get("coverage"), malformed_reason))
        if coverage is not None:
            diagnostics.append(_diagnostic(table, source, coverage, "present"))
    present = [table for table in CROSS_TABLES if metadata.get(table) is not None]
    if not present and lineage.get("legacy_coverage_compatibility") is True:
        return {"status": "PASS", "mode": "LEGACY_COMPATIBLE", "diagnostics": []}
    failures = [item for item in diagnostics if item["reason"] != "present"]
    if len(present) != len(CROSS_TABLES):
        for table in CROSS_TABLES:
            if table not in present and not any(item["table"] == table for item in failures):
                failures.append(_diagnostic(table, sources.get(table, {}), metadata.get(table), "missing_coverage_metadata"))
    if not failures and len(present) == len(CROSS_TABLES):
        parent = metadata["orders"]
        for table in ("payments", "shipments"):
            child = metadata[table]
            if child["runtime_window_id"] != parent["runtime_window_id"]:
                failures.append(_diagnostic(table, sources[table], child, "runtime_window_mismatch"))
            if child["parent_coverage_identity"] != parent["coverage_identity"]:
                failures.append(_diagnostic(table, sources[table], child, "parent_coverage_identity_mismatch"))
            reference = child.get("parent_orders_reference")
            expected = {
                "table": "orders",
                "batch_id": sources["orders"].get("batch_id"),
                "manifest_key": sources["orders"].get("manifest_key"),
                "manifest_identity": sources["orders"].get("manifest_identity"),
            }
            if reference != expected:
                failures.append(_diagnostic(table, sources[table], child, "parent_orders_reference_mismatch"))
    if not present and lineage.get("legacy_coverage_compatibility") is not True:
        for table in CROSS_TABLES:
            if not any(item["table"] == table for item in failures):
                failures.append(_diagnostic(table, sources.get(table, {}), None, "missing_coverage_metadata"))
    if failures:
        all_diagnostics = diagnostics + failures
        raise CoverageContractError(
            "Silver cross-table coverage preflight failed; diagnostics_table="
            + json.dumps(all_diagnostics, sort_keys=True, separators=(",", ":"))
        )
    return {"status": "PASS", "mode": "COVERAGE_CONTRACT", "runtime_window_id": metadata["orders"]["runtime_window_id"], "diagnostics": diagnostics}
