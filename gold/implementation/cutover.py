"""Immutable, read-only Iceberg cutover readiness gates.

This module records why cutover is blocked or ready for a separately approved
operation. It never changes routing, enables writes, or calls external systems.
"""

import hashlib
import json


class CutoverPreflightError(ValueError):
    pass


REQUIRED_PHASES = ("A", "B", "C", "D")
DEFAULT_ROUTE = "parquet"


def _canonical(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def build_cutover_preflight(phase_statuses, *, consumer_parity=False, runtime_certified=False, requested_route=DEFAULT_ROUTE):
    if not isinstance(phase_statuses, dict) or set(phase_statuses) != set(REQUIRED_PHASES):
        raise CutoverPreflightError("Cutover gate requires phases A-D")
    if requested_route != DEFAULT_ROUTE:
        raise CutoverPreflightError("Parquet remains the only active route")
    if not isinstance(consumer_parity, bool) or not isinstance(runtime_certified, bool):
        raise CutoverPreflightError("Cutover gate booleans are invalid")
    reasons = []
    if not runtime_certified or phase_statuses["A"] != "RUNTIME_PASS":
        reasons.append("RUNTIME_CERTIFICATION_PENDING")
    if phase_statuses["B"] != "BUSINESS_SIGNED_OFF":
        reasons.append("SEMANTIC_SIGNOFF_PENDING")
    if phase_statuses["C"] != "OPERATIONAL_PASS":
        reasons.append("LATE_DELETE_OPERATIONAL_PROOF_PENDING")
    if phase_statuses["D"] != "OPERATIONAL_PASS":
        reasons.append("ICEBERG_OPERATIONAL_PROOF_PENDING")
    if not consumer_parity:
        reasons.append("CONSUMER_PARITY_PENDING")
    body = {
        "version": 1,
        "kind": "gold_iceberg_cutover_preflight",
        "current_route": DEFAULT_ROUTE,
        "requested_route": DEFAULT_ROUTE,
        "action": "DRY_RUN",
        "can_route": False,
        "can_write": False,
        "phase_statuses": dict(phase_statuses),
        "consumer_parity": consumer_parity,
        "runtime_certified": runtime_certified,
        "reasons": reasons,
    }
    return {**body, "identity": hashlib.sha256(_canonical(body).encode()).hexdigest()}


def validate_cutover_preflight(document):
    if not isinstance(document, dict) or "identity" not in document:
        raise CutoverPreflightError("Invalid cutover preflight document")
    identity = document["identity"]
    body = {key: value for key, value in document.items() if key != "identity"}
    if not isinstance(identity, str) or hashlib.sha256(_canonical(body).encode()).hexdigest() != identity:
        raise CutoverPreflightError("Cutover preflight identity mismatch")
    if body.get("current_route") != DEFAULT_ROUTE or body.get("requested_route") != DEFAULT_ROUTE:
        raise CutoverPreflightError("Non-Parquet routing is not enabled")
    if body.get("action") != "DRY_RUN" or body.get("can_route") is not False or body.get("can_write") is not False:
        raise CutoverPreflightError("Cutover preflight cannot authorize routing or writes")
    if set(body.get("phase_statuses", {})) != set(REQUIRED_PHASES):
        raise CutoverPreflightError("Cutover gate requires phases A-D")
    return True
