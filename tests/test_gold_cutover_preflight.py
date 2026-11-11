import copy

import pytest

from gold.implementation.cutover import (
    CutoverPreflightError,
    build_cutover_preflight,
    validate_cutover_preflight,
)


PHASES = {"A": "CODE_PASS", "B": "CODE_PASS", "C": "CODE_PASS", "D": "CODE_PASS"}


def test_cutover_preflight_defaults_to_parquet_and_dry_run():
    document = build_cutover_preflight(PHASES)
    assert document["current_route"] == "parquet"
    assert document["action"] == "DRY_RUN"
    assert document["can_route"] is False
    assert document["can_write"] is False
    assert "RUNTIME_CERTIFICATION_PENDING" in document["reasons"]
    assert validate_cutover_preflight(document) is True


def test_cutover_preflight_is_immutable_and_detects_tampering():
    document = build_cutover_preflight(PHASES)
    changed = copy.deepcopy(document)
    changed["current_route"] = "iceberg"
    with pytest.raises(CutoverPreflightError, match="identity mismatch"):
        validate_cutover_preflight(changed)


def test_cutover_preflight_rejects_non_parquet_route_and_missing_phase():
    with pytest.raises(CutoverPreflightError, match="only active route"):
        build_cutover_preflight(PHASES, requested_route="iceberg")
    with pytest.raises(CutoverPreflightError, match="phases A-D"):
        build_cutover_preflight({"A": "CODE_PASS"})
