"""Spark-runtime boundary for historical Silver and Gold execution.

The module is intentionally dependency-light at import time.  Spark, MinIO,
Nessie and the dimensional writer are imported only after the JSON contracts
and historical production guards have been validated.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import re
from datetime import datetime
from typing import Mapping

from ingestion.historical.config import (
    HistoricalBackfillConfig,
    candidate_ref_for_run,
    production_environment,
    require_write_approval,
)
from ingestion.historical.orchestrator import (
    PARENT_FIRST_ORDER,
    require_verified_silver_artifacts,
)

RESULT_MARKER = "HISTORICAL_GOLD_RESULT="
EXPECTED_GOLD_TABLES = (
    "dim_date",
    "dim_customer",
    "dim_product",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_shipments",
)
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


def _load_json(raw: str, name: str) -> dict:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{name} must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return value


def _validate_plan(plan: Mapping, environ: Mapping[str, str]) -> HistoricalBackfillConfig:
    run_id = plan.get("run_id")
    candidate_ref = plan.get("candidate_ref")
    if candidate_ref == "gold_prod_current":
        raise ValueError("historical execution cannot target stable ref gold_prod_current")
    if candidate_ref != candidate_ref_for_run(run_id):
        raise ValueError("plan candidate_ref does not match the run_id")
    if plan.get("tables") != list(PARENT_FIRST_ORDER):
        raise ValueError("historical plan must use the complete parent-first table order")
    mode = plan.get("mode")
    if mode not in {"DRY_RUN", "WRITE"}:
        raise ValueError("historical plan mode must be DRY_RUN or WRITE")
    config = HistoricalBackfillConfig(
        run_id=run_id,
        capture_end=datetime.fromisoformat(plan["capture_end"]),
        dry_run=mode != "WRITE",
    ).validate()
    return config


def _candidate_environment(plan: Mapping, environ: Mapping[str, str]) -> dict[str, str]:
    values = dict(environ)
    # The CLI owns the candidate target.  Do not inherit the scheduler's stable
    # ref into the Spark process; production_environment validates this candidate
    # before any runtime client is constructed.
    values["NESSIE_REF"] = plan["candidate_ref"]
    return values


def _preflight(plan: Mapping, environ: Mapping[str, str]) -> tuple[HistoricalBackfillConfig, dict[str, str]]:
    """Validate the complete historical boundary before any lazy runtime import."""
    config = _validate_plan(plan, environ)
    values = _candidate_environment(plan, environ)
    if plan["mode"] == "WRITE":
        production_environment(values, candidate_ref=plan["candidate_ref"])
        require_write_approval(values)
    return config, values


def _validate_bronze_input(bronze: Mapping) -> None:
    if bronze.get("status") == "DRY_RUN":
        return
    if bronze.get("status") != "PASS" or set(bronze.get("bronze", {})) != set(PARENT_FIRST_ORDER):
        raise ValueError("historical Silver requires six verified Bronze envelopes")
    for table in PARENT_FIRST_ORDER:
        envelope = bronze["bronze"][table]
        required = {"object_key", "manifest_key", "row_count", "checksum_sha256"}
        if not isinstance(envelope, dict) or not required.issubset(envelope):
            raise ValueError(f"historical Bronze envelope is incomplete: {table}")


def _run_silver(plan: dict, bronze: dict, environ: Mapping[str, str]) -> dict:
    _, values = _preflight(plan, environ)
    _validate_bronze_input(bronze)
    if plan["mode"] == "DRY_RUN":
        return {"status": "BLOCKED_REAL_SILVER_REQUIRED", "artifacts": {}}
    from ingestion.historical.pipeline import run_historical_silver

    result = run_historical_silver(bronze, plan, values)
    artifacts = require_verified_silver_artifacts(result.get("artifacts"))
    return {"status": "PASS", "artifacts": artifacts}


def _invoke_gold_writer(writer, config, plan, artifacts, environ):
    return writer(config, plan, artifacts, environ=environ)


def _unwrap_silver_artifacts(silver_input: Mapping) -> Mapping:
    """Accept the Silver task envelope or the legacy direct artifact map."""
    if silver_input.get("status") == "PASS" and "artifacts" in silver_input:
        artifacts = silver_input["artifacts"]
        if not isinstance(artifacts, Mapping):
            raise ValueError("historical Silver envelope artifacts must be a JSON object")
        return artifacts
    return silver_input


def _validate_gold_report(report: Mapping, candidate_ref: str) -> dict:
    if report.get("status") != "VERIFIED" or report.get("candidate_ref") != candidate_ref:
        raise ValueError("historical Gold did not return a candidate-only VERIFIED result")
    if report.get("stable_ref") is not None or report.get("readback_status") != "PASS":
        raise ValueError("historical Gold readback is not candidate-only PASS")
    if tuple(report.get("tables", ())) != EXPECTED_GOLD_TABLES:
        raise ValueError("historical Gold result does not contain the exact seven Gold tables")
    outputs = report.get("outputs")
    if not isinstance(outputs, list) or len(outputs) != len(EXPECTED_GOLD_TABLES):
        raise ValueError("historical Gold requires exactly seven physical readbacks")
    seen = set()
    for output in outputs:
        if not isinstance(output, Mapping) or output.get("table") not in EXPECTED_GOLD_TABLES:
            raise ValueError("historical Gold output has an unexpected table")
        table = output["table"]
        if table in seen:
            raise ValueError(f"historical Gold output is duplicated: {table}")
        seen.add(table)
        snapshot_id = output.get("snapshot_id")
        row_count = output.get("row_count")
        file_count = output.get("file_count")
        file_bytes = output.get("file_bytes")
        if not isinstance(snapshot_id, int) or isinstance(snapshot_id, bool) or snapshot_id <= 0:
            raise ValueError(f"historical Gold snapshot_id is invalid: {table}")
        if not isinstance(row_count, int) or isinstance(row_count, bool) or row_count < 0:
            raise ValueError(f"historical Gold row_count is invalid: {table}")
        for field, value in (("file_count", file_count), ("file_bytes", file_bytes)):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"historical Gold {field} is invalid: {table}")
        if output.get("readback_status") != "PASS" or output.get("consistency_status") != "PASS":
            raise ValueError(f"historical Gold physical readback is not PASS: {table}")
        manifest_identity = output.get("manifest_identity")
        readback_identity = output.get("readback_identity")
        if (
            not isinstance(manifest_identity, str)
            or not _HEX64.fullmatch(manifest_identity)
            or not isinstance(readback_identity, str)
            or not _HEX64.fullmatch(readback_identity)
            or manifest_identity == readback_identity
        ):
            raise ValueError(f"historical Gold identities are invalid or not independent: {table}")
    if seen != set(EXPECTED_GOLD_TABLES):
        raise ValueError("historical Gold readback does not cover all seven Gold tables")
    return dict(report)


def _run_gold(plan: dict, silver_input: dict, environ: Mapping[str, str]) -> dict:
    config, values = _preflight(plan, environ)
    if plan["mode"] != "WRITE":
        return {"status": "BLOCKED_REAL_SILVER_REQUIRED", "reason": "historical Gold WRITE is disabled"}
    verified = require_verified_silver_artifacts(_unwrap_silver_artifacts(silver_input))
    # This is the first point at which the candidate, write approval, exact
    # six-artifact input and production storage identity have all passed.
    from gold.implementation.historical_gold_writer import (
        run_historical_gold_candidate,
    )

    report = _invoke_gold_writer(run_historical_gold_candidate, config, plan, verified, values)
    report = _validate_gold_report(report, plan["candidate_ref"])
    outputs = report["outputs"]
    return {
        "status": "VERIFIED",
        "run_id": report["run_id"],
        "candidate_ref": report["candidate_ref"],
        "stable_ref": None,
        "tables": report["tables"],
        "outputs": outputs,
        "readback_status": "PASS",
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=("silver", "gold"), required=True)
    parser.add_argument("--plan-json", required=True)
    parser.add_argument("--bronze-json")
    parser.add_argument("--silver-artifacts-json")
    args = parser.parse_args(argv)
    try:
        plan = _load_json(args.plan_json, "plan-json")
        if args.stage == "silver":
            if args.bronze_json is None:
                raise ValueError("silver stage requires bronze-json")
            result = _run_silver(plan, _load_json(args.bronze_json, "bronze-json"), os.environ)
        else:
            if args.silver_artifacts_json is None:
                raise ValueError("gold stage requires silver-artifacts-json")
            result = _run_gold(plan, _load_json(args.silver_artifacts_json, "silver-artifacts-json"), os.environ)
        print(f"{RESULT_MARKER}{json.dumps(result, sort_keys=True, separators=(',', ':'))}")
        return 0
    except Exception as exc:
        print(json.dumps({"status": "REJECTED", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
