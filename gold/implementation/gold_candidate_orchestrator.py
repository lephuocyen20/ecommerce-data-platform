"""Candidate-only Gold execution boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .dimensional.contracts import WRITE_ORDER
from .gold_candidate_contract import (
    GoldCandidatePlan, validate_candidate_environment,
    validate_exact_readback, validate_fact_and_dimension_bundle,
)
from .gold_candidate_nessie import assert_source_head_unchanged, ensure_candidate_ref


class CandidateOrchestrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class CandidateDimensionalWriteGuard:
    plan: GoldCandidatePlan
    environ: Mapping[str, str]

    def authorize_dimensional_write(self):
        return (
            self.environ.get("GOLD_PRODUCTION_CANDIDATE_WRITE_APPROVED") == "1"
            and self.plan.candidate_ref != self.plan.source_ref
        )


class ProductionGoldCandidateAdapter:
    """Controlled adapter for the native dimensional Gold writer."""

    def write(self, runtime, plan, inputs):
        from .dimensional.writer import WRITE, run_dimensional_graph

        scope = runtime.get("scope")
        if scope is None or getattr(scope, "nessie_ref", None) != plan.candidate_ref:
            raise CandidateOrchestrationError("production candidate scope must bind ref")
        if getattr(scope, "gold_namespace", None) != plan.target_namespace:
            raise CandidateOrchestrationError("production candidate scope must bind namespace")
        result = run_dimensional_graph(
            runtime["spark"], runtime["client"], runtime["bucket"], runtime["lineage"],
            dim_date_start=runtime["dim_date_start"], dim_date_end=runtime["dim_date_end"],
            mode=WRITE, scope=scope, merge_plan=runtime.get("merge_plan"),
            captured_at=runtime.get("captured_at"),
            write_guard=CandidateDimensionalWriteGuard(plan, runtime["environ"]),
        )
        return _candidate_report(result, plan)


def _candidate_report(result, plan):
    if not isinstance(result, Mapping) or result.get("status") not in {"PUBLISHED", "ALREADY_PUBLISHED"}:
        raise CandidateOrchestrationError("native candidate writer did not complete")
    graph_run = result.get("graph_run")
    if not isinstance(graph_run, Mapping) or not graph_run:
        raise CandidateOrchestrationError("native candidate writer returned no graph manifest")
    raw_tables = result.get("tables")
    if (
        not isinstance(raw_tables, list)
        or len(raw_tables) != len(WRITE_ORDER)
        or [item.get("table") for item in raw_tables if isinstance(item, Mapping)] != list(WRITE_ORDER)
        or any(not isinstance(item, Mapping) for item in raw_tables)
    ):
        raise CandidateOrchestrationError("native candidate writer returned invalid seven-table order")
    manifests = {
        item["table"]: item["manifest"] for item in raw_tables
        if isinstance(item, Mapping) and isinstance(item.get("manifest"), Mapping)
    }
    if set(manifests) != set(WRITE_ORDER):
        raise CandidateOrchestrationError("native candidate writer did not return seven table manifests")
    outputs = []
    for table in WRITE_ORDER:
        manifest = manifests[table]
        readback = manifest.get("quality", {}).get("readback", {})
        outputs.append({
            **readback, "table": table, "status": "PASS", "manifest_status": "PASS",
            "candidate_ref": plan.candidate_ref, "namespace": plan.target_namespace,
            "manifest_identity": manifest.get("identity"),
        })
    return {
        "status": "VERIFIED", "candidate_ref": plan.candidate_ref,
        "target_namespace": plan.target_namespace, "stable_ref": None,
        "published": False, "tables": list(WRITE_ORDER),
        "table_manifests": manifests, "graph_manifest": graph_run,
        "outputs": outputs,
    }


def run_gold_candidate(plan, bundle, environ, *, adapter, runtime_factory, execution_adapter=None):
    """Run candidate WRITE only through the guarded production adapter."""
    plan.validate()
    validate_candidate_environment(environ, plan)
    inputs = validate_fact_and_dimension_bundle(bundle, expected_dag_run_id=plan.silver_dag_run_id)
    candidate = ensure_candidate_ref(adapter, plan.candidate_ref, source_ref=plan.source_ref)
    assert_source_head_unchanged(adapter, plan.source_ref, candidate.hash)
    runtime = runtime_factory(plan, inputs)
    if not isinstance(runtime, Mapping) or runtime.get("spark") is None or runtime.get("client") is None:
        raise CandidateOrchestrationError("candidate runtime factory must provide Spark and MinIO seams")
    if any(runtime.get(key) is None for key in ("scope", "bucket", "lineage", "dim_date_start", "dim_date_end")):
        raise CandidateOrchestrationError("candidate runtime must provide explicit scope, bucket and lineage")
    writer = execution_adapter or ProductionGoldCandidateAdapter()
    if not isinstance(writer, ProductionGoldCandidateAdapter):
        raise CandidateOrchestrationError("candidate WRITE requires ProductionGoldCandidateAdapter")
    runtime = {**runtime, "environ": environ}
    try:
        return validate_exact_readback(writer.write(runtime, plan, inputs))
    finally:
        stop = getattr(runtime.get("spark"), "stop", None)
        if callable(stop):
            stop()


__all__ = ["CandidateDimensionalWriteGuard", "CandidateOrchestrationError", "ProductionGoldCandidateAdapter", "run_gold_candidate"]
