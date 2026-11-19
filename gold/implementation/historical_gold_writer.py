"""Safe historical Gold candidate writer boundary.

This adapter is deliberately separate from the incremental DAG.  It accepts
only verified Silver lineage artifacts, requires the run-scoped production
candidate ref, and returns a candidate certification report only after the
native writer has produced physical seven-table read-back evidence.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Callable, Mapping

from ingestion.historical.config import HistoricalBackfillConfig, production_environment
from common.storage_layout import ProductionGoldScope, PRODUCTION_BUCKET
from gold.implementation.dimensional.contracts import WRITE_ORDER
from gold.implementation.dimensional.lineage import (
    build_dimensional_lineage_from_artifacts,
    dimensional_lineage_identity,
)
from gold.implementation.dimensional.verification import verify_completed_graph
from gold.implementation.dimensional.writer import WRITE, run_dimensional_graph
from .historical_ref_promotion import NessieRestAdapter, ensure_candidate_branch, validate_candidate


class HistoricalGoldWriterUnavailable(RuntimeError):
    """Raised when Spark/MinIO/Nessie runtime cannot be safely constructed."""


def _require_candidate_environment(config: HistoricalBackfillConfig, environ=None):
    values = dict(os.environ if environ is None else environ)
    production_environment(values, candidate_ref=config.candidate_ref)
    if not values.get("NESSIE_URI"):
        raise HistoricalGoldWriterUnavailable("NESSIE_URI is required before creating runtime clients")
    if values.get("NESSIE_REF") == "gold_prod_current":
        raise HistoricalGoldWriterUnavailable("historical Gold writer cannot use stable ref")
    return values


def _default_runtime_factory(plan, environ):
    """Construct clients only after all historical production guards pass."""
    try:
        namespace = environ.get("GOLD_TARGET_NAMESPACE")
        production_runtime = environ.get("BRONZE_BUCKET") == PRODUCTION_BUCKET and environ.get("LAKEHOUSE_STORAGE_ENV") == "production"
        if production_runtime and namespace != "gold_run_20261112_gold_06":
            raise HistoricalGoldWriterUnavailable("historical Gold requires the certified production namespace")
        namespace = namespace or "gold"

        # Validate the complete target identity before creating any external
        # client or bootstrapping a candidate Nessie branch.
        nessie = NessieRestAdapter(environ["NESSIE_URI"])
        ensure_candidate_branch(
            nessie,
            plan["candidate_ref"],
            base_ref=environ.get("HISTORICAL_GOLD_BASE_REF", "main"),
        )
        from common.minio import configure_s3a, create_minio_client
        from pyspark.sql import SparkSession

        scope = ProductionGoldScope(PRODUCTION_BUCKET, plan["candidate_ref"], namespace)
        builder = SparkSession.builder.appName("HistoricalGoldCandidateWriter")
        builder = builder.config("spark.sql.catalog.lakehouse.warehouse", scope.warehouse)
        builder = builder.config("spark.sql.catalog.lakehouse.ref", plan["candidate_ref"])
        spark = builder.getOrCreate()
        configure_s3a(spark)
        client = create_minio_client()
        return {"spark": spark, "client": client, "nessie": nessie, "scope": scope}
    except HistoricalGoldWriterUnavailable:
        raise
    except Exception as exc:
        raise HistoricalGoldWriterUnavailable("Spark/MinIO/Nessie runtime is unavailable") from exc


def _physical_outputs(result):
    outputs = []
    for item in result.get("tables", []):
        manifest = item.get("manifest") if isinstance(item, Mapping) else None
        quality = manifest.get("quality") if isinstance(manifest, Mapping) else {}
        readback = quality.get("readback") if isinstance(quality, Mapping) else None
        if not isinstance(manifest, Mapping) or not isinstance(readback, Mapping):
            raise HistoricalGoldWriterUnavailable("Gold writer returned incomplete physical read-back evidence")
        if readback.get("status") != "PASS" or readback.get("snapshot_id") != manifest.get("snapshot_id"):
            raise HistoricalGoldWriterUnavailable(f"physical Gold read-back failed for {manifest.get('table')}")
        identity = manifest.get("identity")
        readback_identity = readback.get("identity") or readback.get("readback_identity")
        if not isinstance(readback_identity, str) or re.fullmatch(r"[0-9a-f]{64}", readback_identity) is None:
            raise HistoricalGoldWriterUnavailable(
                f"independent physical read-back identity is missing for {manifest.get('table')}"
            )
        output = {
            "table": manifest.get("table"),
            "snapshot_id": manifest.get("snapshot_id"),
            "row_count": readback.get("row_count"),
            "file_count": readback.get("file_count"),
            "file_bytes": readback.get("file_bytes"),
            "manifest_identity": identity,
            "readback_identity": readback_identity,
            "readback_status": readback.get("status"),
            "consistency_status": "PASS" if readback.get("snapshot_id") == manifest.get("snapshot_id") else "FAIL",
        }
        outputs.append(output)
    if tuple(sorted(item.get("table") for item in outputs)) != tuple(sorted(WRITE_ORDER)):
        raise HistoricalGoldWriterUnavailable("Gold writer did not return exactly seven tables")
    return outputs


def run_historical_gold_candidate(
    config: HistoricalBackfillConfig,
    plan: Mapping,
    silver_artifacts: Mapping,
    *,
    environ=None,
    runtime_factory: Callable | None = None,
):
    """Write/read back Gold on the candidate ref, never the stable ref."""
    if plan.get("candidate_ref") != config.candidate_ref or plan.get("mode") != WRITE:
        raise HistoricalGoldWriterUnavailable("historical Gold write requires the run candidate plan")
    values = _require_candidate_environment(config, environ)
    lineage = build_dimensional_lineage_from_artifacts(silver_artifacts)
    factory = runtime_factory or _default_runtime_factory
    runtime = factory(plan, values)
    if not isinstance(runtime, Mapping) or runtime.get("spark") is None or runtime.get("client") is None:
        raise HistoricalGoldWriterUnavailable("runtime factory did not provide Spark and MinIO clients")
    spark, client = runtime["spark"], runtime["client"]
    scope = runtime.get("scope")
    if scope is None:
        namespace = values.get("GOLD_TARGET_NAMESPACE", "gold_run_20261112_gold_06")
        scope = ProductionGoldScope(values["BRONZE_BUCKET"], config.candidate_ref, namespace)
    try:
        result = run_dimensional_graph(
            spark, client, values["BRONZE_BUCKET"], lineage,
            dim_date_start=plan["capture_start"][:10],
            dim_date_end=plan["capture_end"][:10],
            configured_date_start=plan["capture_start"][:10],
            configured_date_end=plan["capture_end"][:10],
            mode=WRITE,
            scope=scope,
            captured_at=datetime.now(timezone.utc).isoformat(),
        )
        verification = verify_completed_graph(result, client, values["BRONZE_BUCKET"], scope=scope)
        if verification.get("status") != "VERIFIED":
            raise HistoricalGoldWriterUnavailable("physical Gold graph verification did not pass")
        outputs = _physical_outputs(result)
        nessie = runtime.get("nessie")
        if nessie is None:
            raise HistoricalGoldWriterUnavailable("candidate ref hash read-back adapter is unavailable")
        candidate_hash = nessie.get_ref(config.candidate_ref).hash
        report = {
            "status": "VERIFIED",
            "run_id": config.run_id,
            "candidate_ref": config.candidate_ref,
            "candidate_hash": candidate_hash,
            "stable_ref": None,
            "tables": list(WRITE_ORDER),
            "outputs": outputs,
            "lineage_identity": dimensional_lineage_identity(lineage),
            "readback_status": "PASS",
        }
        validate_candidate(report, run_id=config.run_id)
        return report
    finally:
        stop = getattr(spark, "stop", None)
        if callable(stop):
            stop()


__all__ = ["HistoricalGoldWriterUnavailable", "run_historical_gold_candidate"]
