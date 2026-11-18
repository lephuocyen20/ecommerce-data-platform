"""Configuration and safety guards for the historical production backfill."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime

from common.storage_layout import PRODUCTION_BUCKET, PRODUCTION_LAYOUT, PRODUCTION_REF, validate_storage_environment

SUPPORTED_TABLES = ("customers", "products", "orders", "order_items", "payments", "shipments")
TABLE_CONFIG = {
    "customers": {"primary_key": "customer_id"},
    "products": {"primary_key": "product_id"},
    "orders": {"primary_key": "order_id"},
    "order_items": {"primary_key": "order_item_id"},
    "payments": {"primary_key": "payment_id"},
    "shipments": {"primary_key": "shipment_id"},
}
START_AT = datetime(2026, 9, 26)
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class HistoricalBackfillSafetyError(ValueError):
    """Raised before any external client or write is allowed."""


@dataclass(frozen=True)
class HistoricalBackfillConfig:
    run_id: str
    capture_end: datetime
    batch_size: int = 1000
    dry_run: bool = True

    @property
    def candidate_ref(self) -> str:
        return candidate_ref_for_run(self.run_id)

    def validate(self) -> "HistoricalBackfillConfig":
        if _RUN_ID.fullmatch(self.run_id or "") is None:
            raise HistoricalBackfillSafetyError("run_id must be an immutable safe identifier")
        if not isinstance(self.capture_end, datetime) or self.capture_end <= START_AT:
            raise HistoricalBackfillSafetyError("capture_end must be after 2026-09-26")
        if not isinstance(self.batch_size, int) or isinstance(self.batch_size, bool) or self.batch_size <= 0:
            raise HistoricalBackfillSafetyError("batch_size must be a positive integer")
        if not isinstance(self.dry_run, bool):
            raise HistoricalBackfillSafetyError("dry_run must be boolean")
        return self


def candidate_ref_for_run(run_id: str) -> str:
    if _RUN_ID.fullmatch(run_id or "") is None:
        raise HistoricalBackfillSafetyError("run_id must be an immutable safe identifier")
    return f"gold_prod_backfill_{run_id}"


def production_environment(environ=None, *, candidate_ref: str | None = None):
    values = os.environ if environ is None else environ
    expected_ref = candidate_ref or PRODUCTION_REF
    if values.get("BRONZE_BUCKET") != PRODUCTION_BUCKET:
        raise HistoricalBackfillSafetyError("historical backfill requires the production bucket")
    if values.get("LAKEHOUSE_STORAGE_ENV", PRODUCTION_LAYOUT) != PRODUCTION_LAYOUT:
        raise HistoricalBackfillSafetyError("historical backfill requires production storage layout")
    if values.get("NESSIE_REF") != expected_ref:
        raise HistoricalBackfillSafetyError("historical backfill ref does not match the requested candidate")
    if values.get("LAKEHOUSE_E2E_APPROVED") == "1" or values.get("AIRFLOW_LOCAL_E2E_MODE"):
        raise HistoricalBackfillSafetyError("historical backfill cannot run with E2E flags")
    # Validate the complete production identity for candidate refs too.  This
    # must happen before any MinIO, Spark, PostgreSQL or Nessie client exists.
    try:
        validate_storage_environment(values)
    except ValueError as exc:
        raise HistoricalBackfillSafetyError(str(exc)) from exc
    return {"bucket": PRODUCTION_BUCKET, "ref": expected_ref, "layout": PRODUCTION_LAYOUT}


def require_write_approval(environ=None):
    values = os.environ if environ is None else environ
    if values.get("HISTORICAL_BACKFILL_WRITE_APPROVED") != "1":
        raise HistoricalBackfillSafetyError("HISTORICAL_BACKFILL_WRITE_APPROVED=1 is required for writes")


def validate_runtime(config: HistoricalBackfillConfig, environ=None, *, write=False):
    config.validate()
    production_environment(environ, candidate_ref=config.candidate_ref)
    if write:
        require_write_approval(environ)
    return config


__all__ = ["HistoricalBackfillConfig", "HistoricalBackfillSafetyError", "SUPPORTED_TABLES", "TABLE_CONFIG", "START_AT", "candidate_ref_for_run", "production_environment", "require_write_approval", "validate_runtime"]
