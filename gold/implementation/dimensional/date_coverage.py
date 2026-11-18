"""Deterministic UTC date coverage preflight for historical Gold."""

import json
from datetime import date, datetime, timezone

from pyspark.sql import functions as F


class DateCoverageError(ValueError):
    """Raised when verified Silver contains an unusable order date."""


def utc_calendar_date(value):
    """Parse one order timestamp using UTC semantics for deterministic tests."""
    if value is None:
        raise DateCoverageError("orders.order_date is null")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        return value
    else:
        raw = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise DateCoverageError("orders.order_date is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).date()


def _iso(value, field):
    if not isinstance(value, str):
        raise DateCoverageError(f"Invalid {field}; expected YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise DateCoverageError(f"Invalid {field}; expected calendar date") from exc


def effective_date_bounds(observed_min, observed_max, capture_start, capture_end):
    """Return inclusive bounds, expanding configured capture bounds as needed."""
    configured_start = _iso(capture_start, "capture_start")
    configured_end = _iso(capture_end, "capture_end")
    if configured_start > configured_end:
        raise DateCoverageError("Invalid capture date range")
    if observed_min is None or observed_max is None:
        raise DateCoverageError("orders.order_date has no observed valid dates")
    observed_start = _iso(observed_min, "observed_order_date_min")
    observed_end = _iso(observed_max, "observed_order_date_max")
    if observed_start > observed_end:
        raise DateCoverageError("Invalid observed orders.order_date range")
    return {
        "configured_start": configured_start.isoformat(),
        "configured_end": configured_end.isoformat(),
        "observed_min": observed_start.isoformat(),
        "observed_max": observed_end.isoformat(),
        "effective_start": min(configured_start, observed_start).isoformat(),
        "effective_end": max(configured_end, observed_end).isoformat(),
    }


def derive_order_date_coverage(orders, capture_start, capture_end):
    """Derive UTC calendar coverage from the already verified Silver orders frame."""
    parsed = orders.select(
        F.col("order_id"),
        F.col("order_date"),
        F.to_timestamp(F.col("order_date").cast("string")).alias("_order_timestamp"),
    )
    null_count = parsed.where(F.col("order_date").isNull()).count()
    invalid_count = parsed.where(
        F.col("order_date").isNotNull() & F.col("_order_timestamp").isNull()
    ).count()
    if null_count or invalid_count:
        bad = (
            parsed.where(F.col("order_date").isNull() | F.col("_order_timestamp").isNull())
            .select("order_id", "order_date")
            .orderBy(F.col("order_id").cast("string"))
            .limit(10)
            .collect()
        )
        diagnostics = {
            "column": "orders.order_date",
            "null_count": int(null_count),
            "invalid_count": int(invalid_count),
            "sample": [{"order_id": row.order_id, "order_date": row.order_date} for row in bad],
        }
        raise DateCoverageError(
            "Invalid orders.order_date; diagnostics="
            + json.dumps(diagnostics, sort_keys=True, default=str, separators=(",", ":"))
        )

    utc_dates = parsed.select(
        F.to_date(F.to_utc_timestamp(F.col("_order_timestamp"), "UTC")).alias("_utc_date")
    )
    bounds = utc_dates.agg(F.min("_utc_date").alias("min_date"), F.max("_utc_date").alias("max_date")).first()
    observed_min = bounds.min_date.isoformat() if bounds and bounds.min_date is not None else None
    observed_max = bounds.max_date.isoformat() if bounds and bounds.max_date is not None else None
    return effective_date_bounds(observed_min, observed_max, capture_start, capture_end)


__all__ = ["DateCoverageError", "derive_order_date_coverage", "effective_date_bounds", "utc_calendar_date"]
