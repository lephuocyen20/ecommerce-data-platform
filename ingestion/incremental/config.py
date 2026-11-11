"""Static configuration and input validation for incremental ingestion."""

import os
import re


# Incremental configuration
TABLE_CONFIG = {
    "customers": {
        "primary_key": "customer_id",
        "watermark_column": "updated_at",
        "watermark_type": "TIMESTAMP",
    },
    "products": {
        "primary_key": "product_id",
        "watermark_column": "updated_at",
        "watermark_type": "TIMESTAMP",
    },
    "orders": {
        "primary_key": "order_id",
        "watermark_column": "updated_at",
        "watermark_type": "TIMESTAMP",
    },
    "shipments": {
        "primary_key": "shipment_id",
        "watermark_column": "updated_at",
        "watermark_type": "TIMESTAMP",
    },
    "order_items": {
        "primary_key": "order_item_id",
        "watermark_column": "updated_at",
        "watermark_type": "TIMESTAMP",
        "change_signal": "updated_at",
    },
    "payments": {
        "primary_key": "payment_id",
        "watermark_column": "updated_at",
        "watermark_type": "TIMESTAMP",
        "change_signal": "updated_at",
    },
}

LATE_DATA_OVERLAP_ENABLED = "LATE_DATA_OVERLAP_ENABLED"
LATE_DATA_OVERLAP_SECONDS = "LATE_DATA_OVERLAP_SECONDS"


def late_data_overlap_enabled(environ=None):
    """Return the explicit late-data replay flag.

    The overlap is processed with a separate replay cursor; the committed
    source watermark remains monotonic.
    """
    values = os.environ if environ is None else environ
    value = values.get(LATE_DATA_OVERLAP_ENABLED, "false")
    if not isinstance(value, str) or value.lower() not in {"true", "false"}:
        raise ValueError(f"{LATE_DATA_OVERLAP_ENABLED} must be true or false")
    return value.lower() == "true"


def get_late_data_overlap_seconds(environ=None):
    values = os.environ if environ is None else environ
    if not late_data_overlap_enabled(values):
        return 0
    raw = values.get(LATE_DATA_OVERLAP_SECONDS, "86400")
    try:
        seconds = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{LATE_DATA_OVERLAP_SECONDS} must be a positive integer") from exc
    if seconds <= 0:
        raise ValueError(f"{LATE_DATA_OVERLAP_SECONDS} must be a positive integer")
    return seconds


def validate_extraction_contract(environ=None):
    seconds = get_late_data_overlap_seconds(environ)
    return {"late_data_overlap": seconds > 0, "late_data_overlap_seconds": seconds, "tombstone_policy": "TOMBSTONE_ONLY"}

EXPECTED_DB_NAME = "ecommerce_platform"
RECOVERABLE_STATUSES = ("PENDING", "UPLOADED")
BLOCKING_STATUSES = (*RECOVERABLE_STATUSES, "FAILED")
VALID_BATCH_ID = re.compile(r"[0-9a-f]{32}")
VALID_CHECKSUM = re.compile(r"[0-9a-f]{64}")


def get_table_config(table_name):
    """Return the supported table configuration for *table_name*."""
    try:
        return TABLE_CONFIG[table_name]
    except KeyError:
        raise ValueError(f"Unsupported table: {table_name}") from None


def validate_positive_integer(value, name):
    """Validate an integer while rejecting bool, which is an int subclass."""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def validate_batch_id(batch_id):
    if not isinstance(batch_id, str) or not VALID_BATCH_ID.fullmatch(batch_id):
        raise ValueError("Invalid batch ID")
