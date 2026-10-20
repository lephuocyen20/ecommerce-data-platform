"""Static configuration and input validation for incremental ingestion."""

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
        "watermark_column": "order_item_id",
        "watermark_type": "ID",
    },
    "payments": {
        "primary_key": "payment_id",
        "watermark_column": "payment_id",
        "watermark_type": "ID",
    },
}

EXPECTED_DB_NAME = "ecommerce_test"
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
