"""Deterministic Bronze/Silver serialization and validation contracts."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import datetime

from common.storage_layout import bronze_table_key, silver_table_key

from .config import TABLE_CONFIG, SUPPORTED_TABLES


def _records(columns, rows):
    return [dict(zip(columns, row)) for row in rows]


def deduplicate_records(records, table_name):
    if table_name not in SUPPORTED_TABLES:
        raise ValueError(f"Unsupported historical table: {table_name}")
    key = TABLE_CONFIG[table_name]["primary_key"]
    selected = {}
    for record in records:
        if key not in record or record[key] is None:
            raise ValueError(f"{table_name} record is missing {key}")
        current = selected.get(record[key])
        if current is None or (record.get("updated_at"), _stable(record)) > (current.get("updated_at"), _stable(current)):
            selected[record[key]] = record
    return sorted(selected.values(), key=lambda item: (item.get("updated_at"), item[key], _stable(item)))


def _stable(value):
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def validate_fk_parity(table_name, records, order_ids, product_ids=()):
    if table_name == "order_items":
        missing_orders = sorted({record["order_id"] for record in records} - set(order_ids))
        missing_products = sorted({record["product_id"] for record in records if record.get("product_id") is not None} - set(product_ids))
        if missing_orders or missing_products:
            raise ValueError(f"order_items contains unresolved FK(s): orders={missing_orders}, products={missing_products}")
        return {"status": "PASS", "missing_order_ids": [], "missing_product_ids": []}
    if table_name not in ("payments", "shipments"):
        return {"status": "NOT_APPLICABLE", "missing_order_ids": []}
    missing = sorted({record["order_id"] for record in records} - set(order_ids))
    if missing:
        raise ValueError(f"{table_name} contains order_id outside verified orders scope: {missing}")
    return {"status": "PASS", "missing_order_ids": []}


def csv_bytes(columns, rows):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def bronze_manifest(table_name, run_id, columns, rows, *, bucket="ecommerce-lakehouse-prod"):
    payload = csv_bytes(columns, rows)
    return {"layer": "bronze", "table": table_name, "run_id": run_id, "bucket": bucket, "object_key": bronze_table_key(table_name, f"run_id={run_id}/data.csv", environ={"BRONZE_BUCKET": bucket, "LAKEHOUSE_STORAGE_ENV": "production", "NESSIE_REF": "gold_prod_current"}), "row_count": len(rows), "checksum_sha256": hashlib.sha256(payload).hexdigest(), "columns": list(columns), "status": "VERIFIED"}


def silver_manifest(table_name, run_id, records, *, order_ids=(), product_ids=(), bucket="ecommerce-lakehouse-prod"):
    deduped = deduplicate_records(records, table_name)
    fk = validate_fk_parity(table_name, deduped, order_ids, product_ids)
    canonical = _stable(deduped).encode("utf-8")
    return {"layer": "silver", "table": table_name, "run_id": run_id, "bucket": bucket, "object_key": silver_table_key(table_name, f"run_id={run_id}/data.parquet", environ={"BRONZE_BUCKET": bucket, "LAKEHOUSE_STORAGE_ENV": "production", "NESSIE_REF": "gold_prod_current"}), "row_count": len(deduped), "checksum_sha256": hashlib.sha256(canonical).hexdigest(), "primary_key": TABLE_CONFIG[table_name]["primary_key"], "fk_parity": fk, "status": "VERIFIED", "records": deduped}


__all__ = ["bronze_manifest", "csv_bytes", "deduplicate_records", "silver_manifest", "validate_fk_parity"]
