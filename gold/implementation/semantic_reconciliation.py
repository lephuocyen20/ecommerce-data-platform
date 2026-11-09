"""Fail-closed semantic comparison for legacy and Iceberg Gold outputs.

The caller supplies small, already-aggregated rows.  Spark/MinIO reads stay outside
this module so it is deterministic, inexpensive to unit test, and safe in CI.
"""

from decimal import Decimal
import hashlib
import json
from datetime import date, datetime

from gold.implementation.contracts.datasets import get_gold_contract


class GoldSemanticMismatch(ValueError):
    pass


def _normalise(value, data_type):
    if value is None:
        return None
    if data_type.startswith("decimal"):
        return format(Decimal(str(value)).normalize(), "f")
    if data_type == "long":
        if isinstance(value, bool):
            raise GoldSemanticMismatch("Boolean is not a long")
        return str(int(value))
    if data_type == "date":
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return str(value)
    return str(value)


def _canonical(row, columns):
    return json.dumps({name: _normalise(row[name], data_type) for name, data_type in columns.items()}, sort_keys=True, separators=(",", ":"))


def reconcile_gold_rows(dataset_name, legacy_rows, iceberg_rows, *, numeric_tolerance=Decimal("0")):
    """Compare two materialized Gold result sets at the declared business grain."""
    if not isinstance(numeric_tolerance, Decimal) or numeric_tolerance < 0:
        raise ValueError("numeric_tolerance must be a non-negative Decimal")
    contract = get_gold_contract(dataset_name)
    keys, columns = contract["primary_key"], contract["columns"]

    def index(rows, label):
        indexed = {}
        for row in rows:
            if not isinstance(row, dict) or set(columns) - set(row):
                raise GoldSemanticMismatch(f"Missing required {label} columns")
            key = tuple(_normalise(row[name], columns[name]) for name in keys)
            if any(value is None for value in key) or key in indexed:
                raise GoldSemanticMismatch(f"Invalid {label} primary key: {key}")
            # Type-normalise every contract value before comparison/hash.  It both
            # validates numeric inputs and prevents Decimal formatting drift.
            for column, data_type in columns.items():
                _normalise(row[column], data_type)
            indexed[key] = row
        return indexed

    legacy, iceberg = index(legacy_rows, "legacy"), index(iceberg_rows, "iceberg")
    missing, extra = sorted(set(legacy) - set(iceberg)), sorted(set(iceberg) - set(legacy))
    differences = []
    for key in sorted(set(legacy) & set(iceberg)):
        for column, data_type in columns.items():
            left, right = _normalise(legacy[key][column], data_type), _normalise(iceberg[key][column], data_type)
            if data_type.startswith(("decimal", "long")) and left is not None and right is not None:
                if abs(Decimal(left) - Decimal(right)) > numeric_tolerance:
                    differences.append({"key": key, "column": column, "legacy": str(left), "iceberg": str(right)})
            elif left != right:
                differences.append({"key": key, "column": column, "legacy": left, "iceberg": right})
    digest = lambda rows: hashlib.sha256("\n".join(sorted(_canonical(row, columns) for row in rows)).encode()).hexdigest()
    report = {"dataset_name": dataset_name, "contract_version": contract["semantic"]["version"], "legacy_row_count": len(legacy), "iceberg_row_count": len(iceberg), "missing_keys": missing, "extra_keys": extra, "differences": differences, "legacy_hash": digest(legacy.values()), "iceberg_hash": digest(iceberg.values())}
    report["status"] = "PASS" if not missing and not extra and not differences and report["legacy_hash"] == report["iceberg_hash"] else "FAIL"
    return report


def require_semantic_parity(*args, **kwargs):
    report = reconcile_gold_rows(*args, **kwargs)
    if report["status"] != "PASS":
        raise GoldSemanticMismatch(f"Gold semantic reconciliation failed: {report}")
    return report
