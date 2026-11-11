"""Read-only operational summary for Lakehouse reconciliation and lineage."""

import json
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/work-dir")

from common.minio import configure_s3a
from lakehouse.models.spec import SPECS


NOT_MIGRATED = "NOT_MIGRATED"
MIGRATED = "MIGRATED"
NO_SNAPSHOT = "NO_SNAPSHOT"
CATALOG_ERROR = "CATALOG_ERROR"
PERMISSION_DENIED = "PERMISSION_DENIED"
QUERY_ERROR = "QUERY_ERROR"

FAILURE_STATES = frozenset(
    {NO_SNAPSHOT, CATALOG_ERROR, PERMISSION_DENIED, QUERY_ERROR}
)
MISSING_TABLE_ERROR_CLASSES = frozenset(
    {"TABLE_OR_VIEW_NOT_FOUND", "TABLE_NOT_FOUND", "NO_SUCH_TABLE"}
)
CATALOG_ERROR_CLASSES = frozenset(
    {"CATALOG_NOT_FOUND", "CATALOG_NOT_CONFIGURED", "SCHEMA_NOT_FOUND"}
)
PERMISSION_ERROR_CLASSES = frozenset(
    {
        "ACCESS_DENIED",
        "INSUFFICIENT_PERMISSIONS",
        "NOT_AUTHORIZED",
        "PERMISSION_DENIED",
        "UNAUTHORIZED",
    }
)


def _error_class(error):
    """Return Spark's structured error class when an exception provides one."""
    getter = getattr(error, "getErrorClass", None)
    if callable(getter):
        return getter()
    return getattr(error, "error_class", None)


def _error_state(error):
    """Classify structured Spark failures; unknown failures remain query errors."""
    error_class = (_error_class(error) or "").upper()

    if error_class in MISSING_TABLE_ERROR_CLASSES:
        return NOT_MIGRATED
    if error_class in PERMISSION_ERROR_CLASSES:
        return PERMISSION_DENIED
    if error_class in CATALOG_ERROR_CLASSES:
        return CATALOG_ERROR
    return QUERY_ERROR


def _result(spec, state, *, rows=None, snapshot_id=None):
    """Keep the established per-table JSON fields while adding migration state."""
    return {
        "rows": rows,
        "snapshot_id": snapshot_id,
        "foreign_keys": spec.foreign_keys or {},
        "status": state,
    }


def audit_table(spark, name, spec):
    """Audit one configured Iceberg table without modifying Lakehouse state."""
    table = f"lakehouse.silver.{name}"
    try:
        if not spark.catalog.tableExists(table):
            return _result(spec, NOT_MIGRATED)

        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).first()
        if snapshot is None:
            return _result(spec, NO_SNAPSHOT)

        return _result(
            spec,
            MIGRATED,
            rows=spark.table(table).count(),
            snapshot_id=int(snapshot.snapshot_id),
        )
    except Exception as error:  # Spark exposes several runtime exception types.
        return _result(spec, _error_state(error))


def run_audit(spark, specs=SPECS):
    """Return the audit output for all configured tables."""
    return {name: audit_table(spark, name, spec) for name, spec in specs.items()}


def exit_code(result):
    """Return a failing exit code only for states unsafe for migration operations."""
    return int(any(item["status"] in FAILURE_STATES for item in result.values()))


def main():
    spark = SparkSession.builder.appName("LakehouseAudit").getOrCreate()
    try:
        configure_s3a(spark)
        result = run_audit(spark)
        print(json.dumps(result, sort_keys=True))
        return exit_code(result)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
