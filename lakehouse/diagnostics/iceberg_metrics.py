"""Read-only Iceberg metadata metrics.

The collector queries Iceberg metadata tables only.  It never issues a write,
maintenance or cleanup statement and rejects mutation-shaped SQL tokens before
execution so it can be safely embedded in diagnostics jobs.
"""

import re


class IcebergMetricsError(ValueError):
    pass


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")
_MUTATION = re.compile(r"\b(?:ALTER|CREATE|DELETE|DROP|INSERT|MERGE|REPLACE|TRUNCATE|UPDATE|VACUUM|WRITE)\b", re.IGNORECASE)


def validate_read_only_statement(statement):
    if not isinstance(statement, str) or not statement.strip():
        raise IcebergMetricsError("Metadata statement must be non-empty SQL")
    if _MUTATION.search(statement):
        raise IcebergMetricsError("Mutation statements are forbidden in read-only metrics")
    return statement


def _table_identifier(table):
    if not isinstance(table, str) or _IDENTIFIER.fullmatch(table) is None:
        raise IcebergMetricsError("Invalid Iceberg table identifier")
    return table


def collect_table_metrics(spark, table):
    """Collect snapshot/file metrics using Iceberg metadata tables only."""
    identifier = _table_identifier(table)
    snapshots_sql = validate_read_only_statement(
        f"SELECT snapshot_id, committed_at, operation FROM {identifier}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    )
    files_sql = validate_read_only_statement(
        f"SELECT COUNT(*) AS file_count, COALESCE(SUM(record_count), 0) AS record_count, "
        f"COALESCE(SUM(file_size_in_bytes), 0) AS bytes FROM {identifier}.files"
    )
    snapshot = spark.sql(snapshots_sql).first()
    files = spark.sql(files_sql).first()
    if snapshot is None:
        return {"table": identifier, "status": "NO_SNAPSHOT", "snapshot_id": None, "file_count": 0, "record_count": 0, "bytes": 0}
    return {
        "table": identifier,
        "status": "PASS",
        "snapshot_id": int(snapshot.snapshot_id),
        "committed_at": str(snapshot.committed_at),
        "operation": str(snapshot.operation),
        "file_count": int(files.file_count),
        "record_count": int(files.record_count),
        "bytes": int(files.bytes),
    }


def collect_metrics(spark, tables):
    if not isinstance(tables, (list, tuple)) or not tables:
        raise IcebergMetricsError("At least one Iceberg table is required")
    return {table: collect_table_metrics(spark, table) for table in tables}
