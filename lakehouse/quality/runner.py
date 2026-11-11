"""Read-only orchestration for the declarative Lakehouse quality checks."""

from __future__ import annotations

from collections.abc import Mapping

from lakehouse.models.spec import SPECS

from .checks import evaluate_check
from .contracts import ERROR, QualityError, TableQualityReport, aggregate_status, build_quality_plan


# These are the repository's configured Iceberg catalog and namespace (see
# spark/conf/spark-defaults.conf), never Spark's implicit default catalog.
CATALOG = "lakehouse"
NAMESPACE = "silver"


def table_identifier(name: str) -> str:
    return f"{CATALOG}.{NAMESPACE}.{name}"


def _validate_selection(specs, table_name, sample_limit):
    if not isinstance(specs, Mapping) or not specs:
        raise ValueError("no registered TableSpecs")
    if isinstance(sample_limit, bool) or not isinstance(sample_limit, int) or sample_limit < 0:
        raise ValueError("sample_limit must be a non-negative integer")
    if table_name is not None and table_name not in specs:
        raise ValueError(f"unknown table name: {table_name}")
    return (table_name,) if table_name is not None else tuple(sorted(specs))


def _error_report(table, error, *, snapshot_id=None, rows=None):
    return TableQualityReport(
        table=table,
        status=ERROR,
        snapshot_id=snapshot_id,
        rows=rows,
        checks=(),
        error=QualityError(type(error).__name__, str(error), {"table": table}),
    )


def _require_catalog(spark):
    """Fail closed when the explicit repository catalog has not been configured."""
    configured = spark.conf.get(f"spark.sql.catalog.{CATALOG}")
    if not configured:
        raise RuntimeError(f"Iceberg catalog '{CATALOG}' is not configured")


def read_table_snapshot(spark, table: str):
    """Read a table only after proving it has a readable current snapshot."""
    _require_catalog(spark)
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"Iceberg table does not exist: {table}")
    snapshot = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()
    if snapshot is None or getattr(snapshot, "snapshot_id", None) is None:
        raise RuntimeError(f"Iceberg table has no valid readable snapshot: {table}")
    return spark.table(table), int(snapshot.snapshot_id)


def make_parent_resolver(spark):
    """Build the explicit, non-caching, read-only FK parent resolver."""
    def resolve(parent_name: str):
        parent_table = table_identifier(parent_name)
        _require_catalog(spark)
        if not spark.catalog.tableExists(parent_table):
            raise RuntimeError(f"Iceberg parent table does not exist: {parent_table}")
        return spark.table(parent_table)
    return resolve


def run_quality(spark, *, specs=SPECS, table_name=None, sample_limit: int = 10, batch_id: str | None = None):
    """Execute every configured quality rule without mutating Spark or Iceberg."""
    selected = _validate_selection(specs, table_name, sample_limit)
    reports = []
    resolver = make_parent_resolver(spark)
    for name in selected:
        table = table_identifier(name)
        try:
            dataframe, snapshot_id = read_table_snapshot(spark, table)
            if batch_id is not None:
                if not isinstance(batch_id, str) or not batch_id:
                    raise ValueError("batch_id must be a non-empty string")
                if "_batch_id" not in dataframe.columns:
                    raise RuntimeError(f"Iceberg table is missing _batch_id for incremental quality scope: {table}")
                dataframe = dataframe.where(dataframe["_batch_id"] == batch_id)
            rows = int(dataframe.count())
            plan = build_quality_plan(specs[name], specs)
            checks = tuple(
                evaluate_check(dataframe, check, table, sample_limit=sample_limit, parent_resolver=resolver)
                for check in plan
            )
            reports.append(TableQualityReport(table, aggregate_status(checks), snapshot_id, rows, checks))
        except Exception as error:
            reports.append(_error_report(table, error))
    return tuple(reports)
