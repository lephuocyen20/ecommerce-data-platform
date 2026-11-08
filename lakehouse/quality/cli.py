"""JSON-only command line entry point for the read-only quality runner."""

from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import SparkSession

from common.minio import configure_s3a
from lakehouse.models.spec import SPECS

from .contracts import ERROR, QualityError, TableQualityReport
from .runner import run_quality, table_identifier


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError(message)


def _parse(argv):
    parser = _Parser(prog="lakehouse-quality")
    parser.add_argument("--table-name")
    parser.add_argument("--sample-limit", type=int, default=10)
    return parser.parse_args(argv)


def _error_reports(specs, table_name, error):
    names = (table_name,) if table_name is not None else tuple(sorted(specs))
    return tuple(TableQualityReport(
        table_identifier(name), ERROR, None, None, (),
        QualityError(type(error).__name__, str(error), {"table": table_identifier(name)}),
    ) for name in names)


def _status_exit(reports):
    statuses = {report.status for report in reports}
    return 2 if ERROR in statuses else 1 if "FAIL" in statuses else 0


def _shutdown_error_reports(reports, error):
    """Do not let a Spark shutdown failure be hidden by earlier PASS checks."""
    return tuple(TableQualityReport(
        report.table, ERROR, report.snapshot_id, report.rows, report.checks,
        QualityError(type(error).__name__, str(error), {"table": report.table, "stage": "spark.stop"}),
    ) for report in reports)


def main(argv=None, *, spark_factory=None, configure=configure_s3a, specs=SPECS):
    """Return process status while writing exactly one JSON value to stdout."""
    try:
        args = _parse(argv)
        # Runner repeats these checks; doing them here keeps invalid invocation
        # and empty registry in the pre-execution exit-3 category.
        if not specs:
            raise ValueError("no registered TableSpecs")
        if args.table_name is not None and args.table_name not in specs:
            raise ValueError(f"unknown table name: {args.table_name}")
        if args.sample_limit < 0:
            raise ValueError("sample_limit must be a non-negative integer")
    except (ValueError, TypeError) as error:
        print(str(error), file=sys.stderr)
        return 3

    spark = None
    try:
        factory = spark_factory or (lambda: SparkSession.builder.appName("LakehouseQuality").getOrCreate())
        spark = factory()
        configure(spark)
        reports = run_quality(spark, specs=specs, table_name=args.table_name, sample_limit=args.sample_limit)
    except Exception as error:
        print(str(error), file=sys.stderr)
        reports = _error_reports(specs, args.table_name, error)
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as error:
                print(str(error), file=sys.stderr)
                reports = _shutdown_error_reports(reports, error)
    print(json.dumps([report.as_dict() for report in reports], sort_keys=True, default=str))
    return _status_exit(reports)


if __name__ == "__main__":
    sys.exit(main())
