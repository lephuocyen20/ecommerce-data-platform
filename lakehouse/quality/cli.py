"""JSON-only command line entry point for the read-only quality runner."""

from __future__ import annotations

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession

from common.minio import configure_s3a
from common.minio import create_minio_client
from lakehouse.evidence import publish_pass_evidence_for_attestation
from lakehouse.models.spec import SPECS

from lakehouse.quality.contracts import ERROR, QualityError, TableQualityReport
from lakehouse.quality.runner import run_quality, table_identifier


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError(message)


def _parse(argv):
    parser = _Parser(prog="lakehouse-quality")
    parser.add_argument("--table-name")
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument("--attestation-identity")
    parser.add_argument("--manifest-key")
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
        if args.manifest_key and args.attestation_identity:
            raise ValueError("Specify only one attestation selector")
        attestation_identity = args.attestation_identity
        if args.manifest_key:
            from lakehouse.migration.manifest import _identity, read_manifest
            from lakehouse.attestation import build_snapshot_attestation
            client = create_minio_client()
            manifest = read_manifest(client, os.environ["BRONZE_BUCKET"], args.manifest_key, args.table_name)
            progress = f"lakehouse.metadata.{args.table_name}_parquet_migration_progress"
            record = spark.sql(f"SELECT snapshot_id FROM {progress} WHERE manifest_identity = '{_identity(manifest)}'").first()
            if record is None:
                raise ValueError("Missing migration progress for quality evidence")
            rows = spark.sql(f"SELECT manifest_identity FROM {progress} WHERE snapshot_id = {int(record.snapshot_id)}").collect()
            attestation_identity = build_snapshot_attestation(table=args.table_name, snapshot_id=int(record.snapshot_id), manifest_identities=[row.manifest_identity for row in rows])["identity"]
        if attestation_identity:
            if len(reports) != 1 or reports[0].status != "PASS" or reports[0].snapshot_id is None:
                raise ValueError("Attestation evidence requires exactly one PASS quality report with a snapshot")
            report = reports[0]
            publication = publish_pass_evidence_for_attestation(
                create_minio_client(), os.environ["BRONZE_BUCKET"], kind="quality",
                table=args.table_name, snapshot_id=int(report.snapshot_id),
                details=report.as_dict(), attestation_identity=attestation_identity,
            )
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
