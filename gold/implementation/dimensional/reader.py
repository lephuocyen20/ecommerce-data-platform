"""Pinned, attested Iceberg reads for the dimensional graph."""

from common.manifest import read_json_object
from lakehouse.attestation import read_snapshot_attestation
from lakehouse.evidence import verify_evidence
from lakehouse.models.spec import SPECS
from .contracts import DIMENSIONAL_SOURCE_TABLES
from .lineage import DimensionalLineageError, validate_dimensional_lineage


def _read_evidence(client, bucket, identity):
    document = read_json_object(client, bucket, f"lakehouse/evidence/v2/{identity}.json")
    if document.get("identity") != identity:
        raise DimensionalLineageError("Evidence identity mismatch")
    return document


def read_dimensional_sources(spark, client, bucket, lineage):
    """Verify every authorization artifact before reading a pinned Silver snapshot."""
    validate_dimensional_lineage(lineage)
    result = {}
    for table in DIMENSIONAL_SOURCE_TABLES:
        source = lineage["sources"][table]
        snapshot_id = source["snapshot_id"]
        read_snapshot_attestation(client, bucket, source["snapshot_attestation_identity"], table=table,
                                  snapshot_id=snapshot_id, catalog="lakehouse", namespace="silver", ref="main")
        for kind, field in (("reconciliation", "reconciliation_evidence"), ("quality", "quality_evidence")):
            expected = source[field]
            document = _read_evidence(client, bucket, expected["identity"])
            verify_evidence(document, kind=kind, table=table, snapshot_id=snapshot_id,
                            catalog="lakehouse", namespace="silver", ref="main",
                            snapshot_attestation_identity=source["snapshot_attestation_identity"])
            if {key: document[key] for key in expected} != expected:
                raise DimensionalLineageError(f"{kind} evidence mismatch for {table}")
        try:
            dataframe = spark.sql(f"SELECT * FROM lakehouse.silver.{table} VERSION AS OF {snapshot_id}")
        except Exception as exc:
            raise DimensionalLineageError(f"Pinned Silver snapshot unavailable: {table}@{snapshot_id}") from exc
        if tuple(dataframe.columns) != SPECS[table].columns:
            raise DimensionalLineageError(f"Silver schema drift for {table}")
        result[table] = dataframe
    return result
