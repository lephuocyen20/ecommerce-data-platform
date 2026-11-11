"""Pinned, attested Iceberg reads for the dimensional graph."""

from common.manifest import read_json_object
from lakehouse.attestation import read_snapshot_attestation
from lakehouse.evidence import verify_evidence
from lakehouse.models.spec import SPECS
from .contracts import DIMENSIONAL_SOURCE_TABLES
from .lineage import DimensionalLineageError, validate_dimensional_lineage


def _read_evidence(client, bucket, identity, scope=None):
    key = f"lakehouse/evidence/v2/{identity}.json"
    if scope is not None:
        key = scope.key(key)
    document = read_json_object(client, bucket, key)
    if document.get("identity") != identity:
        raise DimensionalLineageError("Evidence identity mismatch")
    return document


def read_dimensional_sources(spark, client, bucket, lineage, scope=None):
    """Verify every authorization artifact before reading a pinned Silver snapshot."""
    validate_dimensional_lineage(lineage, scope=scope)
    result = {}
    source_catalog = "lakehouse"
    source_namespace = scope.silver_namespace if scope is not None else "silver"
    for table in DIMENSIONAL_SOURCE_TABLES:
        source = lineage["sources"][table]
        snapshot_id = source["snapshot_id"]
        source_ref = scope.nessie_ref if scope is not None else source.get("ref", "main")
        read_snapshot_attestation(client, bucket, source["snapshot_attestation_identity"], table=table,
                                  snapshot_id=snapshot_id, catalog=source_catalog, namespace=source_namespace, ref=source_ref,
                                  scope=scope)
        for kind, field in (("reconciliation", "reconciliation_evidence"), ("quality", "quality_evidence")):
            expected = source[field]
            document = (_read_evidence(client, bucket, expected["identity"], scope)
                        if scope is not None else _read_evidence(client, bucket, expected["identity"]))
            verify_evidence(document, kind=kind, table=table, snapshot_id=snapshot_id,
                            catalog=source_catalog, namespace=source_namespace, ref=source_ref,
                            snapshot_attestation_identity=source["snapshot_attestation_identity"])
            if {key: document[key] for key in expected} != expected:
                raise DimensionalLineageError(f"{kind} evidence mismatch for {table}")
        try:
            # Silver Iceberg tables intentionally carry ingestion metadata
            # columns (_batch_id/_ingested_at/_record_hash and audit fields).
            # Gold consumes the frozen business contract only; selecting it
            # explicitly prevents operational metadata from being mistaken for
            # schema drift.
            columns = ", ".join(f"`{column}`" for column in SPECS[table].columns)
            dataframe = spark.sql(
                f"SELECT {columns} FROM {source_catalog}.{source_namespace}.{table} VERSION AS OF {snapshot_id}"
            )
        except Exception as exc:
            raise DimensionalLineageError(f"Pinned Silver snapshot unavailable: {table}@{snapshot_id}") from exc
        if tuple(dataframe.columns) != SPECS[table].columns:
            raise DimensionalLineageError(f"Silver schema drift for {table}")
        result[table] = dataframe
    return result
