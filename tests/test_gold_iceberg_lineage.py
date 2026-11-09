import pytest

from gold.implementation.identity import build_gold_snapshot_id_from_lineage
from gold.implementation.source_lineage import (
    ICEBERG_SNAPSHOT,
    GoldSourceLineageError,
    canonical_lineage,
    validate_source_lineage,
)
from lakehouse.evidence import build_evidence, verify_evidence
from lakehouse.attestation import build_snapshot_attestation, verify_snapshot_attestation
from gold.implementation.readers import read_iceberg_gold_sources


class _Response:
    def __init__(self, value): self.value = value
    def read(self): return __import__("json").dumps(self.value).encode("utf-8")
    def close(self): pass
    def release_conn(self): pass


class _Client:
    def __init__(self, objects): self.objects = objects
    def get_object(self, _bucket, key): return _Response(self.objects[key])


class _Spark:
    def __init__(self): self.queries = []
    def sql(self, query): self.queries.append(query); return {"query": query}


def _source(table, snapshot_id):
    attestation = build_snapshot_attestation(table=table, snapshot_id=snapshot_id, manifest_identities=["a" * 64])
    args = {"catalog": "lakehouse", "namespace": "silver", "ref": "main", "snapshot_attestation_identity": attestation["identity"]}
    reconciliation = build_evidence(kind="reconciliation", table=table, snapshot_id=snapshot_id, status="PASS", details={"rows": 1}, **args)
    quality = build_evidence(kind="quality", table=table, snapshot_id=snapshot_id, status="PASS", details={"checks": 2}, **args)
    return {
        "source_type": ICEBERG_SNAPSHOT, "table": table, "catalog": "lakehouse",
        "namespace": "silver", "ref": "main", "snapshot_id": snapshot_id,
        "snapshot_attestation_identity": attestation["identity"],
        "reconciliation_evidence": {key: reconciliation[key] for key in ("identity", "status", "snapshot_id", "table")},
        "quality_evidence": {key: quality[key] for key in ("identity", "status", "snapshot_id", "table")},
    }


def _lineage():
    return {"version": 2, "source_type": ICEBERG_SNAPSHOT, "sources": {
        "orders": _source("orders", 11), "order_items": _source("order_items", 12),
    }}


def test_iceberg_lineage_is_canonical_and_deterministic():
    lineage = _lineage()
    assert validate_source_lineage(lineage, "daily_sales") is True
    assert canonical_lineage(lineage) == canonical_lineage(dict(reversed(list(lineage.items()))))
    assert build_gold_snapshot_id_from_lineage("daily_sales", lineage) == build_gold_snapshot_id_from_lineage("daily_sales", lineage)


def test_iceberg_lineage_rejects_evidence_for_another_snapshot():
    lineage = _lineage()
    lineage["sources"]["orders"]["quality_evidence"]["snapshot_id"] = 999
    with pytest.raises(GoldSourceLineageError, match="snapshot mismatch"):
        validate_source_lineage(lineage, "daily_sales")


def test_evidence_is_content_addressed_and_tamper_evident():
    attestation = build_snapshot_attestation(table="orders", snapshot_id=7, manifest_identities=["a" * 64])
    args = {"catalog": "lakehouse", "namespace": "silver", "ref": "main", "snapshot_attestation_identity": attestation["identity"]}
    evidence = build_evidence(kind="quality", table="orders", snapshot_id=7, status="PASS", details={"checks": 2}, **args)
    assert verify_evidence(evidence, kind="quality", table="orders", snapshot_id=7, **args)
    evidence["details"] = {"checks": 3}
    with pytest.raises(ValueError, match="identity"):
        verify_evidence(evidence, kind="quality", table="orders", snapshot_id=7, **args)


def test_attestation_is_cumulative_and_tamper_evident():
    document = build_snapshot_attestation(
        table="orders", snapshot_id=7,
        manifest_identities=["b" * 64, "a" * 64],
    )
    assert document["manifest_identities"] == ["a" * 64, "b" * 64]
    assert verify_snapshot_attestation(document, table="orders", snapshot_id=7)
    document["manifest_identities"] = ["c" * 64]
    with pytest.raises(ValueError, match="identity"):
        verify_snapshot_attestation(document)


def test_reader_uses_only_pinned_iceberg_snapshot():
    lineage = _lineage()
    objects = {}
    for table, source in lineage["sources"].items():
        attestation = build_snapshot_attestation(
            table=table, snapshot_id=source["snapshot_id"], manifest_identities=["a" * 64],
        )
        assert attestation["identity"] == source["snapshot_attestation_identity"]
        objects[f"lakehouse/attestations/v1/{attestation['identity']}.json"] = attestation
        args = {"catalog": "lakehouse", "namespace": "silver", "ref": "main", "snapshot_attestation_identity": attestation["identity"]}
        for kind, evidence_ref, details in (("reconciliation", source["reconciliation_evidence"], {"rows": 1}), ("quality", source["quality_evidence"], {"checks": 2})):
            evidence = build_evidence(kind=kind, table=table, snapshot_id=source["snapshot_id"], status="PASS", details=details, **args)
            assert evidence["identity"] == evidence_ref["identity"]
            objects[f"lakehouse/evidence/v2/{evidence['identity']}.json"] = evidence
    spark = _Spark()
    read_iceberg_gold_sources(spark, _Client(objects), "bronze", "daily_sales", lineage)
    assert spark.queries == [
        "SELECT * FROM lakehouse.silver.orders VERSION AS OF 11",
        "SELECT * FROM lakehouse.silver.order_items VERSION AS OF 12",
    ]
