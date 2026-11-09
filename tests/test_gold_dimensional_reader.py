import pytest

from lakehouse.models.spec import SPECS
from gold.implementation.dimensional import reader
from gold.implementation.dimensional.lineage import DimensionalLineageError


def dimensional_lineage():
    sources = {}
    for offset, table in enumerate(("customers", "order_items", "orders", "products"), start=1):
        sources[table] = {
            "source_type": "iceberg_snapshot", "table": table, "catalog": "lakehouse", "namespace": "silver", "ref": "main", "snapshot_id": offset,
            "snapshot_attestation_identity": f"{offset:064x}",
            "reconciliation_evidence": {"identity": f"{offset + 10:064x}", "status": "PASS", "snapshot_id": offset, "table": table},
            "quality_evidence": {"identity": f"{offset + 20:064x}", "status": "PASS", "snapshot_id": offset, "table": table},
        }
    return {"version": 1, "source_type": "iceberg_snapshot", "sources": sources}


class Frame:
    def __init__(self, columns): self.columns = columns


class Spark:
    def __init__(self, columns): self.columns = columns; self.statements = []
    def sql(self, statement): self.statements.append(statement); return Frame(self.columns)


def test_reader_verifies_evidence_and_reads_pinned_snapshot(monkeypatch):
    lineage = dimensional_lineage()
    calls = []
    monkeypatch.setattr(reader, "read_snapshot_attestation", lambda *args, **kwargs: calls.append(("attestation", kwargs["table"])))
    monkeypatch.setattr(reader, "_read_evidence", lambda client, bucket, identity: {"identity": identity})
    monkeypatch.setattr(reader, "verify_evidence", lambda document, **kwargs: calls.append((kwargs["kind"], kwargs["table"])))
    # Evidence field comparison needs the expected four fields.
    def evidence(client, bucket, identity):
        for source in lineage["sources"].values():
            for field in ("reconciliation_evidence", "quality_evidence"):
                if source[field]["identity"] == identity:
                    return {**source[field], "identity": identity}
    monkeypatch.setattr(reader, "_read_evidence", evidence)
    spark = Spark(SPECS["customers"].columns)
    # Supply correct schema per statement without using an external Spark service.
    def sql(statement):
        table = next(name for name in SPECS if f".{name} VERSION" in statement)
        spark.statements.append(statement)
        return Frame(SPECS[table].columns)
    spark.sql = sql
    sources = reader.read_dimensional_sources(spark, object(), "bucket", lineage)
    assert set(sources) == set(lineage["sources"])
    assert all("VERSION AS OF" in statement for statement in spark.statements)
    assert len(calls) == 12


def test_reader_fails_closed_on_silver_schema_drift(monkeypatch):
    lineage = dimensional_lineage()
    monkeypatch.setattr(reader, "read_snapshot_attestation", lambda *args, **kwargs: None)
    def evidence(client, bucket, identity):
        for source in lineage["sources"].values():
            for field in ("reconciliation_evidence", "quality_evidence"):
                if source[field]["identity"] == identity:
                    return {**source[field], "identity": identity}
    monkeypatch.setattr(reader, "_read_evidence", evidence)
    monkeypatch.setattr(reader, "verify_evidence", lambda *args, **kwargs: None)
    with pytest.raises(DimensionalLineageError, match="schema drift"):
        reader.read_dimensional_sources(Spark(("wrong",)), object(), "bucket", lineage)
