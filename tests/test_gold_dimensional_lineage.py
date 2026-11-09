import copy

import pytest

from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
from gold.implementation.dimensional.lineage import (
    DimensionalLineageError,
    dimensional_lineage_identity,
    validate_dimensional_lineage,
)


def dimensional_lineage():
    sources = {}
    for offset, table in enumerate(DIMENSIONAL_SOURCE_TABLES, start=1):
        attestation = f"{offset:064x}"
        sources[table] = {
            "source_type": "iceberg_snapshot", "table": table, "catalog": "lakehouse",
            "namespace": "silver", "ref": "main", "snapshot_id": offset,
            "snapshot_attestation_identity": attestation,
            "reconciliation_evidence": {"identity": f"{offset + 10:064x}", "status": "PASS", "snapshot_id": offset, "table": table},
            "quality_evidence": {"identity": f"{offset + 20:064x}", "status": "PASS", "snapshot_id": offset, "table": table},
        }
    return {"version": 1, "source_type": "iceberg_snapshot", "sources": sources}


def test_dimensional_lineage_requires_complete_pinned_silver_graph():
    lineage = dimensional_lineage()
    assert validate_dimensional_lineage(lineage) is True
    assert dimensional_lineage_identity(lineage) == dimensional_lineage_identity(copy.deepcopy(lineage))


def test_dimensional_lineage_rejects_non_silver_or_missing_source():
    lineage = dimensional_lineage()
    lineage["sources"]["orders"]["namespace"] = "gold"
    with pytest.raises(DimensionalLineageError, match="lakehouse.silver"):
        validate_dimensional_lineage(lineage)
    lineage = dimensional_lineage()
    lineage["sources"].pop("products")
    with pytest.raises(DimensionalLineageError, match="source set"):
        validate_dimensional_lineage(lineage)
