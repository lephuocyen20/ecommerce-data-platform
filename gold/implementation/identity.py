
"""Deterministic Gold snapshot identity."""

import hashlib
import json

from gold.implementation.storage.contract import (
    validate_dataset_name,
    validate_source_snapshots,
)
from gold.implementation.source_lineage import canonical_lineage, validate_source_lineage


TRANSFORMATION_VERSION = "1.0"


def build_gold_snapshot_id(
    dataset_name,
    source_snapshots,
    transformation_version=TRANSFORMATION_VERSION,
    contract_version="1.0",
):
    validate_dataset_name(dataset_name)
    validate_source_snapshots(dataset_name, source_snapshots)

    if not isinstance(transformation_version, str) or not transformation_version.strip():
        raise ValueError("Invalid transformation_version")

    if not isinstance(contract_version, str) or not contract_version.strip():
        raise ValueError("Invalid contract_version")

    identity = {
        "dataset_name": dataset_name,
        "source_snapshots": source_snapshots,
        "transformation_version": transformation_version,
        "contract_version": contract_version,
    }

    canonical = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
    )

    digest = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()

    return f"gold_snap_{digest[:32]}"


def build_gold_snapshot_id_from_lineage(
    dataset_name,
    source_lineage,
    transformation_version=TRANSFORMATION_VERSION,
    contract_version="2.0",
):
    """Build an identity for v2 lineage without coercing Iceberg IDs to v1."""
    validate_dataset_name(dataset_name)
    validate_source_lineage(source_lineage, dataset_name)
    if not isinstance(transformation_version, str) or not transformation_version.strip():
        raise ValueError("Invalid transformation_version")
    if not isinstance(contract_version, str) or not contract_version.strip():
        raise ValueError("Invalid contract_version")
    canonical = json.dumps({
        "dataset_name": dataset_name,
        "source_lineage": json.loads(canonical_lineage(source_lineage)),
        "transformation_version": transformation_version,
        "contract_version": contract_version,
    }, sort_keys=True, separators=(",", ":"))
    return f"gold_snap_{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:32]}"
