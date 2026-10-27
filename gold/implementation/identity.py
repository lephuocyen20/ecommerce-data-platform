
"""Deterministic Gold snapshot identity."""

import hashlib
import json

from gold.implementation.storage.contract import (
    validate_dataset_name,
    validate_source_snapshots,
)


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
