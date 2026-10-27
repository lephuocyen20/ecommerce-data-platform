
import re

from gold.implementation.contracts.datasets import (
    get_gold_contract,
)


GOLD_STORAGE_VERSION = "1.0"
GOLD_PREFIX = "gold"
GOLD_SNAPSHOT_PREFIX = "gold_snap_"

SNAPSHOT_ID_PATTERN = re.compile(
    r"^gold_snap_[a-f0-9]{32}$"
)


class GoldStorageContractError(ValueError):
    """Invalid Gold storage contract or snapshot identity."""


def validate_dataset_name(dataset_name):
    if not isinstance(dataset_name, str):
        raise GoldStorageContractError(
            "dataset_name must be a string"
        )

    try:
        get_gold_contract(dataset_name)
    except (KeyError, ValueError, TypeError) as exc:
        raise GoldStorageContractError(
            f"Unsupported Gold dataset: {dataset_name}"
        ) from exc

    return dataset_name


def validate_gold_snapshot_id(snapshot_id):
    if (
        not isinstance(snapshot_id, str)
        or SNAPSHOT_ID_PATTERN.fullmatch(snapshot_id) is None
    ):
        raise GoldStorageContractError(
            f"Invalid Gold snapshot_id: {snapshot_id}"
        )

    return snapshot_id


def get_gold_snapshot_prefix(dataset_name, snapshot_id):
    validate_dataset_name(dataset_name)
    validate_gold_snapshot_id(snapshot_id)

    return (
        f"{GOLD_PREFIX}/{dataset_name}/"
        f"snapshots/{snapshot_id}"
    )


def get_gold_data_prefix(dataset_name, snapshot_id):
    return (
        get_gold_snapshot_prefix(dataset_name, snapshot_id)
        + "/data/"
    )


def get_gold_manifest_key(dataset_name, snapshot_id):
    return (
        get_gold_snapshot_prefix(dataset_name, snapshot_id)
        + "/manifest.json"
    )


def get_gold_storage_paths(dataset_name, snapshot_id):
    return {
        "snapshot_prefix": get_gold_snapshot_prefix(
            dataset_name, snapshot_id
        ),
        "data_prefix": get_gold_data_prefix(
            dataset_name, snapshot_id
        ),
        "manifest_key": get_gold_manifest_key(
            dataset_name, snapshot_id
        ),
    }


def validate_source_snapshots(
    dataset_name,
    source_snapshots,
):
    validate_dataset_name(dataset_name)

    contract = get_gold_contract(dataset_name)
    required_sources = set(contract["sources"])

    if not isinstance(source_snapshots, dict):
        raise GoldStorageContractError(
            "source_snapshots must be a dictionary"
        )

    if set(source_snapshots) != required_sources:
        raise GoldStorageContractError(
            "source_snapshots must match Gold sources"
        )

    for table_name, snapshot_id in source_snapshots.items():
        if (
            not isinstance(snapshot_id, str)
            or not snapshot_id.startswith("snap_")
            or not snapshot_id.strip()
        ):
            raise GoldStorageContractError(
                f"Invalid Silver snapshot for {table_name}"
            )

    return True


def build_gold_storage_metadata(
    dataset_name,
    snapshot_id,
    source_snapshots,
):
    validate_dataset_name(dataset_name)
    validate_gold_snapshot_id(snapshot_id)
    validate_source_snapshots(
        dataset_name,
        source_snapshots,
    )

    contract = get_gold_contract(dataset_name)
    paths = get_gold_storage_paths(
        dataset_name,
        snapshot_id,
    )

    return {
        "manifest_version": GOLD_STORAGE_VERSION,
        "layer": "gold",
        "dataset_name": dataset_name,
        "snapshot_id": snapshot_id,
        "contract_version": "1.0",
        "primary_key": list(contract["primary_key"]),
        "source_snapshots": dict(source_snapshots),
        **paths,
    }
