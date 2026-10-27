
from datetime import datetime, timezone
import re

from gold.implementation.contracts.datasets import (
    get_gold_contract,
)
from gold.implementation.storage.contract import (
    GOLD_STORAGE_VERSION,
    GoldStorageContractError,
    build_gold_storage_metadata,
)
from gold.implementation.identity import (
    TRANSFORMATION_VERSION,
)

SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")


def validate_gold_parquet_objects(
    parquet_objects,
    data_prefix,
):
    """
    Validate deterministic Parquet object metadata.

    Each object must contain:
      object_key, size, sha256
    """

    if not isinstance(parquet_objects, list):
        raise GoldStorageContractError(
            "parquet_objects must be a list"
        )

    if not parquet_objects:
        raise GoldStorageContractError(
            "Gold snapshot has no Parquet objects"
        )

    object_keys = []

    for obj in parquet_objects:
        if not isinstance(obj, dict):
            raise GoldStorageContractError(
                "Invalid Parquet object metadata"
            )

        if set(obj) != {
            "object_key",
            "size",
            "sha256",
        }:
            raise GoldStorageContractError(
                "Unexpected Parquet object fields"
            )

        object_key = obj["object_key"]
        size = obj["size"]
        sha256 = obj["sha256"]

        if (
            not isinstance(object_key, str)
            or not object_key.startswith(data_prefix)
            or not object_key.endswith(".parquet")
        ):
            raise GoldStorageContractError(
                "Invalid Gold Parquet object key"
            )

        if (
            not isinstance(size, int)
            or isinstance(size, bool)
            or size <= 0
        ):
            raise GoldStorageContractError(
                "Invalid Gold Parquet object size"
            )

        if (
            not isinstance(sha256, str)
            or SHA256_PATTERN.fullmatch(sha256) is None
        ):
            raise GoldStorageContractError(
                "Invalid Gold Parquet SHA-256"
            )

        object_keys.append(object_key)

    if len(object_keys) != len(set(object_keys)):
        raise GoldStorageContractError(
            "Duplicate Gold Parquet object keys"
        )

    if object_keys != sorted(object_keys):
        raise GoldStorageContractError(
            "Gold Parquet objects must be sorted"
        )

    return True


def validate_gold_manifest_schema(
    dataset_name,
    schema,
):
    """
    Validate schema against the Gold dataset contract.
    """

    contract = get_gold_contract(dataset_name)

    if not isinstance(schema, dict):
        raise GoldStorageContractError(
            "Gold schema must be a dictionary"
        )

    if schema != contract["columns"]:
        raise GoldStorageContractError(
            "Gold manifest schema mismatch"
        )

    return True


def build_gold_manifest(
    dataset_name,
    snapshot_id,
    source_snapshots,
    row_count,
    schema,
    parquet_objects,
    created_at=None,
    transformation_version=TRANSFORMATION_VERSION,
):
    """
    Build immutable Gold snapshot manifest metadata.

    Does not write data or publish the manifest.
    """

    metadata = build_gold_storage_metadata(
        dataset_name=dataset_name,
        snapshot_id=snapshot_id,
        source_snapshots=source_snapshots,
    )

    if (
        not isinstance(row_count, int)
        or isinstance(row_count, bool)
        or row_count < 0
    ):
        raise GoldStorageContractError(
            "Invalid Gold row count"
        )

    validate_gold_manifest_schema(
        dataset_name,
        schema,
    )

    validate_gold_parquet_objects(
        parquet_objects,
        metadata["data_prefix"],
    )

    if created_at is None:
        created_at = datetime.now(
            timezone.utc
        ).isoformat()
    else:
        if not isinstance(created_at, str):
            raise GoldStorageContractError(
                "Invalid Gold created_at"
            )

        try:
            parsed = datetime.fromisoformat(created_at)
        except ValueError as exc:
            raise GoldStorageContractError(
                "Invalid Gold created_at"
            ) from exc

        if parsed.tzinfo is None:
            raise GoldStorageContractError(
                "Gold created_at must include timezone"
            )

    if (
        not isinstance(transformation_version, str)
        or not transformation_version.strip()
    ):
        raise GoldStorageContractError(
            "Invalid Gold transformation_version"
        )


    return {
        **metadata,
        "transformation_version": transformation_version,
        "status": "COMPLETED",
        "row_count": row_count,
        "schema": dict(schema),
        "parquet_objects": [
            dict(obj) for obj in parquet_objects
        ],
        "parquet_file_count": len(parquet_objects),
        "created_at": created_at,
        
    }
