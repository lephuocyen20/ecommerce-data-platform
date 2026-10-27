
from silver.snapshots.object_discovery import (
    discover_snapshot_parquet_objects,
)
from common.object_integrity import calculate_object_sha256
from gold.implementation.storage.manifest import (
    validate_gold_parquet_objects,
)


def discover_gold_parquet_objects(
    client,
    bucket,
    data_prefix,
):
    """
    Discover Gold Parquet objects with SHA-256 checksums.

    Returns a deterministic list sorted by object_key.
    """

    objects = discover_snapshot_parquet_objects(
        client=client,
        bucket=bucket,
        data_prefix=data_prefix,
    )

    result = []

    for obj in objects:
        result.append({
            "object_key": obj["object_key"],
            "size": obj["size"],
            "sha256": calculate_object_sha256(
                client=client,
                bucket=bucket,
                object_key=obj["object_key"],
            ),
        })

    validate_gold_parquet_objects(
        result,
        data_prefix,
    )

    return result
