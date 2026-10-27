
"""Read-only discovery of published Gold snapshots by lineage."""

from gold.implementation.storage.contract import (
    validate_dataset_name,
    validate_source_snapshots,
    get_gold_storage_paths,
)
from gold.implementation.storage.publisher import (
    read_published_manifest,
)


class GoldLineageConflictError(RuntimeError):
    pass


def discover_gold_lineage(
    client,
    bucket,
    dataset_name,
    source_snapshots,
):
    """
    Discover published Gold snapshots with matching Silver lineage.

    Read-only:
        - No writes
        - No deletes
        - No publication

    Returns matching published manifests.
    """
    validate_dataset_name(dataset_name)
    validate_source_snapshots(dataset_name, source_snapshots)

    prefix = f"gold/{dataset_name}/snapshots/"
    matches = []

    for obj in client.list_objects(
        bucket,
        prefix=prefix,
        recursive=True,
    ):
        object_key = obj.object_name

        if not object_key.endswith("/manifest.json"):
            continue

        snapshot_id = object_key.split("/")[-2]

        expected_paths = get_gold_storage_paths(
            dataset_name,
            snapshot_id,
        )

        if object_key != expected_paths["manifest_key"]:
            continue

        manifest = read_published_manifest(
            client=client,
            bucket=bucket,
            manifest_key=object_key,
        )

        if not isinstance(manifest, dict):
            raise GoldLineageConflictError(
                f"Invalid published manifest: {object_key}"
            )

        if (
            manifest.get("dataset_name") != dataset_name
            or manifest.get("snapshot_id") != snapshot_id
            or manifest.get("manifest_key") != object_key
        ):
            raise GoldLineageConflictError(
                f"Manifest identity mismatch: {object_key}"
            )

        if manifest.get("source_snapshots") == source_snapshots:
            matches.append(
                {
                    "dataset_name": dataset_name,
                    "snapshot_id": snapshot_id,
                    "manifest_key": object_key,
                    "manifest": manifest,
                }
            )

    return sorted(
        matches,
        key=lambda item: item["snapshot_id"],
    )
