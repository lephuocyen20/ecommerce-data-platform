from ingestion.incremental.config import get_table_config
from silver.snapshots.publisher import (
    get_snapshot_manifest_key,
)
from common.paths import silver_snapshot_paths


def inspect_snapshot_state(
    client,
    bucket,
    table_name,
    snapshot_id,
):
    """
    Inspect persisted Silver snapshot objects.

    Read-only operation.
    Does not verify Parquet contents or manifest integrity.
    """

    get_table_config(table_name)

    if not isinstance(bucket, str) or not bucket:
        raise ValueError("Invalid snapshot bucket")

    manifest_key = get_snapshot_manifest_key(
        table_name=table_name,
        snapshot_id=snapshot_id,
    )

    paths = silver_snapshot_paths(table_name, snapshot_id)
    snapshot_prefix = paths["snapshot_prefix"]
    data_prefix = paths["data_prefix"]

    object_keys = [
        obj.object_name
        for obj in client.list_objects(
            bucket,
            prefix=snapshot_prefix,
            recursive=True,
        )
    ]

    if len(object_keys) != len(set(object_keys)):
        raise ValueError("Duplicate snapshot object keys")

    manifest_exists = manifest_key in object_keys

    if manifest_exists:
        state = "PUBLISHED"

    elif not object_keys:
        state = "NEW"

    else:
        parquet_keys = [
            key
            for key in object_keys
            if (
                key.startswith(data_prefix)
                and key.endswith(".parquet")
            )
        ]

        success_key = f"{data_prefix}_SUCCESS"

        if parquet_keys and success_key in object_keys:
            state = "UNPUBLISHED"
        else:
            state = "INCOMPLETE"

    return {
        "snapshot_id": snapshot_id,
        "table_name": table_name,
        "state": state,
        "snapshot_prefix": snapshot_prefix,
        "data_prefix": data_prefix,
        "manifest_key": manifest_key,
        "object_count": len(object_keys),
    }
