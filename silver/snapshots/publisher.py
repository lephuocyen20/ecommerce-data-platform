from minio.error import S3Error

from silver.snapshots.verifier import (
    verify_snapshot,
)
from common.manifest import (
    put_json_object_if_absent,
    read_json_object,
)
from common.paths import silver_snapshot_paths


def publish_manifest(client, bucket, manifest_key, manifest):
    """
    Publish manifest using create-if-absent semantics.
    """

    return put_json_object_if_absent(client, bucket, manifest_key, manifest)


def get_snapshot_manifest_key(table_name, snapshot_id):
    """
    Return the immutable snapshot manifest location.
    """

    if not isinstance(table_name, str) or not table_name:
        raise ValueError("Invalid snapshot table")

    if (
        not isinstance(snapshot_id, str)
        or not snapshot_id
        or not all(
            char.isalnum() or char in "-_"
            for char in snapshot_id
        )
    ):
        raise ValueError("Invalid snapshot ID")

    return silver_snapshot_paths(table_name, snapshot_id)["manifest_key"]


def read_published_manifest(client, bucket, manifest_key):
    """
    Read an existing snapshot manifest from MinIO.
    """

    return read_json_object(client, bucket, manifest_key)


def publish_snapshot(
    spark,
    client,
    bucket,
    manifest,
):
    """
    Verify and publish an immutable Silver snapshot.

    Retry behavior:
    - New manifest: publish.
    - Existing identical manifest: idempotent success.
    - Existing different manifest: reject.
    - Other MinIO errors: propagate.
    """

    if not isinstance(manifest, dict):
        raise ValueError("Invalid snapshot manifest")

    table_name = manifest.get("table_name")
    snapshot_id = manifest.get("snapshot_id")

    manifest_key = get_snapshot_manifest_key(
        table_name=table_name,
        snapshot_id=snapshot_id,
    )

    # Always verify snapshot data before publication.
    verify_snapshot(
        spark=spark,
        client=client,
        bucket=bucket,
        manifest=manifest,
    )

    publication_status = "PUBLISHED"

    try:
        publish_manifest(
            client=client,
            bucket=bucket,
            manifest_key=manifest_key,
            manifest=manifest,
        )

    except S3Error as exc:
        if exc.code != "PreconditionFailed":
            raise

        existing_manifest = read_published_manifest(
            client=client,
            bucket=bucket,
            manifest_key=manifest_key,
        )

        if existing_manifest != manifest:
            raise ValueError(
                "Snapshot manifest conflict: "
                "existing manifest differs from requested manifest"
            ) from exc

        publication_status = "ALREADY_PUBLISHED"

    return {
        "snapshot_id": snapshot_id,
        "table_name": table_name,
        "manifest_key": manifest_key,
        "status": "COMPLETED",
        "publication_status": publication_status,
    }
