
from minio.error import S3Error

from silver.snapshots.publisher import (
    publish_manifest,
    read_published_manifest,
)
from gold.implementation.storage.verifier import (
    verify_gold_snapshot,
)


class GoldSnapshotPublicationError(RuntimeError):
    pass


def _read_existing_manifest(client, bucket, manifest_key):
    """
    Read manifest if present.

    Return None only when the object does not exist.
    Propagate all other MinIO errors.
    """
    try:
        return read_published_manifest(
            client,
            bucket,
            manifest_key,
        )
    except S3Error as exc:
        if exc.code in ("NoSuchKey", "NoSuchObject"):
            return None
        raise


def _confirm_published_manifest(
    client,
    bucket,
    manifest_key,
    expected_manifest,
):
    """
    Read back the published manifest and compare its content.
    """
    actual = read_published_manifest(
        client,
        bucket,
        manifest_key,
    )

    if actual != expected_manifest:
        raise GoldSnapshotPublicationError(
            "Published Gold manifest content mismatch"
        )

    return True


def publish_gold_snapshot(
    spark,
    client,
    bucket,
    manifest,
):
    """
    Verify and publish an immutable Gold snapshot.

    Publication marker:
        manifest.json

    Returns:
        PUBLISHED
        ALREADY_PUBLISHED

    Never overwrites an existing manifest.
    """

    # 1. Validate metadata and verify real MinIO data.
    verification = verify_gold_snapshot(
        spark=spark,
        client=client,
        bucket=bucket,
        manifest=manifest,
    )

    if verification["status"] != "VERIFIED":
        raise GoldSnapshotPublicationError(
            "Gold snapshot verification did not pass"
        )

    manifest_key = manifest["manifest_key"]

    # 2. Check whether publication already exists.
    existing = _read_existing_manifest(
        client,
        bucket,
        manifest_key,
    )

    if existing is not None:
        if existing != manifest:
            raise GoldSnapshotPublicationError(
                "Conflicting Gold manifest already published"
            )

        return {
            "dataset_name": manifest["dataset_name"],
            "snapshot_id": manifest["snapshot_id"],
            "manifest_key": manifest_key,
            "status": "ALREADY_PUBLISHED",
        }

    # 3. Publish with create-if-absent semantics.
    try:
        publish_manifest(
            client=client,
            bucket=bucket,
            manifest_key=manifest_key,
            manifest=manifest,
        )

    except S3Error as exc:
        # Another publisher may have won the race.
        if exc.code not in (
            "PreconditionFailed",
            "ConditionalRequestConflict",
        ):
            raise

        existing = _read_existing_manifest(
            client,
            bucket,
            manifest_key,
        )

        if existing != manifest:
            raise GoldSnapshotPublicationError(
                "Concurrent Gold publication conflict"
            ) from exc

        _confirm_published_manifest(
            client,
            bucket,
            manifest_key,
            manifest,
        )

        return {
            "dataset_name": manifest["dataset_name"],
            "snapshot_id": manifest["snapshot_id"],
            "manifest_key": manifest_key,
            "status": "ALREADY_PUBLISHED",
        }

    # 4. Confirm publication from MinIO.
    _confirm_published_manifest(
        client,
        bucket,
        manifest_key,
        manifest,
    )

    return {
        "dataset_name": manifest["dataset_name"],
        "snapshot_id": manifest["snapshot_id"],
        "manifest_key": manifest_key,
        "status": "PUBLISHED",
    }
