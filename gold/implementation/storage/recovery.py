
from gold.implementation.storage.contract import (
    get_gold_storage_paths,
)


class GoldRecoveryInspectionError(RuntimeError):
    """Unable to inspect Gold snapshot storage state."""


def inspect_gold_snapshot(
    client,
    bucket,
    dataset_name,
    snapshot_id,
):
    """
    Inspect Gold snapshot objects without modifying MinIO.

    This function does not validate Parquet contents,
    checksums, schema, or manifest metadata.
    """

    if not isinstance(bucket, str) or not bucket.strip():
        raise ValueError("bucket must be a non-empty string")

    paths = get_gold_storage_paths(
        dataset_name,
        snapshot_id,
    )

    snapshot_prefix = paths["snapshot_prefix"]
    data_prefix = paths["data_prefix"]
    manifest_key = paths["manifest_key"]

    listing_prefix = snapshot_prefix + "/"

    try:
        objects = list(
            client.list_objects(
                bucket,
                prefix=listing_prefix,
                recursive=True,
            )
        )
    except Exception as exc:
        raise GoldRecoveryInspectionError(
            "Failed to inspect Gold snapshot objects"
        ) from exc

    object_keys = {
        obj.object_name
        for obj in objects
    }

    parquet_keys = sorted(
        key
        for key in object_keys
        if (
            key.startswith(data_prefix)
            and key.endswith(".parquet")
        )
    )

    success_key = data_prefix + "_SUCCESS"

    has_manifest = manifest_key in object_keys
    has_success_marker = success_key in object_keys
    has_parquet = bool(parquet_keys)

    unexpected_keys = sorted(
        key
        for key in object_keys
        if not (
            key == manifest_key
            or key.startswith(data_prefix)
        )
    )

    if not object_keys:
        state = "NOT_FOUND"

    elif unexpected_keys:
        state = "INVALID"

    elif has_manifest:
        if has_parquet and has_success_marker:
            state = "PUBLISHED"
        else:
            state = "INVALID"

    elif has_parquet and has_success_marker:
        state = "UNPUBLISHED"

    else:
        state = "INCOMPLETE"

    return {
        "dataset_name": dataset_name,
        "snapshot_id": snapshot_id,
        "bucket": bucket,
        "state": state,
        "snapshot_prefix": snapshot_prefix,
        "data_prefix": data_prefix,
        "manifest_key": manifest_key,
        "object_count": len(object_keys),
        "parquet_file_count": len(parquet_keys),
        "has_manifest": has_manifest,
        "has_success_marker": has_success_marker,
        "parquet_keys": parquet_keys,
        "unexpected_keys": unexpected_keys,
    }

class GoldRecoveryDecisionError(ValueError):
    """Invalid Gold recovery inspection or decision."""


RECOVERY_ACTIONS = {
    "NOT_FOUND": "NEW_SNAPSHOT",
    "INCOMPLETE": "REJECT",
    "UNPUBLISHED": "VERIFY_AND_PUBLISH",
    "PUBLISHED": "VERIFY_PUBLISHED",
    "INVALID": "REJECT",
}


def decide_gold_recovery(inspection):
    """
    Select a recovery action from an inspection result.

    This function is pure:
        - No MinIO access
        - No Spark operations
        - No writes or deletes
        - No publication

    The returned action is not authorization to bypass
    Gold snapshot verification.
    """

    if not isinstance(inspection, dict):
        raise GoldRecoveryDecisionError(
            "inspection must be a dictionary"
        )

    required_fields = {
        "dataset_name",
        "snapshot_id",
        "bucket",
        "state",
        "snapshot_prefix",
        "data_prefix",
        "manifest_key",
        "object_count",
        "parquet_file_count",
        "has_manifest",
        "has_success_marker",
        "parquet_keys",
        "unexpected_keys",
    }

    missing_fields = required_fields - set(inspection)

    if missing_fields:
        raise GoldRecoveryDecisionError(
            f"Missing inspection fields: {sorted(missing_fields)}"
        )

    state = inspection["state"]

    if state not in RECOVERY_ACTIONS:
        raise GoldRecoveryDecisionError(
            f"Unsupported Gold recovery state: {state}"
        )

    action = RECOVERY_ACTIONS[state]

    return {
        "dataset_name": inspection["dataset_name"],
        "snapshot_id": inspection["snapshot_id"],
        "bucket": inspection["bucket"],
        "state": state,
        "action": action,
        "requires_verification": action in {
            "VERIFY_AND_PUBLISH",
            "VERIFY_PUBLISHED",
        },
        "can_publish": False,
        "can_overwrite": False,
        "can_delete": False,
    }



from gold.implementation.storage.publisher import (
    publish_gold_snapshot,
    read_published_manifest,
)
from gold.implementation.storage.verifier import (
    verify_gold_snapshot,
)


class GoldRecoveryExecutionError(RuntimeError):
    """Gold recovery execution was rejected or failed."""


def execute_gold_recovery(
    spark,
    client,
    bucket,
    dataset_name,
    snapshot_id,
    candidate_manifest=None,
    expected_manifest=None,
):
    """
    Execute a safe Gold snapshot recovery.

    Never overwrites or deletes snapshot objects.
    """

    inspection = inspect_gold_snapshot(
        client=client,
        bucket=bucket,
        dataset_name=dataset_name,
        snapshot_id=snapshot_id,
    )

    decision = decide_gold_recovery(inspection)
    action = decision["action"]

    if action == "NEW_SNAPSHOT":
        return {
            "dataset_name": dataset_name,
            "snapshot_id": snapshot_id,
            "action": action,
            "status": "NOT_FOUND",
        }

    if action == "REJECT":
        raise GoldRecoveryExecutionError(
            f"Recovery rejected: {inspection['state']}"
        )

    if action == "VERIFY_AND_PUBLISH":
        if candidate_manifest is None:
            raise GoldRecoveryExecutionError(
                "Candidate manifest is required"
            )

        if not isinstance(candidate_manifest, dict):
            raise GoldRecoveryExecutionError(
                "Candidate manifest must be a dictionary"
            )

        expected_paths = get_gold_storage_paths(
            dataset_name,
            snapshot_id,
        )

        if (
            candidate_manifest.get("dataset_name") != dataset_name
            or candidate_manifest.get("snapshot_id") != snapshot_id
            or candidate_manifest.get("manifest_key")
            != expected_paths["manifest_key"]
            or candidate_manifest.get("snapshot_prefix")
            != expected_paths["snapshot_prefix"]
            or candidate_manifest.get("data_prefix")
            != expected_paths["data_prefix"]
        ):
            raise GoldRecoveryExecutionError(
                "Candidate manifest storage identity mismatch"
            )

        latest_inspection = inspect_gold_snapshot(
            client=client,
            bucket=bucket,
            dataset_name=dataset_name,
            snapshot_id=snapshot_id,
        )

        if latest_inspection["state"] != "UNPUBLISHED":
            raise GoldRecoveryExecutionError(
                "Gold snapshot state changed during recovery"
            )


        result = publish_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=candidate_manifest,
        )

        return {
            "dataset_name": dataset_name,
            "snapshot_id": snapshot_id,
            "action": action,
            "status": result["status"],
        }

    
    if action == "VERIFY_PUBLISHED":
        manifest = read_published_manifest(
            client=client,
            bucket=bucket,
            manifest_key=inspection["manifest_key"],
        )

        if not isinstance(manifest, dict):
            raise GoldRecoveryExecutionError(
                "Published manifest must be a dictionary"
            )

        # ====================================================
        # 1. MANIFEST CONSISTENCY GUARD
        # ====================================================

        if expected_manifest is not None:
            if not isinstance(expected_manifest, dict):
                raise GoldRecoveryExecutionError(
                    "Expected manifest must be a dictionary"
                )

            if manifest != expected_manifest:
                raise GoldRecoveryExecutionError(
                    "Published Gold manifest changed "
                    "between preflight and verification"
                )

        # ====================================================
        # 2. VALIDATE PUBLISHED MANIFEST IDENTITY
        # ====================================================

        if (
            manifest.get("dataset_name") != dataset_name
            or manifest.get("snapshot_id") != snapshot_id
            or manifest.get("manifest_key")
            != inspection["manifest_key"]
        ):
            raise GoldRecoveryExecutionError(
                "Published manifest identity mismatch"
            )

        # ====================================================
        # 3. VERIFY GOLD SNAPSHOT
        # ====================================================

        verification = verify_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        if verification["status"] != "VERIFIED":
            raise GoldRecoveryExecutionError(
                "Published Gold snapshot verification failed"
            )

        return {
            "dataset_name": dataset_name,
            "snapshot_id": snapshot_id,
            "action": action,
            "status": "ALREADY_PUBLISHED",
        }

    raise GoldRecoveryExecutionError(
        f"Unsupported recovery action: {action}"
    )
