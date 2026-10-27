
"""Read-only Gold execution preflight."""

from gold.implementation.identity import (
    build_gold_snapshot_id,
)
from gold.implementation.storage.lineage import (
    discover_gold_lineage,
)
from gold.implementation.storage.recovery import (
    inspect_gold_snapshot,
    execute_gold_recovery,
)
from gold.implementation.execution_decision import (
    decide_gold_execution,
)
from gold.implementation.version_policy import (
    evaluate_gold_manifest_version,
)

class GoldPreflightError(RuntimeError):
    pass


def run_gold_preflight(
    spark,
    client,
    bucket,
    dataset_name,
    source_snapshots,
    preferred_snapshot_id=None,
):
    """
    Inspect Gold lineage and verify reusable snapshots.

    No writes, publication, deletion or recovery publication.
    """

    deterministic_id = build_gold_snapshot_id(
        dataset_name=dataset_name,
        source_snapshots=source_snapshots,
    )

    matches = discover_gold_lineage(
        client=client,
        bucket=bucket,
        dataset_name=dataset_name,
        source_snapshots=source_snapshots,
    )

    target = inspect_gold_snapshot(
        client=client,
        bucket=bucket,
        dataset_name=dataset_name,
        snapshot_id=deterministic_id,
    )

    decision = decide_gold_execution(
        dataset_name=dataset_name,
        deterministic_snapshot_id=deterministic_id,
        lineage_matches=matches,
        target_inspection=target,
        preferred_snapshot_id=preferred_snapshot_id,
    )

    result = {
        "dataset_name": dataset_name,
        "deterministic_snapshot_id": deterministic_id,
        "target_state": target["state"],
        "matching_snapshot_ids": [
            item["snapshot_id"] for item in matches
        ],
        "action": decision["action"],
        "reason": decision["reason"],
        "can_write": False,
        "can_publish": False,
    }

    if decision["action"] != "VERIFY_EXISTING":
        result["status"] = (
            "CANDIDATE"
            if decision["action"] == "NEW_SNAPSHOT_CANDIDATE"
            else "BLOCKED"
        )
        return result

    selected_id = decision["snapshot_id"]

    selected_matches = [
        item
        for item in matches
        if item["snapshot_id"] == selected_id
    ]

    if len(selected_matches) != 1:
        raise GoldPreflightError(
            "Selected snapshot lineage metadata is missing or ambiguous"
        )

    selected_manifest = selected_matches[0]["manifest"]

    version_result = evaluate_gold_manifest_version(
        selected_manifest
    )

    result["selected_snapshot_id"] = selected_id
    result["version_status"] = version_result["status"]
    result["version_reason"] = version_result["reason"]

    if not version_result["can_reuse"]:
        result["status"] = "BLOCKED"
        result["reason"] = version_result["reason"]
        return result

    selected_inspection = inspect_gold_snapshot(
        client=client,
        bucket=bucket,
        dataset_name=dataset_name,
        snapshot_id=selected_id,
    )

    if selected_inspection["state"] != "PUBLISHED":
        raise GoldPreflightError(
            f"Selected snapshot is not PUBLISHED: {selected_id}"
        )

    verification = execute_gold_recovery(
        spark=spark,
        client=client,
        bucket=bucket,
        dataset_name=dataset_name,
        snapshot_id=selected_id,
        expected_manifest=selected_manifest,
    )

    if verification["action"] != "VERIFY_PUBLISHED":
        raise GoldPreflightError(
            "Unexpected recovery action"
        )

    if verification["status"] not in {
        "VERIFIED",
        "ALREADY_PUBLISHED",
    }:
        raise GoldPreflightError(
            f"Unexpected verification status: "
            f"{verification['status']}"
        )

    result.update(
        {
            "status": "VERIFIED_EXISTING",
            "selected_snapshot_id": selected_id,
            "verification_status": verification["status"],
            "version_status": "COMPATIBLE",
        }
    )

    return result
