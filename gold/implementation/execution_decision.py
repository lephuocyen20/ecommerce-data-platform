
"""Pure, read-only Gold execution decision policy."""


class GoldExecutionDecisionError(ValueError):
    pass


def decide_gold_execution(
    dataset_name,
    deterministic_snapshot_id,
    lineage_matches,
    target_inspection,
    preferred_snapshot_id=None,
):
    """
    Produce a conservative execution decision.

    This function performs no storage operations.
    It does not authorize writes, publication, or deletion.
    """

    # ========================================================
    # 1. VALIDATE INPUTS
    # ========================================================

    if not isinstance(lineage_matches, list):
        raise GoldExecutionDecisionError(
            "lineage_matches must be a list"
        )

    if not isinstance(target_inspection, dict):
        raise GoldExecutionDecisionError(
            "target_inspection must be a dictionary"
        )

    if target_inspection.get("snapshot_id") != deterministic_snapshot_id:
        raise GoldExecutionDecisionError(
            "Target inspection identity mismatch"
        )

    if target_inspection.get("dataset_name") != dataset_name:
        raise GoldExecutionDecisionError(
            "Target dataset mismatch"
        )

    state = target_inspection.get("state")

    if state not in {
        "NOT_FOUND",
        "INCOMPLETE",
        "UNPUBLISHED",
        "PUBLISHED",
        "INVALID",
    }:
        raise GoldExecutionDecisionError(
            f"Unsupported target state: {state}"
        )

    # ========================================================
    # 2. VALIDATE LINEAGE MATCHES
    # ========================================================

    matching_ids = [
        item["snapshot_id"]
        for item in lineage_matches
    ]

    if len(matching_ids) != len(set(matching_ids)):
        raise GoldExecutionDecisionError(
            "Duplicate snapshot IDs in lineage discovery"
        )

    # ========================================================
    # 3. REJECT UNSAFE TARGET STATES
    # ========================================================

    if state in {"INVALID", "INCOMPLETE"}:
        return {
            "action": "REJECT",
            "reason": f"TARGET_{state}",
            "can_write": False,
            "can_publish": False,
        }

    # ========================================================
    # 4. REQUIRE RECOVERY FOR UNPUBLISHED TARGET
    # ========================================================

    if state == "UNPUBLISHED":
        return {
            "action": "REQUIRES_RECOVERY",
            "reason": "TARGET_UNPUBLISHED",
            "snapshot_id": deterministic_snapshot_id,
            "can_write": False,
            "can_publish": False,
        }

    # ========================================================
    # 5. PROTECT PUBLISHED TARGET LINEAGE
    # ========================================================

    if (
        state == "PUBLISHED"
        and deterministic_snapshot_id not in matching_ids
    ):
        return {
            "action": "REJECT",
            "reason": "TARGET_PUBLISHED_WITH_DIFFERENT_LINEAGE",
            "can_write": False,
            "can_publish": False,
        }

    # ========================================================
    # 6. EXPLICIT SNAPSHOT SELECTION
    # ========================================================

    if preferred_snapshot_id is not None:
        if preferred_snapshot_id not in matching_ids:
            return {
                "action": "REJECT",
                "reason": "PREFERRED_SNAPSHOT_NOT_IN_LINEAGE",
                "can_write": False,
                "can_publish": False,
            }

        return {
            "action": "VERIFY_EXISTING",
            "reason": "EXPLICIT_SNAPSHOT_SELECTION",
            "snapshot_id": preferred_snapshot_id,
            "can_write": False,
            "can_publish": False,
        }

    # ========================================================
    # 7. REJECT AMBIGUOUS LINEAGE
    # ========================================================

    if len(lineage_matches) > 1:
        return {
            "action": "REJECT",
            "reason": "AMBIGUOUS_LINEAGE",
            "matching_snapshot_ids": sorted(matching_ids),
            "can_write": False,
            "can_publish": False,
        }

    # ========================================================
    # 8. VERIFY EXISTING PUBLISHED SNAPSHOT
    # ========================================================

    if len(lineage_matches) == 1:
        return {
            "action": "VERIFY_EXISTING",
            "reason": "MATCHING_PUBLISHED_LINEAGE",
            "snapshot_id": matching_ids[0],
            "can_write": False,
            "can_publish": False,
        }

    # ========================================================
    # 9. NEW SNAPSHOT CANDIDATE
    # ========================================================

    return {
        "action": "NEW_SNAPSHOT_CANDIDATE",
        "reason": "NO_PUBLISHED_LINEAGE_MATCH",
        "snapshot_id": deterministic_snapshot_id,
        "can_write": False,
        "can_publish": False,
    }
