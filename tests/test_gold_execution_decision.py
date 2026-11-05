
from gold.implementation.execution_decision import (
    decide_gold_execution,
)


DATASET = "daily_sales"
TARGET = "gold_snap_" + "a" * 32


def inspection(state):
    return {
        "dataset_name": DATASET,
        "snapshot_id": TARGET,
        "state": state,
    }


def match(snapshot_id):
    return {"snapshot_id": snapshot_id}


def test_new_candidate():
    result = decide_gold_execution(
        DATASET, TARGET, [], inspection("NOT_FOUND")
    )
    assert result["action"] == "NEW_SNAPSHOT_CANDIDATE"
    assert result["can_write"] is False


def test_existing():
    existing = "gold_snap_" + "b" * 32
    result = decide_gold_execution(
        DATASET,
        TARGET,
        [match(existing)],
        inspection("NOT_FOUND"),
    )
    assert result["action"] == "VERIFY_EXISTING"
    assert result["snapshot_id"] == existing


def test_ambiguous():
    result = decide_gold_execution(
        DATASET,
        TARGET,
        [
            match("gold_snap_" + "b" * 32),
            match("gold_snap_" + "c" * 32),
        ],
        inspection("NOT_FOUND"),
    )
    assert result["action"] == "REJECT"
    assert result["reason"] == "AMBIGUOUS_LINEAGE"


def test_unpublished():
    result = decide_gold_execution(
        DATASET, TARGET, [], inspection("UNPUBLISHED")
    )
    assert result["action"] == "REQUIRES_RECOVERY"


def test_invalid():
    result = decide_gold_execution(
        DATASET, TARGET, [], inspection("INVALID")
    )
    assert result["action"] == "REJECT"


def test_explicit_selection():
    existing = "gold_snap_" + "b" * 32
    result = decide_gold_execution(
        DATASET,
        TARGET,
        [match(existing), match("gold_snap_" + "c" * 32)],
        inspection("NOT_FOUND"),
        preferred_snapshot_id=existing,
    )
    assert result["action"] == "VERIFY_EXISTING"
    assert result["snapshot_id"] == existing


def test_preferred_cannot_bypass_invalid_target():
    existing = "gold_snap_" + "b" * 32

    result = decide_gold_execution(
        DATASET,
        TARGET,
        [match(existing)],
        inspection("INVALID"),
        preferred_snapshot_id=existing,
    )

    assert result["action"] == "REJECT"
    assert result["reason"] == "TARGET_INVALID"


def test_preferred_cannot_bypass_incomplete_target():
    existing = "gold_snap_" + "b" * 32

    result = decide_gold_execution(
        DATASET,
        TARGET,
        [match(existing)],
        inspection("INCOMPLETE"),
        preferred_snapshot_id=existing,
    )

    assert result["action"] == "REJECT"
    assert result["reason"] == "TARGET_INCOMPLETE"


def test_preferred_cannot_bypass_unpublished_target():
    existing = "gold_snap_" + "b" * 32

    result = decide_gold_execution(
        DATASET,
        TARGET,
        [match(existing)],
        inspection("UNPUBLISHED"),
        preferred_snapshot_id=existing,
    )

    assert result["action"] == "REQUIRES_RECOVERY"


def test_published_target_with_different_lineage():
    existing = "gold_snap_" + "b" * 32

    result = decide_gold_execution(
        DATASET,
        TARGET,
        [match(existing)],
        inspection("PUBLISHED"),
        preferred_snapshot_id=existing,
    )

    assert result["action"] == "REJECT"
    assert result["reason"] == (
        "TARGET_PUBLISHED_WITH_DIFFERENT_LINEAGE"
    )


if __name__ == "__main__":
    tests = [
        test_new_candidate,
        test_existing,
        test_ambiguous,
        test_unpublished,
        test_invalid,
        test_explicit_selection,
        test_preferred_cannot_bypass_invalid_target,
        test_preferred_cannot_bypass_incomplete_target,
        test_preferred_cannot_bypass_unpublished_target,
        test_published_target_with_different_lineage,
    ]

    for test in tests:
        test()
        print(f"{test.__name__}: PASS")

    print(f"GOLD EXECUTION DECISION: PASS {len(tests)}/{len(tests)}")
