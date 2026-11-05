
"""Gold Preflight Safety Unit Tests."""

from unittest.mock import Mock, patch

from gold.implementation.preflight import (
    GoldPreflightError,
    run_gold_preflight,
)


DATASET = "payment_summary"
SNAPSHOT_ID = "gold_snap_" + "a" * 32


def make_manifest(transformation_version="1.0"):
    return {
        "dataset_name": DATASET,
        "snapshot_id": SNAPSHOT_ID,
        "contract_version": "1.0",
        "transformation_version": transformation_version,
    }


def make_matches(manifest):
    return [{
        "dataset_name": DATASET,
        "snapshot_id": SNAPSHOT_ID,
        "manifest_key": "test/manifest.json",
        "manifest": manifest,
    }]


def run_case(
    manifest,
    selected_state="PUBLISHED",
    verification_status="ALREADY_PUBLISHED",
):
    with (
        patch(
            "gold.implementation.preflight.build_gold_snapshot_id",
            return_value=SNAPSHOT_ID,
        ),
        patch(
            "gold.implementation.preflight.discover_gold_lineage",
            return_value=make_matches(manifest),
        ),
        patch(
            "gold.implementation.preflight.inspect_gold_snapshot",
            side_effect=[
                {"state": "PUBLISHED"},
                {"state": selected_state},
            ],
        ) as inspect_mock,
        patch(
            "gold.implementation.preflight.decide_gold_execution",
            return_value={
                "action": "VERIFY_EXISTING",
                "reason": "EXISTING_LINEAGE",
                "snapshot_id": SNAPSHOT_ID,
            },
        ),
        patch(
            "gold.implementation.preflight.execute_gold_recovery",
            return_value={
                "action": "VERIFY_PUBLISHED",
                "status": verification_status,
            },
        ) as recovery_mock,
    ):
        try:
            result = run_gold_preflight(
                spark=Mock(),
                client=Mock(),
                bucket="test-bucket",
                dataset_name=DATASET,
                source_snapshots={"payments": "snap_test"},
            )
            return result, inspect_mock.call_count, recovery_mock.call_count
        except Exception as exc:
            return exc, inspect_mock.call_count, recovery_mock.call_count


def test_compatible_verified():
    result, inspections, recoveries = run_case(
        make_manifest()
    )

    assert result["status"] == "VERIFIED_EXISTING"
    assert result["version_status"] == "COMPATIBLE"
    assert result["can_write"] is False
    assert result["can_publish"] is False
    assert inspections == 2
    assert recoveries == 1


def test_legacy_blocked():
    manifest = make_manifest()
    del manifest["transformation_version"]

    result, inspections, recoveries = run_case(manifest)

    assert result["status"] == "BLOCKED"
    assert result["version_status"] == "LEGACY_UNKNOWN"
    assert result["can_write"] is False
    assert result["can_publish"] is False
    assert inspections == 1
    assert recoveries == 0


def test_version_mismatch_blocked():
    result, inspections, recoveries = run_case(
        make_manifest("2.0")
    )

    assert result["status"] == "BLOCKED"
    assert result["version_status"] == "VERSION_MISMATCH"
    assert result["can_write"] is False
    assert result["can_publish"] is False
    assert inspections == 1
    assert recoveries == 0


def test_selected_not_published():
    result, inspections, recoveries = run_case(
        make_manifest(),
        selected_state="UNPUBLISHED",
    )

    assert isinstance(result, GoldPreflightError)
    assert inspections == 2
    assert recoveries == 0


def test_verification_failure():
    result, inspections, recoveries = run_case(
        make_manifest(),
        verification_status="FAILED",
    )

    assert isinstance(result, GoldPreflightError)
    assert inspections == 2
    assert recoveries == 1


def test_invalid_version_blocked():
    result, inspections, recoveries = run_case(
        make_manifest(""),
    )

    assert result["status"] == "BLOCKED"
    assert result["version_status"] == "INVALID"
    assert result["can_write"] is False
    assert result["can_publish"] is False
    assert inspections == 1
    assert recoveries == 0


def main():
    tests = [
        test_compatible_verified,
        test_legacy_blocked,
        test_version_mismatch_blocked,
        test_selected_not_published,
        test_verification_failure,
        test_invalid_version_blocked,
    ]

    for test in tests:
        test()
        print(f"{test.__name__}: PASS")

    print(
        f"GOLD PREFLIGHT SAFETY: PASS {len(tests)}/{len(tests)}"
    )


if __name__ == "__main__":
    main()
