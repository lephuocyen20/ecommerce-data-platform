
"""Gold Manifest Consistency Guard unit tests."""

from unittest.mock import Mock, patch

from gold.implementation.storage.recovery import (
    GoldRecoveryExecutionError,
    execute_gold_recovery,
)


DATASET = "payment_summary"
SNAPSHOT_ID = "gold_snap_" + "a" * 32
MANIFEST_KEY = (
    f"gold/{DATASET}/snapshots/"
    f"{SNAPSHOT_ID}/manifest.json"
)


def make_manifest():
    return {
        "dataset_name": DATASET,
        "snapshot_id": SNAPSHOT_ID,
        "manifest_key": MANIFEST_KEY,
        "contract_version": "1.0",
        "transformation_version": "1.0",
        "source_snapshots": {
            "payments": "snap_test",
        },
        "parquet_objects": [{
            "object_key": "test.parquet",
            "size": 100,
            "sha256": "a" * 64,
        }],
    }


def run_case(actual, expected, read_error=None):
    inspection = {
        "dataset_name": DATASET,
        "snapshot_id": SNAPSHOT_ID,
        "bucket": "test-bucket",
        "state": "PUBLISHED",
        "snapshot_prefix": (
            f"gold/{DATASET}/snapshots/{SNAPSHOT_ID}"
        ),
        "data_prefix": (
            f"gold/{DATASET}/snapshots/{SNAPSHOT_ID}/data/"
        ),
        "manifest_key": MANIFEST_KEY,
        "object_count": 3,
        "parquet_file_count": 1,
        "has_manifest": True,
        "has_success_marker": True,
        "parquet_keys": ["test.parquet"],
        "unexpected_keys": [],
    }

    with (
        patch(
            "gold.implementation.storage.recovery."
            "inspect_gold_snapshot",
            return_value=inspection,
        ),
        patch(
            "gold.implementation.storage.recovery."
            "read_published_manifest",
            side_effect=read_error if read_error else None,
            return_value=actual,
        ),
        patch(
            "gold.implementation.storage.recovery."
            "verify_gold_snapshot",
            return_value={"status": "VERIFIED"},
        ) as verifier,
    ):
        try:
            result = execute_gold_recovery(
                spark=Mock(),
                client=Mock(),
                bucket="test-bucket",
                dataset_name=DATASET,
                snapshot_id=SNAPSHOT_ID,
                expected_manifest=expected,
            )
            return result, verifier.call_count
        except Exception as exc:
            return exc, verifier.call_count


def test_identical_manifest():
    manifest = make_manifest()

    result, calls = run_case(manifest, manifest.copy())

    assert result["status"] == "ALREADY_PUBLISHED"
    assert calls == 1


def test_version_changed():
    expected = make_manifest()
    actual = make_manifest()
    actual["transformation_version"] = "2.0"

    result, calls = run_case(actual, expected)

    assert isinstance(result, GoldRecoveryExecutionError)
    assert calls == 0


def test_checksum_changed():
    expected = make_manifest()
    actual = make_manifest()
    actual["parquet_objects"][0]["sha256"] = "b" * 64

    result, calls = run_case(actual, expected)

    assert isinstance(result, GoldRecoveryExecutionError)
    assert calls == 0


def test_lineage_changed():
    expected = make_manifest()
    actual = make_manifest()
    actual["source_snapshots"] = {
        "payments": "snap_changed",
    }

    result, calls = run_case(actual, expected)

    assert isinstance(result, GoldRecoveryExecutionError)
    assert calls == 0


def test_manifest_read_failure():
    expected = make_manifest()

    result, calls = run_case(
        actual=None,
        expected=expected,
        read_error=RuntimeError("MinIO read failed"),
    )

    assert isinstance(result, RuntimeError)
    assert calls == 0


def test_invalid_expected_manifest():
    result, calls = run_case(
        actual=make_manifest(),
        expected="invalid",
    )

    assert isinstance(result, GoldRecoveryExecutionError)
    assert calls == 0


def test_legacy_caller():
    manifest = make_manifest()

    result, calls = run_case(
        actual=manifest,
        expected=None,
    )

    assert result["status"] == "ALREADY_PUBLISHED"
    assert calls == 1


def main():
    tests = [
        test_identical_manifest,
        test_version_changed,
        test_checksum_changed,
        test_lineage_changed,
        test_manifest_read_failure,
        test_invalid_expected_manifest,
        test_legacy_caller,
    ]

    for test in tests:
        test()
        print(f"{test.__name__}: PASS")

    print(
        f"GOLD MANIFEST CONSISTENCY: "
        f"PASS {len(tests)}/{len(tests)}"
    )


if __name__ == "__main__":
    main()
