
"""Unit tests for Gold manifest version compatibility."""

from gold.implementation.version_policy import (
    evaluate_gold_manifest_version,
    GoldVersionPolicyError,
)


def test_compatible():
    result = evaluate_gold_manifest_version({
        "contract_version": "1.0",
        "transformation_version": "1.0",
    })

    assert result["status"] == "COMPATIBLE"
    assert result["can_reuse"] is True


def test_legacy_unknown():
    result = evaluate_gold_manifest_version({
        "contract_version": "1.0",
    })

    assert result["status"] == "LEGACY_UNKNOWN"
    assert result["can_reuse"] is False


def test_contract_mismatch():
    result = evaluate_gold_manifest_version({
        "contract_version": "2.0",
        "transformation_version": "1.0",
    })

    assert result["status"] == "VERSION_MISMATCH"
    assert result["can_reuse"] is False


def test_transformation_mismatch():
    result = evaluate_gold_manifest_version({
        "contract_version": "1.0",
        "transformation_version": "2.0",
    })

    assert result["status"] == "VERSION_MISMATCH"
    assert result["can_reuse"] is False


def test_invalid_contract():
    result = evaluate_gold_manifest_version({
        "contract_version": None,
        "transformation_version": "1.0",
    })

    assert result["status"] == "INVALID"
    assert result["can_reuse"] is False


def test_invalid_transformation():
    result = evaluate_gold_manifest_version({
        "contract_version": "1.0",
        "transformation_version": "",
    })

    assert result["status"] == "INVALID"
    assert result["can_reuse"] is False


def test_invalid_manifest():
    try:
        evaluate_gold_manifest_version(None)
    except GoldVersionPolicyError:
        return

    raise AssertionError(
        "Invalid manifest must raise GoldVersionPolicyError"
    )


def main():
    tests = [
        test_compatible,
        test_legacy_unknown,
        test_contract_mismatch,
        test_transformation_mismatch,
        test_invalid_contract,
        test_invalid_transformation,
        test_invalid_manifest,
    ]

    for test in tests:
        test()
        print(f"{test.__name__}: PASS")

    print(
        f"GOLD VERSION POLICY: PASS {len(tests)}/{len(tests)}"
    )


if __name__ == "__main__":
    main()
