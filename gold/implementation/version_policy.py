
"""Read-only Gold manifest version compatibility policy."""

from gold.implementation.identity import (
    TRANSFORMATION_VERSION,
)
from gold.implementation.contracts.datasets import (
    GOLD_CONTRACT_VERSION,
)


class GoldVersionPolicyError(ValueError):
    pass


def evaluate_gold_manifest_version(
    manifest,
    expected_transformation_version=TRANSFORMATION_VERSION,
    expected_contract_version=GOLD_CONTRACT_VERSION,
):
    """
    Evaluate whether a Gold manifest is version-compatible.

    No storage operations or mutations are performed.
    """

    if not isinstance(manifest, dict):
        raise GoldVersionPolicyError(
            "Gold manifest must be a dictionary"
        )

    for version in (
        expected_transformation_version,
        expected_contract_version,
    ):
        if not isinstance(version, str) or not version.strip():
            raise GoldVersionPolicyError(
                "Expected versions must be non-empty strings"
            )

    contract_version = manifest.get("contract_version")
    transformation_version = manifest.get(
        "transformation_version"
    )

    if (
        not isinstance(contract_version, str)
        or not contract_version.strip()
    ):
        return {
            "status": "INVALID",
            "reason": "INVALID_CONTRACT_VERSION",
            "can_reuse": False,
        }

    if (
        contract_version != expected_contract_version
    ):
        return {
            "status": "VERSION_MISMATCH",
            "reason": "CONTRACT_VERSION_MISMATCH",
            "can_reuse": False,
        }

    if "transformation_version" not in manifest:
        return {
            "status": "LEGACY_UNKNOWN",
            "reason": "MISSING_TRANSFORMATION_VERSION",
            "can_reuse": False,
        }

    if (
        not isinstance(transformation_version, str)
        or not transformation_version.strip()
    ):
        return {
            "status": "INVALID",
            "reason": "INVALID_TRANSFORMATION_VERSION",
            "can_reuse": False,
        }

    if (
        transformation_version
        != expected_transformation_version
    ):
        return {
            "status": "VERSION_MISMATCH",
            "reason": "TRANSFORMATION_VERSION_MISMATCH",
            "can_reuse": False,
        }

    return {
        "status": "COMPATIBLE",
        "reason": "VERSIONS_MATCH",
        "can_reuse": True,
    }
