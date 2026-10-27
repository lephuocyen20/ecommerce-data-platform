
"""Gold execution fencing contract.

Phase B.4.9.3a:
- Execution attempt identity
- Isolated staging path
- Fencing token validation
- Pure publication eligibility checks

No database, Spark, or MinIO operations.
"""

import re
import uuid
from dataclasses import dataclass

from gold.implementation.storage.contract import (
    validate_dataset_name,
    validate_gold_snapshot_id,
)


ATTEMPT_ID_PATTERN = re.compile(
    r"^attempt_[0-9a-f]{32}$"
)


class GoldFencingError(RuntimeError):
    """Invalid fencing contract or execution state."""


@dataclass(frozen=True)
class GoldExecutionAttempt:
    dataset_name: str
    snapshot_id: str
    attempt_id: str
    fencing_token: int

    def __post_init__(self):
        validate_dataset_name(self.dataset_name)
        validate_gold_snapshot_id(self.snapshot_id)

        if not isinstance(self.attempt_id, str):
            raise GoldFencingError(
                "Attempt ID must be a string"
            )

        if not ATTEMPT_ID_PATTERN.fullmatch(
            self.attempt_id
        ):
            raise GoldFencingError(
                "Invalid Gold execution attempt ID"
            )

        if (
            isinstance(self.fencing_token, bool)
            or not isinstance(self.fencing_token, int)
            or self.fencing_token <= 0
            or self.fencing_token > 9223372036854775807
        ):
            raise GoldFencingError(
                "Fencing token must be a positive signed BIGINT"
            )

    @property
    def staging_prefix(self):
        return (
            f"gold/{self.dataset_name}/staging/"
            f"{self.snapshot_id}/{self.attempt_id}/"
        )

    @property
    def data_prefix(self):
        return f"{self.staging_prefix}data/"


def generate_gold_attempt_id():
    """Generate a unique execution attempt identifier."""
    return f"attempt_{uuid.uuid4().hex}"


def validate_publication_eligibility(
    attempt,
    current_fencing_token,
    ownership_alive,
):
    """Pure eligibility check; does not authorize publication.

    Actual publication requires an atomic, durable ownership
    check at the commit boundary, implemented separately.
    """

    if not isinstance(attempt, GoldExecutionAttempt):
        raise GoldFencingError(
            "Invalid Gold execution attempt"
        )

    if ownership_alive is not True:
        raise GoldFencingError(
            "Gold ownership is not confirmed alive"
        )

    if (
        isinstance(current_fencing_token, bool)
        or not isinstance(current_fencing_token, int)
        or current_fencing_token <= 0
        or current_fencing_token > 9223372036854775807
    ):
        raise GoldFencingError(
            "Invalid current fencing token"
        )

    if attempt.fencing_token != current_fencing_token:
        raise GoldFencingError(
            "Stale Gold execution attempt rejected"
        )

    return {
        "status": "ELIGIBLE_FOR_COMMIT_CHECK",
        "dataset_name": attempt.dataset_name,
        "snapshot_id": attempt.snapshot_id,
        "attempt_id": attempt.attempt_id,
        "fencing_token": attempt.fencing_token,
        "can_write": False,
        "can_publish": False,
    }
