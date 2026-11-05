
"""Gold fencing contract unit tests."""

import unittest

from gold.implementation.fencing import (
    GoldExecutionAttempt,
    GoldFencingError,
    generate_gold_attempt_id,
    validate_publication_eligibility,
)


DATASET = "daily_sales"
SNAPSHOT = "gold_snap_" + "a" * 32
ATTEMPT = "attempt_" + "b" * 32


def make_attempt(token=42, attempt_id=ATTEMPT):
    return GoldExecutionAttempt(
        dataset_name=DATASET,
        snapshot_id=SNAPSHOT,
        attempt_id=attempt_id,
        fencing_token=token,
    )


class TestGoldFencing(unittest.TestCase):

    def test_valid_execution_attempt(self):
        attempt = make_attempt()

        self.assertEqual(attempt.dataset_name, DATASET)
        self.assertEqual(attempt.snapshot_id, SNAPSHOT)
        self.assertEqual(attempt.fencing_token, 42)

    def test_unique_attempt_ids(self):
        first = generate_gold_attempt_id()
        second = generate_gold_attempt_id()

        self.assertNotEqual(first, second)
        self.assertTrue(first.startswith("attempt_"))
        self.assertEqual(len(first), 40)

        make_attempt(attempt_id=first)
        make_attempt(attempt_id=second)

    def test_isolated_staging_paths(self):
        first = make_attempt(
            attempt_id="attempt_" + "1" * 32
        )
        second = make_attempt(
            attempt_id="attempt_" + "2" * 32
        )

        self.assertNotEqual(
            first.staging_prefix,
            second.staging_prefix,
        )

        self.assertTrue(
            first.data_prefix.startswith(
                first.staging_prefix
            )
        )

    def test_invalid_attempt_id_rejected(self):
        with self.assertRaises(GoldFencingError):
            make_attempt(attempt_id="../unsafe")

    def test_zero_token_rejected(self):
        with self.assertRaises(GoldFencingError):
            make_attempt(token=0)

    def test_negative_token_rejected(self):
        with self.assertRaises(GoldFencingError):
            make_attempt(token=-1)

    def test_boolean_token_rejected(self):
        with self.assertRaises(GoldFencingError):
            make_attempt(token=True)

    def test_token_overflow_rejected(self):
        with self.assertRaises(GoldFencingError):
            make_attempt(token=2**63)

    def test_current_owner_eligible_for_commit_check(self):
        result = validate_publication_eligibility(
            attempt=make_attempt(token=42),
            current_fencing_token=42,
            ownership_alive=True,
        )

        self.assertEqual(
            result["status"],
            "ELIGIBLE_FOR_COMMIT_CHECK",
        )
        self.assertFalse(result["can_write"])
        self.assertFalse(result["can_publish"])

    def test_stale_token_rejected(self):
        with self.assertRaises(GoldFencingError):
            validate_publication_eligibility(
                attempt=make_attempt(token=41),
                current_fencing_token=42,
                ownership_alive=True,
            )

    def test_lost_ownership_rejected(self):
        with self.assertRaises(GoldFencingError):
            validate_publication_eligibility(
                attempt=make_attempt(),
                current_fencing_token=42,
                ownership_alive=False,
            )

    def test_unknown_ownership_rejected(self):
        with self.assertRaises(GoldFencingError):
            validate_publication_eligibility(
                attempt=make_attempt(),
                current_fencing_token=42,
                ownership_alive=None,
            )

    def test_invalid_current_token_rejected(self):
        for token in (None, True, 0, -1, 2**63):
            with self.subTest(token=token):
                with self.assertRaises(GoldFencingError):
                    validate_publication_eligibility(
                        attempt=make_attempt(),
                        current_fencing_token=token,
                        ownership_alive=True,
                    )

    def test_invalid_attempt_object_rejected(self):
        with self.assertRaises(GoldFencingError):
            validate_publication_eligibility(
                attempt={},
                current_fencing_token=42,
                ownership_alive=True,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
