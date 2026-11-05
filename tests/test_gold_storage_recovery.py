
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from gold.implementation.storage.recovery import (
    GoldRecoveryInspectionError,
    inspect_gold_snapshot,
)

from gold.implementation.storage.recovery import (
    GoldRecoveryDecisionError,
    decide_gold_recovery,
)

from unittest.mock import patch

from gold.implementation.storage.recovery import (
    GoldRecoveryExecutionError,
    execute_gold_recovery,
)

DATASET = "daily_sales"
SNAPSHOT = "gold_snap_" + "a" * 32

BASE = f"gold/{DATASET}/snapshots/{SNAPSHOT}"
DATA = BASE + "/data/"
MANIFEST = BASE + "/manifest.json"


def make_client(keys):
    client = MagicMock()
    client.list_objects.return_value = [
        SimpleNamespace(object_name=key)
        for key in keys
    ]
    return client


def inspect(keys):
    return inspect_gold_snapshot(
        client=make_client(keys),
        bucket="test-bucket",
        dataset_name=DATASET,
        snapshot_id=SNAPSHOT,
    )


class TestGoldRecoveryInspector(unittest.TestCase):

    def test_not_found(self):
        result = inspect([])
        self.assertEqual(result["state"], "NOT_FOUND")

    def test_incomplete_parquet_only(self):
        result = inspect([
            DATA + "part-00000.parquet",
        ])
        self.assertEqual(result["state"], "INCOMPLETE")

    def test_incomplete_success_only(self):
        result = inspect([
            DATA + "_SUCCESS",
        ])
        self.assertEqual(result["state"], "INCOMPLETE")

    def test_unpublished(self):
        result = inspect([
            DATA + "part-00000.parquet",
            DATA + "_SUCCESS",
        ])
        self.assertEqual(result["state"], "UNPUBLISHED")

    def test_published(self):
        result = inspect([
            DATA + "part-00000.parquet",
            DATA + "_SUCCESS",
            MANIFEST,
        ])
        self.assertEqual(result["state"], "PUBLISHED")

    def test_manifest_without_data(self):
        result = inspect([MANIFEST])
        self.assertEqual(result["state"], "INVALID")

    def test_unexpected_object(self):
        result = inspect([
            DATA + "part-00000.parquet",
            DATA + "_SUCCESS",
            BASE + "/unexpected.json",
        ])
        self.assertEqual(result["state"], "INVALID")

    def test_multiple_parquet_files(self):
        result = inspect([
            DATA + "part-00000.parquet",
            DATA + "part-00001.parquet",
            DATA + "_SUCCESS",
        ])

        self.assertEqual(result["state"], "UNPUBLISHED")
        self.assertEqual(result["parquet_file_count"], 2)

    def test_invalid_bucket(self):
        with self.assertRaises(ValueError):
            inspect_gold_snapshot(
                client=make_client([]),
                bucket="",
                dataset_name=DATASET,
                snapshot_id=SNAPSHOT,
            )

    def test_invalid_snapshot_id(self):
        with self.assertRaises(ValueError):
            inspect_gold_snapshot(
                client=make_client([]),
                bucket="test-bucket",
                dataset_name=DATASET,
                snapshot_id="invalid",
            )

    def test_minio_error(self):
        client = MagicMock()
        client.list_objects.side_effect = RuntimeError(
            "MinIO unavailable"
        )

        with self.assertRaises(GoldRecoveryInspectionError):
            inspect_gold_snapshot(
                client=client,
                bucket="test-bucket",
                dataset_name=DATASET,
                snapshot_id=SNAPSHOT,
            )


class TestGoldRecoveryDecision(unittest.TestCase):

    def test_not_found_decision(self):
        decision = decide_gold_recovery(inspect([]))

        self.assertEqual(
            decision["action"],
            "NEW_SNAPSHOT",
        )
        self.assertFalse(decision["requires_verification"])

    def test_incomplete_decision(self):
        decision = decide_gold_recovery(inspect([
            DATA + "part-00000.parquet",
        ]))

        self.assertEqual(decision["action"], "REJECT")
        self.assertFalse(decision["can_delete"])

    def test_unpublished_decision(self):
        decision = decide_gold_recovery(inspect([
            DATA + "part-00000.parquet",
            DATA + "_SUCCESS",
        ]))

        self.assertEqual(
            decision["action"],
            "VERIFY_AND_PUBLISH",
        )
        self.assertTrue(decision["requires_verification"])
        self.assertFalse(decision["can_publish"])

    def test_published_decision(self):
        decision = decide_gold_recovery(inspect([
            DATA + "part-00000.parquet",
            DATA + "_SUCCESS",
            MANIFEST,
        ]))

        self.assertEqual(
            decision["action"],
            "VERIFY_PUBLISHED",
        )
        self.assertTrue(decision["requires_verification"])

    def test_invalid_decision(self):
        decision = decide_gold_recovery(inspect([
            MANIFEST,
        ]))

        self.assertEqual(decision["action"], "REJECT")

    def test_invalid_inspection_type(self):
        with self.assertRaises(GoldRecoveryDecisionError):
            decide_gold_recovery(None)

    def test_missing_inspection_fields(self):
        with self.assertRaises(GoldRecoveryDecisionError):
            decide_gold_recovery({
                "state": "UNPUBLISHED",
            })

    def test_unknown_state(self):
        inspection = inspect([])
        inspection["state"] = "UNKNOWN"

        with self.assertRaises(GoldRecoveryDecisionError):
            decide_gold_recovery(inspection)

    def test_no_mutating_permissions(self):
        for keys in (
            [],
            [DATA + "part-00000.parquet"],
            [DATA + "part-00000.parquet", DATA + "_SUCCESS"],
            [DATA + "part-00000.parquet", DATA + "_SUCCESS", MANIFEST],
            [MANIFEST],
        ):
            decision = decide_gold_recovery(inspect(keys))

            self.assertFalse(decision["can_publish"])
            self.assertFalse(decision["can_overwrite"])
            self.assertFalse(decision["can_delete"])



class TestGoldRecoveryExecutor(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.client = MagicMock()
        self.bucket = "test-bucket"

        self.manifest = {
            "dataset_name": DATASET,
            "snapshot_id": SNAPSHOT,
            "snapshot_prefix": BASE,
            "data_prefix": DATA,
            "manifest_key": MANIFEST,
        }

    def execute(self, inspection, candidate=None):
        with patch(
            "gold.implementation.storage.recovery."
            "inspect_gold_snapshot",
            return_value=inspection,
        ):
            return execute_gold_recovery(
                spark=self.spark,
                client=self.client,
                bucket=self.bucket,
                dataset_name=DATASET,
                snapshot_id=SNAPSHOT,
                candidate_manifest=candidate,
            )

    def test_not_found(self):
        result = self.execute(inspect([]))

        self.assertEqual(result["status"], "NOT_FOUND")

    def test_incomplete_rejected(self):
        with self.assertRaises(GoldRecoveryExecutionError):
            self.execute(inspect([
                DATA + "part-00000.parquet",
            ]))

    def test_invalid_rejected(self):
        with self.assertRaises(GoldRecoveryExecutionError):
            self.execute(inspect([MANIFEST]))

    def test_unpublished_requires_manifest(self):
        with self.assertRaises(GoldRecoveryExecutionError):
            self.execute(inspect([
                DATA + "part-00000.parquet",
                DATA + "_SUCCESS",
            ]))

    def test_unpublished_identity_mismatch(self):
        invalid = dict(self.manifest)
        invalid["snapshot_id"] = "gold_snap_" + "b" * 32

        with self.assertRaises(GoldRecoveryExecutionError):
            self.execute(
                inspect([
                    DATA + "part-00000.parquet",
                    DATA + "_SUCCESS",
                ]),
                candidate=invalid,
            )

    @patch(
        "gold.implementation.storage.recovery."
        "publish_gold_snapshot"
    )
    def test_unpublished_publish(self, publish_mock):
        publish_mock.return_value = {
            "status": "PUBLISHED",
        }

        result = self.execute(
            inspect([
                DATA + "part-00000.parquet",
                DATA + "_SUCCESS",
            ]),
            candidate=self.manifest,
        )

        self.assertEqual(result["status"], "PUBLISHED")
        publish_mock.assert_called_once()

    @patch(
        "gold.implementation.storage.recovery."
        "read_published_manifest"
    )
    @patch(
        "gold.implementation.storage.recovery."
        "verify_gold_snapshot"
    )
    def test_published_verification(
        self,
        verify_mock,
        read_mock,
    ):
        read_mock.return_value = dict(self.manifest)
        verify_mock.return_value = {
            "status": "VERIFIED",
        }

        result = self.execute(inspect([
            DATA + "part-00000.parquet",
            DATA + "_SUCCESS",
            MANIFEST,
        ]))

        self.assertEqual(
            result["status"],
            "ALREADY_PUBLISHED",
        )
        verify_mock.assert_called_once()

    @patch(
        "gold.implementation.storage.recovery."
        "read_published_manifest"
    )
    def test_published_identity_mismatch(self, read_mock):
        invalid = dict(self.manifest)
        invalid["snapshot_id"] = "gold_snap_" + "c" * 32
        read_mock.return_value = invalid

        with self.assertRaises(GoldRecoveryExecutionError):
            self.execute(inspect([
                DATA + "part-00000.parquet",
                DATA + "_SUCCESS",
                MANIFEST,
            ]))

    @patch(
        "gold.implementation.storage.recovery."
        "read_published_manifest"
    )
    @patch(
        "gold.implementation.storage.recovery."
        "verify_gold_snapshot"
    )
    def test_published_verification_failure(
        self,
        verify_mock,
        read_mock,
    ):
        read_mock.return_value = dict(self.manifest)
        verify_mock.return_value = {
            "status": "FAILED",
        }

        with self.assertRaises(GoldRecoveryExecutionError):
            self.execute(inspect([
                DATA + "part-00000.parquet",
                DATA + "_SUCCESS",
                MANIFEST,
            ]))

    @patch(
        "gold.implementation.storage.recovery."
        "publish_gold_snapshot"
    )
    def test_publisher_failure_propagates(self, publish_mock):
        publish_mock.side_effect = RuntimeError(
            "Publication failed"
        )

        with self.assertRaises(RuntimeError):
            self.execute(
                inspect([
                    DATA + "part-00000.parquet",
                    DATA + "_SUCCESS",
                ]),
                candidate=self.manifest,
            )

    def test_candidate_data_prefix_mismatch(self):
        invalid_manifest = dict(self.manifest)
        invalid_manifest["data_prefix"] = (
            "gold/daily_sales/snapshots/"
            "gold_snap_" + "b" * 32 + "/data/"
        )

        with patch(
            "gold.implementation.storage.recovery."
            "publish_gold_snapshot"
        ) as publish_mock:

            with self.assertRaises(GoldRecoveryExecutionError):
                self.execute(
                    inspect([
                        DATA + "part-00000.parquet",
                        DATA + "_SUCCESS",
                    ]),
                    candidate=invalid_manifest,
                )

            publish_mock.assert_not_called()


    def test_snapshot_state_changed_during_recovery(self):
        unpublished = inspect([
            DATA + "part-00000.parquet",
            DATA + "_SUCCESS",
        ])

        published = inspect([
            DATA + "part-00000.parquet",
            DATA + "_SUCCESS",
            MANIFEST,
        ])

        with patch(
            "gold.implementation.storage.recovery."
            "inspect_gold_snapshot",
            side_effect=[unpublished, published],
        ) as inspect_mock:

            with patch(
                "gold.implementation.storage.recovery."
                "publish_gold_snapshot"
            ) as publish_mock:

                with self.assertRaises(GoldRecoveryExecutionError):
                    execute_gold_recovery(
                        spark=self.spark,
                        client=self.client,
                        bucket=self.bucket,
                        dataset_name=DATASET,
                        snapshot_id=SNAPSHOT,
                        candidate_manifest=self.manifest,
                    )

                self.assertEqual(inspect_mock.call_count, 2)
                publish_mock.assert_not_called()



if __name__ == "__main__":
    unittest.main(verbosity=2)
