
import unittest
from unittest.mock import MagicMock, patch

from minio.error import S3Error

from gold.implementation.storage.publisher import (
    GoldSnapshotPublicationError,
    publish_gold_snapshot,
)


MANIFEST = {
    "dataset_name": "daily_sales",
    "snapshot_id": "gold_snap_0123456789abcdef0123456789abcdef",
    "manifest_key": (
        "gold/daily_sales/snapshots/"
        "gold_snap_0123456789abcdef0123456789abcdef/"
        "manifest.json"
    ),
}


def s3_error(code):
    return S3Error(
        code=code,
        message="Test MinIO error",
        resource="/test",
        request_id="test-request",
        host_id="test-host",
        response=None,
    )


class TestGoldPublisher(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.client = MagicMock()
        self.bucket = "test-bucket"
        self.manifest = dict(MANIFEST)

        self.verify_patcher = patch(
            "gold.implementation.storage.publisher."
            "verify_gold_snapshot"
        )
        self.read_patcher = patch(
            "gold.implementation.storage.publisher."
            "read_published_manifest"
        )
        self.write_patcher = patch(
            "gold.implementation.storage.publisher."
            "publish_manifest"
        )

        self.verify_mock = self.verify_patcher.start()
        self.read_mock = self.read_patcher.start()
        self.write_mock = self.write_patcher.start()

        self.addCleanup(self.verify_patcher.stop)
        self.addCleanup(self.read_patcher.stop)
        self.addCleanup(self.write_patcher.stop)

        self.verify_mock.return_value = {
            "status": "VERIFIED"
        }

    def publish(self):
        return publish_gold_snapshot(
            spark=self.spark,
            client=self.client,
            bucket=self.bucket,
            manifest=self.manifest,
        )

    def test_new_publication(self):
        self.read_mock.side_effect = [
            s3_error("NoSuchKey"),
            dict(self.manifest),
        ]

        result = self.publish()

        self.assertEqual(result["status"], "PUBLISHED")
        self.write_mock.assert_called_once()
        self.verify_mock.assert_called_once()

    def test_already_published(self):
        self.read_mock.return_value = dict(self.manifest)

        result = self.publish()

        self.assertEqual(
            result["status"],
            "ALREADY_PUBLISHED",
        )
        self.write_mock.assert_not_called()
        self.verify_mock.assert_called_once()

    def test_existing_manifest_conflict(self):
        existing = dict(self.manifest)
        existing["snapshot_id"] = "gold_snap_" + "f" * 32

        self.read_mock.return_value = existing

        with self.assertRaises(GoldSnapshotPublicationError):
            self.publish()

        self.write_mock.assert_not_called()

    def test_verification_failure(self):
        self.verify_mock.side_effect = ValueError(
            "Gold verification failed"
        )

        with self.assertRaises(ValueError):
            self.publish()

        self.read_mock.assert_not_called()
        self.write_mock.assert_not_called()

    def test_verification_not_verified(self):
        self.verify_mock.return_value = {
            "status": "FAILED"
        }

        with self.assertRaises(GoldSnapshotPublicationError):
            self.publish()

        self.write_mock.assert_not_called()

    def test_concurrent_same_manifest(self):
        self.read_mock.side_effect = [
            s3_error("NoSuchKey"),
            dict(self.manifest),
            dict(self.manifest),
        ]
        self.write_mock.side_effect = s3_error(
            "PreconditionFailed"
        )

        result = self.publish()

        self.assertEqual(
            result["status"],
            "ALREADY_PUBLISHED",
        )

    def test_concurrent_different_manifest(self):
        conflicting = dict(self.manifest)
        conflicting["status"] = "CORRUPTED"

        self.read_mock.side_effect = [
            s3_error("NoSuchKey"),
            conflicting,
        ]
        self.write_mock.side_effect = s3_error(
            "PreconditionFailed"
        )

        with self.assertRaises(GoldSnapshotPublicationError):
            self.publish()

    def test_conditional_request_conflict(self):
        self.read_mock.side_effect = [
            s3_error("NoSuchKey"),
            dict(self.manifest),
            dict(self.manifest),
        ]
        self.write_mock.side_effect = s3_error(
            "ConditionalRequestConflict"
        )

        result = self.publish()

        self.assertEqual(
            result["status"],
            "ALREADY_PUBLISHED",
        )

    def test_minio_read_error_propagates(self):
        self.read_mock.side_effect = s3_error(
            "AccessDenied"
        )

        with self.assertRaises(S3Error):
            self.publish()

        self.write_mock.assert_not_called()

    def test_minio_write_error_propagates(self):
        self.read_mock.side_effect = s3_error(
            "NoSuchKey"
        )
        self.write_mock.side_effect = s3_error(
            "AccessDenied"
        )

        with self.assertRaises(S3Error):
            self.publish()

    def test_publication_readback_mismatch(self):
        mismatched = dict(self.manifest)
        mismatched["status"] = "INVALID"

        self.read_mock.side_effect = [
            s3_error("NoSuchKey"),
            mismatched,
        ]

        with self.assertRaises(GoldSnapshotPublicationError):
            self.publish()

    def test_publication_arguments(self):
        self.read_mock.side_effect = [
            s3_error("NoSuchKey"),
            dict(self.manifest),
        ]

        self.publish()

        self.write_mock.assert_called_once_with(
            client=self.client,
            bucket=self.bucket,
            manifest_key=self.manifest["manifest_key"],
            manifest=self.manifest,
        )

    def test_verification_receives_correct_arguments(self):
        self.read_mock.return_value = dict(self.manifest)

        self.publish()

        self.verify_mock.assert_called_once_with(
            spark=self.spark,
            client=self.client,
            bucket=self.bucket,
            manifest=self.manifest,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
