import unittest
from unittest.mock import MagicMock, patch, call
from minio.error import S3Error
from silver.snapshots.publisher import (
    get_snapshot_manifest_key,
    publish_snapshot,
)


class TestSnapshotPublisher(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.client = MagicMock()
        self.bucket = "test-bucket"

        self.manifest = {
            "snapshot_id": "snapshot001",
            "table_name": "orders",
            "status": "COMPLETED",
        }

        self.expected_key = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snapshot001/"
            "_SNAPSHOT_METADATA.json"
        )

    def publish(self):
        return publish_snapshot(
            spark=self.spark,
            client=self.client,
            bucket=self.bucket,
            manifest=self.manifest,
        )

    def test_valid_manifest_key(self):
        result = get_snapshot_manifest_key(
            "orders",
            "snapshot001",
        )

        self.assertEqual(result, self.expected_key)

    def test_reject_invalid_table(self):
        for table_name in ["", None, 123]:
            with self.subTest(table_name=table_name):
                with self.assertRaisesRegex(
                    ValueError,
                    "Invalid snapshot table",
                ):
                    get_snapshot_manifest_key(
                        table_name,
                        "snapshot001",
                    )

    def test_reject_invalid_snapshot_id(self):
        for snapshot_id in ["", None, "../invalid", "a/b", "a b"]:
            with self.subTest(snapshot_id=snapshot_id):
                with self.assertRaisesRegex(
                    ValueError,
                    "Invalid snapshot ID",
                ):
                    get_snapshot_manifest_key(
                        "orders",
                        snapshot_id,
                    )

    @patch(
        "silver.snapshots.publisher.publish_manifest"
    )
    @patch(
        "silver.snapshots.publisher.verify_snapshot"
    )
    def test_successful_publication(
        self,
        mock_verify,
        mock_publish,
    ):
        result = self.publish()

        mock_verify.assert_called_once_with(
            spark=self.spark,
            client=self.client,
            bucket=self.bucket,
            manifest=self.manifest,
        )

        mock_publish.assert_called_once_with(
            client=self.client,
            bucket=self.bucket,
            manifest_key=self.expected_key,
            manifest=self.manifest,
        )

        self.assertEqual(
            result,
            {
                "snapshot_id": "snapshot001",
                "table_name": "orders",
                "manifest_key": self.expected_key,
                "status": "COMPLETED",
                "publication_status": "PUBLISHED",
            },
        )

    @patch(
        "silver.snapshots.publisher.publish_manifest"
    )
    @patch(
        "silver.snapshots.publisher.verify_snapshot"
    )
    def test_verification_failure_prevents_publication(
        self,
        mock_verify,
        mock_publish,
    ):
        mock_verify.side_effect = ValueError(
            "Snapshot row count mismatch"
        )

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot row count mismatch",
        ):
            self.publish()

        mock_publish.assert_not_called()

    @patch(
        "silver.snapshots.publisher.publish_manifest"
    )
    @patch(
        "silver.snapshots.publisher.verify_snapshot"
    )
    def test_publication_failure_propagates(
        self,
        mock_verify,
        mock_publish,
    ):
        mock_publish.side_effect = RuntimeError(
            "Manifest already exists"
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "Manifest already exists",
        ):
            self.publish()

        mock_verify.assert_called_once()
        mock_publish.assert_called_once()

    @patch(
        "silver.snapshots.publisher.publish_manifest"
    )
    @patch(
        "silver.snapshots.publisher.verify_snapshot"
    )
    def test_reject_invalid_manifest(
        self,
        mock_verify,
        mock_publish,
    ):
        self.manifest = None

        with self.assertRaisesRegex(
            ValueError,
            "Invalid snapshot manifest",
        ):
            self.publish()

        mock_verify.assert_not_called()
        mock_publish.assert_not_called()

    def make_s3_error(self, code):
        return S3Error(
            code=code,
            message="Test MinIO error",
            resource="/test-bucket/manifest.json",
            request_id="test-request",
            host_id="test-host",
            response=None,
        )

    @patch("silver.snapshots.publisher.read_published_manifest")
    @patch("silver.snapshots.publisher.publish_manifest")
    @patch("silver.snapshots.publisher.verify_snapshot")
    def test_idempotent_retry_identical_manifest(
        self, mock_verify, mock_publish, mock_read
    ):
        mock_publish.side_effect = self.make_s3_error(
            "PreconditionFailed"
        )
        mock_read.return_value = self.manifest.copy()

        result = self.publish()

        self.assertEqual(
            result["publication_status"],
            "ALREADY_PUBLISHED",
        )

        mock_verify.assert_called_once()
        mock_publish.assert_called_once()
        mock_read.assert_called_once_with(
            client=self.client,
            bucket=self.bucket,
            manifest_key=self.expected_key,
        )

    @patch("silver.snapshots.publisher.read_published_manifest")
    @patch("silver.snapshots.publisher.publish_manifest")
    @patch("silver.snapshots.publisher.verify_snapshot")
    def test_reject_conflicting_manifest(
        self, mock_verify, mock_publish, mock_read
    ):
        mock_publish.side_effect = self.make_s3_error(
            "PreconditionFailed"
        )

        mock_read.return_value = {
            **self.manifest,
            "row_count": 999,
        }

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot manifest conflict",
        ):
            self.publish()

        mock_verify.assert_called_once()
        mock_publish.assert_called_once()
        mock_read.assert_called_once()

    @patch("silver.snapshots.publisher.read_published_manifest")
    @patch("silver.snapshots.publisher.publish_manifest")
    @patch("silver.snapshots.publisher.verify_snapshot")
    def test_other_minio_error_propagates(
        self, mock_verify, mock_publish, mock_read
    ):
        mock_publish.side_effect = self.make_s3_error(
            "AccessDenied"
        )

        with self.assertRaises(S3Error) as context:
            self.publish()

        self.assertEqual(
            context.exception.code,
            "AccessDenied",
        )

        mock_read.assert_not_called()

    @patch("silver.snapshots.publisher.read_published_manifest")
    @patch("silver.snapshots.publisher.publish_manifest")
    @patch("silver.snapshots.publisher.verify_snapshot")
    def test_retry_verification_failure_prevents_publication(
        self, mock_verify, mock_publish, mock_read
    ):
        mock_verify.side_effect = ValueError(
            "Snapshot contains duplicate primary keys"
        )

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot contains duplicate primary keys",
        ):
            self.publish()

        mock_publish.assert_not_called()
        mock_read.assert_not_called()



if __name__ == "__main__":
    unittest.main()