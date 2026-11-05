import json
import os
import unittest
import uuid

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

from silver.snapshots.publisher import publish_manifest


load_dotenv()


class TestSnapshotPublisherMinIO(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        endpoint = endpoint.replace("http://", "").replace("https://", "")

        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")

        if not access_key or not secret_key:
            raise ValueError("Missing MinIO credentials")

        cls.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )

        cls.bucket = "snapshot-publication-test"

        # Use an existing bucket only; never create or delete buckets.
        if not cls.client.bucket_exists(cls.bucket):
            raise unittest.SkipTest(
                f"Test bucket does not exist: {cls.bucket}"
            )

    def test_immutable_manifest_publication(self):
        snapshot_id = uuid.uuid4().hex

        object_key = (
            f"integration_tests/snapshot_publisher/"
            f"{snapshot_id}/manifest.json"
        )

        original_manifest = {
            "snapshot_id": snapshot_id,
            "table_name": "orders",
            "status": "COMPLETED",
            "row_count": 2,
        }

        modified_manifest = {
            **original_manifest,
            "row_count": 999,
        }

        try:
            # 1. First publication must succeed.
            publish_manifest(
                client=self.client,
                bucket=self.bucket,
                manifest_key=object_key,
                manifest=original_manifest,
            )

            # 2. Read back and verify.
            response = self.client.get_object(
                self.bucket,
                object_key,
            )

            try:
                stored_manifest = json.loads(
                    response.read().decode("utf-8")
                )
            finally:
                response.close()
                response.release_conn()

            self.assertEqual(
                stored_manifest,
                original_manifest,
            )

            # 3. Second publication must fail.
            with self.assertRaises(S3Error) as context:
                publish_manifest(
                    client=self.client,
                    bucket=self.bucket,
                    manifest_key=object_key,
                    manifest=modified_manifest,
                )

            self.assertIn(
                context.exception.code,
                {"PreconditionFailed", "ConditionalRequestConflict"},
            )

            # 4. Verify original manifest was preserved.
            response = self.client.get_object(
                self.bucket,
                object_key,
            )

            try:
                final_manifest = json.loads(
                    response.read().decode("utf-8")
                )
            finally:
                response.close()
                response.release_conn()

            self.assertEqual(
                final_manifest,
                original_manifest,
            )

        finally:
            # Cleanup only the exact object created by this test.
            self.client.remove_object(
                self.bucket,
                object_key,
            )


if __name__ == "__main__":
    unittest.main()