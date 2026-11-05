import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from silver.snapshots.recovery import (
    inspect_snapshot_state,
)


class TestSnapshotRecovery(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.bucket = "test-bucket"
        self.table_name = "orders"
        self.snapshot_id = "snap_test001"

        self.prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snap_test001/"
        )

        self.data_prefix = f"{self.prefix}data/"

    def inspect(self, keys):
        self.client.list_objects.return_value = [
            SimpleNamespace(object_name=key)
            for key in keys
        ]

        return inspect_snapshot_state(
            client=self.client,
            bucket=self.bucket,
            table_name=self.table_name,
            snapshot_id=self.snapshot_id,
        )

    def test_new_snapshot(self):
        result = self.inspect([])

        self.assertEqual(result["state"], "NEW")

    def test_unpublished_snapshot(self):
        result = self.inspect([
            f"{self.data_prefix}part-00000.parquet",
            f"{self.data_prefix}_SUCCESS",
        ])

        self.assertEqual(result["state"], "UNPUBLISHED")

    def test_incomplete_without_success_marker(self):
        result = self.inspect([
            f"{self.data_prefix}part-00000.parquet",
        ])

        self.assertEqual(result["state"], "INCOMPLETE")

    def test_incomplete_without_parquet(self):
        result = self.inspect([
            f"{self.data_prefix}_SUCCESS",
        ])

        self.assertEqual(result["state"], "INCOMPLETE")

    def test_published_snapshot(self):
        result = self.inspect([
            f"{self.data_prefix}part-00000.parquet",
            f"{self.data_prefix}_SUCCESS",
            f"{self.prefix}_SNAPSHOT_METADATA.json",
        ])

        self.assertEqual(result["state"], "PUBLISHED")

    def test_reject_duplicate_object_keys(self):
        key = f"{self.data_prefix}part-00000.parquet"

        with self.assertRaisesRegex(
            ValueError,
            "Duplicate snapshot object keys",
        ):
            self.inspect([key, key])

    def test_reject_invalid_bucket(self):
        with self.assertRaisesRegex(
            ValueError,
            "Invalid snapshot bucket",
        ):
            inspect_snapshot_state(
                client=self.client,
                bucket="",
                table_name=self.table_name,
                snapshot_id=self.snapshot_id,
            )


if __name__ == "__main__":
    unittest.main()