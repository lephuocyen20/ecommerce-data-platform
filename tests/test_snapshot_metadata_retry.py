import unittest
from datetime import datetime, timezone

from silver.snapshots.metadata import (
    build_snapshot_metadata,
)


class TestSnapshotMetadataRetry(unittest.TestCase):

    def setUp(self):
        self.table_name = "orders"
        self.snapshot_id = "snap_retry_test"

        self.data_prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snap_retry_test/data/"
        )

        self.batches = [
            {
                "batch_id": "batch_001",
                "table_name": "orders",
                "watermark_type": "TIMESTAMP",
                "end_timestamp": datetime(
                    2026, 10, 26, tzinfo=timezone.utc
                ),
                "end_id": 100,
            }
        ]

        self.parquet_objects = [
            {
                "object_key": (
                    self.data_prefix + "part-00000.parquet"
                ),
                "size": 1024,
            }
        ]

    def build(self, created_at):
        return build_snapshot_metadata(
            table_name=self.table_name,
            snapshot_id=self.snapshot_id,
            data_prefix=self.data_prefix,
            batches=self.batches,
            row_count=100,
            parquet_objects=self.parquet_objects,
            created_at=created_at,
        )

    def test_same_timestamp_produces_same_metadata(self):
        timestamp = "2026-10-26T00:00:00+00:00"

        first = self.build(timestamp)
        second = self.build(timestamp)

        self.assertEqual(first, second)

    def test_preserves_creation_timestamp(self):
        timestamp = "2026-10-26T00:00:00+00:00"

        manifest = self.build(timestamp)

        self.assertEqual(
            manifest["created_at"],
            timestamp,
        )

    def test_reject_empty_creation_timestamp(self):
        with self.assertRaisesRegex(
            ValueError,
            "Invalid snapshot creation timestamp",
        ):
            self.build("")

    def test_reject_non_string_creation_timestamp(self):
        with self.assertRaisesRegex(
            ValueError,
            "Invalid snapshot creation timestamp",
        ):
            self.build(12345)


if __name__ == "__main__":
    unittest.main()