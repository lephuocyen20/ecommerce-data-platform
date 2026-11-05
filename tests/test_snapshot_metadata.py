import unittest
from datetime import datetime, timezone

from silver.snapshots.metadata import (
    build_snapshot_metadata,
)


class TestSnapshotMetadata(unittest.TestCase):

    def setUp(self):
        self.batches = [
            {
                "batch_id": 1,
                "table_name": "orders",
                "watermark_type": "TIMESTAMP",
                "end_timestamp": datetime(
                    2026, 1, 1, tzinfo=timezone.utc
                ),
                "end_id": 10,
            },
            {
                "batch_id": 2,
                "table_name": "orders",
                "watermark_type": "TIMESTAMP",
                "end_timestamp": datetime(
                    2026, 1, 2, tzinfo=timezone.utc
                ),
                "end_id": 20,
            },
        ]

        self.parquet_objects = [
            {
                "object_key": "silver_snapshots/orders/data/part-00000.parquet",
                "size": 1024,
            }
        ]

    def build_metadata(self, **overrides):
        params = {
            "table_name": "orders",
            "snapshot_id": "snapshot-001",
            "data_prefix": "silver_snapshots/orders/data/",
            "batches": self.batches,
            "row_count": 15,
            "parquet_objects": self.parquet_objects,
        }

        params.update(overrides)

        return build_snapshot_metadata(**params)

    def test_valid_metadata(self):
        metadata = self.build_metadata()

        self.assertEqual(metadata["status"], "COMPLETED")
        self.assertEqual(metadata["table_name"], "orders")
        self.assertEqual(metadata["snapshot_id"], "snapshot-001")
        self.assertEqual(metadata["row_count"], 15)

        self.assertEqual(metadata["source_batch_ids"], [1, 2])
        self.assertEqual(metadata["source_batch_count"], 2)

        self.assertEqual(metadata["watermark_type"], "TIMESTAMP")
        self.assertEqual(
            metadata["end_timestamp"],
            "2026-01-02T00:00:00+00:00",
        )
        self.assertEqual(metadata["end_id"], 20)

        self.assertEqual(metadata["parquet_file_count"], 1)

    def test_reject_empty_batches(self):
        with self.assertRaisesRegex(
            ValueError,
            "Cannot create snapshot metadata without source batches",
        ):
            self.build_metadata(batches=[])

    def test_reject_negative_row_count(self):
        with self.assertRaisesRegex(
            ValueError,
            "Invalid snapshot row count",
        ):
            self.build_metadata(row_count=-1)

    def test_reject_empty_parquet_objects(self):
        with self.assertRaisesRegex(
            ValueError,
            "Snapshot must contain Parquet objects",
        ):
            self.build_metadata(parquet_objects=[])


    def test_reject_empty_snapshot_id(self):
        with self.assertRaisesRegex(
            ValueError, "Invalid snapshot ID"
        ):
            self.build_metadata(snapshot_id="")

    def test_reject_invalid_data_prefix(self):
        with self.assertRaisesRegex(
            ValueError, "Invalid snapshot data prefix"
        ):
            self.build_metadata(data_prefix="invalid-prefix")

    def test_reject_source_table_mismatch(self):
        batches = [
            dict(batch, table_name="payments")
            for batch in self.batches
        ]

        with self.assertRaisesRegex(
            ValueError, "Snapshot source table mismatch"
        ):
            self.build_metadata(batches=batches)

    def test_reject_watermark_type_mismatch(self):
        batches = [
            dict(batch, watermark_type="ID")
            for batch in self.batches
        ]

        with self.assertRaisesRegex(
            ValueError, "Snapshot watermark type mismatch"
        ):
            self.build_metadata(batches=batches)

    def test_reject_duplicate_source_batches(self):
        batches = [
            self.batches[0],
            dict(self.batches[0]),
        ]

        with self.assertRaisesRegex(
            ValueError, "Duplicate snapshot source batches"
        ):
            self.build_metadata(batches=batches)

    def test_reject_parquet_outside_prefix(self):
        objects = [
            {
                "object_key": "other/path/part-00000.parquet",
                "size": 1024,
            }
        ]

        with self.assertRaisesRegex(
            ValueError, "Invalid snapshot Parquet object path"
        ):
            self.build_metadata(parquet_objects=objects)

    def test_reject_invalid_parquet_size(self):
        objects = [
            {
                "object_key": self.parquet_objects[0]["object_key"],
                "size": 0,
            }
        ]

        with self.assertRaisesRegex(
            ValueError, "Invalid snapshot Parquet object size"
        ):
            self.build_metadata(parquet_objects=objects)

    def test_reject_duplicate_parquet_objects(self):
        objects = [
            self.parquet_objects[0],
            dict(self.parquet_objects[0]),
        ]

        with self.assertRaisesRegex(
            ValueError, "Duplicate snapshot Parquet objects"
        ):
            self.build_metadata(parquet_objects=objects)


if __name__ == "__main__":
    unittest.main()