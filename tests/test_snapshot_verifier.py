import unittest
from unittest.mock import MagicMock, patch

from silver.snapshots.verifier import verify_snapshot


class TestSnapshotVerifier(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.client = MagicMock()
        self.bucket = "test-bucket"

        self.col_patcher = patch(
        "silver.snapshots.verifier.F.col")

        self.mock_col = self.col_patcher.start()

        # Support PySpark column comparison:
        # F.col("count") > 1
        self.mock_col.return_value.__gt__.return_value = MagicMock()

        self.addCleanup(
    self.col_patcher.stop
)

        self.prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snapshot001/data/"
        )

        self.objects = [
            {
                "object_key": self.prefix + "part-00000.parquet",
                "size": 1024,
            }
        ]

        self.manifest = {
            "snapshot_id": "snapshot001",
            "table_name": "orders",
            "status": "COMPLETED",
            "data_prefix": self.prefix,
            "row_count": 2,
            "parquet_file_count": 1,
            "parquet_objects": self.objects,
            "watermark_type": "TIMESTAMP",
        }

        self.df = MagicMock()
        self.df.columns = [
            "order_id",
            "status",
            "updated_at",
        ]

        self.df.count.return_value = 2

        # Used by null-PK and duplicate-PK checks.
        self.df.filter.return_value.limit.return_value.count.return_value = 0
        self.df.groupBy.return_value.count.return_value.filter.return_value.limit.return_value.count.return_value = 0

        self.spark.read.parquet.return_value = self.df

        self.discovery_patcher = patch(
            "silver.snapshots.verifier."
            "discover_snapshot_parquet_objects",
            return_value=self.objects,
        )
        self.mock_discovery = self.discovery_patcher.start()
        self.addCleanup(self.discovery_patcher.stop)

    def verify(self):
        return verify_snapshot(
            spark=self.spark,
            client=self.client,
            bucket=self.bucket,
            manifest=self.manifest,
        )

    def test_valid_snapshot(self):
        result = self.verify()

        self.assertIs(result, self.manifest)

        self.spark.read.parquet.assert_called_once_with(
            f"s3a://{self.bucket}/{self.prefix}"
        )

    def test_reject_invalid_manifest(self):
        self.manifest = None

        with self.assertRaisesRegex(
            ValueError, "Invalid snapshot manifest"
        ):
            self.verify()

    def test_reject_incomplete_snapshot(self):
        self.manifest["status"] = "PENDING"

        with self.assertRaisesRegex(
            ValueError, "Snapshot is not completed"
        ):
            self.verify()

    def test_reject_invalid_snapshot_id(self):
        self.manifest["snapshot_id"] = "../invalid"

        with self.assertRaisesRegex(
            ValueError, "Invalid snapshot ID"
        ):
            self.verify()

    def test_reject_prefix_mismatch(self):
        self.manifest["data_prefix"] = "invalid/data/"

        with self.assertRaisesRegex(
            ValueError, "Snapshot data prefix mismatch"
        ):
            self.verify()

    def test_reject_watermark_mismatch(self):
        self.manifest["watermark_type"] = "ID"

        with self.assertRaisesRegex(
            ValueError, "Snapshot watermark type mismatch"
        ):
            self.verify()

    def test_reject_invalid_row_count(self):
        self.manifest["row_count"] = -1

        with self.assertRaisesRegex(
            ValueError, "Invalid snapshot row count"
        ):
            self.verify()

    def test_reject_missing_object_metadata(self):
        self.manifest["parquet_objects"] = None

        with self.assertRaisesRegex(
            ValueError, "Missing snapshot Parquet metadata"
        ):
            self.verify()

    def test_reject_object_mismatch(self):
        self.manifest["parquet_objects"] = []

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot Parquet objects do not match manifest",
        ):
            self.verify()

    def test_reject_file_count_mismatch(self):
        self.manifest["parquet_file_count"] = 2

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot Parquet file count mismatch",
        ):
            self.verify()

    def test_reject_missing_primary_key(self):
        self.df.columns = ["status", "updated_at"]

        with self.assertRaisesRegex(
            ValueError, "Snapshot primary key is missing"
        ):
            self.verify()

    def test_reject_internal_batch_sequence(self):
        self.df.columns.append("_batch_sequence")

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot contains internal batch sequence",
        ):
            self.verify()

    def test_reject_row_count_mismatch(self):
        self.df.count.return_value = 3

        with self.assertRaisesRegex(
            ValueError, "Snapshot row count mismatch"
        ):
            self.verify()

    def test_reject_null_primary_keys(self):
        self.df.filter.return_value.limit.return_value.count.return_value = 1

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot contains null primary keys",
        ):
            self.verify()

    def test_reject_duplicate_primary_keys(self):
        (
            self.df.groupBy.return_value
            .count.return_value
            .filter.return_value
            .limit.return_value
            .count.return_value
        ) = 1

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot contains duplicate primary keys",
        ):
            self.verify()


if __name__ == "__main__":
    unittest.main()