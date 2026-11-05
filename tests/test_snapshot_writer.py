import unittest
from unittest.mock import MagicMock, patch

from silver.snapshots.writer import write_snapshot_parquet


class TestSnapshotWriter(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.snapshot_df = MagicMock()

        self.snapshot_df.columns = [
            "order_id",
            "status",
            "updated_at",
            "_batch_sequence",
        ]

        self.output_df = MagicMock()
        self.snapshot_df.drop.return_value = self.output_df

    def write_snapshot(self, **overrides):
        params = {
            "spark": self.spark,
            "snapshot_df": self.snapshot_df,
            "bucket": "test-bucket",
            "table_name": "orders",
            "snapshot_id": "snapshot-001",
        }

        params.update(overrides)

        return write_snapshot_parquet(**params)

    def test_valid_snapshot_write(self):
        result = self.write_snapshot()

        expected_path = (
            "s3a://test-bucket/"
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snapshot-001/data/"
        )

        self.assertEqual(result["snapshot_id"], "snapshot-001")
        self.assertEqual(result["output_path"], expected_path)

        self.snapshot_df.drop.assert_called_once_with(
            "_batch_sequence"
        )

        self.output_df.write.mode.assert_called_once_with(
            "errorifexists"
        )

        self.output_df.write.mode.return_value.parquet.assert_called_once_with(
            expected_path
        )

    @patch("silver.snapshots.writer.uuid.uuid4")
    def test_generate_snapshot_id(self, mock_uuid):
        mock_uuid.return_value.hex = "generated123"

        result = self.write_snapshot(snapshot_id=None)

        self.assertEqual(
            result["snapshot_id"],
            "generated123",
        )

    def test_reject_invalid_snapshot_id(self):
        for invalid_id in ["", "../invalid", "invalid/id", "a b"]:
            with self.subTest(snapshot_id=invalid_id):
                with self.assertRaisesRegex(
                    ValueError,
                    "Invalid snapshot ID",
                ):
                    self.write_snapshot(snapshot_id=invalid_id)

        self.snapshot_df.drop.assert_not_called()

    def test_reject_invalid_bucket(self):
        for invalid_bucket in ["", None, 123]:
            with self.subTest(bucket=invalid_bucket):
                with self.assertRaisesRegex(
                    ValueError,
                    "Invalid snapshot bucket",
                ):
                    self.write_snapshot(bucket=invalid_bucket)

        self.snapshot_df.drop.assert_not_called()

    def test_reject_missing_batch_sequence(self):
        self.snapshot_df.columns = [
            "order_id",
            "status",
            "updated_at",
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Missing snapshot batch sequence",
        ):
            self.write_snapshot()

        self.snapshot_df.drop.assert_not_called()


if __name__ == "__main__":
    unittest.main()