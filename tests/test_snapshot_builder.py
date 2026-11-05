import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from silver.snapshots.builder import build_silver_snapshot


class TestSnapshotBuilder(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("snapshot-builder-test")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @patch("silver.snapshots.builder.assemble_silver_batches")
    def test_timestamp_watermark(self, mock_assembler):

        combined_df = self.spark.createDataFrame(
            [
                (1, "PENDING", datetime(2026, 1, 1), 1),
                (1, "SHIPPED", datetime(2026, 1, 2), 2),
                (2, "PROCESSING", datetime(2026, 1, 1), 1),
            ],
            ["order_id", "status", "updated_at", "_batch_sequence"],
        )

        mock_assembler.return_value = combined_df

        batches = [
            {"table_name": "orders", "watermark_type": "TIMESTAMP"},
        ]

        result = build_silver_snapshot(
            spark=self.spark,
            client=MagicMock(),
            bucket="test-bucket",
            batches=batches,
            table_name="orders",
        )

        actual = {
            row.order_id: row.status
            for row in result.collect()
        }

        self.assertEqual(
            actual,
            {
                1: "SHIPPED",
                2: "PROCESSING",
            },
        )

        self.assertEqual(result.count(), 2)
        mock_assembler.assert_called_once()

    @patch("silver.snapshots.builder.assemble_silver_batches")
    def test_id_watermark(self, mock_assembler):

        combined_df = self.spark.createDataFrame(
            [
                (1, "SUCCESS", 1),
                (2, "FAILED", 1),
                (3, "SUCCESS", 2),
            ],
            ["payment_id", "status", "_batch_sequence"],
        )

        mock_assembler.return_value = combined_df

        batches = [
            {"table_name": "payments", "watermark_type": "ID"},
        ]

        result = build_silver_snapshot(
            spark=self.spark,
            client=MagicMock(),
            bucket="test-bucket",
            batches=batches,
            table_name="payments",
        )

        actual = {
            row.payment_id: row.status
            for row in result.collect()
        }

        self.assertEqual(
            actual,
            {
                1: "SUCCESS",
                2: "FAILED",
                3: "SUCCESS",
            },
        )

        self.assertEqual(result.count(), 3)
        mock_assembler.assert_called_once()


    @patch("silver.snapshots.builder.assemble_silver_batches")
    def test_reject_mismatched_table(self, mock_assembler):
        batches = [
            {
                "table_name": "payments",
                "watermark_type": "ID",
            }
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot batches do not match requested table",
        ):
            build_silver_snapshot(
                spark=self.spark,
                client=MagicMock(),
                bucket="test-bucket",
                batches=batches,
                table_name="orders",
            )

        mock_assembler.assert_not_called()

    @patch("silver.snapshots.builder.assemble_silver_batches")
    def test_reject_mismatched_watermark_type(self, mock_assembler):
        batches = [
            {
                "table_name": "orders",
                "watermark_type": "ID",
            }
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot watermark type does not match table configuration",
        ):
            build_silver_snapshot(
                spark=self.spark,
                client=MagicMock(),
                bucket="test-bucket",
                batches=batches,
                table_name="orders",
            )

        mock_assembler.assert_not_called()



if __name__ == "__main__":
    unittest.main()