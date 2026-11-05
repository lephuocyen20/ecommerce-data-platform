import unittest
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from silver.snapshots.assembler import assemble_silver_batches


class TestSnapshotAssembler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("snapshot-assembler-test")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @patch("silver.snapshots.assembler.read_verified_silver_batch")
    def test_assemble_two_batches(self, mock_reader):
        batches = [
            {
                "table_name": "orders",
                "batch_id": 1,
                "watermark_type": "ID",
                "start_timestamp": None,
                "start_id": 0,
                "end_timestamp": None,
                "end_id": 10,
            },
            {
                "table_name": "orders",
                "batch_id": 2,
                "watermark_type": "ID",
                "start_timestamp": None,
                "start_id": 10,
                "end_timestamp": None,
                "end_id": 20,
            },
        ]

        df1 = self.spark.createDataFrame(
            [(1, "PENDING", 1)],
            ["order_id", "status", "_batch_sequence"],
        )

        df2 = self.spark.createDataFrame(
            [(1, "SHIPPED", 2)],
            ["order_id", "status", "_batch_sequence"],
        )

        mock_reader.side_effect = [df1, df2]

        result = assemble_silver_batches(
            spark=self.spark,
            client=MagicMock(),
            bucket="test-bucket",
            batches=batches,
        )

        actual = {
            (row.order_id, row.status, row._batch_sequence)
            for row in result.collect()
        }

        self.assertEqual(
            actual,
            {
                (1, "PENDING", 1),
                (1, "SHIPPED", 2),
            },
        )

        self.assertEqual(mock_reader.call_count, 2)


    @patch("silver.snapshots.assembler.read_verified_silver_batch")
    def test_reject_duplicate_batch_ids(self, mock_reader):
        batches = [
            {
            "table_name": "payments",
            "batch_id": 1,
            "watermark_type": "ID",
            "start_timestamp": None,
            "start_id": 0,
            "end_timestamp": None,
            "end_id": 10,
            },
            {
            "table_name": "payments",
            "batch_id": 1,
            "watermark_type": "ID",
            "start_timestamp": None,
            "start_id": 10,
            "end_timestamp": None,
            "end_id": 20,
            },
        ]

        with self.assertRaisesRegex(ValueError, "Duplicate batch IDs"):
            assemble_silver_batches(
            self.spark,
            MagicMock(),
            "test-bucket",
            batches,
            )

        mock_reader.assert_not_called()


    @patch("silver.snapshots.assembler.read_verified_silver_batch")
    def test_reject_incorrect_batch_order(self, mock_reader):
        batches = [
            {
            "table_name": "payments",
            "batch_id": 2,
            "watermark_type": "ID",
            "start_timestamp": None,
            "start_id": 10,
            "end_timestamp": None,
            "end_id": 20,
            },
            {
            "table_name": "payments",
            "batch_id": 1,
            "watermark_type": "ID",
            "start_timestamp": None,
            "start_id": 0,
            "end_timestamp": None,
            "end_id": 10,
            },
        ]

        with self.assertRaisesRegex(
        ValueError,
        "Batches are not in watermark order",
        ):
            assemble_silver_batches(
                self.spark,
                MagicMock(),
                "test-bucket",
                batches,
            )

        mock_reader.assert_not_called()

    @patch("silver.snapshots.assembler.read_verified_silver_batch")
    def test_reject_schema_mismatch(self, mock_reader):
        batches = [
            {
            "table_name": "payments",
            "batch_id": 1,
            "watermark_type": "ID",
            "start_timestamp": None,
            "start_id": 0,
            "end_timestamp": None,
            "end_id": 10,
            },
            {
            "table_name": "payments",
            "batch_id": 2,
            "watermark_type": "ID",
            "start_timestamp": None,
            "start_id": 10,
            "end_timestamp": None,
            "end_id": 20,
            },
        ]

        df1 = self.spark.createDataFrame(
            [(1, "SUCCESS", 1)],
            ["payment_id", "status", "_batch_sequence"],
        )

        df2 = self.spark.createDataFrame(
            [(2, "FAILED", 100.0, 2)],
            ["payment_id", "status", "amount", "_batch_sequence"],
        )

        mock_reader.side_effect = [df1, df2]

        with self.assertRaisesRegex(
            ValueError,
            "Silver schema mismatch in batch 2",
        ):
            assemble_silver_batches(
                spark=self.spark,
                client=MagicMock(),
                bucket="test-bucket",
                batches=batches,
            )

        self.assertEqual(mock_reader.call_count, 2)

if __name__ == "__main__":
    unittest.main()