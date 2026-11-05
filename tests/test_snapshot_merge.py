import unittest
from pyspark.sql import SparkSession
from silver.snapshots.merge import merge_silver_batches


class TestSnapshotMerge(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("snapshot-merge-test")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_latest_batch_wins(self):
        df = self.spark.createDataFrame(
            [
                (1, "PENDING", 1, 1),
                (1, "DELIVERED", 2, 2),
                (2, "SHIPPED", 1, 1),
            ],
            ["order_id", "status", "version", "_batch_sequence"],
        )

        result = merge_silver_batches(
            df, "order_id", "version"
        )

        actual = {
            row.order_id: row.status
            for row in result.collect()
        }

        self.assertEqual(
            actual,
            {1: "DELIVERED", 2: "SHIPPED"},
        )

    def test_same_watermark_latest_batch_wins(self):
        df = self.spark.createDataFrame(
            [
                (1, "PROCESSING", 100, 1),
                (1, "SHIPPED", 100, 2),
                (2, "PENDING", 50, 1),
            ],
            ["order_id", "status", "version", "_batch_sequence"],
        )

        result = merge_silver_batches(
            df,
            "order_id",
            "version",
        )

        actual = {
            row.order_id: row.status
            for row in result.collect()
        }

        self.assertEqual(
            actual,
            {
                1: "SHIPPED",
                2: "PENDING",
            },
        )
    def test_reject_ambiguous_duplicate_versions(self):
        df = self.spark.createDataFrame(
            [
                (1, "PROCESSING", 100, 2),
                    (1, "SHIPPED", 100, 2),
            ],
            ["order_id", "status", "version", "_batch_sequence"],
        )

        with self.assertRaisesRegex(
            ValueError,
            "Ambiguous duplicate record versions",
        ):
            merge_silver_batches(
                df,
                "order_id",
                "version",
            )

if __name__ == "__main__":
    unittest.main()