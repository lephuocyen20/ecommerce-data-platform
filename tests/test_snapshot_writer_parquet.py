import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


class TestSnapshotWriterParquet(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("snapshot-writer-parquet-test")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_parquet_roundtrip_and_no_overwrite(self):

        with tempfile.TemporaryDirectory() as temp_dir:

            output_path = str(
                Path(temp_dir) / "snapshot-data"
            )

            snapshot_df = self.spark.createDataFrame(
                [
                    (1, "PENDING", 1),
                    (2, "SHIPPED", 2),
                ],
                ["order_id", "status", "_batch_sequence"],
            )

            # Remove internal merge column.
            output_df = snapshot_df.drop("_batch_sequence")

            # Write real Parquet.
            output_df.write.mode(
                "errorifexists"
            ).parquet(output_path)

            # Read back using Spark.
            restored_df = self.spark.read.parquet(output_path)

            self.assertEqual(
                restored_df.count(),
                2,
            )

            self.assertNotIn(
                "_batch_sequence",
                restored_df.columns,
            )

            actual = {
                row.order_id: row.status
                for row in restored_df.collect()
            }

            self.assertEqual(
                actual,
                {
                    1: "PENDING",
                    2: "SHIPPED",
                },
            )

            # Existing snapshot must not be overwritten.
            with self.assertRaises(AnalysisException):
                output_df.write.mode(
                    "errorifexists"
                ).parquet(output_path)


if __name__ == "__main__":
    unittest.main()