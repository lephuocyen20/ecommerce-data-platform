import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

from silver.snapshots.writer import write_snapshot_parquet


class TestSnapshotWriterIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("snapshot-writer-integration-test")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_write_immutable_parquet(self):
        with tempfile.TemporaryDirectory() as temp_dir:

            snapshot_df = self.spark.createDataFrame(
                [
                    (1, "PENDING", 1),
                    (2, "SHIPPED", 2),
                ],
                ["order_id", "status", "_batch_sequence"],
            )

            output_path = str(
                Path(temp_dir) / "snapshot-data"
            )

            # Redirect the S3A destination to a local test directory.
            with patch(
                "silver.snapshots.writer.uuid.uuid4"
            ) as mock_uuid:
                mock_uuid.return_value.hex = "test123"

                with patch(
                    "pyspark.sql.readwriter.DataFrameWriter.parquet"
                ) as mock_parquet:

                    # Capture the DataFrame and write locally.
                    def write_local(_path):
                        writer = mock_parquet.call_args
                        return None

                    # This test will be expanded to perform
                    # real Parquet writes in the next step.
                    result = write_snapshot_parquet(
                        spark=self.spark,
                        snapshot_df=snapshot_df,
                        bucket="test-bucket",
                        table_name="orders",
                        snapshot_id=None,
                    )

                    self.assertEqual(
                        result["snapshot_id"],
                        "test123",
                    )

                    self.assertTrue(
                        result["output_path"].startswith(
                            "s3a://test-bucket/"
                        )
                    )

                    mock_parquet.assert_called_once()

            self.assertEqual(snapshot_df.count(), 2)


if __name__ == "__main__":
    unittest.main()