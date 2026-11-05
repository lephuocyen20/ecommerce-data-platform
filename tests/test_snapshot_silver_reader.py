import unittest
from pyspark.sql import SparkSession
from silver.snapshots import silver_reader
from unittest.mock import MagicMock, patch
from pyspark.sql.readwriter import DataFrameReader
from silver.snapshots.silver_reader import (
    read_verified_silver_batch,
)


class TestSilverReader(unittest.TestCase):

    @patch(
        "silver.snapshots.silver_reader.get_completion_manifest"
    )
    def test_missing_manifest_rejected(self, mock_get_manifest):
        mock_get_manifest.return_value = None

        batch = {
            "table_name": "orders",
            "batch_id": 101,
            "object_key": (
                "bronze_incremental/orders/"
                "batch_id=101/data.csv"
            ),
            "checksum_sha256": "test-checksum",
        }

        with self.assertRaisesRegex(
            ValueError,
            "Missing Silver manifest",
        ):
            read_verified_silver_batch(
                spark=MagicMock(),
                client=MagicMock(),
                bucket="ecommerce",
                batch=batch,
                batch_sequence=1,
            )

        mock_get_manifest.assert_called_once_with(
            unittest.mock.ANY,
            "ecommerce",
            "silver_incremental/orders/"
            "batch_id=101/_BATCH_METADATA.json",
        )

    @patch("silver.snapshots.silver_reader.F.lit")
    @patch(
    "silver.snapshots.silver_reader.verify_published_manifest"
)
    @patch(
    "silver.snapshots.silver_reader.get_completion_manifest"
)
    def test_verified_batch_read_success(
        self,
        mock_get_manifest,
        mock_verify,
        mock_lit,
    ):
        spark = MagicMock()
        client = MagicMock()

        batch = {
        "table_name": "orders",
        "batch_id": 101,
        "object_key": (
            "bronze_incremental/orders/"
            "batch_id=101/data.csv"
        ),
        "checksum_sha256": "test-checksum",
    }

        manifest = {
        "data_prefix": (
            "silver_incremental/orders/"
            "batch_id=101/runs/abc123/"
        )
    }

        mock_get_manifest.return_value = manifest
        mock_verify.return_value = manifest

        result = read_verified_silver_batch(
            spark=spark,
            client=client,
            bucket="test-bucket",
            batch=batch,
            batch_sequence=5,
        )

        mock_lit.assert_called_once_with(5)

        spark.read.parquet.return_value.withColumn.assert_called_once_with(
            "_batch_sequence",
            mock_lit.return_value,
            )

        self.assertIs(
    result,
    spark.read.parquet.return_value.withColumn.return_value,
)

        spark.read.parquet.assert_called_once_with(
            "s3a://test-bucket/"
            "silver_incremental/orders/"
            "batch_id=101/runs/abc123/"
        )

        spark.read.parquet.return_value.withColumn.assert_called_once()
        self.assertIs(
            result,
            spark.read.parquet.return_value.withColumn.return_value,
)

    def test_batch_sequence_with_real_spark(self):
        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("snapshot-silver-reader-test")
            .getOrCreate()
        )

        try:
            source_df = spark.createDataFrame(
                [
                    (101, "PENDING"),
                    (102, "SHIPPED"),
                ],
                ["order_id", "status"],
            )

            manifest = {
                "data_prefix": (
                    "silver_incremental/orders/"
                    "batch_id=10/runs/"
                    + "a" * 32 + "/"
                )
            }

            batch = {
            "table_name": "orders",
            "batch_id": 10,
            "object_key": (
                "bronze_incremental/orders/"
                "batch_id=10/data.csv"
            ),
            "checksum_sha256": "test-checksum",
            }

            with patch.object(
                silver_reader,
                "get_completion_manifest",
                return_value=manifest,
            ), patch.object(
                silver_reader,
            "verify_published_manifest",
                return_value=manifest,
            ), patch.object(
                DataFrameReader,
                "parquet",
                return_value=source_df,
        ):
                result = read_verified_silver_batch(
                spark=spark,
                client=MagicMock(),
                bucket="test-bucket",
                batch=batch,
                batch_sequence=5,
            )

            self.assertEqual(result.count(), 2)

            sequences = {
            row["_batch_sequence"]
            for row in result.select("_batch_sequence").collect()
        }

            self.assertEqual(sequences, {5})

        finally:
            spark.stop()



if __name__ == "__main__":
    unittest.main()