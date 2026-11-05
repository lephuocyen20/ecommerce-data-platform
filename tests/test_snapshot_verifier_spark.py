import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)
import tempfile
from pathlib import Path

from silver.snapshots.verifier import verify_snapshot


class TestSnapshotVerifierSpark(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("snapshot-verifier-spark-test")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.client = MagicMock()
        self.bucket = "test-bucket"

        self.prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=test001/data/"
        )

        self.objects = [
            {
                "object_key": self.prefix + "part-00000.parquet",
                "size": 1024,
            }
        ]

        self.manifest = {
            "snapshot_id": "test001",
            "table_name": "orders",
            "status": "COMPLETED",
            "data_prefix": self.prefix,
            "row_count": 2,
            "parquet_file_count": 1,
            "parquet_objects": self.objects,
            "watermark_type": "TIMESTAMP",
        }

        self.schema = StructType([
            StructField("order_id", LongType(), True),
            StructField("status", StringType(), True),
        ])

    def make_df(self, rows):
        return self.spark.createDataFrame(
            rows,
            schema=self.schema,
        )

    def verify(self, df):
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_path = str(
                Path(temp_dir) / "snapshot-data"
            )

            # Write actual Spark DataFrame to local Parquet.
            df.write.mode("overwrite").parquet(parquet_path)

            # Mock only the external MinIO object discovery.
            with patch(
                "silver.snapshots.verifier."
                "discover_snapshot_parquet_objects",
                return_value=self.objects,
            ):
                # Redirect the S3A read to the local Parquet.
                # Keep Spark's actual parquet reader intact.
                original_parquet = type(self.spark.read).parquet

                def read_local(reader, *paths, **kwargs):
                    return original_parquet(
                        reader,
                        parquet_path,
                        **kwargs,
                    )

                with patch(
                    "pyspark.sql.readwriter.DataFrameReader.parquet",
                    new=read_local,
                ):
                    return verify_snapshot(
                    spark=self.spark,
                    client=self.client,
                    bucket=self.bucket,
                    manifest=self.manifest,
                    )

    def test_valid_snapshot(self):
        df = self.make_df([
            (1, "PENDING"),
            (2, "SHIPPED"),
        ])

        result = self.verify(df)

        self.assertIs(result, self.manifest)

    def test_reject_null_primary_key(self):
        df = self.make_df([
            (1, "PENDING"),
            (None, "SHIPPED"),
        ])

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot contains null primary keys",
        ):
            self.verify(df)

    def test_reject_duplicate_primary_key(self):
        df = self.make_df([
            (1, "PENDING"),
            (1, "SHIPPED"),
        ])

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot contains duplicate primary keys",
        ):
            self.verify(df)

    def test_reject_row_count_mismatch(self):
        df = self.make_df([
            (1, "PENDING"),
            (2, "SHIPPED"),
            (3, "DELIVERED"),
        ])

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot row count mismatch",
        ):
            self.verify(df)


if __name__ == "__main__":
    unittest.main()