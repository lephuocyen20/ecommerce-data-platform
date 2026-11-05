import unittest
from unittest.mock import patch, MagicMock

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)

from silver.snapshots.recovery_validator import (
    validate_recovery_snapshot,
)


class TestSnapshotRecoveryValidator(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.client = MagicMock()
        self.bucket = "test-bucket"
        self.table_name = "orders"

        self.data_prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snap_test001/data/"
        )

        self.schema = StructType([
            StructField("order_id", LongType(), True),
            StructField("status", StringType(), True),
        ])

    @patch(
        "silver.snapshots.recovery_validator."
        "build_silver_snapshot"
    )
    def test_success(self, mock_build):
        expected = MagicMock()
        actual = MagicMock()

        expected.columns = [
            "order_id",
            "status",
            "_batch_sequence",
        ]

        expected.drop.return_value = expected
        expected.schema = self.schema
        actual.schema = self.schema
        expected.count.return_value = 3
        actual.count.return_value = 3

        expected.select.return_value.exceptAll.return_value.limit.return_value.count.return_value = 0
        actual.select.return_value.exceptAll.return_value.limit.return_value.count.return_value = 0

        mock_build.return_value = expected
        self.spark.read.parquet.return_value = actual

        result = validate_recovery_snapshot(
            spark=self.spark,
            client=self.client,
            bucket=self.bucket,
            table_name=self.table_name,
            batches=[{"batch_id": "batch_001"}],
            data_prefix=self.data_prefix,
        )

        self.assertEqual(
            result["validation_status"],
            "VERIFIED",
        )
        self.assertEqual(result["row_count"], 3)

    def test_invalid_prefix(self):
        with self.assertRaisesRegex(
            ValueError,
            "Invalid recovery data prefix",
        ):
            validate_recovery_snapshot(
                spark=self.spark,
                client=self.client,
                bucket=self.bucket,
                table_name=self.table_name,
                batches=[],
                data_prefix="wrong/path/",
            )

    @patch(
        "silver.snapshots.recovery_validator."
        "build_silver_snapshot"
    )
    def test_schema_mismatch(self, mock_build):
        expected = MagicMock()
        actual = MagicMock()

        expected.columns = [
            "order_id",
            "_batch_sequence",
        ]
        expected.drop.return_value = expected

        expected.schema = self.schema
        actual.schema = StructType([
            StructField("order_id", LongType(), True),
        ])

        mock_build.return_value = expected
        self.spark.read.parquet.return_value = actual

        with self.assertRaisesRegex(
            ValueError,
            "schema mismatch",
        ):
            validate_recovery_snapshot(
                spark=self.spark,
                client=self.client,
                bucket=self.bucket,
                table_name=self.table_name,
                batches=[{"batch_id": "batch_001"}],
                data_prefix=self.data_prefix,
            )

    @patch(
        "silver.snapshots.recovery_validator."
        "build_silver_snapshot"
    )
    def test_row_count_mismatch(self, mock_build):
        expected = MagicMock()
        actual = MagicMock()

        expected.columns = [
            "order_id",
            "_batch_sequence",
        ]
        expected.drop.return_value = expected

        expected.schema = self.schema
        actual.schema = self.schema

        expected.count.return_value = 5
        actual.count.return_value = 3

        mock_build.return_value = expected
        self.spark.read.parquet.return_value = actual

        with self.assertRaisesRegex(
            ValueError,
            "row count mismatch",
        ):
            validate_recovery_snapshot(
                spark=self.spark,
                client=self.client,
                bucket=self.bucket,
                table_name=self.table_name,
                batches=[{"batch_id": "batch_001"}],
                data_prefix=self.data_prefix,
            )

    @patch(
        "silver.snapshots.recovery_validator."
        "build_silver_snapshot"
    )
    def test_data_mismatch(self, mock_build):
        expected = MagicMock()
        actual = MagicMock()

        expected.columns = [
            "order_id",
            "_batch_sequence",
        ]
        expected.drop.return_value = expected

        expected.schema = self.schema
        actual.schema = self.schema

        expected.count.return_value = 3
        actual.count.return_value = 3

        expected.select.return_value.exceptAll.return_value.limit.return_value.count.return_value = 1
        actual.select.return_value.exceptAll.return_value.limit.return_value.count.return_value = 0

        mock_build.return_value = expected
        self.spark.read.parquet.return_value = actual

        with self.assertRaisesRegex(
            ValueError,
            "data mismatch",
        ):
            validate_recovery_snapshot(
                spark=self.spark,
                client=self.client,
                bucket=self.bucket,
                table_name=self.table_name,
                batches=[{"batch_id": "batch_001"}],
                data_prefix=self.data_prefix,
            )


if __name__ == "__main__":
    unittest.main()