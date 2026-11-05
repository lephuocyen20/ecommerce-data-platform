
import unittest
from unittest.mock import MagicMock, patch

from gold.implementation.storage.contract import (
    GoldStorageContractError,
    validate_gold_snapshot_id,
)
from gold.implementation.storage.writer import (
    write_gold_snapshot_parquet,
)


VALID_ID = "gold_snap_0123456789abcdef0123456789abcdef"


class TestGoldStorageWriter(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.df = MagicMock()
        self.bucket = "test-gold-bucket"

        self.df.write.mode.return_value = self.df.write

    def write(self, **kwargs):
        arguments = {
            "spark": self.spark,
            "gold_df": self.df,
            "bucket": self.bucket,
            "dataset_name": "daily_sales",
            "snapshot_id": VALID_ID,
        }
        arguments.update(kwargs)

        return write_gold_snapshot_parquet(**arguments)

    def test_valid_write(self):
        result = self.write()

        self.assertEqual(result["status"], "DATA_WRITTEN")
        self.df.write.parquet.assert_called_once()

    def test_auto_generated_snapshot_id(self):
        result = self.write(snapshot_id=None)

        validate_gold_snapshot_id(result["snapshot_id"])
        self.assertTrue(
            result["snapshot_id"].startswith("gold_snap_")
        )

    def test_invalid_snapshot_id(self):
        with self.assertRaises(GoldStorageContractError):
            self.write(snapshot_id="../invalid")

        self.df.write.parquet.assert_not_called()

    def test_invalid_dataset(self):
        with self.assertRaises(GoldStorageContractError):
            self.write(dataset_name="unknown_dataset")

        self.df.write.parquet.assert_not_called()

    def test_empty_bucket(self):
        with self.assertRaises(GoldStorageContractError):
            self.write(bucket="")

        self.df.write.parquet.assert_not_called()

    def test_spark_write_failure(self):
        self.df.write.parquet.side_effect = RuntimeError(
            "Simulated Spark write failure"
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "Simulated Spark write failure",
        ):
            self.write()

    def test_errorifexists_mode(self):
        self.write()

        self.df.write.mode.assert_called_once_with(
            "errorifexists"
        )

    def test_output_path(self):
        result = self.write()

        expected = (
            "s3a://test-gold-bucket/"
            "gold/daily_sales/snapshots/"
            f"{VALID_ID}/data/"
        )

        self.df.write.parquet.assert_called_once_with(
            expected
        )

        self.assertEqual(result["output_path"], expected)

    def test_manifest_not_published(self):
        with patch(
            "silver.snapshots.publisher.publish_manifest"
        ) as publish_mock:
            self.write()

            publish_mock.assert_not_called()

    def test_return_metadata(self):
        result = self.write()

        self.assertEqual(result["dataset_name"], "daily_sales")
        self.assertEqual(result["snapshot_id"], VALID_ID)
        self.assertEqual(result["bucket"], self.bucket)

        self.assertEqual(
            result["manifest_key"],
            (
                "gold/daily_sales/snapshots/"
                f"{VALID_ID}/manifest.json"
            ),
        )

        self.assertEqual(
            result["data_prefix"],
            (
                "gold/daily_sales/snapshots/"
                f"{VALID_ID}/data/"
            ),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
