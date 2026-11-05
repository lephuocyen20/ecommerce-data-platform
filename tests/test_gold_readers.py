
import unittest
from unittest.mock import MagicMock, patch

from gold.implementation.readers import (
    read_gold_source,
    read_gold_sources,
)


MODULE = "gold.implementation.readers"

SNAPSHOT_ID = "snap_test123"

MANIFEST = {
    "snapshot_id": SNAPSHOT_ID,
    "table_name": "orders",
    "data_prefix": (
        "silver_snapshots/orders/snapshots/"
        f"snapshot_id={SNAPSHOT_ID}/data/"
    ),
}


class TestGoldReaders(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.client = MagicMock()
        self.bucket = "ecommerce-data-lake"

    def read_orders(self, snapshot_id=SNAPSHOT_ID):
        return read_gold_source(
            spark=self.spark,
            client=self.client,
            bucket=self.bucket,
            dataset_name="daily_sales",
            table_name="orders",
            snapshot_id=snapshot_id,
        )

    def test_reject_unsupported_dataset(self):
        with self.assertRaises(ValueError):
            read_gold_source(
                self.spark,
                self.client,
                self.bucket,
                "unknown_dataset",
                "orders",
                SNAPSHOT_ID,
            )

    def test_reject_unexpected_source(self):
        with self.assertRaises(ValueError):
            read_gold_source(
                self.spark,
                self.client,
                self.bucket,
                "daily_sales",
                "payments",
                SNAPSHOT_ID,
            )

    def test_reject_empty_snapshot_id(self):
        with self.assertRaises(ValueError):
            self.read_orders(snapshot_id="")

    def test_reject_unpublished_snapshot(self):
        with patch(
            f"{MODULE}.inspect_snapshot_state",
            return_value={"state": "UNPUBLISHED"},
        ):
            with self.assertRaises(ValueError):
                self.read_orders()

    def test_reject_incomplete_snapshot(self):
        with patch(
            f"{MODULE}.inspect_snapshot_state",
            return_value={"state": "INCOMPLETE"},
        ):
            with self.assertRaises(ValueError):
                self.read_orders()

    def test_reject_new_snapshot(self):
        with patch(
            f"{MODULE}.inspect_snapshot_state",
            return_value={"state": "NEW"},
        ):
            with self.assertRaises(ValueError):
                self.read_orders()

    def test_reject_manifest_snapshot_mismatch(self):
        invalid_manifest = {
            **MANIFEST,
            "snapshot_id": "different_snapshot",
        }

        with (
            patch(
                f"{MODULE}.inspect_snapshot_state",
                return_value={"state": "PUBLISHED"},
            ),
            patch(
                f"{MODULE}.read_published_manifest",
                return_value=invalid_manifest,
            ),
        ):
            with self.assertRaises(ValueError):
                self.read_orders()

    def test_reject_manifest_table_mismatch(self):
        invalid_manifest = {
            **MANIFEST,
            "table_name": "customers",
        }

        with (
            patch(
                f"{MODULE}.inspect_snapshot_state",
                return_value={"state": "PUBLISHED"},
            ),
            patch(
                f"{MODULE}.read_published_manifest",
                return_value=invalid_manifest,
            ),
        ):
            with self.assertRaises(ValueError):
                self.read_orders()

    def test_read_verified_snapshot(self):
        with (
            patch(
                f"{MODULE}.inspect_snapshot_state",
                return_value={"state": "PUBLISHED"},
            ),
            patch(
                f"{MODULE}.read_published_manifest",
                return_value=MANIFEST,
            ),
            patch(
                f"{MODULE}.verify_snapshot",
                return_value=MANIFEST,
            ) as verify_mock,
        ):
            result = self.read_orders()

        verify_mock.assert_called_once()

        self.spark.read.parquet.assert_called_once_with(
            f"s3a://{self.bucket}/{MANIFEST['data_prefix']}"
        )

        self.assertEqual(
            result["snapshot_id"],
            SNAPSHOT_ID,
        )
        self.assertEqual(
            result["table_name"],
            "orders",
        )
        self.assertEqual(
            result["manifest"],
            MANIFEST,
        )

    def test_reject_missing_snapshot_sources(self):
        with self.assertRaises(ValueError):
            read_gold_sources(
                self.spark,
                self.client,
                self.bucket,
                "daily_sales",
                {"orders": SNAPSHOT_ID},
            )

    def test_reject_extra_snapshot_sources(self):
        with self.assertRaises(ValueError):
            read_gold_sources(
                self.spark,
                self.client,
                self.bucket,
                "payment_summary",
                {
                    "payments": SNAPSHOT_ID,
                    "orders": SNAPSHOT_ID,
                },
            )

    def test_read_all_required_sources(self):
        with patch(
            f"{MODULE}.read_gold_source"
        ) as reader_mock:
            result = read_gold_sources(
                self.spark,
                self.client,
                self.bucket,
                "daily_sales",
                {
                    "orders": "snap_orders",
                    "order_items": "snap_items",
                },
            )

        self.assertEqual(reader_mock.call_count, 2)

        self.assertEqual(
            set(result),
            {"orders", "order_items"},
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
