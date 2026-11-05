
import unittest

from gold.implementation.storage.contract import (
    GoldStorageContractError,
    get_gold_storage_paths,
    build_gold_storage_metadata,
    validate_gold_snapshot_id,
    validate_source_snapshots,
)


SNAPSHOT_ID = (
    "gold_snap_0123456789abcdef0123456789abcdef"
)

SOURCE_SNAPSHOTS = {
    "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
    "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
}


class TestGoldStorageContract(unittest.TestCase):

    def test_valid_snapshot_id(self):
        self.assertEqual(
            validate_gold_snapshot_id(SNAPSHOT_ID),
            SNAPSHOT_ID,
        )

    def test_invalid_snapshot_id(self):
        invalid_ids = [
            "",
            None,
            "snap_123",
            "gold_snap_123",
            "gold_snap_" + "g" * 32,
            "../gold_snap_0123456789abcdef0123456789abcdef",
        ]

        for snapshot_id in invalid_ids:
            with self.subTest(snapshot_id=snapshot_id):
                with self.assertRaises(
                    GoldStorageContractError
                ):
                    validate_gold_snapshot_id(snapshot_id)

    def test_storage_paths(self):
        paths = get_gold_storage_paths(
            "daily_sales",
            SNAPSHOT_ID,
        )

        expected_prefix = (
            "gold/daily_sales/snapshots/"
            + SNAPSHOT_ID
        )

        self.assertEqual(
            paths["snapshot_prefix"],
            expected_prefix,
        )

        self.assertEqual(
            paths["data_prefix"],
            expected_prefix + "/data/",
        )

        self.assertEqual(
            paths["manifest_key"],
            expected_prefix + "/manifest.json",
        )

    def test_invalid_dataset(self):
        with self.assertRaises(
            GoldStorageContractError
        ):
            get_gold_storage_paths(
                "unknown_dataset",
                SNAPSHOT_ID,
            )

    def test_valid_source_snapshots(self):
        self.assertTrue(
            validate_source_snapshots(
                "daily_sales",
                SOURCE_SNAPSHOTS,
            )
        )

    def test_missing_source(self):
        with self.assertRaises(
            GoldStorageContractError
        ):
            validate_source_snapshots(
                "daily_sales",
                {"orders": SOURCE_SNAPSHOTS["orders"]},
            )

    def test_extra_source(self):
        invalid = {
            **SOURCE_SNAPSHOTS,
            "customers": "snap_extra",
        }

        with self.assertRaises(
            GoldStorageContractError
        ):
            validate_source_snapshots(
                "daily_sales",
                invalid,
            )

    def test_invalid_source_snapshot_id(self):
        invalid = {
            **SOURCE_SNAPSHOTS,
            "orders": "",
        }

        with self.assertRaises(
            GoldStorageContractError
        ):
            validate_source_snapshots(
                "daily_sales",
                invalid,
            )

    def test_metadata_lineage(self):
        metadata = build_gold_storage_metadata(
            "daily_sales",
            SNAPSHOT_ID,
            SOURCE_SNAPSHOTS,
        )

        self.assertEqual(
            metadata["layer"],
            "gold",
        )

        self.assertEqual(
            metadata["dataset_name"],
            "daily_sales",
        )

        self.assertEqual(
            metadata["snapshot_id"],
            SNAPSHOT_ID,
        )

        self.assertEqual(
            metadata["primary_key"],
            ["order_date"],
        )

        self.assertEqual(
            metadata["source_snapshots"],
            SOURCE_SNAPSHOTS,
        )

        self.assertEqual(
            metadata["manifest_version"],
            "1.0",
        )

    def test_metadata_is_not_alias(self):
        metadata = build_gold_storage_metadata(
            "daily_sales",
            SNAPSHOT_ID,
            SOURCE_SNAPSHOTS,
        )

        metadata["source_snapshots"]["orders"] = "changed"

        self.assertEqual(
            SOURCE_SNAPSHOTS["orders"],
            "snap_59aa507d4eb73cfc989daaff65fa0c8d",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
