import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from silver.snapshots.object_discovery import (
    discover_snapshot_parquet_objects,
)


class TestSnapshotObjectDiscovery(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.bucket = "test-bucket"
        self.prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=test123/data/"
        )

    def make_object(self, name, size=1024):
        return SimpleNamespace(
            object_name=name,
            size=size,
        )

    def discover(self):
        return discover_snapshot_parquet_objects(
            client=self.client,
            bucket=self.bucket,
            data_prefix=self.prefix,
        )

    def test_discover_valid_parquet_objects(self):
        self.client.list_objects.return_value = [
            self.make_object(self.prefix + "part-00002.parquet", 2048),
            self.make_object(self.prefix + "part-00001.parquet", 1024),
            self.make_object(self.prefix + "_SUCCESS", 0),
        ]

        result = self.discover()

        self.assertEqual(
            result,
            [
                {
                    "object_key": self.prefix + "part-00001.parquet",
                    "size": 1024,
                },
                {
                    "object_key": self.prefix + "part-00002.parquet",
                    "size": 2048,
                },
            ],
        )

        self.client.list_objects.assert_called_once_with(
            self.bucket,
            prefix=self.prefix,
            recursive=True,
        )

    def test_reject_missing_parquet(self):
        self.client.list_objects.return_value = [
            self.make_object(self.prefix + "_SUCCESS", 0),
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Snapshot Parquet files are missing",
        ):
            self.discover()

    def test_reject_duplicate_objects(self):
        object_key = self.prefix + "part-00000.parquet"

        self.client.list_objects.return_value = [
            self.make_object(object_key),
            self.make_object(object_key),
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Duplicate snapshot Parquet objects",
        ):
            self.discover()

    def test_reject_invalid_object_size(self):
        self.client.list_objects.return_value = [
            self.make_object(
                self.prefix + "part-00000.parquet",
                size=0,
            ),
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Invalid snapshot Parquet object size",
        ):
            self.discover()

    def test_ignore_objects_outside_prefix(self):
        self.client.list_objects.return_value = [
            self.make_object(
                "silver_snapshots/other/part-00000.parquet"
            ),
            self.make_object(
                self.prefix + "part-00001.parquet"
            ),
        ]

        result = self.discover()

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result[0]["object_key"],
            self.prefix + "part-00001.parquet",
        )

    def test_reject_invalid_bucket(self):
        with self.assertRaisesRegex(
            ValueError,
            "Invalid snapshot bucket",
        ):
            discover_snapshot_parquet_objects(
                self.client,
                "",
                self.prefix,
            )

        self.client.list_objects.assert_not_called()

    def test_reject_invalid_prefix(self):
        with self.assertRaisesRegex(
            ValueError,
            "Invalid snapshot data prefix",
        ):
            discover_snapshot_parquet_objects(
                self.client,
                self.bucket,
                "invalid-prefix",
            )

        self.client.list_objects.assert_not_called()


if __name__ == "__main__":
    unittest.main()