
import unittest
from copy import deepcopy

from gold.implementation.contracts.datasets import (
    get_gold_contract,
)
from gold.implementation.storage.contract import (
    GoldStorageContractError,
)
from gold.implementation.storage.manifest import (
    build_gold_manifest,
    validate_gold_manifest_schema,
    validate_gold_parquet_objects,
)

import hashlib
from unittest.mock import MagicMock, patch

from gold.implementation.storage.objects import (
    calculate_object_sha256,
    discover_gold_parquet_objects,
)



SNAPSHOT_ID = "gold_snap_0123456789abcdef0123456789abcdef"

SOURCE_SNAPSHOTS = {
    "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
    "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
}

DATA_PREFIX = (
    f"gold/daily_sales/snapshots/{SNAPSHOT_ID}/data/"
)

VALID_OBJECTS = [
    {
        "object_key": DATA_PREFIX + "part-00000.parquet",
        "size": 1024,
        "sha256": "a" * 64,
    },
    {
        "object_key": DATA_PREFIX + "part-00001.parquet",
        "size": 2048,
        "sha256": "b" * 64,
    },
]


class TestGoldStorageManifest(unittest.TestCase):

    def setUp(self):
        self.schema = dict(
            get_gold_contract("daily_sales")["columns"]
        )

    def build(self, **overrides):
        arguments = {
            "dataset_name": "daily_sales",
            "snapshot_id": SNAPSHOT_ID,
            "source_snapshots": deepcopy(SOURCE_SNAPSHOTS),
            "row_count": 205,
            "schema": deepcopy(self.schema),
            "parquet_objects": deepcopy(VALID_OBJECTS),
            "created_at": "2026-10-26T00:00:00+00:00",
        }
        arguments.update(overrides)
        return build_gold_manifest(**arguments)

    def test_valid_manifest(self):
        manifest = self.build()

        self.assertEqual(manifest["layer"], "gold")
        self.assertEqual(manifest["status"], "COMPLETED")
        self.assertEqual(manifest["row_count"], 205)
        self.assertEqual(manifest["parquet_file_count"], 2)

    def test_manifest_identity(self):
        manifest = self.build()

        self.assertEqual(manifest["dataset_name"], "daily_sales")
        self.assertEqual(manifest["snapshot_id"], SNAPSHOT_ID)
        self.assertEqual(manifest["manifest_version"], "1.0")
        self.assertEqual(manifest["contract_version"], "1.0")

    def test_manifest_paths(self):
        manifest = self.build()

        self.assertEqual(manifest["data_prefix"], DATA_PREFIX)
        self.assertEqual(
            manifest["manifest_key"],
            f"gold/daily_sales/snapshots/{SNAPSHOT_ID}/manifest.json",
        )

    def test_source_lineage(self):
        manifest = self.build()

        self.assertEqual(
            manifest["source_snapshots"],
            SOURCE_SNAPSHOTS,
        )
        self.assertEqual(manifest["primary_key"], ["order_date"])

    def test_zero_row_count_allowed(self):
        self.assertEqual(
            self.build(row_count=0)["row_count"],
            0,
        )

    def test_negative_row_count_rejected(self):
        with self.assertRaises(GoldStorageContractError):
            self.build(row_count=-1)

    def test_boolean_row_count_rejected(self):
        with self.assertRaises(GoldStorageContractError):
            self.build(row_count=True)

    def test_invalid_schema_rejected(self):
        schema = deepcopy(self.schema)
        schema["order_count"] = "string"

        with self.assertRaises(GoldStorageContractError):
            self.build(schema=schema)

    def test_missing_schema_column_rejected(self):
        schema = deepcopy(self.schema)
        del schema["order_date"]

        with self.assertRaises(GoldStorageContractError):
            self.build(schema=schema)

    def test_invalid_source_lineage_rejected(self):
        with self.assertRaises(GoldStorageContractError):
            self.build(source_snapshots={"orders": "snap_123"})

    def test_empty_objects_rejected(self):
        with self.assertRaises(GoldStorageContractError):
            self.build(parquet_objects=[])

    def test_invalid_object_size_rejected(self):
        objects = deepcopy(VALID_OBJECTS)
        objects[0]["size"] = 0

        with self.assertRaises(GoldStorageContractError):
            self.build(parquet_objects=objects)

    def test_invalid_sha256_rejected(self):
        objects = deepcopy(VALID_OBJECTS)
        objects[0]["sha256"] = "invalid"

        with self.assertRaises(GoldStorageContractError):
            self.build(parquet_objects=objects)

    def test_duplicate_objects_rejected(self):
        objects = deepcopy(VALID_OBJECTS)
        objects.append(deepcopy(objects[0]))

        with self.assertRaises(GoldStorageContractError):
            self.build(parquet_objects=objects)

    def test_unsorted_objects_rejected(self):
        with self.assertRaises(GoldStorageContractError):
            self.build(
                parquet_objects=list(reversed(VALID_OBJECTS))
            )

    def test_object_outside_prefix_rejected(self):
        objects = deepcopy(VALID_OBJECTS)
        objects[0]["object_key"] = (
            "silver_snapshots/orders/data/part-00000.parquet"
        )

        with self.assertRaises(GoldStorageContractError):
            self.build(parquet_objects=objects)

    def test_naive_timestamp_rejected(self):
        with self.assertRaises(GoldStorageContractError):
            self.build(created_at="2026-10-26T00:00:00")

    def test_invalid_timestamp_rejected(self):
        with self.assertRaises(GoldStorageContractError):
            self.build(created_at="not-a-timestamp")

    def test_auto_timestamp(self):
        manifest = self.build(created_at=None)

        self.assertIsInstance(manifest["created_at"], str)
        self.assertIn("+00:00", manifest["created_at"])

    def test_input_isolation(self):
        sources = deepcopy(SOURCE_SNAPSHOTS)
        schema = deepcopy(self.schema)
        objects = deepcopy(VALID_OBJECTS)

        manifest = self.build(
            source_snapshots=sources,
            schema=schema,
            parquet_objects=objects,
        )

        manifest["source_snapshots"]["orders"] = "changed"
        manifest["schema"]["order_count"] = "changed"
        manifest["parquet_objects"][0]["size"] = 999

        self.assertEqual(sources, SOURCE_SNAPSHOTS)
        self.assertEqual(schema, self.schema)
        self.assertEqual(objects, VALID_OBJECTS)

    def test_schema_validator(self):
        self.assertTrue(
            validate_gold_manifest_schema(
                "daily_sales",
                self.schema,
            )
        )

    def test_object_validator(self):
        self.assertTrue(
            validate_gold_parquet_objects(
                VALID_OBJECTS,
                DATA_PREFIX,
            )
        )


    def test_transformation_version_present(self):
        from unittest.mock import patch

        from gold.implementation.storage.manifest import (
            build_gold_manifest,
        )

        from gold.implementation.identity import (
            TRANSFORMATION_VERSION,
        )

        with patch(
            "gold.implementation.storage.manifest."
            "build_gold_storage_metadata"
        ) as metadata_mock, patch(
            "gold.implementation.storage.manifest."
            "validate_gold_manifest_schema"
        ), patch(
            "gold.implementation.storage.manifest."
            "validate_gold_parquet_objects"
        ):
            metadata_mock.return_value = {
                "data_prefix": "gold/test/data/",
            }

            manifest = build_gold_manifest(
                dataset_name="daily_sales",
                snapshot_id="gold_snap_" + "a" * 32,
                source_snapshots={},
                row_count=0,
                schema={},
                parquet_objects=[{
                    "object_key": "gold/test/data/part.parquet",
                    "size": 1,
                    "sha256": "a" * 64,
                }],
            )

        self.assertEqual(
            manifest["transformation_version"],
            TRANSFORMATION_VERSION,
        )

    def test_custom_transformation_version(self):
        from unittest.mock import patch

        from gold.implementation.storage.manifest import (
            build_gold_manifest,
        )

        with patch(
            "gold.implementation.storage.manifest."
            "build_gold_storage_metadata"
        ) as metadata_mock, patch(
            "gold.implementation.storage.manifest."
            "validate_gold_manifest_schema"
        ), patch(
            "gold.implementation.storage.manifest."
            "validate_gold_parquet_objects"
        ):
            metadata_mock.return_value = {
                "data_prefix": "gold/test/data/",
            }

            manifest = build_gold_manifest(
                dataset_name="daily_sales",
                snapshot_id="gold_snap_" + "a" * 32,
                source_snapshots={},
                row_count=0,
                schema={},
                parquet_objects=[{
                    "object_key": "gold/test/data/part.parquet",
                    "size": 1,
                    "sha256": "a" * 64,
                }],
                transformation_version="2.0",
            )

        self.assertEqual(
            manifest["transformation_version"],
            "2.0",
        )

    def test_invalid_transformation_version_rejected(self):
        from unittest.mock import patch

        from gold.implementation.storage.manifest import (
            build_gold_manifest,
        )

        from gold.implementation.storage.contract import (
            GoldStorageContractError,
        )

        with patch(
            "gold.implementation.storage.manifest."
            "build_gold_storage_metadata"
        ) as metadata_mock, patch(
            "gold.implementation.storage.manifest."
            "validate_gold_manifest_schema"
        ), patch(
            "gold.implementation.storage.manifest."
            "validate_gold_parquet_objects"
        ):
            metadata_mock.return_value = {
                "data_prefix": "gold/test/data/",
            }

            with self.assertRaises(GoldStorageContractError):
                build_gold_manifest(
                    dataset_name="daily_sales",
                    snapshot_id="gold_snap_" + "a" * 32,
                    source_snapshots={},
                    row_count=0,
                    schema={},
                    parquet_objects=[{
                        "object_key": "gold/test/data/part.parquet",
                        "size": 1,
                        "sha256": "a" * 64,
                    }],
                    transformation_version="",
                )



class TestGoldObjectDiscovery(unittest.TestCase):

    def test_sha256_streaming(self):
        client = MagicMock()
        response = MagicMock()

        response.read.side_effect = [
            b"hello ",
            b"gold",
            b"",
        ]

        client.get_object.return_value = response

        actual = calculate_object_sha256(
            client,
            "test-bucket",
            "gold/test.parquet",
        )

        expected = hashlib.sha256(
            b"hello gold"
        ).hexdigest()

        self.assertEqual(actual, expected)
        response.close.assert_called_once()
        response.release_conn.assert_called_once()

    def test_sha256_cleanup_on_failure(self):
        client = MagicMock()
        response = MagicMock()

        response.read.side_effect = RuntimeError(
            "Read failed"
        )

        client.get_object.return_value = response

        with self.assertRaisesRegex(
            RuntimeError,
            "Read failed",
        ):
            calculate_object_sha256(
                client,
                "test-bucket",
                "gold/test.parquet",
            )

        response.close.assert_called_once()
        response.release_conn.assert_called_once()

    @patch(
        "gold.implementation.storage.objects."
        "calculate_object_sha256"
    )
    @patch(
        "gold.implementation.storage.objects."
        "discover_snapshot_parquet_objects"
    )
    def test_discover_with_sha256(
        self,
        discover_mock,
        sha_mock,
    ):
        discover_mock.return_value = [
            {
                "object_key": DATA_PREFIX + "part-00000.parquet",
                "size": 1024,
            },
            {
                "object_key": DATA_PREFIX + "part-00001.parquet",
                "size": 2048,
            },
        ]

        sha_mock.side_effect = [
            "a" * 64,
            "b" * 64,
        ]

        client = MagicMock()

        result = discover_gold_parquet_objects(
            client,
            "test-bucket",
            DATA_PREFIX,
        )

        self.assertEqual(result, VALID_OBJECTS)
        self.assertEqual(sha_mock.call_count, 2)

    @patch(
        "gold.implementation.storage.objects."
        "discover_snapshot_parquet_objects"
    )
    def test_empty_objects_rejected(self, discover_mock):
        discover_mock.return_value = []

        with self.assertRaises(GoldStorageContractError):
            discover_gold_parquet_objects(
                MagicMock(),
                "test-bucket",
                DATA_PREFIX,
            )

    @patch(
        "gold.implementation.storage.objects."
        "calculate_object_sha256"
    )
    @patch(
        "gold.implementation.storage.objects."
        "discover_snapshot_parquet_objects"
    )
    def test_invalid_sha256_rejected(
        self,
        discover_mock,
        sha_mock,
    ):
        discover_mock.return_value = [
            {
                "object_key": DATA_PREFIX + "part-00000.parquet",
                "size": 1024,
            },
        ]

        sha_mock.return_value = "invalid"

        with self.assertRaises(GoldStorageContractError):
            discover_gold_parquet_objects(
                MagicMock(),
                "test-bucket",
                DATA_PREFIX,
            )



if __name__ == "__main__":
    unittest.main(verbosity=2)
