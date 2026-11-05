
import unittest
from copy import deepcopy
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StructField,
    StructType,
)

from gold.implementation.contracts.datasets import get_gold_contract
from gold.implementation.storage.manifest import build_gold_manifest
from gold.implementation.storage.verifier import (
    GoldSnapshotVerificationError,
    verify_gold_manifest_metadata,
    verify_gold_object_integrity,
    verify_gold_dataframe,
    verify_gold_snapshot,
)


SNAPSHOT_ID = "gold_snap_0123456789abcdef0123456789abcdef"

SOURCES = {
    "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
    "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
}

PREFIX = f"gold/daily_sales/snapshots/{SNAPSHOT_ID}/data/"

OBJECTS = [
    {
        "object_key": PREFIX + "part-00000.parquet",
        "size": 1024,
        "sha256": "a" * 64,
    }
]


def make_manifest():
    return build_gold_manifest(
        dataset_name="daily_sales",
        snapshot_id=SNAPSHOT_ID,
        source_snapshots=deepcopy(SOURCES),
        row_count=2,
        schema=dict(get_gold_contract("daily_sales")["columns"]),
        parquet_objects=deepcopy(OBJECTS),
        created_at="2026-10-26T00:00:00+00:00",
    )


class TestGoldVerifierMetadata(unittest.TestCase):

    def test_valid_manifest(self):
        self.assertTrue(
            verify_gold_manifest_metadata(make_manifest())
        )

    def test_non_dictionary_manifest(self):
        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_manifest_metadata(None)

    def test_wrong_snapshot_path(self):
        manifest = make_manifest()
        manifest["snapshot_prefix"] = "gold/wrong/path"

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_manifest_metadata(manifest)

    def test_invalid_lineage(self):
        manifest = make_manifest()
        manifest["source_snapshots"] = {
            "orders": "snap_123"
        }

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_manifest_metadata(manifest)

    def test_wrong_status(self):
        manifest = make_manifest()
        manifest["status"] = "PENDING"

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_manifest_metadata(manifest)

    def test_wrong_row_count(self):
        manifest = make_manifest()
        manifest["row_count"] = -1

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_manifest_metadata(manifest)

    def test_wrong_parquet_count(self):
        manifest = make_manifest()
        manifest["parquet_file_count"] = 99

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_manifest_metadata(manifest)

    def test_wrong_schema(self):
        manifest = make_manifest()
        manifest["schema"]["order_count"] = "string"

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_manifest_metadata(manifest)

    def test_missing_created_at(self):
        manifest = make_manifest()
        del manifest["created_at"]

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_manifest_metadata(manifest)


    def test_invalid_created_at(self):
        manifest = make_manifest()
        manifest["created_at"] = "invalid-timestamp"

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_manifest_metadata(manifest)


    def test_naive_created_at(self):
        manifest = make_manifest()
        manifest["created_at"] = "2026-10-26T10:00:00"

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_manifest_metadata(manifest)

class TestGoldVerifierObjects(unittest.TestCase):

    @patch(
        "gold.implementation.storage.verifier."
        "discover_gold_parquet_objects"
    )
    def test_matching_objects(self, discover_mock):
        discover_mock.return_value = deepcopy(OBJECTS)

        self.assertTrue(
            verify_gold_object_integrity(
                MagicMock(),
                "test-bucket",
                make_manifest(),
            )
        )

    @patch(
        "gold.implementation.storage.verifier."
        "discover_gold_parquet_objects"
    )
    def test_checksum_mismatch(self, discover_mock):
        objects = deepcopy(OBJECTS)
        objects[0]["sha256"] = "b" * 64
        discover_mock.return_value = objects

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_object_integrity(
                MagicMock(),
                "test-bucket",
                make_manifest(),
            )

    @patch(
        "gold.implementation.storage.verifier."
        "discover_gold_parquet_objects"
    )
    def test_missing_objects(self, discover_mock):
        discover_mock.return_value = []

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_object_integrity(
                MagicMock(),
                "test-bucket",
                make_manifest(),
            )

    @patch(
        "gold.implementation.storage.verifier."
        "discover_gold_parquet_objects"
    )
    def test_size_mismatch(self, discover_mock):
        objects = deepcopy(OBJECTS)
        objects[0]["size"] = 999
        discover_mock.return_value = objects

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_object_integrity(
                MagicMock(),
                "test-bucket",
                make_manifest(),
            )


class TestGoldVerifierDataframe(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("GoldVerifierUnitTests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def make_df(self, rows):
        schema = StructType([
            StructField("order_date", DateType(), True),
            StructField("order_count", LongType(), True),
            StructField("delivered_order_count", LongType(), True),
            StructField("cancelled_order_count", LongType(), True),
            StructField(
                "gross_merchandise_value",
                DecimalType(20, 2),
                True,
            ),
            StructField(
                "discount_amount",
                DecimalType(20, 2),
                True,
            ),
            StructField(
                "net_order_value",
                DecimalType(20, 2),
                True,
            ),
            StructField(
                "delivered_order_value",
                DecimalType(20, 2),
                True,
            ),
        ])

        return self.spark.createDataFrame(rows, schema)

    def valid_rows(self):
        return [
            (
                date(2026, 10, 1),
                2, 1, 0,
                Decimal("300000.00"),
                Decimal("10000.00"),
                Decimal("290000.00"),
                Decimal("200000.00"),
            ),
            (
                date(2026, 10, 2),
                1, 1, 0,
                Decimal("150000.00"),
                Decimal("0.00"),
                Decimal("150000.00"),
                Decimal("150000.00"),
            ),
        ]

    def test_valid_dataframe(self):
        df = self.make_df(self.valid_rows())

        self.assertTrue(
            verify_gold_dataframe(df, make_manifest())
        )

    def test_duplicate_primary_key(self):
        rows = self.valid_rows()
        rows[1] = (rows[0][0], *rows[1][1:])

        df = self.make_df(rows)

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_dataframe(df, make_manifest())

    def test_null_primary_key(self):
        rows = self.valid_rows()
        rows[1] = (None, *rows[1][1:])

        df = self.make_df(rows)

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_dataframe(df, make_manifest())

    def test_row_count_mismatch(self):
        df = self.make_df(self.valid_rows()[:1])

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_dataframe(df, make_manifest())

    def test_column_mismatch(self):
        df = self.make_df(
            self.valid_rows()
        ).drop("order_count")

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_dataframe(df, make_manifest())

    def test_type_mismatch(self):
        df = self.make_df(
            self.valid_rows()
        ).withColumn(
            "order_count",
            F.col("order_count").cast("string"),
        )

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_dataframe(df, make_manifest())


class TestGoldVerifierOrchestration(unittest.TestCase):

    @patch(
        "gold.implementation.storage.verifier."
        "verify_gold_dataframe"
    )
    @patch(
        "gold.implementation.storage.verifier."
        "verify_gold_object_integrity"
    )
    @patch(
        "gold.implementation.storage.verifier."
        "verify_gold_manifest_metadata"
    )
    def test_full_verification(
        self,
        metadata_mock,
        object_mock,
        dataframe_mock,
    ):
        spark = MagicMock()
        client = MagicMock()
        manifest = make_manifest()

        result = verify_gold_snapshot(
            spark,
            client,
            "test-bucket",
            manifest,
        )

        self.assertEqual(result["status"], "VERIFIED")
        self.assertEqual(result["row_count"], 2)

        metadata_mock.assert_called_once()
        object_mock.assert_called_once()
        dataframe_mock.assert_called_once()

    @patch(
        "gold.implementation.storage.verifier."
        "verify_gold_manifest_metadata"
    )
    def test_metadata_failure_stops_pipeline(
        self,
        metadata_mock,
    ):
        metadata_mock.side_effect = (
            GoldSnapshotVerificationError(
                "Invalid metadata"
            )
        )

        spark = MagicMock()

        with self.assertRaises(GoldSnapshotVerificationError):
            verify_gold_snapshot(
                spark,
                MagicMock(),
                "test-bucket",
                make_manifest(),
            )

        spark.read.parquet.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
