import os
from copy import deepcopy
from datetime import date
from decimal import Decimal
from uuid import uuid4

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StructField,
    StructType,
)

from gold.implementation.storage.objects import (
    calculate_object_sha256,
    discover_gold_parquet_objects,
)
from gold.implementation.contracts.datasets import get_gold_contract
from gold.implementation.storage.manifest import build_gold_manifest
from gold.implementation.storage.publisher import (
    GoldSnapshotPublicationError,
    publish_gold_snapshot,
)
from gold.implementation.storage.verifier import (
    GoldSnapshotVerificationError,
    verify_gold_snapshot,
)
from gold.implementation.storage.writer import write_gold_snapshot_parquet
from silver.snapshots.publisher import read_published_manifest


def normalized_rows(dataframe):
    return sorted(
        [tuple(row) for row in dataframe.collect()],
        key=lambda row: row[0],
    )


def main():
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]
    bucket = os.environ["BRONZE_BUCKET"]

    spark = SparkSession.builder.appName("GoldWriterE2E").getOrCreate()

    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", f"http://{endpoint}")
        hadoop_conf.set("fs.s3a.access.key", access_key)
        hadoop_conf.set("fs.s3a.secret.key", secret_key)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        schema = StructType(
            [
                StructField("order_date", DateType(), False),
                StructField("order_count", LongType(), False),
                StructField("delivered_order_count", LongType(), False),
                StructField("cancelled_order_count", LongType(), False),
                StructField("gross_merchandise_value", DecimalType(20, 2), False),
                StructField("discount_amount", DecimalType(20, 2), False),
                StructField("net_order_value", DecimalType(20, 2), False),
                StructField("delivered_order_value", DecimalType(20, 2), False),
            ]
        )

        rows = [
            (
                date(2026, 10, 1),
                2,
                1,
                0,
                Decimal("300000.00"),
                Decimal("10000.00"),
                Decimal("290000.00"),
                Decimal("200000.00"),
            ),
            (
                date(2026, 10, 2),
                1,
                1,
                0,
                Decimal("150000.00"),
                Decimal("0.00"),
                Decimal("150000.00"),
                Decimal("150000.00"),
            ),
        ]
        df = spark.createDataFrame(rows, schema)
        snapshot_id = "gold_snap_" + uuid4().hex

        result = write_gold_snapshot_parquet(
            spark=spark,
            gold_df=df,
            bucket=bucket,
            dataset_name="daily_sales",
            snapshot_id=snapshot_id,
        )
        assert result["status"] == "DATA_WRITTEN", result
        print("GOLD WRITE: PASS", flush=True)

        output_path = result["output_path"]
        read_df = spark.read.parquet(output_path)
        assert read_df.count() == len(rows), "Gold readback row count mismatch"
        assert read_df.columns == df.columns, "Gold readback column order mismatch"

        # Parquet can change nullability; compare field names and types instead.
        actual_types = [(f.name, f.dataType) for f in read_df.schema.fields]
        expected_types = [(f.name, f.dataType) for f in schema.fields]
        assert (
            actual_types == expected_types
        ), f"Gold schema mismatch: expected={expected_types}, actual={actual_types}"
        expected_rows = sorted(rows, key=lambda row: row[0])
        assert normalized_rows(read_df) == expected_rows, "Gold readback data mismatch"
        print("GOLD PARQUET READBACK: PASS", flush=True)

        # A second write to the same prefix must fail.
        try:
            write_gold_snapshot_parquet(
                spark=spark,
                gold_df=df,
                bucket=bucket,
                dataset_name="daily_sales",
                snapshot_id=snapshot_id,
            )
        except Exception as exc:
            print(f"EXPECTED OVERWRITE REJECTION: {type(exc).__name__}", flush=True)
            print("GOLD OVERWRITE REJECTION: PASS", flush=True)
        else:
            raise AssertionError("Gold Writer allowed snapshot overwrite")

        after_df = spark.read.parquet(output_path)
        assert after_df.count() == len(
            rows
        ), "Gold row count changed after rejected overwrite"
        assert (
            normalized_rows(after_df) == expected_rows
        ), "Gold data changed after rejected overwrite"
        print("GOLD IMMUTABILITY READBACK: PASS")

        # ============================================================
        # Gold Parquet Object Discovery + SHA-256 Integration Test
        # ============================================================

        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )

        objects = discover_gold_parquet_objects(
            client=client,
            bucket=bucket,
            data_prefix=result["data_prefix"],
        )

        assert objects, "Gold Parquet objects are missing"

        assert all(len(obj["sha256"]) == 64 for obj in objects)

        print("GOLD OBJECT DISCOVERY: PASS")

        for obj in objects:
            actual_sha256 = calculate_object_sha256(
                client=client,
                bucket=bucket,
                object_key=obj["object_key"],
            )

            assert (
                actual_sha256 == obj["sha256"]
            ), f"SHA-256 mismatch: {obj['object_key']}"

        print("GOLD SHA256 VERIFICATION: PASS")
        print("GOLD WRITER E2E: PASS 6/6")

        # ============================================================
        # Gold Snapshot Verifier E2E
        # ============================================================

        source_snapshots = {
            "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
            "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
        }

        manifest = build_gold_manifest(
            dataset_name="daily_sales",
            snapshot_id=result["snapshot_id"],
            source_snapshots=source_snapshots,
            row_count=read_df.count(),
            schema=dict(get_gold_contract("daily_sales")["columns"]),
            parquet_objects=objects,
        )

        verification = verify_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert verification["status"] == "VERIFIED"
        assert verification["snapshot_id"] == result["snapshot_id"]
        assert verification["row_count"] == len(rows)

        print("GOLD SNAPSHOT VERIFICATION: PASS", flush=True)

        # ============================================================
        # Negative test: reject an incorrect checksum
        # ============================================================

        invalid_manifest = deepcopy(manifest)
        invalid_manifest["parquet_objects"][0]["sha256"] = "0" * 64

        try:
            verify_gold_snapshot(
                spark=spark,
                client=client,
                bucket=bucket,
                manifest=invalid_manifest,
            )
        except GoldSnapshotVerificationError:
            print("GOLD CHECKSUM REJECTION: PASS", flush=True)
        else:
            raise AssertionError("Gold Verifier accepted an invalid checksum")

        # ============================================================
        # Gold Publisher E2E
        # ============================================================

        # 1. Publish verified snapshot
        publication = publish_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert publication["status"] == "PUBLISHED", publication

        print("GOLD FIRST PUBLICATION: PASS", flush=True)

        # 2. Read published manifest from MinIO
        published_manifest = read_published_manifest(
            client=client,
            bucket=bucket,
            manifest_key=manifest["manifest_key"],
        )

        assert (
            published_manifest == manifest
        ), "Published Gold manifest content mismatch"

        print("GOLD MANIFEST READBACK: PASS", flush=True)

        # 3. Re-publish identical manifest
        second_publication = publish_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert second_publication["status"] == ("ALREADY_PUBLISHED"), second_publication

        print("GOLD IDEMPOTENT PUBLICATION: PASS", flush=True)

        # 4. Reject conflicting manifest
        # Change only valid metadata, not Parquet data.
        conflicting_manifest = deepcopy(manifest)
        conflicting_manifest["created_at"] = "2026-10-27T00:00:00+00:00"

        assert conflicting_manifest != manifest

        try:
            publish_gold_snapshot(
                spark=spark,
                client=client,
                bucket=bucket,
                manifest=conflicting_manifest,
            )

        except GoldSnapshotPublicationError:
            print("GOLD PUBLICATION CONFLICT: PASS", flush=True)

        else:
            raise AssertionError("Gold Publisher accepted conflicting manifest")

        # 5. Reject invalid checksum before publication
        invalid_manifest = deepcopy(manifest)
        invalid_manifest["parquet_objects"][0]["sha256"] = "0" * 64

        try:
            publish_gold_snapshot(
                spark=spark,
                client=client,
                bucket=bucket,
                manifest=invalid_manifest,
            )

        except GoldSnapshotVerificationError:
            print("GOLD INVALID CHECKSUM REJECTION: PASS", flush=True)

        else:
            raise AssertionError("Gold Publisher accepted invalid checksum")

        # 6. Ensure rejected attempts did not change manifest
        final_manifest = read_published_manifest(
            client=client,
            bucket=bucket,
            manifest_key=manifest["manifest_key"],
        )

        assert (
            final_manifest == manifest
        ), "Published manifest changed after rejected attempts"

        print("GOLD PUBLISHED MANIFEST IMMUTABILITY: PASS", flush=True)

        print("GOLD PUBLISHER E2E: PASS 6/6", flush=True)

        print("GOLD WRITER + VERIFIER E2E: PASS 8/8", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
