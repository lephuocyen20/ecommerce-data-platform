
import os
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

from gold.implementation.contracts.datasets import get_gold_contract
from gold.implementation.storage.contract import get_gold_storage_paths
from gold.implementation.storage.manifest import build_gold_manifest
from gold.implementation.storage.objects import discover_gold_parquet_objects
from gold.implementation.storage.recovery import (
    GoldRecoveryExecutionError,
    execute_gold_recovery,
    inspect_gold_snapshot,
)
from gold.implementation.storage.writer import write_gold_snapshot_parquet


def main():
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]
    bucket = os.environ["BRONZE_BUCKET"]

    dataset_name = "daily_sales"
    snapshot_id = "gold_snap_" + uuid4().hex

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    spark = (
        SparkSession.builder
        .appName("GoldRecoveryE2E")
        .getOrCreate()
    )

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

        def inspect(sid):
            return inspect_gold_snapshot(
                client=client,
                bucket=bucket,
                dataset_name=dataset_name,
                snapshot_id=sid,
            )

        def recover(sid, candidate=None):
            return execute_gold_recovery(
                spark=spark,
                client=client,
                bucket=bucket,
                dataset_name=dataset_name,
                snapshot_id=sid,
                candidate_manifest=candidate,
            )

        # ----------------------------------------------------
        # 1. NOT_FOUND
        # ----------------------------------------------------
        assert inspect(snapshot_id)["state"] == "NOT_FOUND"

        result = recover(snapshot_id)
        assert result["status"] == "NOT_FOUND"

        print("GOLD RECOVERY NOT_FOUND: PASS", flush=True)

        # ----------------------------------------------------
        # 2. Write a fresh, synthetic Gold snapshot
        # ----------------------------------------------------
        schema = StructType([
            StructField("order_date", DateType(), False),
            StructField("order_count", LongType(), False),
            StructField("delivered_order_count", LongType(), False),
            StructField("cancelled_order_count", LongType(), False),
            StructField(
                "gross_merchandise_value",
                DecimalType(20, 2),
                False,
            ),
            StructField(
                "discount_amount",
                DecimalType(20, 2),
                False,
            ),
            StructField(
                "net_order_value",
                DecimalType(20, 2),
                False,
            ),
            StructField(
                "delivered_order_value",
                DecimalType(20, 2),
                False,
            ),
        ])

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
        ]

        df = spark.createDataFrame(rows, schema)

        write_result = write_gold_snapshot_parquet(
            spark=spark,
            gold_df=df,
            bucket=bucket,
            dataset_name=dataset_name,
            snapshot_id=snapshot_id,
        )

        assert write_result["status"] == "DATA_WRITTEN"

        # ----------------------------------------------------
        # 3. UNPUBLISHED
        # ----------------------------------------------------
        inspection = inspect(snapshot_id)

        assert inspection["state"] == "UNPUBLISHED", inspection

        print("GOLD RECOVERY UNPUBLISHED: PASS", flush=True)

        # ----------------------------------------------------
        # 4. Build a candidate manifest
        # ----------------------------------------------------
        objects = discover_gold_parquet_objects(
            client=client,
            bucket=bucket,
            data_prefix=write_result["data_prefix"],
        )

        read_df = spark.read.parquet(write_result["output_path"])

        source_snapshots = {
            "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
            "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
        }

        manifest = build_gold_manifest(
            dataset_name=dataset_name,
            snapshot_id=snapshot_id,
            source_snapshots=source_snapshots,
            row_count=read_df.count(),
            schema=dict(
                get_gold_contract(dataset_name)["columns"]
            ),
            parquet_objects=objects,
        )

        # ----------------------------------------------------
        # 5. Recover UNPUBLISHED -> PUBLISHED
        # ----------------------------------------------------
        publication = recover(
            snapshot_id,
            candidate=manifest,
        )

        assert publication["status"] == "PUBLISHED", publication

        print("GOLD RECOVERY PUBLICATION: PASS", flush=True)

        # ----------------------------------------------------
        # 6. Inspect published snapshot
        # ----------------------------------------------------
        inspection = inspect(snapshot_id)

        assert inspection["state"] == "PUBLISHED", inspection

        print("GOLD RECOVERY PUBLISHED STATE: PASS", flush=True)

        # ----------------------------------------------------
        # 7. Recover already published snapshot
        # ----------------------------------------------------
        recovered = recover(snapshot_id)

        assert recovered["status"] == "ALREADY_PUBLISHED", recovered

        print("GOLD RECOVERY IDEMPOTENCY: PASS", flush=True)

        # ----------------------------------------------------
        # 8. Reject incomplete snapshot
        # ----------------------------------------------------
        incomplete_id = "gold_snap_" + uuid4().hex

        paths = get_gold_storage_paths(
            dataset_name,
            incomplete_id,
        )

        incomplete_key = paths["data_prefix"] + "partial.tmp"

        client.put_object(
            bucket,
            incomplete_key,
            data=__import__("io").BytesIO(b"incomplete"),
            length=len(b"incomplete"),
        )

        incomplete_inspection = inspect(incomplete_id)

        assert incomplete_inspection["state"] == "INCOMPLETE"

        try:
            recover(incomplete_id)
        except GoldRecoveryExecutionError:
            pass
        else:
            raise AssertionError(
                "Recovery accepted incomplete snapshot"
            )

        print("GOLD RECOVERY INCOMPLETE REJECTION: PASS", flush=True)

        # ----------------------------------------------------
        # 9. Verify no mutation of published manifest
        # ----------------------------------------------------
        from silver.snapshots.publisher import (
            read_published_manifest,
        )

        published_manifest = read_published_manifest(
            client,
            bucket,
            manifest["manifest_key"],
        )

        assert published_manifest == manifest

        print("GOLD RECOVERY MANIFEST INTEGRITY: PASS", flush=True)

        print("GOLD RECOVERY E2E: PASS 8/8", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
