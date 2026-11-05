
"""Gold daily_sales publication end-to-end test."""

import os
from decimal import Decimal

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.implementation.contracts.datasets import get_gold_contract
from gold.implementation.readers import read_gold_sources
from gold.implementation.transformations.daily_sales import build_daily_sales
from gold.implementation.quality.daily_sales import (
    validate_daily_sales_sources,
    validate_daily_sales_output,
)
from gold.implementation.storage.writer import write_gold_snapshot_parquet
from gold.implementation.storage.objects import discover_gold_parquet_objects
from gold.implementation.storage.manifest import build_gold_manifest
from gold.implementation.storage.verifier import verify_gold_snapshot
from gold.implementation.storage.publisher import publish_gold_snapshot
from silver.snapshots.publisher import read_published_manifest


DATASET_NAME = "daily_sales"

SILVER_SNAPSHOTS = {
    "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
    "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
}

MONEY_TYPE = "decimal(20,2)"
ZERO = Decimal("0.00")

EXPECTED_ORDER_COUNT = 500
EXPECTED_GOLD_ROW_COUNT = 205


def configure_s3a(spark):
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    conf.set("fs.s3a.endpoint", f"http://{os.environ['MINIO_ENDPOINT']}")
    conf.set("fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
    conf.set("fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def check_equal(name, actual, expected):
    if actual != expected:
        raise AssertionError(
            f"{name}: actual={actual}, expected={expected}"
        )

    print(f"{name}: PASS | value={actual}", flush=True)


def main():
    spark = None
    gold_df = None

    try:
        spark = (
            SparkSession.builder
            .appName("GoldDailySalesPublicationE2E")
            .master("local[2]")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")
        configure_s3a(spark)

        client = Minio(
            os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )

        bucket = os.environ["BRONZE_BUCKET"]

        # 1. Read verified published Silver snapshots.
        sources = read_gold_sources(
            spark=spark,
            client=client,
            bucket=bucket,
            dataset_name=DATASET_NAME,
            snapshot_ids=SILVER_SNAPSHOTS,
        )

        for table_name, snapshot_id in SILVER_SNAPSHOTS.items():
            assert sources[table_name]["snapshot_id"] == snapshot_id
            assert (
                sources[table_name]["manifest"]["snapshot_id"]
                == snapshot_id
            )

        print(
            "GOLD DAILY SALES PUBLISHED SILVER SOURCES: PASS",
            flush=True,
        )

        orders = sources["orders"]["dataframe"]
        items = sources["order_items"]["dataframe"]

        # 2. Validate Silver source quality.
        source_result = validate_daily_sales_sources(
            orders,
            items,
        )

        assert source_result["status"] == "PASS"

        print(
            "GOLD DAILY SALES SOURCE VALIDATION: PASS",
            flush=True,
        )

        # 3. Transform and validate Gold output.
        gold_df = build_daily_sales(
            orders,
            items,
        ).cache()

        output_result = validate_daily_sales_output(
            gold_df
        )

        assert output_result["status"] == "PASS"

        gold_count = gold_df.count()

        check_equal(
            "GOLD DAILY SALES ROW COUNT",
            gold_count,
            EXPECTED_GOLD_ROW_COUNT,
        )

        summary = gold_df.agg(
            F.sum("order_count").alias("orders"),
            F.sum("delivered_order_count").alias("delivered"),
            F.sum("cancelled_order_count").alias("cancelled"),
            F.sum("gross_merchandise_value").alias("gross"),
            F.sum("discount_amount").alias("discount"),
            F.sum("net_order_value").alias("net"),
            F.sum("delivered_order_value").alias("delivered_value"),
        ).first()

        # Reconcile order counts.
        check_equal(
            "GOLD DAILY SALES ORDER COUNT",
            summary["orders"],
            orders.count(),
        )

        check_equal(
            "GOLD DAILY SALES EXPECTED ORDER COUNT",
            summary["orders"],
            EXPECTED_ORDER_COUNT,
        )

        check_equal(
            "GOLD DAILY SALES DELIVERED COUNT",
            summary["delivered"],
            orders.filter(
                F.col("status") == "DELIVERED"
            ).count(),
        )

        check_equal(
            "GOLD DAILY SALES CANCELLED COUNT",
            summary["cancelled"],
            orders.filter(
                F.col("status") == "CANCELLED"
            ).count(),
        )

        # Reconcile monetary values.
        item_totals = items.agg(
            F.sum(
                F.col("quantity") * F.col("unit_price")
            ).cast(MONEY_TYPE).alias("gross"),
            F.sum("discount_amount")
            .cast(MONEY_TYPE).alias("discount"),
        ).first()

        expected_gross = item_totals["gross"] or ZERO
        expected_discount = item_totals["discount"] or ZERO

        check_equal(
            "GOLD DAILY SALES GROSS VALUE",
            summary["gross"],
            expected_gross,
        )

        check_equal(
            "GOLD DAILY SALES DISCOUNT",
            summary["discount"],
            expected_discount,
        )

        check_equal(
            "GOLD DAILY SALES NET VALUE",
            summary["net"],
            expected_gross - expected_discount,
        )

        # Reconcile delivered order value.
        order_item_totals = items.groupBy("order_id").agg(
            F.sum(
                F.col("quantity") * F.col("unit_price")
                - F.col("discount_amount")
            ).cast(MONEY_TYPE).alias("net")
        )

        expected_delivered = (
            orders
            .filter(F.col("status") == "DELIVERED")
            .join(
                order_item_totals,
                on="order_id",
                how="left",
            )
            .agg(
                F.sum("net")
                .cast(MONEY_TYPE)
                .alias("total")
            )
            .first()["total"]
            or ZERO
        )

        check_equal(
            "GOLD DAILY SALES DELIVERED VALUE",
            summary["delivered_value"],
            expected_delivered,
        )

        check_equal(
            "GOLD DAILY SALES UNIQUE DATE GRAIN",
            gold_count,
            gold_df.select("order_date").distinct().count(),
        )

        print(
            "GOLD DAILY SALES TRANSFORMATION QUALITY: PASS",
            flush=True,
        )

        # 4. Write NEW immutable Gold snapshot.
        write_result = write_gold_snapshot_parquet(
            spark=spark,
            gold_df=gold_df,
            bucket=bucket,
            dataset_name=DATASET_NAME,
        )

        assert write_result["status"] == "DATA_WRITTEN"

        snapshot_id = write_result["snapshot_id"]

        print(
            f"GOLD DAILY SALES SNAPSHOT ID: {snapshot_id}",
            flush=True,
        )

        print(
            "GOLD DAILY SALES PARQUET WRITE: PASS",
            flush=True,
        )

        # 5. Discover Parquet files and calculate SHA-256.
        parquet_objects = discover_gold_parquet_objects(
            client=client,
            bucket=bucket,
            data_prefix=write_result["data_prefix"],
        )

        assert len(parquet_objects) > 0

        print(
            "GOLD DAILY SALES PARQUET CHECKSUMS: PASS",
            flush=True,
        )

        # 6. Build manifest with actual Silver lineage.
        contract = get_gold_contract(DATASET_NAME)

        manifest = build_gold_manifest(
            dataset_name=DATASET_NAME,
            snapshot_id=snapshot_id,
            source_snapshots={
                table_name: sources[table_name]["manifest"]["snapshot_id"]
                for table_name in contract["sources"]
            },
            row_count=gold_count,
            schema=contract["columns"],
            parquet_objects=parquet_objects,
        )

        assert manifest["source_snapshots"] == SILVER_SNAPSHOTS
        assert manifest["row_count"] == gold_count
        assert manifest["snapshot_id"] == snapshot_id

        print(
            "GOLD DAILY SALES MANIFEST LINEAGE: PASS",
            flush=True,
        )

        # 7. Verify snapshot before publication.
        verification = verify_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert verification["status"] == "VERIFIED"
        assert verification["row_count"] == gold_count

        print(
            "GOLD DAILY SALES SNAPSHOT VERIFICATION: PASS",
            flush=True,
        )

        # 8. Publish verified snapshot.
        publication = publish_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert publication["status"] == "PUBLISHED"

        print(
            "GOLD DAILY SALES FIRST PUBLICATION: PASS",
            flush=True,
        )

        # 9. Read back persisted manifest.
        published_manifest = read_published_manifest(
            client=client,
            bucket=bucket,
            manifest_key=write_result["manifest_key"],
        )

        assert published_manifest == manifest

        print(
            "GOLD DAILY SALES MANIFEST READBACK: PASS",
            flush=True,
        )

        # 10. Verify idempotent publication.
        second_publication = publish_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert second_publication["status"] == "ALREADY_PUBLISHED"

        print(
            "GOLD DAILY SALES IDEMPOTENT PUBLICATION: PASS",
            flush=True,
        )

        print(
            f"GOLD DAILY SALES PUBLISHED ROW COUNT: {gold_count}",
            flush=True,
        )

        print(
            "GOLD DAILY SALES PUBLICATION E2E: PASS 10/10",
            flush=True,
        )

    finally:
        if gold_df is not None:
            gold_df.unpersist()

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
