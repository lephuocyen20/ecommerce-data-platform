
"""Real Gold payment_summary publication E2E."""

import os
from decimal import Decimal

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.implementation.contracts.datasets import get_gold_contract
from gold.implementation.readers import read_gold_sources
from gold.implementation.transformations.payment_summary import (
    build_payment_summary,
)
from gold.implementation.quality.payment_summary import (
    validate_payment_summary_sources,
    validate_payment_summary_output,
)
from gold.implementation.storage.writer import write_gold_snapshot_parquet
from gold.implementation.storage.objects import discover_gold_parquet_objects
from gold.implementation.storage.manifest import build_gold_manifest
from gold.implementation.storage.verifier import verify_gold_snapshot
from gold.implementation.storage.publisher import publish_gold_snapshot
from silver.snapshots.publisher import read_published_manifest


DATASET_NAME = "payment_summary"

SILVER_SNAPSHOTS = {
    "payments": "snap_4be50f0c3dd9715decad9d2456b4898c",
}

EXPECTED_PAYMENT_ROWS = 615
EXPECTED_GOLD_ROWS = 12
EXPECTED_TOTAL_AMOUNT = Decimal("53814808050.00")


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
            .master("local[2]")
            .appName("GoldPaymentSummaryPublicationE2E")
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

        # 1. Read verified published Silver snapshot.
        sources = read_gold_sources(
            spark=spark,
            client=client,
            bucket=bucket,
            dataset_name=DATASET_NAME,
            snapshot_ids=SILVER_SNAPSHOTS,
        )

        source = sources["payments"]

        check_equal(
            "GOLD PAYMENT SILVER SNAPSHOT",
            source["snapshot_id"],
            SILVER_SNAPSHOTS["payments"],
        )

        check_equal(
            "GOLD PAYMENT SILVER MANIFEST",
            source["manifest"]["snapshot_id"],
            SILVER_SNAPSHOTS["payments"],
        )

        payments = source["dataframe"]

        check_equal(
            "GOLD PAYMENT SOURCE ROW COUNT",
            payments.count(),
            EXPECTED_PAYMENT_ROWS,
        )

        print(
            "GOLD PAYMENT PUBLISHED SILVER SOURCES: PASS",
            flush=True,
        )

        # 2. Source quality.
        source_result = validate_payment_summary_sources(
            payments
        )

        assert source_result["status"] == "PASS"

        print(
            "GOLD PAYMENT SOURCE VALIDATION: PASS",
            flush=True,
        )

        # 3. Transformation and output quality.
        gold_df = build_payment_summary(
            payments
        ).cache()

        output_result = validate_payment_summary_output(
            gold_df
        )

        assert output_result["status"] == "PASS"

        gold_count = gold_df.count()

        check_equal(
            "GOLD PAYMENT ROW COUNT",
            gold_count,
            EXPECTED_GOLD_ROWS,
        )

        summary = gold_df.agg(
            F.sum("payment_count").alias("payment_count"),
            F.sum("total_payment_amount").alias("total_amount"),
        ).first()

        check_equal(
            "GOLD PAYMENT COUNT RECONCILIATION",
            summary["payment_count"],
            EXPECTED_PAYMENT_ROWS,
        )

        source_total = payments.agg(
            F.sum(
                F.col("amount").cast("decimal(20,2)")
            ).cast("decimal(20,2)").alias("total")
        ).first()["total"]

        check_equal(
            "GOLD PAYMENT FINANCIAL RECONCILIATION",
            summary["total_amount"],
            source_total,
        )

        check_equal(
            "GOLD PAYMENT EXPECTED TOTAL AMOUNT",
            summary["total_amount"],
            EXPECTED_TOTAL_AMOUNT,
        )

        source_grain_count = (
            payments
            .select("payment_method", "status")
            .distinct()
            .count()
        )

        check_equal(
            "GOLD PAYMENT GRAIN RECONCILIATION",
            gold_count,
            source_grain_count,
        )

        expected_distinct_orders = (
            payments
            .groupBy("payment_method", "status")
            .agg(
                F.countDistinct("order_id").alias("expected_count")
            )
            .withColumnRenamed("status", "payment_status")
        )

        mismatches = (
            gold_df
            .join(
                expected_distinct_orders,
                on=["payment_method", "payment_status"],
                how="left",
            )
            .filter(
                F.col("expected_count").isNull()
                | (
                    F.col("distinct_order_count")
                    != F.col("expected_count")
                )
            )
            .count()
        )

        check_equal(
            "GOLD PAYMENT DISTINCT ORDER RECONCILIATION",
            mismatches,
            0,
        )

        print(
            "GOLD PAYMENT TRANSFORMATION QUALITY: PASS",
            flush=True,
        )

        # 4. Write immutable Gold Parquet snapshot.
        write_result = write_gold_snapshot_parquet(
            spark=spark,
            gold_df=gold_df,
            bucket=bucket,
            dataset_name=DATASET_NAME,
        )

        assert write_result["status"] == "DATA_WRITTEN"

        snapshot_id = write_result["snapshot_id"]

        print(
            f"GOLD PAYMENT SNAPSHOT ID: {snapshot_id}",
            flush=True,
        )

        print(
            "GOLD PAYMENT PARQUET WRITE: PASS",
            flush=True,
        )

        # 5. Discover Parquet objects and checksums.
        parquet_objects = discover_gold_parquet_objects(
            client=client,
            bucket=bucket,
            data_prefix=write_result["data_prefix"],
        )

        assert parquet_objects

        print(
            "GOLD PAYMENT PARQUET CHECKSUMS: PASS",
            flush=True,
        )

        # 6. Build Gold manifest with Silver lineage.
        contract = get_gold_contract(DATASET_NAME)

        source_snapshots = {
            table_name: sources[table_name]["manifest"]["snapshot_id"]
            for table_name in contract["sources"]
        }

        manifest = build_gold_manifest(
            dataset_name=DATASET_NAME,
            snapshot_id=snapshot_id,
            source_snapshots=source_snapshots,
            row_count=gold_count,
            schema=contract["columns"],
            parquet_objects=parquet_objects,
        )

        assert manifest["snapshot_id"] == snapshot_id
        assert manifest["row_count"] == gold_count
        assert manifest["source_snapshots"] == SILVER_SNAPSHOTS

        print(
            "GOLD PAYMENT MANIFEST LINEAGE: PASS",
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
            "GOLD PAYMENT SNAPSHOT VERIFICATION: PASS",
            flush=True,
        )

        # 8. First publication.
        publication = publish_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert publication["status"] == "PUBLISHED"

        print(
            "GOLD PAYMENT FIRST PUBLICATION: PASS",
            flush=True,
        )

        # 9. Persisted manifest readback.
        published_manifest = read_published_manifest(
            client=client,
            bucket=bucket,
            manifest_key=write_result["manifest_key"],
        )

        assert published_manifest == manifest

        print(
            "GOLD PAYMENT MANIFEST READBACK: PASS",
            flush=True,
        )

        # 10. Idempotent publication.
        second_publication = publish_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert second_publication["status"] == "ALREADY_PUBLISHED"

        print(
            "GOLD PAYMENT IDEMPOTENT PUBLICATION: PASS",
            flush=True,
        )

        print(
            f"GOLD PAYMENT PUBLISHED ROW COUNT: {gold_count}",
            flush=True,
        )

        print(
            "GOLD PAYMENT PUBLICATION E2E: PASS 10/10",
            flush=True,
        )

    finally:
        if gold_df is not None:
            gold_df.unpersist()

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
