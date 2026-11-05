
"""Read-only Gold payment_summary Silver-to-Gold E2E."""

import os
from decimal import Decimal

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.implementation.readers import read_gold_sources
from gold.implementation.transformations.payment_summary import (
    build_payment_summary,
)
from gold.implementation.quality.payment_summary import (
    validate_payment_summary_sources,
    validate_payment_summary_output,
)


DATASET_NAME = "payment_summary"

SNAPSHOT_IDS = {
    "payments": "snap_4be50f0c3dd9715decad9d2456b4898c",
}

EXPECTED_PAYMENT_ROWS = 615
ZERO = Decimal("0.00")


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
    gold = None

    try:
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("GoldPaymentSummaryE2E")
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

        sources = read_gold_sources(
            spark=spark,
            client=client,
            bucket=os.environ["BRONZE_BUCKET"],
            dataset_name=DATASET_NAME,
            snapshot_ids=SNAPSHOT_IDS,
        )

        payments_source = sources["payments"]

        check_equal(
            "GOLD PAYMENT SILVER SNAPSHOT",
            payments_source["snapshot_id"],
            SNAPSHOT_IDS["payments"],
        )

        payments = payments_source["dataframe"]

        print(
            "GOLD PAYMENT SILVER SCHEMA:",
            flush=True,
        )
        payments.printSchema()

        check_equal(
            "GOLD PAYMENT SILVER ROW COUNT",
            payments.count(),
            EXPECTED_PAYMENT_ROWS,
        )

        print(
            "GOLD PAYMENT SILVER READ: PASS",
            flush=True,
        )

        # 1. Source quality.
        source_result = validate_payment_summary_sources(payments)

        assert source_result["status"] == "PASS"

        print(
            "GOLD PAYMENT SOURCE QUALITY: PASS",
            flush=True,
        )

        # 2. Transformation.
        gold = build_payment_summary(payments).cache()

        output_result = validate_payment_summary_output(gold)

        assert output_result["status"] == "PASS"

        print(
            "GOLD PAYMENT OUTPUT QUALITY: PASS",
            flush=True,
        )

        # 3. Payment count reconciliation.
        summary = gold.agg(
            F.sum("payment_count").alias("payment_count"),
            F.sum("total_payment_amount").alias("total_amount"),
        ).first()

        check_equal(
            "GOLD PAYMENT COUNT RECONCILIATION",
            summary["payment_count"],
            payments.count(),
        )

        # 4. Financial reconciliation.
        source_amount = (
            payments
            .agg(
                F.sum(
                    F.col("amount").cast("decimal(20,2)")
                ).cast("decimal(20,2)").alias("total")
            )
            .first()["total"]
            or ZERO
        )

        gold_amount = summary["total_amount"] or ZERO

        check_equal(
            "GOLD PAYMENT FINANCIAL RECONCILIATION",
            gold_amount,
            source_amount,
        )

        # 5. Grain reconciliation.
        source_grain_count = (
            payments
            .select("payment_method", "status")
            .distinct()
            .count()
        )

        check_equal(
            "GOLD PAYMENT GRAIN RECONCILIATION",
            gold.count(),
            source_grain_count,
        )

        # 6. Distinct order count by group.
        source_distinct_orders = (
            payments
            .groupBy("payment_method", "status")
            .agg(
                F.countDistinct("order_id").alias("expected_count")
            )
            .withColumnRenamed("status", "payment_status")
        )

        mismatches = (
            gold
            .join(
                source_distinct_orders,
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
        )

        check_equal(
            "GOLD PAYMENT DISTINCT ORDER RECONCILIATION",
            mismatches.count(),
            0,
        )

        print(
            f"GOLD PAYMENT SUMMARY ROW COUNT: {gold.count()}",
            flush=True,
        )

        print(
            "\nGOLD PAYMENT SUMMARY E2E: PASS 6/6",
            flush=True,
        )

    finally:
        if gold is not None:
            gold.unpersist()

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
