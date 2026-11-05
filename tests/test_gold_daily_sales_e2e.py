"""Read-only Gold daily_sales quality and reconciliation E2E."""
import os
from decimal import Decimal

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.implementation.readers import read_gold_sources
from gold.implementation.transformations.daily_sales import build_daily_sales
from gold.implementation.quality.daily_sales import (
    validate_daily_sales_sources,
    validate_daily_sales_output,
)

SNAPSHOT_IDS = {
    "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
    "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
}
MONEY_TYPE = "decimal(20,2)"
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
        raise AssertionError(f"{name}: actual={actual}, expected={expected}")
    print(f"{name}: PASS | value={actual}", flush=True)


def main():
    spark = None
    try:
        spark = (
            SparkSession.builder
            .appName("GoldDailySalesQualityE2E")
            .master("local[2]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
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
            dataset_name="daily_sales",
            snapshot_ids=SNAPSHOT_IDS,
        )
        orders = sources["orders"]["dataframe"]
        items = sources["order_items"]["dataframe"]

        source_quality = validate_daily_sales_sources(orders, items)
        assert source_quality["status"] == "PASS"
        print("GOLD DAILY SALES SOURCE QUALITY: PASS", flush=True)

        gold = build_daily_sales(orders, items).cache()
        try:
            output_quality = validate_daily_sales_output(gold)
            assert output_quality["status"] == "PASS"
            print("GOLD DAILY SALES OUTPUT QUALITY: PASS", flush=True)

            gold.show(20, truncate=False)
            summary = gold.agg(
                F.sum("order_count").alias("orders"),
                F.sum("delivered_order_count").alias("delivered"),
                F.sum("cancelled_order_count").alias("cancelled"),
                F.sum("gross_merchandise_value").alias("gross"),
                F.sum("discount_amount").alias("discount"),
                F.sum("net_order_value").alias("net"),
                F.sum("delivered_order_value").alias("delivered_value"),
            ).first()

            check_equal("ORDER COUNT", summary["orders"], orders.count())
            check_equal(
                "DELIVERED COUNT",
                summary["delivered"],
                orders.filter(F.col("status") == "DELIVERED").count(),
            )
            check_equal(
                "CANCELLED COUNT",
                summary["cancelled"],
                orders.filter(F.col("status") == "CANCELLED").count(),
            )

            item_totals = items.agg(
                F.sum(F.col("quantity") * F.col("unit_price"))
                .cast(MONEY_TYPE).alias("gross"),
                F.sum("discount_amount").cast(MONEY_TYPE).alias("discount"),
            ).first()
            expected_gross = item_totals["gross"] or ZERO
            expected_discount = item_totals["discount"] or ZERO
            check_equal("GROSS VALUE", summary["gross"], expected_gross)
            check_equal("DISCOUNT", summary["discount"], expected_discount)
            check_equal(
                "NET VALUE", summary["net"], expected_gross - expected_discount
            )

            order_item_totals = items.groupBy("order_id").agg(
                F.sum(
                    F.col("quantity") * F.col("unit_price")
                    - F.col("discount_amount")
                ).cast(MONEY_TYPE).alias("net")
            )
            expected_delivered = (
                orders.filter(F.col("status") == "DELIVERED")
                .join(order_item_totals, on="order_id", how="left")
                .agg(F.sum("net").cast(MONEY_TYPE).alias("total"))
                .first()["total"] or ZERO
            )
            check_equal(
                "DELIVERED VALUE", summary["delivered_value"], expected_delivered
            )
            check_equal(
                "UNIQUE DATE GRAIN",
                gold.count(),
                gold.select("order_date").distinct().count(),
            )
            print("\nGOLD DAILY SALES E2E: PASS 8/8", flush=True)
        finally:
            gold.unpersist()
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
