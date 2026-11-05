import os

from decimal import Decimal

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.implementation.readers import read_gold_sources

from gold.implementation.transformations.product_performance import (
    build_product_performance,
)

from gold.implementation.quality.product_performance import (
    validate_product_performance_sources,
    validate_product_performance_output,
)


DATASET_NAME = "product_performance"

SILVER_SNAPSHOTS = {
    "products": "snap_272c20e295221e271d0ac903ca631c24",
    "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
    "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
}

EXPECTED_SOURCE_ROWS = {
    "products": 50,
    "orders": 500,
    "order_items": 1520,
}


def main():
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]
    bucket = os.environ["BRONZE_BUCKET"]

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    spark = (
        SparkSession.builder
        .appName("gold-product-performance-e2e")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        conf = spark.sparkContext._jsc.hadoopConfiguration()

        conf.set("fs.s3a.endpoint", f"http://{endpoint}")
        conf.set("fs.s3a.access.key", access_key)
        conf.set("fs.s3a.secret.key", secret_key)
        conf.set("fs.s3a.path.style.access", "true")
        conf.set("fs.s3a.connection.ssl.enabled", "false")
        conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        # 1. Read verified, published Silver snapshots.
        sources = read_gold_sources(
            spark=spark,
            client=client,
            bucket=bucket,
            dataset_name=DATASET_NAME,
            snapshot_ids=SILVER_SNAPSHOTS,
        )

        assert set(sources) == set(SILVER_SNAPSHOTS)

        for name, expected_count in EXPECTED_SOURCE_ROWS.items():
            actual_count = sources[name]["dataframe"].count()
            assert actual_count == expected_count, (
                f"{name}: expected {expected_count}, got {actual_count}"
            )

        print("GOLD PRODUCT SILVER READ: PASS", flush=True)

        products = sources["products"]["dataframe"]
        orders = sources["orders"]["dataframe"]
        items = sources["order_items"]["dataframe"]

        # 2. Validate source quality.
        source_validation = validate_product_performance_sources(
            products,
            orders,
            items,
        )

        assert source_validation["status"] == "PASS"

        print("GOLD PRODUCT SOURCE QUALITY: PASS", flush=True)

        # 3. Transform Silver to Gold.
        gold_df = build_product_performance(
            products,
            orders,
            items,
        )

        print("GOLD PRODUCT TRANSFORMATION: PASS", flush=True)

        # 4. Validate Gold output.
        output_validation = validate_product_performance_output(
            gold_df
        )

        assert output_validation["status"] == "PASS"

        print("GOLD PRODUCT OUTPUT QUALITY: PASS", flush=True)

        # 5. Reconcile product grain and total units.
        product_count = products.count()
        gold_count = gold_df.count()

        assert gold_count == product_count, (
            f"Product count mismatch: {gold_count} != {product_count}"
        )

        expected_units = items.agg(
            F.sum("quantity").alias("total")
        ).first()["total"] or 0

        actual_units = gold_df.agg(
            F.sum("units_ordered").alias("total")
        ).first()["total"] or 0

        assert actual_units == expected_units, (
            f"Units mismatch: {actual_units} != {expected_units}"
        )

        print("GOLD PRODUCT RECONCILIATION: PASS", flush=True)

        # 6. Reconcile financial totals independently.
        expected_money = items.agg(
            F.sum(
                F.col("quantity") * F.col("unit_price")
            ).alias("gross"),
            F.sum("discount_amount").alias("discount"),
        ).first()

        actual_money = gold_df.agg(
            F.sum("gross_merchandise_value").alias("gross"),
            F.sum("discount_amount").alias("discount"),
            F.sum("net_order_value").alias("net"),
        ).first()

        expected_gross = expected_money["gross"] or Decimal("0")
        expected_discount = (
            expected_money["discount"] or Decimal("0")
        )

        assert actual_money["gross"] == expected_gross
        assert actual_money["discount"] == expected_discount
        assert actual_money["net"] == (
            expected_gross - expected_discount
        )

        print("GOLD PRODUCT FINANCIAL RECONCILIATION: PASS", flush=True)

        print(
            f"GOLD PRODUCT ROW COUNT: {gold_count}",
            flush=True,
        )

        print(
            "GOLD PRODUCT PERFORMANCE E2E: PASS 6/6",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()