import os
from decimal import Decimal

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.implementation.readers import read_gold_sources
from gold.implementation.transformations.customer_metrics import (
    build_customer_metrics,
)
from gold.implementation.quality.customer_metrics import (
    validate_customer_metrics_sources,
    validate_customer_metrics_output,
)


SNAPSHOT_IDS = {
    "customers": "snap_fdb8aabc86dbb4a98d1d4e01c78f7fc4",
    "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
    "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
}

EXPECTED_ROWS = {
    "customers": 101,
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
        .appName("gold-customer-metrics-e2e")
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

        # 1. Read and verify published Silver snapshots.
        sources = read_gold_sources(
            spark=spark,
            client=client,
            bucket=bucket,
            dataset_name="customer_metrics",
            snapshot_ids=SNAPSHOT_IDS,
        )

        for name, expected in EXPECTED_ROWS.items():
            assert sources[name]["snapshot_id"] == SNAPSHOT_IDS[name]
            assert sources[name]["dataframe"].count() == expected

        print("GOLD CUSTOMER SILVER READ: PASS", flush=True)

        customers = sources["customers"]["dataframe"]
        orders = sources["orders"]["dataframe"]
        items = sources["order_items"]["dataframe"]

        print("\n========== SILVER ORDERS SCHEMA ==========", flush=True)

        orders.printSchema()

        print(
            "SILVER ORDERS COLUMNS:",
            orders.columns,
            flush=True,
        )

        print(
            "SILVER ORDERS SNAPSHOT:",
            sources["orders"]["snapshot_id"],
            flush=True,
        )

        print("==========================================\n", flush=True)

        # 2. Validate Silver source quality.
        result = validate_customer_metrics_sources(
            customers, orders, items
        )
        assert result["status"] == "PASS"

        print("GOLD CUSTOMER SOURCE QUALITY: PASS", flush=True)

        # 3. Transform.
        gold_df = build_customer_metrics(
            customers, orders, items
        )

        # 4. Validate Gold output.
        result = validate_customer_metrics_output(gold_df)
        assert result["status"] == "PASS"

        print("GOLD CUSTOMER OUTPUT QUALITY: PASS", flush=True)

        # 5. Reconcile customer and order counts.
        gold_count = gold_df.count()
        assert gold_count == customers.count()

        source_order_count = orders.count()

        gold_order_count = gold_df.agg(
            F.sum("order_count").alias("total")
        ).first()["total"] or 0

        assert gold_order_count == source_order_count

        print("GOLD CUSTOMER COUNT RECONCILIATION: PASS", flush=True)

        # 6. Reconcile total order value independently.
        order_values = (
            items
            .withColumn(
                "net",
                (
                    F.col("quantity") * F.col("unit_price")
                    - F.col("discount_amount")
                ).cast("decimal(20,2)"),
            )
            .agg(F.sum("net").alias("total"))
            .first()["total"]
            or Decimal("0.00")
        )

        gold_value = (
            gold_df
            .agg(F.sum("total_order_value").alias("total"))
            .first()["total"]
            or Decimal("0.00")
        )

        assert gold_value == order_values, (
            f"Financial mismatch: {gold_value} != {order_values}"
        )

        print("GOLD CUSTOMER FINANCIAL RECONCILIATION: PASS", flush=True)

        print(f"GOLD CUSTOMER ROW COUNT: {gold_count}", flush=True)
        print("GOLD CUSTOMER METRICS E2E: PASS 5/5", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()