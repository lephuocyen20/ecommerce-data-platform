
"""Gold Execution Preflight E2E - read-only."""

import os

from minio import Minio
from pyspark.sql import SparkSession

from gold.implementation.preflight import (
    run_gold_preflight,
)


SOURCES = {
    "payment_summary": {
        "payments": "snap_4be50f0c3dd9715decad9d2456b4898c",
    },
    "daily_sales": {
        "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
        "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
    },
    "product_performance": {
        "products": "snap_272c20e295221e271d0ac903ca631c24",
        "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
        "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
    },
    "customer_metrics": {
        "customers": "snap_fdb8aabc86dbb4a98d1d4e01c78f7fc4",
        "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
        "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
    },
}


EXPECTED = {
    "payment_summary": "gold_snap_98eaba83dfe04c22a50de73db0c0bd4e",
    "daily_sales": "gold_snap_fd4b1f8c20f3461a9f22902e97d9a4f5",
    "product_performance": "gold_snap_83963a2c1462490b904dfa9dd1b936b1",
    "customer_metrics": "gold_snap_12a2e84a51e745419b7324f7de9cf113",
}


def configure_s3a(spark):
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    conf.set(
        "fs.s3a.endpoint",
        f"http://{os.environ['MINIO_ENDPOINT']}",
    )
    conf.set(
        "fs.s3a.access.key",
        os.environ["MINIO_ACCESS_KEY"],
    )
    conf.set(
        "fs.s3a.secret.key",
        os.environ["MINIO_SECRET_KEY"],
    )
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def assert_read_only(result):
    assert result["can_write"] is False
    assert result["can_publish"] is False



def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("GoldPreflightVersionAwareE2E")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        configure_s3a(spark)

        client = Minio(
            os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )

        bucket = os.environ["BRONZE_BUCKET"]

        # ================================================
        # 1. LEGACY SNAPSHOTS - MUST BLOCK REUSE
        # ================================================

        for dataset_name in (
            "payment_summary",
            "product_performance",
            "customer_metrics",
        ):
            result = run_gold_preflight(
                spark=spark,
                client=client,
                bucket=bucket,
                dataset_name=dataset_name,
                source_snapshots=SOURCES[dataset_name],
            )

            assert result["status"] == "BLOCKED", result
            assert result["action"] == "VERIFY_EXISTING", result

            assert (
                result["selected_snapshot_id"]
                == EXPECTED[dataset_name]
            ), result

            assert (
                result["version_status"]
                == "LEGACY_UNKNOWN"
            ), result

            assert (
                result["reason"]
                == "MISSING_TRANSFORMATION_VERSION"
            ), result

            assert_read_only(result)

            print(
                f"{dataset_name}: PASS "
                "(LEGACY_UNKNOWN -> BLOCKED)",
                flush=True,
            )

        # ================================================
        # 2. DAILY SALES - AMBIGUOUS LINEAGE
        # ================================================

        ambiguous = run_gold_preflight(
            spark=spark,
            client=client,
            bucket=bucket,
            dataset_name="daily_sales",
            source_snapshots=SOURCES["daily_sales"],
        )

        assert ambiguous["status"] == "BLOCKED", ambiguous
        assert ambiguous["action"] == "REJECT", ambiguous

        assert (
            ambiguous["reason"]
            == "AMBIGUOUS_LINEAGE"
        ), ambiguous

        assert len(
            ambiguous["matching_snapshot_ids"]
        ) == 3, ambiguous

        assert_read_only(ambiguous)

        print(
            "daily_sales ambiguous lineage: PASS "
            "(AMBIGUOUS_LINEAGE -> BLOCKED)",
            flush=True,
        )

        # ================================================
        # 3. DAILY SALES - EXPLICIT LEGACY SELECTION
        # ================================================

        selected = run_gold_preflight(
            spark=spark,
            client=client,
            bucket=bucket,
            dataset_name="daily_sales",
            source_snapshots=SOURCES["daily_sales"],
            preferred_snapshot_id=EXPECTED["daily_sales"],
        )

        assert selected["status"] == "BLOCKED", selected
        assert selected["action"] == "VERIFY_EXISTING", selected

        assert (
            selected["selected_snapshot_id"]
            == EXPECTED["daily_sales"]
        ), selected

        assert (
            selected["version_status"]
            == "LEGACY_UNKNOWN"
        ), selected

        assert (
            selected["reason"]
            == "MISSING_TRANSFORMATION_VERSION"
        ), selected

        assert_read_only(selected)

        print(
            "daily_sales explicit selection: PASS "
            "(LEGACY_UNKNOWN -> BLOCKED)",
            flush=True,
        )

        print(
            "GOLD VERSION-AWARE PREFLIGHT E2E: PASS 5/5",
            flush=True,
        )

    finally:
        spark.stop()



if __name__ == "__main__":
    main()
