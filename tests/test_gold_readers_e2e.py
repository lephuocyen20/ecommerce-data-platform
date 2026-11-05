
import os

from minio import Minio
from pyspark.sql import SparkSession

from gold.implementation.contracts.datasets import GOLD_DATASETS
from gold.implementation.readers import read_gold_sources


SNAPSHOT_IDS = {
    "customers": "snap_fdb8aabc86dbb4a98d1d4e01c78f7fc4",
    "products": "snap_272c20e295221e271d0ac903ca631c24",
    "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
    "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
    "payments": "snap_4be50f0c3dd9715decad9d2456b4898c",
    "shipments": "snap_6866b962237dd8e13b6e1be123b76699",
}

EXPECTED_ROW_COUNTS = {
    "customers": 101,
    "products": 50,
    "orders": 500,
    "order_items": 1520,
    "payments": 615,
    "shipments": 455,
}


def configure_s3a(spark):
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    conf.set(
        "fs.s3a.endpoint",
        f"http://{os.environ['MINIO_ENDPOINT']}",
    )
    conf.set("fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
    conf.set("fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def main():
    spark = None
    client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )
    bucket = os.environ["BRONZE_BUCKET"]

    try:
        spark = (
            SparkSession.builder
            .appName("GoldReaderE2E")
            .master("local[*]")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        configure_s3a(spark)

        passed = 0

        for dataset_name, contract in GOLD_DATASETS.items():
            print(
                f"\n========== {dataset_name} ==========",
                flush=True,
            )

            source_ids = {
                table_name: SNAPSHOT_IDS[table_name]
                for table_name in contract["sources"]
            }

            sources = read_gold_sources(
                spark=spark,
                client=client,
                bucket=bucket,
                dataset_name=dataset_name,
                snapshot_ids=source_ids,
            )

            assert set(sources) == set(contract["sources"])

            for table_name, source in sources.items():
                df = source["dataframe"]
                manifest = source["manifest"]

                actual_count = df.count()

                assert actual_count == EXPECTED_ROW_COUNTS[table_name], (
                    f"{table_name}: row count mismatch"
                )

                assert manifest["row_count"] == actual_count

                assert source["snapshot_id"] == SNAPSHOT_IDS[table_name]

                print(
                    f"{table_name}: VERIFIED | rows={actual_count}",
                    flush=True,
                )

            passed += 1

            print(
                f"{dataset_name}: PASS",
                flush=True,
            )

        print(
            f"\nGOLD READER E2E: PASS {passed}/{len(GOLD_DATASETS)}",
            flush=True,
        )

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
