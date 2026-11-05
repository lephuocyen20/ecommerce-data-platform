
import os
from decimal import Decimal

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.implementation.contracts.datasets import (
    get_gold_contract,
)
from gold.implementation.readers import (
    read_gold_sources,
)
from gold.implementation.transformations.customer_metrics import (
    build_customer_metrics,
)
from gold.implementation.quality.customer_metrics import (
    validate_customer_metrics_sources,
    validate_customer_metrics_output,
)
from gold.implementation.storage.writer import (
    write_gold_snapshot_parquet,
)
from gold.implementation.storage.objects import (
    discover_gold_parquet_objects,
)
from gold.implementation.storage.manifest import (
    build_gold_manifest,
)
from gold.implementation.storage.verifier import (
    verify_gold_snapshot,
)
from gold.implementation.storage.publisher import (
    publish_gold_snapshot,
)
from silver.snapshots.publisher import (
    read_published_manifest,
)


DATASET_NAME = "customer_metrics"

SILVER_SNAPSHOTS = {
    "customers": "snap_fdb8aabc86dbb4a98d1d4e01c78f7fc4",
    "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
    "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
}

EXPECTED_CUSTOMER_COUNT = 101
EXPECTED_ORDER_COUNT = 500


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
        .appName("gold-customer-metrics-publication-e2e")
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

        # 1. Read and verify published Silver sources.
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
            "GOLD CUSTOMER PUBLISHED SILVER SOURCES: PASS",
            flush=True,
        )

        customers = sources["customers"]["dataframe"]
        orders = sources["orders"]["dataframe"]
        items = sources["order_items"]["dataframe"]

        # 2. Validate Silver source quality.
        source_result = validate_customer_metrics_sources(
            customers,
            orders,
            items,
        )

        assert source_result["status"] == "PASS"

        print(
            "GOLD CUSTOMER SOURCE VALIDATION: PASS",
            flush=True,
        )

        # 3. Build and validate Gold DataFrame.
        gold_df = build_customer_metrics(
            customers,
            orders,
            items,
        )

        output_result = validate_customer_metrics_output(
            gold_df
        )

        assert output_result["status"] == "PASS"

        gold_count = gold_df.count()
        customer_count = customers.count()

        assert gold_count == customer_count
        assert gold_count == EXPECTED_CUSTOMER_COUNT

        # Reconcile order counts.
        source_order_count = orders.count()

        gold_order_count = gold_df.agg(
            F.sum("order_count").alias("total")
        ).first()["total"] or 0

        assert source_order_count == EXPECTED_ORDER_COUNT
        assert gold_order_count == source_order_count

        # Reconcile financial values.
        source_value = (
            items
            .withColumn(
                "net_amount",
                (
                    F.col("quantity") * F.col("unit_price")
                    - F.col("discount_amount")
                ).cast("decimal(20,2)"),
            )
            .agg(
                F.sum("net_amount").alias("total")
            )
            .first()["total"]
            or Decimal("0.00")
        )

        gold_value = (
            gold_df
            .agg(
                F.sum("total_order_value").alias("total")
            )
            .first()["total"]
            or Decimal("0.00")
        )

        assert gold_value == source_value, (
            f"Financial mismatch: {gold_value} != {source_value}"
        )

        print(
            "GOLD CUSTOMER TRANSFORMATION QUALITY: PASS",
            flush=True,
        )

        # 4. Write a NEW immutable Gold snapshot.
        write_result = write_gold_snapshot_parquet(
            spark=spark,
            gold_df=gold_df,
            bucket=bucket,
            dataset_name=DATASET_NAME,
        )

        assert write_result["status"] == "DATA_WRITTEN"

        snapshot_id = write_result["snapshot_id"]

        print(
            f"GOLD CUSTOMER SNAPSHOT ID: {snapshot_id}",
            flush=True,
        )

        print(
            "GOLD CUSTOMER PARQUET WRITE: PASS",
            flush=True,
        )

        # 5. Discover Parquet objects and calculate SHA-256.
        parquet_objects = discover_gold_parquet_objects(
            client=client,
            bucket=bucket,
            data_prefix=write_result["data_prefix"],
        )

        assert len(parquet_objects) > 0

        print(
            "GOLD CUSTOMER PARQUET CHECKSUMS: PASS",
            flush=True,
        )

        # 6. Build manifest using actual Silver lineage.
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
            "GOLD CUSTOMER MANIFEST LINEAGE: PASS",
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
            "GOLD CUSTOMER SNAPSHOT VERIFICATION: PASS",
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
            "GOLD CUSTOMER FIRST PUBLICATION: PASS",
            flush=True,
        )

        # 9. Read back the persisted manifest.
        published_manifest = read_published_manifest(
            client=client,
            bucket=bucket,
            manifest_key=write_result["manifest_key"],
        )

        assert published_manifest == manifest

        print(
            "GOLD CUSTOMER MANIFEST READBACK: PASS",
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
            "GOLD CUSTOMER IDEMPOTENT PUBLICATION: PASS",
            flush=True,
        )

        print(
            f"GOLD CUSTOMER PUBLISHED ROW COUNT: {gold_count}",
            flush=True,
        )

        print(
            "GOLD CUSTOMER PUBLICATION E2E: PASS 10/10",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
