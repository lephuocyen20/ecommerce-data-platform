import os

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.implementation.contracts.datasets import (
    get_gold_contract,
)
from gold.implementation.readers import (
    read_gold_sources,
)
from gold.implementation.transformations.product_performance import (
    build_product_performance,
)
from gold.implementation.quality.product_performance import (
    validate_product_performance_sources,
    validate_product_performance_output,
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


DATASET_NAME = "product_performance"

SILVER_SNAPSHOTS = {
    "products": "snap_272c20e295221e271d0ac903ca631c24",
    "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
    "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
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
        .appName("gold-product-performance-publication-e2e")
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

        # 1. Read verified Silver snapshots.
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

        print("GOLD PRODUCT PUBLISHED SILVER SOURCES: PASS", flush=True)

        products = sources["products"]["dataframe"]
        orders = sources["orders"]["dataframe"]
        items = sources["order_items"]["dataframe"]

        # 2. Validate sources.
        source_result = validate_product_performance_sources(
            products,
            orders,
            items,
        )

        assert source_result["status"] == "PASS"

        print("GOLD PRODUCT SOURCE VALIDATION: PASS", flush=True)

        # 3. Build Gold DataFrame.
        gold_df = build_product_performance(
            products,
            orders,
            items,
        )

        output_result = validate_product_performance_output(
            gold_df
        )

        assert output_result["status"] == "PASS"

        gold_count = gold_df.count()
        product_count = products.count()

        assert gold_count == product_count
        assert gold_count == 50

        # Reconcile quantity before writing.
        source_units = items.agg(
            F.sum("quantity").alias("total")
        ).first()["total"] or 0

        gold_units = gold_df.agg(
            F.sum("units_ordered").alias("total")
        ).first()["total"] or 0

        assert source_units == gold_units

        print("GOLD PRODUCT TRANSFORMATION QUALITY: PASS", flush=True)

        # 4. Write a NEW Gold snapshot.
        write_result = write_gold_snapshot_parquet(
            spark=spark,
            gold_df=gold_df,
            bucket=bucket,
            dataset_name=DATASET_NAME,
        )

        assert write_result["status"] == "DATA_WRITTEN"

        snapshot_id = write_result["snapshot_id"]

        print(
            f"GOLD PRODUCT SNAPSHOT ID: {snapshot_id}",
            flush=True,
        )
        print("GOLD PRODUCT PARQUET WRITE: PASS", flush=True)

        # 5. Discover Parquet files and calculate SHA-256.
        parquet_objects = discover_gold_parquet_objects(
            client=client,
            bucket=bucket,
            data_prefix=write_result["data_prefix"],
        )

        assert len(parquet_objects) > 0

        print("GOLD PRODUCT PARQUET CHECKSUMS: PASS", flush=True)

        # 6. Build manifest using ACTUAL Silver lineage.
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

        print("GOLD PRODUCT MANIFEST LINEAGE: PASS", flush=True)

        # 7. Verify snapshot before publication.
        verification = verify_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert verification["status"] == "VERIFIED"
        assert verification["row_count"] == gold_count

        print("GOLD PRODUCT SNAPSHOT VERIFICATION: PASS", flush=True)

        # 8. Publish verified snapshot.
        publication = publish_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert publication["status"] == "PUBLISHED"

        print("GOLD PRODUCT FIRST PUBLICATION: PASS", flush=True)

        # 9. Read back the published manifest.
        published_manifest = read_published_manifest(
            client=client,
            bucket=bucket,
            manifest_key=write_result["manifest_key"],
        )

        assert published_manifest == manifest

        print("GOLD PRODUCT MANIFEST READBACK: PASS", flush=True)

        # 10. Verify idempotent publication.
        second_publication = publish_gold_snapshot(
            spark=spark,
            client=client,
            bucket=bucket,
            manifest=manifest,
        )

        assert second_publication["status"] == "ALREADY_PUBLISHED"

        print("GOLD PRODUCT IDEMPOTENT PUBLICATION: PASS", flush=True)

        print(
            f"GOLD PRODUCT PUBLISHED ROW COUNT: {gold_count}",
            flush=True,
        )

        print(
            "GOLD PRODUCT PUBLICATION E2E: PASS 10/10",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()