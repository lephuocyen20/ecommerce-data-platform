import os
import argparse
import json


from pyspark.sql import SparkSession

try:
    from transformation.config import BRONZE_BUCKET, MINIO_ENDPOINT
    from transformation.transforms.customers import transform_customers
    from transformation.transforms.products import transform_products
    from transformation.transforms.orders import transform_orders
    from transformation.transforms.order_items import transform_order_items
    from transformation.transforms.payments import transform_payments
    from transformation.transforms.shipments import transform_shipments
    from common.minio import (
        configure_s3a,
        create_minio_client,
    )
except ImportError:
    from config import BRONZE_BUCKET, MINIO_ENDPOINT
    from transforms.customers import transform_customers
    from transforms.products import transform_products
    from transforms.orders import transform_orders
    from transforms.order_items import transform_order_items
    from transforms.payments import transform_payments
    from transforms.shipments import transform_shipments
    from common.minio import configure_s3a, create_minio_client


def get_latest_bronze_path(client, bucket_name, table):
    """Get the latest Bronze path for a given table in MinIO."""
    # List all objects in the bucket
    objects = client.list_objects(
        bucket_name, prefix=f"bronze/{table}/", recursive=True
    )

    csv_objects = [obj for obj in objects if obj.object_name.endswith(".csv")]

    latest = max(csv_objects, key=lambda obj: obj.last_modified, default=None)

    if latest is None:
        raise ValueError(f"No CSV files found for table '{table}'.")

    latest_bronze_path = f"s3a://{bucket_name}/{latest.object_name}"
    return latest_bronze_path


def configure_s3(spark):
    """Backward-compatible name for the shared Spark S3A configuration."""
    configure_s3a(spark)


def read_bronze(spark, bronze_path):
    """Read and return the Bronze customers CSV DataFrame."""
    return spark.read.option("header", "true").csv(bronze_path)

def load_manifest(client, bucket_name, manifest_key):
    response = client.get_object(bucket_name, manifest_key)

    try:
        manifest = json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()

    if "tables" not in manifest:
        raise ValueError("Invalid manifest: missing 'tables'.")

    return manifest["tables"]

def write_silver(df, silver_path):
    # Write Silver Parquet
    df.write.mode("overwrite").parquet(silver_path)

def process_table(spark, bucket_name, table, object_name, transform_function):
    expected_prefix = f"bronze/{table}/"

    if not object_name.startswith(expected_prefix) or not object_name.endswith(".csv"):
        raise ValueError(f"Invalid Bronze object for {table}: {object_name}")

    bronze_path = f"s3a://{bucket_name}/{object_name}"

    print(f"Bronze snapshot for {table}: {bronze_path}")

    bronze_df = read_bronze(spark, bronze_path)
    silver_df = transform_function(bronze_df)

    silver_path = f"s3a://{bucket_name}/silver/{table}/"
    write_silver(silver_df, silver_path)

    # Read back the Silver data to validate
    silver_read_df = spark.read.parquet(silver_path)
    print(f"Silver records for {table} after read: {silver_read_df.count()}")
    silver_read_df.printSchema()

TRANSFORMS = {
            "customers": transform_customers,
            "products": transform_products,
            "orders": transform_orders,
            "order_items": transform_order_items,
            "payments": transform_payments,
            "shipments": transform_shipments,
            }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-key", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master("local[2]").appName("BronzeToSilver").getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        configure_s3(spark)

        client = create_minio_client()

        manifest = load_manifest(
            client,
            BRONZE_BUCKET,
            args.manifest_key,
        )

        if set(manifest) != set(TRANSFORMS):
            raise ValueError("Manifest tables do not match expected tables.")

        for table, transform_function in TRANSFORMS.items():
            process_table(
                spark,
                BRONZE_BUCKET,
                table,
                manifest[table],
                transform_function,
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
