import os

from minio import Minio
from pyspark.sql import SparkSession

from config import BRONZE_BUCKET, MINIO_ENDPOINT
from transforms.customers import transform_customers
from transforms.products import transform_products
from transforms.orders import transform_orders
from transforms.order_items import transform_order_items
from transforms.payments import transform_payments
from transforms.shipments import transform_shipments


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
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set(
        "fs.s3a.access.key",
        os.environ["MINIO_ACCESS_KEY"],
    )
    hadoop_conf.set(
        "fs.s3a.secret.key",
        os.environ["MINIO_SECRET_KEY"],
    )


def read_bronze(spark, bronze_path):
    """Read and return the Bronze customers CSV DataFrame."""
    return spark.read.option("header", "true").csv(bronze_path)


def write_silver(df, silver_path):
    # Write Silver Parquet
    df.write.mode("overwrite").parquet(silver_path)


def process_table(spark, client, bucket_name, table, transform_function):
    """Process a table from Bronze to Silver."""
    latest_bronze_path = get_latest_bronze_path(client, bucket_name, table)
    print(f"Latest Bronze for {table}: {latest_bronze_path}")

    bronze_df = read_bronze(spark, latest_bronze_path)
    silver_df = transform_function(bronze_df)

    silver_path = f"s3a://{bucket_name}/silver/{table}/"
    write_silver(silver_df, silver_path)

    # Read back the Silver data to validate
    silver_read_df = spark.read.parquet(silver_path)
    print(f"Silver records for {table} after read: {silver_read_df.count()}")
    silver_read_df.printSchema()


def main():
    spark = (
        SparkSession.builder.master("local[2]").appName("BronzeToSilver").getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        configure_s3(spark)

        client = Minio(
            MINIO_ENDPOINT,
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )

        process_table(spark, client, BRONZE_BUCKET, "customers", transform_customers)
        process_table(spark, client, BRONZE_BUCKET, "products", transform_products)
        process_table(spark, client, BRONZE_BUCKET, "orders", transform_orders)
        process_table(
            spark, client, BRONZE_BUCKET, "order_items", transform_order_items
        )
        process_table(spark, client, BRONZE_BUCKET, "payments", transform_payments)
        process_table(spark, client, BRONZE_BUCKET, "shipments", transform_shipments)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
