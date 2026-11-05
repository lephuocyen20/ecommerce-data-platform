import os
import uuid

from minio import Minio
from pyspark.sql import SparkSession


def main():
    bucket = "snapshot-publication-test"
    endpoint = os.environ["MINIO_ENDPOINT"]

    client = Minio(
        endpoint=endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )

    if not client.bucket_exists(bucket):
        raise RuntimeError(f"Test bucket does not exist: {bucket}")

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("snapshot-s3a-connectivity-test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    test_id = uuid.uuid4().hex
    prefix = f"integration_tests/s3a_connectivity/{test_id}/"
    path = f"s3a://{bucket}/{prefix}"

    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

        hadoop_conf.set("fs.s3a.endpoint", f"http://{endpoint}")
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

        df = spark.createDataFrame(
            [
                (1, "PENDING"),
                (2, "SHIPPED"),
            ],
            ["order_id", "status"],
        )

        df.write.mode("errorifexists").parquet(path)

        result = spark.read.parquet(path).orderBy("order_id").collect()

        assert len(result) == 2
        assert result[0]["order_id"] == 1
        assert result[1]["order_id"] == 2

        print("SPARK S3A WRITE: PASS")
        print("SPARK S3A READ: PASS")
        print("FINAL RESULT: PASS 2/2")

    finally:
        try:
            objects = client.list_objects(
                bucket,
                prefix=prefix,
                recursive=True,
            )

            for obj in objects:
                client.remove_object(bucket, obj.object_name)

            print("TEST OBJECT CLEANUP: COMPLETE")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()