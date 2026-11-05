
import os
import uuid
from unittest.mock import patch

from minio import Minio
from pyspark.sql import SparkSession

from silver.snapshots.recovery_validator import (
    validate_recovery_snapshot,
)


BUCKET = "snapshot-publication-test"
TABLE_NAME = "orders"


def main():
    endpoint = os.environ["MINIO_ENDPOINT"]

    client = Minio(
        endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )

    spark = (
        SparkSession.builder
        .appName("SnapshotRecoveryValidatorE2E")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Configure Spark S3A to access MinIO
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    hadoop_conf.set(
        "fs.s3a.endpoint",
        f"http://{endpoint}",
    )

    hadoop_conf.set(
        "fs.s3a.access.key",
        os.environ["MINIO_ACCESS_KEY"],
    )

    hadoop_conf.set(
        "fs.s3a.secret.key",
        os.environ["MINIO_SECRET_KEY"],
    )

    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true",
    )

    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "false",
    )

    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

    snapshot_id = f"recovery_{uuid.uuid4().hex}"

    snapshot_prefix = (
        f"silver_snapshots/{TABLE_NAME}/"
        f"snapshots/snapshot_id={snapshot_id}/"
    )

    data_prefix = f"{snapshot_prefix}data/"
    output_path = f"s3a://{BUCKET}/{data_prefix}"

    try:
        rows = [
            (1, "PENDING", 1),
            (2, "SHIPPED", 1),
            (3, "DELIVERED", 1),
        ]

        expected_df = spark.createDataFrame(
            rows,
            ["order_id", "status", "_batch_sequence"],
        )

        expected_df.drop("_batch_sequence").write.mode(
            "errorifexists"
        ).parquet(output_path)

        with patch(
            "silver.snapshots.recovery_validator."
            "build_silver_snapshot",
            return_value=expected_df,
        ):
            result = validate_recovery_snapshot(
                spark=spark,
                client=client,
                bucket=BUCKET,
                table_name=TABLE_NAME,
                batches=[{"batch_id": "batch_001"}],
                data_prefix=data_prefix,
            )

        assert result["validation_status"] == "VERIFIED"
        assert result["row_count"] == 3

        print("REAL SPARK RECOVERY MATCH: PASS")

        changed_df = spark.createDataFrame(
            [
                (1, "PENDING", 1),
                (2, "CANCELLED", 1),
                (3, "DELIVERED", 1),
            ],
            ["order_id", "status", "_batch_sequence"],
        )

        with patch(
            "silver.snapshots.recovery_validator."
            "build_silver_snapshot",
            return_value=changed_df,
        ):
            try:
                validate_recovery_snapshot(
                    spark=spark,
                    client=client,
                    bucket=BUCKET,
                    table_name=TABLE_NAME,
                    batches=[{"batch_id": "batch_001"}],
                    data_prefix=data_prefix,
                )
            except ValueError as exc:
                assert "data mismatch" in str(exc)
            else:
                raise AssertionError(
                    "Changed recovery data was not rejected"
                )

        print("REAL SPARK RECOVERY MISMATCH: PASS")

        print("FINAL RESULT: PASS 2/2")

    finally:
        for obj in client.list_objects(
            BUCKET,
            prefix=snapshot_prefix,
            recursive=True,
        ):
            client.remove_object(BUCKET, obj.object_name)

        print("TEST SNAPSHOT CLEANUP: COMPLETE")
        spark.stop()


if __name__ == "__main__":
    main()
