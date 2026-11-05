
import os
import json
from unittest.mock import patch

import psycopg2
from minio import Minio
from pyspark.sql import SparkSession

from silver.snapshots.pipeline import run_snapshot_pipeline
from silver.snapshots.batch_discovery import get_committed_batches
from silver.snapshots.identity import generate_snapshot_id
from silver.snapshots.recovery import inspect_snapshot_state


TABLE_NAME = "payments"


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


def get_objects(client, bucket, prefix):
    return {
        obj.object_name: (obj.size, obj.etag)
        for obj in client.list_objects(
            bucket,
            prefix=prefix,
            recursive=True,
        )
    }


def main():
    conn = None
    spark = None

    try:
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
        )

        client = Minio(
            os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )

        bucket = os.environ["BRONZE_BUCKET"]

        spark = (
            SparkSession.builder
            .appName("SnapshotPaymentsRecoveryE2E")
            .master("local[*]")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        configure_s3a(spark)

        batches = get_committed_batches(conn, TABLE_NAME)
        snapshot_id = generate_snapshot_id(TABLE_NAME, batches)

        def inspect():
            return inspect_snapshot_state(
                client, bucket, TABLE_NAME, snapshot_id
            )

        before = inspect()

        assert before["state"] == "NEW", (
            f"Expected NEW, got {before['state']}"
        )

        print("INITIAL STATE: NEW")

        # ====================================================
        # PHASE 1: INJECT FAILURE BEFORE MANIFEST PUBLICATION
        # ====================================================

        def simulate_publication_failure(*args, **kwargs):
            raise RuntimeError(
                "INJECTED_PUBLICATION_FAILURE"
            )

        try:
            with patch(
                "silver.snapshots.pipeline.publish_snapshot",
                side_effect=simulate_publication_failure,
            ):
                run_snapshot_pipeline(
                    conn=conn,
                    spark=spark,
                    client=client,
                    bucket=bucket,
                    table_name=TABLE_NAME,
                )

            raise AssertionError(
                "Expected injected publication failure"
            )

        except RuntimeError as exc:
            assert str(exc) == "INJECTED_PUBLICATION_FAILURE"
            print("FAILURE INJECTION: PASS")

        unpublished = inspect()

        assert unpublished["state"] == "UNPUBLISHED", (
            f"Expected UNPUBLISHED, got {unpublished['state']}"
        )

        objects_before = get_objects(
            client,
            bucket,
            unpublished["data_prefix"],
        )

        assert any(
            key.endswith(".parquet")
            for key in objects_before
        )

        assert (
            unpublished["manifest_key"]
            not in get_objects(
                client,
                bucket,
                unpublished["snapshot_prefix"],
            )
        )

        print("STATE AFTER FAILURE: UNPUBLISHED")
        print("PARQUET OBJECTS BEFORE:", len(objects_before))

        # ====================================================
        # PHASE 2: RECOVER EXISTING SNAPSHOT
        # ====================================================

        result = run_snapshot_pipeline(
            conn=conn,
            spark=spark,
            client=client,
            bucket=bucket,
            table_name=TABLE_NAME,
        )

        print(
            "RECOVERY RESULT:",
            json.dumps(result, indent=2, default=str),
        )

        assert result["publication_status"] == "PUBLISHED"

        published = inspect()

        assert published["state"] == "PUBLISHED"

        objects_after = get_objects(
            client,
            bucket,
            published["data_prefix"],
        )

        assert objects_before == objects_after, (
            "Snapshot Parquet objects changed during recovery"
        )

        assert published["manifest_key"] in get_objects(
            client,
            bucket,
            published["snapshot_prefix"],
        )

        print("STATE AFTER RECOVERY: PUBLISHED")
        print("PARQUET OBJECTS UNCHANGED: PASS")
        print("MANIFEST PUBLICATION: PASS")
        print("SNAPSHOT PAYMENTS RECOVERY E2E: PASS")

    finally:
        if spark is not None:
            spark.stop()

        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
