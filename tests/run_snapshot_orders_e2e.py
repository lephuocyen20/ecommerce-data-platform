
import os
import json

import psycopg2
from minio import Minio
from pyspark.sql import SparkSession

from silver.snapshots.pipeline import (
    run_snapshot_pipeline,
)
from silver.snapshots.batch_discovery import (
    get_committed_batches,
)
from silver.snapshots.identity import (
    generate_snapshot_id,
)
from silver.snapshots.recovery import (
    inspect_snapshot_state,
)


TABLE_NAME = "orders"


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
            .appName("SnapshotOrdersFullE2E")
            .master("local[*]")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        hadoop_conf = (
            spark.sparkContext
            ._jsc.hadoopConfiguration()
        )

        hadoop_conf.set(
            "fs.s3a.endpoint",
            f"http://{os.environ['MINIO_ENDPOINT']}",
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

        batches = get_committed_batches(
            conn,
            TABLE_NAME,
        )

        snapshot_id = generate_snapshot_id(
            TABLE_NAME,
            batches,
        )

        before = inspect_snapshot_state(
            client,
            bucket,
            TABLE_NAME,
            snapshot_id,
        )

        print("BEFORE STATE:", before["state"])

        if before["state"] != "NEW":
            raise RuntimeError(
                "Expected NEW snapshot before first E2E run. "
                f"Actual: {before['state']}"
            )

        result = run_snapshot_pipeline(
            conn=conn,
            spark=spark,
            client=client,
            bucket=bucket,
            table_name=TABLE_NAME,
        )

        print(
            "PIPELINE RESULT:",
            json.dumps(
                result,
                indent=2,
                default=str,
            ),
        )

        after = inspect_snapshot_state(
            client,
            bucket,
            TABLE_NAME,
            snapshot_id,
        )

        print("AFTER STATE:", after["state"])

        if after["state"] != "PUBLISHED":
            raise AssertionError(
                "Snapshot was not published successfully"
            )

        print("FULL SNAPSHOT ORDERS E2E: PASS")

    finally:
        if spark is not None:
            spark.stop()

        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
