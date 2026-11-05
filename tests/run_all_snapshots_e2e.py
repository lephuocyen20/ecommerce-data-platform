
import os
import json

import psycopg2
from minio import Minio
from pyspark.sql import SparkSession

from silver.snapshots.pipeline import run_snapshot_pipeline
from silver.snapshots.batch_discovery import get_committed_batches
from silver.snapshots.continuity import validate_batch_continuity
from silver.snapshots.identity import generate_snapshot_id
from silver.snapshots.recovery import inspect_snapshot_state


TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
]


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
    results = []

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
            .appName("AllSilverSnapshotsE2E")
            .master("local[*]")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        configure_s3a(spark)

        for table_name in TABLES:
            print(f"\n========== {table_name} ==========", flush=True)

            batches = get_committed_batches(conn, table_name)
            validate_batch_continuity(batches)

            snapshot_id = generate_snapshot_id(
                table_name, batches
            )

            before = inspect_snapshot_state(
                client, bucket, table_name, snapshot_id
            )

            print("BEFORE:", before["state"], flush=True)

            if before["state"] == "INCOMPLETE":
                raise RuntimeError(
                    f"Incomplete snapshot for {table_name}"
                )

            objects_before = get_objects(
                client, bucket, before["snapshot_prefix"]
            )

            result = run_snapshot_pipeline(
                conn=conn,
                spark=spark,
                client=client,
                bucket=bucket,
                table_name=table_name,
            )

            after = inspect_snapshot_state(
                client, bucket, table_name, snapshot_id
            )

            assert after["state"] == "PUBLISHED", (
                f"{table_name}: expected PUBLISHED"
            )

            if before["state"] == "PUBLISHED":
                assert result["publication_status"] == "ALREADY_PUBLISHED"

                objects_after = get_objects(
                    client, bucket, after["snapshot_prefix"]
                )

                assert objects_before == objects_after, (
                    f"{table_name}: published objects changed"
                )

            else:
                assert result["publication_status"] == "PUBLISHED"

            results.append({
                "table": table_name,
                "snapshot_id": snapshot_id,
                "before": before["state"],
                "after": after["state"],
                "publication_status": result["publication_status"],
            })

            print(
                f"{table_name}: PASS | "
                f"{before['state']} -> {after['state']}",
                flush=True,
            )

        print("\nFINAL RESULTS")
        print(json.dumps(results, indent=2))
        print(f"FINAL RESULT: PASS {len(results)}/{len(TABLES)}")

    finally:
        if spark is not None:
            spark.stop()

        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
