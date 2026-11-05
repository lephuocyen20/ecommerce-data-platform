import json
import os
import uuid
from datetime import datetime, timezone
from minio.error import S3Error
from minio import Minio
from pyspark.sql import SparkSession
from silver.snapshots.recovery import inspect_snapshot_state

from silver.snapshots.writer import write_snapshot_parquet
from silver.snapshots.object_discovery import (
    discover_snapshot_parquet_objects,
)
from silver.snapshots.metadata import build_snapshot_metadata
from silver.snapshots.publisher import (
    publish_snapshot,
    get_snapshot_manifest_key,
)


BUCKET = "snapshot-publication-test"


def read_json_object(client, bucket, object_key):
    response = client.get_object(bucket, object_key)

    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def main():
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    if not client.bucket_exists(BUCKET):
        raise RuntimeError(f"Test bucket does not exist: {BUCKET}")

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("snapshot-publication-e2e-test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    snapshot_id = f"e2e_{uuid.uuid4().hex}"
    table_name = "orders"

    snapshot_prefix = (
        f"silver_snapshots/{table_name}/"
        f"snapshots/snapshot_id={snapshot_id}/"
    )

    try:
        # 1. Configure Spark S3A.
        conf = spark.sparkContext._jsc.hadoopConfiguration()

        conf.set("fs.s3a.endpoint", f"http://{endpoint}")
        conf.set("fs.s3a.path.style.access", "true")
        conf.set("fs.s3a.connection.ssl.enabled", "false")
        conf.set("fs.s3a.access.key", access_key)
        conf.set("fs.s3a.secret.key", secret_key)

        # 2. Build a test snapshot DataFrame.
        df = spark.createDataFrame(
            [
                (1, "PENDING", 0),
                (2, "SHIPPED", 0),
                (3, "DELIVERED", 0),
            ],
            ["order_id", "status", "_batch_sequence"],
        )

        expected_rows = 3

        # 3. Write immutable Parquet.
        writer_result = write_snapshot_parquet(
            spark=spark,
            snapshot_df=df,
            bucket=BUCKET,
            table_name=table_name,
            snapshot_id=snapshot_id,
        )

        print("SNAPSHOT PARQUET WRITE: PASS")

        # Verify real Spark S3A output before publication.
        unpublished_state = inspect_snapshot_state(
            client=client,
            bucket=BUCKET,
            table_name=table_name,
            snapshot_id=snapshot_id,
        )

        assert unpublished_state["state"] == "UNPUBLISHED", (
            f"Expected UNPUBLISHED, got {unpublished_state['state']}"
        )

        print("REAL MINIO UNPUBLISHED STATE: PASS")

        # 4. Discover actual Parquet objects.
        parquet_objects = discover_snapshot_parquet_objects(
            client=client,
            bucket=BUCKET,
            data_prefix=writer_result["data_prefix"],
        )

        print("SNAPSHOT OBJECT DISCOVERY: PASS")

        # 5. Build snapshot metadata.
        batches = [
            {
                "batch_id": f"batch_{snapshot_id}",
                "table_name": table_name,
                "watermark_type": "TIMESTAMP",
                "end_timestamp": datetime(
                    2026, 10, 26, tzinfo=timezone.utc
                ),
                "end_id": None,
            }
        ]

        manifest = build_snapshot_metadata(
            table_name=table_name,
            snapshot_id=snapshot_id,
            data_prefix=writer_result["data_prefix"],
            batches=batches,
            row_count=expected_rows,
            parquet_objects=parquet_objects,
        )

        print("SNAPSHOT METADATA: PASS")

        # 6. Verify and publish manifest.
        publication = publish_snapshot(
            spark=spark,
            client=client,
            bucket=BUCKET,
            manifest=manifest,
        )

        print("SNAPSHOT VERIFICATION AND PUBLICATION: PASS")

        # 7. Read back the published manifest.
        manifest_key = get_snapshot_manifest_key(
            table_name,
            snapshot_id,
        )

        assert publication["manifest_key"] == manifest_key

        stored_manifest = read_json_object(
            client,
            BUCKET,
            manifest_key,
        )

        assert stored_manifest == manifest
        assert stored_manifest["row_count"] == expected_rows
        assert stored_manifest["source_batch_count"] == 1

        print("PUBLISHED MANIFEST READBACK: PASS")

        published_state = inspect_snapshot_state(
            client=client,
            bucket=BUCKET,
            table_name=table_name,
            snapshot_id=snapshot_id,
        )

        assert published_state["state"] == "PUBLISHED", (
            f"Expected PUBLISHED, got {published_state['state']}"
        )

        print("REAL MINIO PUBLISHED STATE: PASS")

        # 8. Retry with the exact same manifest.
        retry_result = publish_snapshot(
            spark=spark,
            client=client,
            bucket=BUCKET,
            manifest=manifest,
        )

        assert (
            retry_result["publication_status"]
            == "ALREADY_PUBLISHED"
        )

        print("IDEMPOTENT SNAPSHOT RETRY: PASS")

        # 9. Attempt to publish conflicting metadata.
        conflicting_manifest = {
            **manifest,
            "created_at": "2000-01-01T00:00:00+00:00",
        }

        try:
            publish_snapshot(
                spark=spark,
                client=client,
                bucket=BUCKET,
                manifest=conflicting_manifest,
            )
        except ValueError as exc:
            assert "Snapshot manifest conflict" in str(exc)
        else:
            raise AssertionError(
                "Conflicting manifest was unexpectedly accepted"
            )

        print("CONFLICTING MANIFEST REJECTED: PASS")

        # 10. Confirm stored manifest was not overwritten.
        final_manifest = read_json_object(
            client,
            BUCKET,
            manifest_key,
        )

        assert final_manifest == manifest

        print("ORIGINAL MANIFEST PRESERVED: PASS")

        # 8. Verify persisted Parquet contents.
        persisted_df = spark.read.parquet(
            writer_result["output_path"]
        )

        assert persisted_df.count() == expected_rows
        assert "_batch_sequence" not in persisted_df.columns

        print("PERSISTED SNAPSHOT DATA: PASS")

        # Recovery scenario:
        # Parquet exists, but the manifest has not been published.

        recovery_id = f"recovery_{uuid.uuid4().hex}"

        recovery_prefix = (
            f"silver_snapshots/{table_name}/"
            f"snapshots/snapshot_id={recovery_id}/"
        )

        recovery_writer = write_snapshot_parquet(
            spark=spark,
            snapshot_df=df,
            bucket=BUCKET,
            table_name=table_name,
            snapshot_id=recovery_id,
        )

        recovery_key = get_snapshot_manifest_key(
            table_name,
            recovery_id,
        )

        try:
            # Confirm the manifest is not published yet.
            try:
                response = client.stat_object(
                    BUCKET,
                    recovery_key,
                )
            except S3Error as exc:
                assert exc.code == "NoSuchKey"
            else:
                raise AssertionError(
                    "Recovery manifest already exists"
                )

            print("UNPUBLISHED PARQUET RECOVERY STATE: PASS")

            recovery_state = inspect_snapshot_state(
                client=client,
                bucket=BUCKET,
                table_name=table_name,
                snapshot_id=recovery_id,
            )

            assert recovery_state["state"] == "UNPUBLISHED"

            print("REAL MINIO RECOVERY STATE: PASS")

            # Discover existing Parquet, without writing again.
            recovery_objects = discover_snapshot_parquet_objects(
                client=client,
                bucket=BUCKET,
                data_prefix=recovery_writer["data_prefix"],
            )

            recovery_batches = [
                {
                    "batch_id": f"batch_{recovery_id}",
                    "table_name": table_name,
                    "watermark_type": "TIMESTAMP",
                    "end_timestamp": datetime(
                        2026, 10, 26, tzinfo=timezone.utc
                    ),
                    "end_id": None,
                }
            ]

            recovery_manifest = build_snapshot_metadata(
                table_name=table_name,
                snapshot_id=recovery_id,
                data_prefix=recovery_writer["data_prefix"],
                batches=recovery_batches,
                row_count=expected_rows,
                parquet_objects=recovery_objects,
            )

            recovery_result = publish_snapshot(
                spark=spark,
                client=client,
                bucket=BUCKET,
                manifest=recovery_manifest,
            )

            assert recovery_result["publication_status"] == "PUBLISHED"

            print("RECOVERY PUBLICATION: PASS")

            # Verify published recovery manifest.
            stored_recovery = read_json_object(
                client,
                BUCKET,
                recovery_key,
            )

            assert stored_recovery == recovery_manifest

            print("RECOVERY MANIFEST READBACK: PASS")

        finally:
            # Cleanup only the recovery snapshot.
            for obj in client.list_objects(
                BUCKET,
                prefix=recovery_prefix,
                recursive=True,
            ):
                client.remove_object(
                    BUCKET,
                    obj.object_name,
                )
        # Simulate an incomplete Spark output:
        # A Parquet-like object exists, but _SUCCESS is missing.

        incomplete_id = f"incomplete_{uuid.uuid4().hex}"

        incomplete_prefix = (
            f"silver_snapshots/{table_name}/"
            f"snapshots/snapshot_id={incomplete_id}/"
        )

        incomplete_key = (
            f"{incomplete_prefix}data/part-00000.parquet"
        )

        try:
            from io import BytesIO

            incomplete_payload = b"incomplete-test-data"

            client.put_object(
                BUCKET,
                incomplete_key,
                BytesIO(incomplete_payload),
                length=len(incomplete_payload),
            )

            incomplete_state = inspect_snapshot_state(
                client=client,
                bucket=BUCKET,
                table_name=table_name,
                snapshot_id=incomplete_id,
            )

            assert incomplete_state["state"] == "INCOMPLETE", (
                f"Expected INCOMPLETE, "
                f"got {incomplete_state['state']}"
            )

            print("REAL MINIO INCOMPLETE STATE: PASS")

        finally:
            for obj in client.list_objects(
                BUCKET,
                prefix=incomplete_prefix,
                recursive=True,
            ):
                client.remove_object(
                    BUCKET,
                    obj.object_name,
                )
        print("FINAL RESULT: PASS 16/16 ")

    finally:
        # Only remove objects belonging to this unique test snapshot.
        # Never delete a shared bucket or another snapshot.
        try:
            objects = client.list_objects(
                BUCKET,
                prefix=snapshot_prefix,
                recursive=True,
            )

            for obj in objects:
                client.remove_object(BUCKET, obj.object_name)

            print("TEST SNAPSHOT CLEANUP: COMPLETE")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()