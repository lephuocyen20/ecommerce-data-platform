
"""Transform one validated incremental Bronze batch into Silver Parquet.

Run with spark-submit inside the ecommerce-spark image.

A batch is published only after a complete, verified Parquet write.
The immutable run prefix is referenced by a completion manifest.
"""

import argparse
import io
import json
import os
import re
import uuid
import hashlib
from datetime import datetime, timezone

from minio import Minio
from minio.error import S3Error
from pyspark.sql import SparkSession

from config import BRONZE_BUCKET, MINIO_ENDPOINT
from bronze_to_silver import configure_s3, TRANSFORMS


VALID_BATCH_ID = re.compile(r"^[0-9a-f]{32}$")

TABLE_PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "shipments": "shipment_id",
}



def validate_arguments(table_name, batch_id, object_key, bronze_checksum):
    if table_name not in TABLE_PRIMARY_KEYS:
        raise ValueError(f"Unsupported table: {table_name}")

    if not VALID_BATCH_ID.fullmatch(batch_id):
        raise ValueError(f"Invalid batch ID: {batch_id}")

    expected_key = (
        f"bronze_incremental/{table_name}/"
        f"batch_id={batch_id}/data.csv"
    )

    if object_key != expected_key:
        raise ValueError(
            f"Unexpected Bronze object key: {object_key}"
        )

    if not re.fullmatch(r"[0-9a-f]{64}", bronze_checksum):
        raise ValueError("Invalid Bronze SHA-256 checksum")


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


def verify_bronze_checksum(
    client,
    bucket,
    object_key,
    expected_checksum,
):
    """Verify the exact Bronze object before Spark reads it."""

    response = client.get_object(bucket, object_key)
    digest = hashlib.sha256()

    try:
        for chunk in response.stream(1024 * 1024):
            digest.update(chunk)
    finally:
        response.close()
        response.release_conn()

    actual_checksum = digest.hexdigest()

    if actual_checksum != expected_checksum:
        raise ValueError(
            f"Bronze checksum mismatch for {object_key}: "
            f"expected={expected_checksum}, "
            f"actual={actual_checksum}"
        )

    return True



def read_json_object(client, bucket, object_key):
    response = client.get_object(bucket, object_key)

    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def get_completion_manifest(client, bucket, manifest_key):
    try:
        client.stat_object(bucket, manifest_key)
    except Exception as exc:
        from minio.error import S3Error

        if isinstance(exc, S3Error) and exc.code in (
            "NoSuchKey",
            "NoSuchObject",
        ):
            return None

        raise

    return read_json_object(client, bucket, manifest_key)


def verify_published_manifest(
    spark,
    client,
    bucket,
    manifest,
    table_name,
    batch_id,
    object_key,
    bronze_checksum,
):
    if manifest.get("status") != "COMPLETED":
        raise ValueError("Invalid Silver manifest status")

    if manifest.get("table_name") != table_name:
        raise ValueError("Silver manifest table mismatch")

    if manifest.get("batch_id") != batch_id:
        raise ValueError("Silver manifest batch ID mismatch")

    if manifest.get("bronze_object_key") != object_key:
        raise ValueError("Silver manifest Bronze key mismatch")
    if manifest.get("bronze_checksum_sha256") != bronze_checksum:
        raise ValueError("Silver manifest Bronze checksum mismatch")
    expected_prefix = (
        f"silver_incremental/{table_name}/"
        f"batch_id={batch_id}/runs/"
    )

    data_prefix = manifest.get("data_prefix")

    if (
        not isinstance(data_prefix, str)
        or not data_prefix.startswith(expected_prefix)
        or not re.fullmatch(
            r"[0-9a-f]{32}/",
            data_prefix[len(expected_prefix):],
        )
    ):
        raise ValueError("Invalid Silver data prefix")

    expected_count = manifest.get("silver_row_count")

    if (
        not isinstance(expected_count, int)
        or isinstance(expected_count, bool)
        or expected_count < 0
    ):
        raise ValueError("Invalid Silver row count")

    actual_parquet_objects = sorted(
    [
        {
            "object_key": obj.object_name,
            "size": obj.size,
            "etag": obj.etag.strip('"'),
        }
        for obj in client.list_objects(
            bucket,
            prefix=data_prefix,
            recursive=True,
        )
        if obj.object_name.endswith(".parquet")
    ],
    key=lambda item: item["object_key"],
)

    if not actual_parquet_objects:
        raise ValueError(
            "Published Silver Parquet files are missing"
            )
    
    expected_parquet_objects = manifest.get("parquet_objects")

    if not isinstance(expected_parquet_objects, list):
        raise ValueError(
            "Silver manifest missing Parquet object metadata"
        )

    if actual_parquet_objects != expected_parquet_objects:
        raise ValueError(
            "Published Silver Parquet objects do not match manifest"
        )

    if manifest.get("parquet_file_count") != len(actual_parquet_objects):
        raise ValueError(
            "Published Silver Parquet file count mismatch"
        )

    silver_path = f"s3a://{bucket}/{data_prefix}"

    df = spark.read.parquet(silver_path)

    if TABLE_PRIMARY_KEYS[table_name] not in df.columns:
        raise ValueError("Published Silver primary key is missing")

    actual_count = df.count()

    if actual_count != expected_count:
        raise ValueError(
            "Published Silver row count mismatch: "
            f"expected={expected_count}, actual={actual_count}"
        )

    
    # Verify primary key integrity when reusing published Silver.
    primary_key = TABLE_PRIMARY_KEYS[table_name]

    if df.filter(
        df[primary_key].isNull()
    ).limit(1).count():
        raise ValueError(
        "Published Silver contains null primary keys"
        )

    duplicate_pk_count = (
        df.groupBy(primary_key)
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
        )

    if duplicate_pk_count:
        raise ValueError(
        "Published Silver contains duplicate primary keys"
    )


    return manifest



def publish_manifest(client, bucket, manifest_key, manifest):
    """
    Publish a completion manifest only when its key does not exist.

    Uses the MinIO SDK 7.2.10 private API because the public
    put_object() method does not expose conditional headers.
    """
    payload = json.dumps(
        manifest,
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")

    return client._put_object(
        bucket_name=bucket,
        object_name=manifest_key,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "If-None-Match": "*",
        },
    )



def transform_batch( spark, client, table_name, batch_id, object_key, bronze_checksum):
    bucket = BRONZE_BUCKET

    if not client.bucket_exists(bucket):
        raise ValueError(f"MinIO bucket does not exist: {bucket}")
    verify_bronze_checksum(
            client,
            bucket,
         object_key,
            bronze_checksum,)
    batch_prefix = (
        f"silver_incremental/{table_name}/"
        f"batch_id={batch_id}/"
    )

    manifest_key = batch_prefix + "_BATCH_METADATA.json"

    existing_manifest = get_completion_manifest(
        client,
        bucket,
        manifest_key,
    )

    if existing_manifest is not None:
        verified = verify_published_manifest(
            spark,
            client,
            bucket,
            existing_manifest,
            table_name,
            batch_id,
            object_key,
            bronze_checksum,
        )

        print(
            json.dumps(
                {
                    "status": "ALREADY_COMPLETED",
                    "table_name": table_name,
                    "batch_id": batch_id,
                    "manifest_key": manifest_key,
                    "silver_row_count": verified["silver_row_count"],
                }
            )
        )
        return

    bronze_path = f"s3a://{bucket}/{object_key}"

    bronze_df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .option("mode", "FAILFAST")
    .csv(bronze_path)
)

    transform_function = TRANSFORMS[table_name]

    silver_df = transform_function(bronze_df)

    run_id = uuid.uuid4().hex
    data_prefix = batch_prefix + f"runs/{run_id}/"
    silver_path = f"s3a://{bucket}/{data_prefix}"

    # Each attempt uses a unique prefix.
    # Never overwrite an existing published batch.
    silver_df.write.mode("errorifexists").parquet(silver_path)

    verified_df = spark.read.parquet(silver_path)

    primary_key = TABLE_PRIMARY_KEYS[table_name]

    if primary_key not in verified_df.columns:
        raise ValueError("Silver primary key is missing")

    silver_count = verified_df.count()

    if verified_df.filter(
        verified_df[primary_key].isNull()
    ).limit(1).count():
        raise ValueError("Silver contains null primary keys")

    
    # Verify primary key uniqueness in persisted Silver data.
    duplicate_pk_count = (
        verified_df.groupBy(primary_key)
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
    )   

    if duplicate_pk_count:
        raise ValueError(
            f"Silver contains duplicate primary keys: {primary_key}"
        )

    
    parquet_objects = sorted(
    [
        {
            "object_key": obj.object_name,
            "size": obj.size,
            "etag": obj.etag.strip('"'),
        }
        for obj in client.list_objects(
            bucket,
            prefix=data_prefix,
            recursive=True,
        )
        if obj.object_name.endswith(".parquet")
    ],
    key=lambda item: item["object_key"],
)

    if not parquet_objects:
        raise ValueError("Spark did not produce Parquet files")

    manifest = {
        "status": "COMPLETED",
        "table_name": table_name,
        "batch_id": batch_id,
        "bronze_object_key": object_key,
        "data_prefix": data_prefix,
        "silver_row_count": silver_count,
        "parquet_file_count": len(parquet_objects),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "bronze_checksum_sha256": bronze_checksum,
        "parquet_objects": parquet_objects,
    }

    try:
        publish_manifest(
            client,
            bucket,
            manifest_key,
            manifest,
        )
    except S3Error as exc:
        if exc.code not in (
        "PreconditionFailed",
        "ConditionalRequestConflict",
    ):
            raise

    # Another attempt may have published the manifest first.
        existing_manifest = get_completion_manifest(
            client,
            bucket,
            manifest_key,
        )

        if existing_manifest is None:
            raise

        verify_published_manifest(
            spark,
            client,
            bucket,
            existing_manifest,
            table_name,
            batch_id,
            object_key,
            bronze_checksum,
        )

        print(
            json.dumps(
                {
                    "status": "ALREADY_COMPLETED",
                    "table_name": table_name,
                    "batch_id": batch_id,
                    "manifest_key": manifest_key,
                }
            )
        )
        return

    print(
        json.dumps(
            {
                "status": "COMPLETED",
                "table_name": table_name,
                "batch_id": batch_id,
                "silver_row_count": silver_count,
                "manifest_key": manifest_key,
                "data_prefix": data_prefix,
            }
        )
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--table-name", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--object-key", required=True)
    parser.add_argument("--bronze-checksum", required=True)

    args = parser.parse_args()

    validate_arguments(
        args.table_name,
        args.batch_id,
        args.object_key,
        args.bronze_checksum,
)

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("IncrementalBronzeToSilver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        configure_s3(spark)

        client = get_minio_client()

        transform_batch(
            spark,
            client,
            args.table_name,
            args.batch_id,
            args.object_key,
            args.bronze_checksum,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
