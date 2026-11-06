"""Production-path E2E: write a fresh verified Parquet manifest and migrate it.

The disposable Iceberg table and progress table keep this regression test from
changing the canonical customers state.  Its MinIO objects are removed in the
same run so cumulative reconciliation never discovers test data.
"""

import json
import os
import sys
import uuid
from datetime import datetime, timezone

from minio import Minio
from minio.deleteobjects import DeleteObject
from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a, get_minio_settings
from common.object_integrity import discover_parquet_objects
from lakehouse.parquet_customers import migrate_manifest


def main():
    suffix = uuid.uuid4().hex
    batch_id = uuid.uuid4().hex
    prefix = f"silver_incremental/customers/batch_id={batch_id}/runs/e2e-{suffix}/"
    manifest_key = f"silver_incremental/customers/batch_id={batch_id}/_BATCH_METADATA.json"
    table = f"lakehouse.validation.customers_manifest_{suffix}"
    progress_table = f"lakehouse.validation.customers_manifest_progress_{suffix}"
    settings = get_minio_settings()
    bucket = os.environ["BRONZE_BUCKET"]
    client = Minio(settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=False)
    spark = SparkSession.builder.appName("CustomersManifestE2E").getOrCreate()
    try:
        configure_s3a(spark)
        source = spark.createDataFrame([
            (900001, "Manifest Ada", "manifest-ada@example.test", "2026-01-01 00:00:00", "2026-04-01 00:00:00"),
            (900002, "Manifest Grace", "manifest-grace@example.test", "2026-01-02 00:00:00", "2026-04-02 00:00:00"),
        ], ["customer_id", "full_name", "email", "created_at", "updated_at"])
        source.write.mode("overwrite").parquet(f"s3a://{bucket}/{prefix}")
        objects = discover_parquet_objects(client, bucket, prefix, include_etag=True)
        manifest = {
            "status": "COMPLETED", "table_name": "customers", "batch_id": batch_id,
            "data_prefix": prefix, "silver_row_count": 2, "parquet_file_count": len(objects),
            "completed_at": datetime.now(timezone.utc).isoformat(), "parquet_objects": objects,
        }
        client.put_object(bucket, manifest_key, __import__("io").BytesIO(json.dumps(manifest).encode()), len(json.dumps(manifest).encode()), content_type="application/json")
        first = migrate_manifest(spark, client, bucket, manifest_key, table=table, progress_table=progress_table)
        replay = migrate_manifest(spark, client, bucket, manifest_key, table=table, progress_table=progress_table)
        assert first["status"] == "MIGRATED" and first["row_count"] == 2
        assert replay["status"] == "ALREADY_MIGRATED"
        assert spark.table(table).select("customer_id").distinct().count() == 2
        print("ICEBERG_CUSTOMERS_MANIFEST_E2E: PASS")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(f"DROP TABLE IF EXISTS {progress_table}")
        object_keys = [item.object_name for item in client.list_objects(bucket, prefix=f"silver_incremental/customers/batch_id={batch_id}/", recursive=True)]
        if object_keys:
            list(client.remove_objects(bucket, (DeleteObject(key) for key in object_keys)))
        spark.stop()


if __name__ == "__main__":
    main()
