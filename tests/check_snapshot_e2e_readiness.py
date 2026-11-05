import os

import psycopg2
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

from silver.snapshots.batch_discovery import (
    get_committed_batches,
)
from silver.snapshots.continuity import (
    validate_batch_continuity,
)
from silver.snapshots.identity import (
    generate_snapshot_id,
)


TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
]


def main():
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

    try:
        for table_name in TABLES:
            batches = get_committed_batches(
                conn=conn,
                table_name=table_name,
            )

            if not batches:
                raise AssertionError(
                    f"No committed batches: {table_name}"
                )

            validate_batch_continuity(batches)

            missing_manifests = []

            for batch in batches:
                manifest_key = (
                    f"silver_incremental/{table_name}/"
                    f"batch_id={batch['batch_id']}/"
                    "_BATCH_METADATA.json"
                )

                objects = list(
                    client.list_objects(
                        bucket,
                        prefix=manifest_key,
                        recursive=True,
                    )
                )

                if not any(
                    obj.object_name == manifest_key
                    for obj in objects
                ):
                    missing_manifests.append(manifest_key)

            if missing_manifests:
                raise AssertionError(
                    f"Missing Silver manifests for {table_name}: "
                    f"{missing_manifests}"
                )

            snapshot_id = generate_snapshot_id(
                table_name=table_name,
                batches=batches,
            )

            print(
                f"{table_name}: READY | "
                f"batches={len(batches)} | "
                f"snapshot_id={snapshot_id}"
            )

        print("FINAL RESULT: READY 6/6")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
