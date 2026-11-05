
import os

import psycopg2
from minio import Minio

from silver.snapshots.batch_discovery import (
    get_committed_batches,
)
from silver.snapshots.continuity import (
    validate_batch_continuity,
)
from silver.snapshots.identity import (
    generate_snapshot_id,
)
from silver.snapshots.recovery import (
    inspect_snapshot_state,
)


def main():
    table_name = "orders"

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
        batches = get_committed_batches(
            conn,
            table_name,
        )

        if not batches:
            raise RuntimeError("No committed orders batches")

        validate_batch_continuity(batches)

        snapshot_id = generate_snapshot_id(
            table_name,
            batches,
        )

        state = inspect_snapshot_state(
            client,
            bucket,
            table_name,
            snapshot_id,
        )

        print("TABLE:", table_name)
        print("COMMITTED BATCHES:", len(batches))
        print("SNAPSHOT ID:", snapshot_id)
        print("SNAPSHOT STATE:", state)

        if state["state"] == "NEW":
            print("RESULT: READY FOR FIRST PUBLICATION")

        elif state["state"] == "PUBLISHED":
            print("RESULT: READY FOR IDEMPOTENT RETRY")

        elif state["state"] == "UNPUBLISHED":
            print("RESULT: RECOVERY REQUIRED")

        elif state["state"] == "INCOMPLETE":
            print("RESULT: INCOMPLETE SNAPSHOT - DO NOT OVERWRITE")

        else:
            raise RuntimeError(
                f"Unknown snapshot state: {state}"
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
