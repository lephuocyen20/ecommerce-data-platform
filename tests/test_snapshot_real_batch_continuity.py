from dotenv import load_dotenv

from ingestion.incremental.database import get_db_connection
from silver.snapshots.batch_discovery import (
    get_committed_batches,
)
from silver.snapshots.continuity import (
    validate_batch_continuity,
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
    load_dotenv()

    conn = get_db_connection()

    try:
        total_batches = 0

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

            total_batches += len(batches)

            print(
                f"{table_name}: "
                f"{len(batches)} batches - "
                f"CONTINUITY PASS"
            )

        print(f"TOTAL BATCHES: {total_batches}")
        print("FINAL RESULT: PASS 6/6")

    finally:
        conn.close()


if __name__ == "__main__":
    main()