"""Warehouse loader orchestrator.

Modes:
    --dry-run : Process pending batches, then rollback everything.
    --commit  : Commit each completed batch atomically.
"""

import argparse
import os

from incremental_silver_to_warehouse import (
    get_connection,
    get_committed_batches,
    get_minio_client,
)
from warehouse_batch_loader import load_one_batch


def process_batches(dry_run=True):
    bucket = os.environ.get(
        "MINIO_BUCKET",
        "ecommerce-data-lake",
    )

    client = get_minio_client()

    loaded_batches = 0
    loaded_rows = 0
    skipped_batches = 0

    conn = get_connection()

    try:
        conn.autocommit = False

        # Discover pending batches before processing.
        batches = get_committed_batches(conn)

        print(f"PENDING BATCHES: {len(batches)}")
        print(
            "MODE: DRY RUN"
            if dry_run
            else "MODE: COMMIT"
        )

        # End the metadata-reading transaction before
        # beginning individual batch transactions.
        conn.rollback()

        for batch in batches:
            try:
                result = load_one_batch(
                    conn,
                    client,
                    bucket,
                    batch,
                )

                if result["status"] == "LOADED":
                    loaded_batches += 1
                    loaded_rows += result["loaded_row_count"]

                elif result["status"] == "SKIPPED":
                    skipped_batches += 1

                else:
                    raise ValueError(
                        f"Unexpected status: {result['status']}"
                    )

                if dry_run:
                    # Keep all dry-run writes in the same
                    # transaction so watermark continuity
                    # works across multiple batches.
                    pass
                else:
                    # Raw UPSERT and checkpoint are committed
                    # together for this batch.
                    conn.commit()

                print(
                    f"table={result['table_name']} "
                    f"batch_id={result['batch_id']} "
                    f"status={result['status']} "
                    f"rows={result['loaded_row_count']}"
                )

            except Exception:
                conn.rollback()
                raise

        print(
            f"SUMMARY: "
            f"loaded_batches={loaded_batches} "
            f"loaded_rows={loaded_rows} "
            f"skipped_batches={skipped_batches}"
        )

        if dry_run:
            conn.rollback()
            print("DRY RUN ROLLBACK: COMPLETED")
        else:
            print("WAREHOUSE COMMIT: COMPLETED")

    finally:
        if not conn.closed:
            conn.rollback()
            conn.close()


def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Process all batches and rollback",
    )

    mode.add_argument(
        "--commit",
        action="store_true",
        help="Commit each batch atomically",
    )

    args = parser.parse_args()

    process_batches(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
