"""Warehouse loader orchestrator.

Modes:
    --dry-run : Process pending batches, then rollback everything.
    --commit  : Commit each completed batch atomically.
"""
from pathlib import Path
from dotenv import load_dotenv
import argparse
import os

from incremental_silver_to_warehouse import (
    get_connection,
    get_committed_batches,
    get_minio_client,
)
from warehouse_batch_loader import load_one_batch


def process_batches(dry_run=True, selected_tables=None, selected_batch_ids=None, strict_batch_selection=False,):
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
        batches = get_committed_batches(
            conn,
            selected_tables=selected_tables,
            selected_batch_ids=selected_batch_ids,
        )

        if strict_batch_selection:
            if selected_tables is None:
                raise ValueError(
                    "Strict mode requires selected_tables"
                )

            if selected_batch_ids is None:
                raise ValueError(
                    "Strict mode requires selected_batch_ids"
                )

            requested_ids = set(selected_batch_ids)
            found_ids = {
                batch["batch_id"]
                for batch in batches
            }

            if found_ids != requested_ids:
                missing_ids = sorted(requested_ids - found_ids)
                unexpected_ids = sorted(found_ids - requested_ids)

                raise RuntimeError(
                    "Strict Warehouse batch selection failed: "
                    f"missing={missing_ids}, "
                    f"unexpected={unexpected_ids}"
                )

        pending_batches = len(batches)

        print(f"PENDING BATCHES: {pending_batches}")
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
                    if strict_batch_selection:
                        raise RuntimeError(
                            "Strict Warehouse batch was already loaded "
                            "during processing: "
                            f"table={result['table_name']}, "
                            f"batch_id={result['batch_id']}"
                        )
                    skipped_batches += 1

                else:
                    raise ValueError(
                        f"Unexpected status: {result['status']}"
                    )

                if not dry_run and not strict_batch_selection:
                    # Preserve per-batch commits for manual recovery.
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
            if strict_batch_selection:
                # Commit all selected batches atomically.
                conn.commit()

            print("WAREHOUSE COMMIT: COMPLETED")

        return {
            "status": "PASS",
            "mode": "DRY_RUN" if dry_run else "COMMIT",
            "selected_tables": (
                list(selected_tables)
                if selected_tables is not None
                else None
            ),
            "selected_batch_ids": (
                list(selected_batch_ids)
                if selected_batch_ids is not None
                else None
            ),
            "strict_batch_selection": strict_batch_selection,
            "pending_batches": pending_batches,
            "loaded_batches": loaded_batches,
            "loaded_rows": loaded_rows,
            "skipped_batches": skipped_batches,
        }

    finally:
        if not conn.closed:
            conn.rollback()
            conn.close()


def main():

    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")

    for key in ("HOST", "PORT", "NAME", "USER", "PASSWORD"):
        warehouse_key = f"WAREHOUSE_DB_{key}"
        source_key = f"DB_{key}"

        if warehouse_key not in os.environ and source_key in os.environ:
            os.environ[warehouse_key] = os.environ[source_key]


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
