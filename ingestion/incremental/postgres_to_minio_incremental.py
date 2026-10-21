"""Primary entrypoint for incremental PostgreSQL-to-MinIO ingestion.

Implementation lives in :mod:`ingestion.incremental`; this module intentionally
keeps the public API used by existing callers and tests.
"""

if __package__ in (None, ""):
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.incremental.orchestrator import *  # noqa: F403
from ingestion.incremental.orchestrator import _execute_two_transaction_ingestion



def main():
    import argparse
    import json
    from datetime import date, datetime
    from pathlib import Path

    from ingestion.incremental.config import TABLE_CONFIG

    parser = argparse.ArgumentParser(
        description="Incremental PostgreSQL to MinIO ingestion"
    )

    parser.add_argument(
        "--table",
        required=True,
        choices=sorted(TABLE_CONFIG),
        help="Source table to ingest",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Maximum number of records per batch",
    )

    parser.add_argument(
        "--staging-root",
        default="staging",
        help="Local staging directory",
    )

    parser.add_argument(
        "--write",
        action="store_true",
        help="Enable write-mode (also requires environment permission)",
    )

    args = parser.parse_args()

    if args.write:
        result = run_incremental_ingestion_write(
            table_name=args.table,
            batch_size=args.batch_size,
            staging_root=args.staging_root,
            allow_write=True,
        )
    else:
        result = run_incremental_ingestion(
            table_name=args.table,
            batch_size=args.batch_size,
            staging_root=args.staging_root,
            dry_run=True,
        )

    print(
        json.dumps(
            result,
            indent=2,
            default=lambda obj: (
                obj.isoformat()
                if isinstance(obj, (datetime, date))
                else str(obj)
            ),
        )
    )


if __name__ == "__main__":
    main()
