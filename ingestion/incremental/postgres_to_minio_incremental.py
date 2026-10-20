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