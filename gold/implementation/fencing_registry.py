
"""PostgreSQL Gold fencing token allocation.

Phase B.4.9.3j:
- Validate execution identity
- Require live PostgreSQL advisory-lock ownership
- Allocate monotonically increasing fencing tokens
- Atomically persist registry and attempt history
- Preserve original errors during transaction cleanup
- Invalidate unsafe connections
- Fail closed on uncertain transaction outcomes

Does not authorize Gold WRITE or PUBLISH.
"""

import psycopg2

from gold.implementation.fencing import (
    ATTEMPT_ID_PATTERN,
    GoldExecutionAttempt,
    GoldFencingError,
)
from gold.implementation.ownership import (
    GoldOwnershipError,
    GoldOwnershipHandle,
)


MAX_FENCING_TOKEN = 9223372036854775807


ALLOCATION_SQL = """
INSERT INTO public.gold_execution_fencing (
    dataset_name,
    snapshot_id,
    fencing_token,
    attempt_id
)
VALUES (%s, %s, 1, %s)
ON CONFLICT (dataset_name, snapshot_id)
DO UPDATE SET
    fencing_token =
        gold_execution_fencing.fencing_token + 1,
    attempt_id = EXCLUDED.attempt_id,
    updated_at = NOW()
WHERE gold_execution_fencing.fencing_token < %s
  AND gold_execution_fencing.attempt_id
      IS DISTINCT FROM EXCLUDED.attempt_id
RETURNING fencing_token
"""


HISTORY_EXISTS_SQL = """
SELECT 1
FROM public.gold_execution_attempt_history
WHERE dataset_name = %s
  AND snapshot_id = %s
  AND attempt_id = %s
"""


INSERT_HISTORY_SQL = """
INSERT INTO public.gold_execution_attempt_history (
    dataset_name,
    snapshot_id,
    attempt_id,
    fencing_token
)
VALUES (%s, %s, %s, %s)
"""


def _add_cleanup_note(original_error, stage, cleanup_error):
    """Attach cleanup failure details without replacing the original error."""

    if hasattr(original_error, "add_note"):
        original_error.add_note(
            f"Gold fencing {stage} failed: "
            f"{type(cleanup_error).__name__}: "
            f"{cleanup_error}"
        )


def _close_unsafe_connection(conn, original_error=None):
    """Best-effort close of a connection that cannot be safely reused."""

    try:
        if not conn.closed:
            conn.close()
    except Exception as close_error:
        if original_error is not None:
            _add_cleanup_note(
                original_error,
                "connection close",
                close_error,
            )



def allocate_gold_fencing_token(handle, attempt_id):
    """Allocate a fencing token and persist history atomically.

    Fail closed on transaction, connection, or ownership failures.
    Does not authorize Gold WRITE or PUBLISH.
    """

    if not isinstance(handle, GoldOwnershipHandle):
        raise GoldFencingError("Invalid Gold ownership handle")

    if (
        not isinstance(attempt_id, str)
        or ATTEMPT_ID_PATTERN.fullmatch(attempt_id) is None
    ):
        raise GoldFencingError("Invalid Gold execution attempt ID")

    handle.assert_alive()
    conn = handle._conn

    if conn is None or conn.closed:
        raise GoldOwnershipError(
            "Gold ownership connection is unavailable"
        )

    if not conn.autocommit:
        raise GoldOwnershipError(
            "Gold fencing allocation requires autocommit"
        )

    token = None
    transaction_error = None
    cleanup_error = None
    unsafe_connection = False
    commit_attempted = False
    transaction_started = False

    try:
        conn.autocommit = False
        transaction_started = True

        with conn.cursor() as cursor:
            cursor.execute(
                HISTORY_EXISTS_SQL,
                (
                    handle.dataset_name,
                    handle.snapshot_id,
                    attempt_id,
                ),
            )

            if cursor.fetchone() is not None:
                raise GoldFencingError(
                    "Gold execution attempt ID already allocated"
                )

            cursor.execute(
                ALLOCATION_SQL,
                (
                    handle.dataset_name,
                    handle.snapshot_id,
                    attempt_id,
                    MAX_FENCING_TOKEN,
                ),
            )

            row = cursor.fetchone()

            if row is None or len(row) != 1:
                raise GoldFencingError(
                    "Gold fencing token allocation returned no token"
                )

            token = row[0]

            if (
                isinstance(token, bool)
                or not isinstance(token, int)
                or token <= 0
                or token > MAX_FENCING_TOKEN
            ):
                raise GoldFencingError(
                    "Invalid allocated Gold fencing token"
                )

            cursor.execute(
                INSERT_HISTORY_SQL,
                (
                    handle.dataset_name,
                    handle.snapshot_id,
                    attempt_id,
                    token,
                ),
            )

        commit_attempted = True
        conn.commit()

    except Exception as exc:
        transaction_error = exc

        if commit_attempted:
            # COMMIT outcome may be unknown.
            unsafe_connection = True

        elif transaction_started:
            try:
                if not conn.closed:
                    conn.rollback()
                else:
                    unsafe_connection = True
            except Exception as rollback_error:
                unsafe_connection = True
                _add_cleanup_note(
                    exc,
                    "rollback",
                    rollback_error,
                )
        else:
            # Failed while entering transaction mode.
            unsafe_connection = True

    # Restore autocommit only for reusable connections.
    if not unsafe_connection:
        try:
            if conn.closed:
                unsafe_connection = True
                cleanup_error = GoldFencingError(
                    "Gold fencing connection closed during allocation"
                )
            else:
                conn.autocommit = True
        except Exception as restore_error:
            unsafe_connection = True

            if transaction_error is not None:
                _add_cleanup_note(
                    transaction_error,
                    "autocommit restoration",
                    restore_error,
                )
            else:
                cleanup_error = GoldFencingError(
                    "Failed to restore Gold fencing autocommit"
                )
                cleanup_error.__cause__ = restore_error

    if unsafe_connection:
        primary_error = transaction_error or cleanup_error

        if primary_error is None:
            primary_error = GoldFencingError(
                "Gold fencing connection became unsafe"
            )

        _close_unsafe_connection(conn, primary_error)

    if transaction_error is not None:
        raise transaction_error.with_traceback(
            transaction_error.__traceback__
        )

    if cleanup_error is not None:
        raise cleanup_error

    if unsafe_connection:
        raise GoldFencingError(
            "Gold fencing connection is unsafe"
        )

    # A successful COMMIT is not sufficient if ownership was lost.
    handle.assert_alive()

    return GoldExecutionAttempt(
        dataset_name=handle.dataset_name,
        snapshot_id=handle.snapshot_id,
        attempt_id=attempt_id,
        fencing_token=token,
    )
