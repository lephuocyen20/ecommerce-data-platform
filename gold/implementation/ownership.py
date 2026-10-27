
"""PostgreSQL advisory-lock ownership for Gold execution."""

import hashlib
import struct
from contextlib import contextmanager

import psycopg2

from gold.implementation.storage.contract import (
    validate_dataset_name,
    validate_gold_snapshot_id,
)
from common.database import connect_postgres


class GoldOwnershipError(RuntimeError):
    """Gold execution ownership could not be established."""


class GoldOwnershipBusyError(GoldOwnershipError):
    """Another session currently owns the Gold execution lock."""


def build_gold_lock_key(dataset_name, snapshot_id):
    """Build a stable signed 64-bit PostgreSQL advisory lock key."""

    validate_dataset_name(dataset_name)
    validate_gold_snapshot_id(snapshot_id)

    identity = (
        f"gold-execution:v1:{dataset_name}:{snapshot_id}"
    ).encode("utf-8")

    digest = hashlib.sha256(identity).digest()

    return struct.unpack(">q", digest[:8])[0]


def connect_gold_lock_db():
    """Create a dedicated PostgreSQL connection for ownership."""

    import os

    return connect_postgres(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
        application_name="gold_execution_ownership",
    )


@contextmanager
def acquire_gold_ownership(
    dataset_name,
    snapshot_id,
    connection_factory=connect_gold_lock_db,
):
    """
    Acquire an exclusive PostgreSQL session advisory lock.

    The connection remains open while the context is active.

    This is a coordination primitive, not a fencing mechanism.
    It does not authorize production WRITE.
    """

    lock_key = build_gold_lock_key(
        dataset_name,
        snapshot_id,
    )

    conn = None
    acquired = False
    handle = None

    try:
        conn = connection_factory()

        if conn is None or conn.closed:
            raise GoldOwnershipError(
                "Ownership database connection is unavailable"
            )

        conn.autocommit = True

        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (lock_key,),
            )
            row = cursor.fetchone()

        if row is None or row[0] is not True:
            raise GoldOwnershipBusyError(
                "Gold snapshot ownership is already held"
            )

        acquired = True

        handle = GoldOwnershipHandle(
            conn=conn,
            dataset_name=dataset_name,
            snapshot_id=snapshot_id,
            lock_key=lock_key,
        )

        yield handle

    except (psycopg2.Error, OSError) as exc:
        raise GoldOwnershipError(
            "Gold ownership database operation failed"
        ) from exc

    finally:
        if handle is not None:
            handle._deactivate()

        if conn is not None:
            try:
                if acquired and not conn.closed:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "SELECT pg_advisory_unlock(%s)",
                            (lock_key,),
                        )
                        row = cursor.fetchone()

                    if row is None or row[0] is not True:
                        raise GoldOwnershipError(
                            "Gold advisory lock release failed"
                        )
            finally:
                conn.close()


def assert_gold_ownership_alive(conn, lock_key):
    """
    Verify that the current PostgreSQL session still owns
    the expected session-level advisory lock.

    Read-only health check:
    - Does not acquire another lock
    - Does not release a lock
    - Does not authorize Gold WRITE
    """

    if conn is None or conn.closed:
        raise GoldOwnershipError(
            "Gold ownership connection is closed"
        )

    if not isinstance(lock_key, int):
        raise GoldOwnershipError(
            "Gold ownership lock key must be an integer"
        )

    if not (-(2**63) <= lock_key < 2**63):
        raise GoldOwnershipError(
            "Gold ownership lock key is out of range"
        )

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE locktype = 'advisory'
                      AND pid = pg_backend_pid()
                      AND granted = TRUE
                      AND classid = (
                          (%s::bigint >> 32) & 4294967295
                      )::oid
                      AND objid = (
                          %s::bigint & 4294967295
                      )::oid
                      AND objsubid = 1
                )
                """,
                (lock_key, lock_key),
            )

            row = cursor.fetchone()

    except (psycopg2.Error, OSError) as exc:
        raise GoldOwnershipError(
            "Gold ownership health check failed"
        ) from exc

    if row is None or row[0] is not True:
        raise GoldOwnershipError(
            "Gold advisory lock is no longer owned"
        )

    return {
        "lock_key": lock_key,
        "status": "HEALTHY",
    }


class GoldOwnershipHandle:
    """
    Read-only ownership handle for an acquired Gold lock.

    Does not expose the PostgreSQL connection publicly.
    Does not authorize Gold WRITE.
    """

    def __init__(self, conn, dataset_name, snapshot_id, lock_key):
        self._conn = conn
        self.dataset_name = dataset_name
        self.snapshot_id = snapshot_id
        self.lock_key = lock_key
        self._active = True

    @property
    def status(self):
        return "ACQUIRED" if self._active else "RELEASED"

    def assert_alive(self):
        if not self._active:
            raise GoldOwnershipError(
                "Gold ownership handle is no longer active"
            )

        return assert_gold_ownership_alive(
            self._conn,
            self.lock_key,
        )

    def _deactivate(self):
        self._active = False

    def allocate_fencing_token(self, attempt_id):
        """Allocate a token through the active ownership session."""
        if not self._active:
            raise GoldOwnershipError(
                "Gold ownership handle is no longer active"
            )

        # Local import avoids a circular dependency.
        from gold.implementation.fencing_registry import (
            allocate_gold_fencing_token,
        )

        return allocate_gold_fencing_token(
            self,
            attempt_id,
        )
