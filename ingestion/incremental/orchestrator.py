from pathlib import Path

from dotenv import load_dotenv

from ingestion.incremental.recovery import (
    execute_pending_recovery,
    execute_uploaded_recovery,
    recover_staged_batch,
    recover_uploaded_batch,
    upload_recovered_batch,
    validate_recovery_metadata,
)
from ingestion.incremental.batch_repository import (
    acquire_table_lock,
    get_batch_metadata,
    get_recoverable_batches,
    has_blocking_batch,
    has_failed_batch,
    has_unfinished_batch,
    lock_batch_metadata,
    mark_batch_uploaded,
    register_pending_batch,
)
from ingestion.incremental.minio_storage import (
    check_minio_bucket,
    get_minio_client,
    object_exists,
    put_object_if_absent,
    upload_incremental_batch,
    verify_minio_object,
)
from ingestion.incremental.watermark import (
    commit_uploaded_batch,
    get_locked_watermark,
    get_watermark,
    watermark_matches_batch_start,
)

try:
    from ingestion.incremental.minio_storage import build_object_key
    from ingestion.incremental.staging import (
        generate_batch_id,
    load_staged_batch,
        prepare_batch_metadata,
        records_to_csv,
    stage_batch_csv,
    )
except ModuleNotFoundError as exc:
    if exc.name != "ingestion":
        raise
    try:
        from incremental.minio_storage import build_object_key
        from incremental.staging import (
            generate_batch_id,
            prepare_batch_metadata,
            records_to_csv,
        )
    except ModuleNotFoundError as fallback_exc:
        if fallback_exc.name != "incremental":
            raise
        from minio_storage import build_object_key
        from staging import generate_batch_id, prepare_batch_metadata, records_to_csv

try:
    from ingestion.incremental.database import get_db_connection, require_transaction
except ModuleNotFoundError as exc:
    if exc.name != "ingestion":
        raise
    try:
        from incremental.database import get_db_connection, require_transaction
    except ModuleNotFoundError as fallback_exc:
        if fallback_exc.name != "incremental":
            raise
        from database import get_db_connection, require_transaction

try:
    from ingestion.incremental.extractor import extract_incremental
except ModuleNotFoundError as exc:
    if exc.name != "ingestion":
        raise
    try:
        from incremental.extractor import extract_incremental
    except ModuleNotFoundError as fallback_exc:
        if fallback_exc.name != "incremental":
            raise
        from extractor import extract_incremental

try:
    from ingestion.incremental.config import (
        BLOCKING_STATUSES,
        EXPECTED_DB_NAME,
        RECOVERABLE_STATUSES,
        TABLE_CONFIG,
        VALID_BATCH_ID,
        VALID_CHECKSUM,
        get_table_config,
        validate_batch_id,
        validate_positive_integer,
    )
except ModuleNotFoundError as exc:
    if exc.name != "ingestion":
        raise
    try:
        from incremental.config import (
            BLOCKING_STATUSES,
            EXPECTED_DB_NAME,
            RECOVERABLE_STATUSES,
            TABLE_CONFIG,
            VALID_BATCH_ID,
            VALID_CHECKSUM,
            get_table_config,
            validate_batch_id,
            validate_positive_integer,
        )
    except ModuleNotFoundError as fallback_exc:
        if fallback_exc.name != "incremental":
            raise
        from config import (
            BLOCKING_STATUSES,
            EXPECTED_DB_NAME,
            RECOVERABLE_STATUSES,
            TABLE_CONFIG,
            VALID_BATCH_ID,
            VALID_CHECKSUM,
            get_table_config,
            validate_batch_id,
            validate_positive_integer,
        )

load_dotenv(dotenv_path=".env")


def prepare_pending_batch(
    conn,
    table_name,
    batch_size=1000,
    staging_root="staging",
):
    """
    Prepare and register one PENDING batch safely.

    Does not COMMIT.
    Caller must COMMIT or ROLLBACK the transaction.
    """

    get_table_config(table_name)
    validate_positive_integer(batch_size, "batch_size")
    require_transaction(conn)

    # 1. Acquire per-table transaction lock
    if not acquire_table_lock(conn, table_name):
        raise RuntimeError(f"Table is locked: {table_name}")

    # 2. Block new ingestion if recovery is required
    if has_blocking_batch(conn, table_name):
        raise RuntimeError(f"Blocking batch exists for {table_name}")

    # 3. Read current watermark
    watermark = get_watermark(conn, table_name)

    # 4. Extract incremental rows
    batch = extract_incremental(
        conn,
        table_name,
        watermark,
        batch_size=batch_size,
    )

    if batch["row_count"] == 0:
        return None

    # 5. Build CSV and metadata
    csv_content = records_to_csv(
        batch["columns"],
        batch["rows"],
    )

    metadata = prepare_batch_metadata(
        table_name,
        watermark,
        batch,
        csv_content,
    )

    # 6. Persist CSV before registering PENDING
    staged_path = stage_batch_csv(
        metadata,
        csv_content,
        staging_root=staging_root,
    )

    # 7. Register PENDING in current transaction
    register_pending_batch(conn, metadata)

    return {
        "metadata": metadata,
        "staged_path": staged_path,
    }


def select_ingestion_action(
    table_name,
    recoverable_batches,
    has_blocking,
):
    """
    Decide the next ingestion action without writing data.
    """

    get_table_config(table_name)

    if len(recoverable_batches) > 1:
        raise RuntimeError(f"Multiple unfinished batches: {table_name}")

    if recoverable_batches:
        batch = recoverable_batches[0]
        status = batch["status"]

        if status == "PENDING":
            return "RECOVER_PENDING"

        if status == "UPLOADED":
            return "RECOVER_UPLOADED"

        raise RuntimeError(f"Unexpected recoverable status: {status}")

    if has_blocking:
        raise RuntimeError(f"Blocking batch requires investigation: {table_name}")

    return "NEW_BATCH"


def run_incremental_ingestion(
    table_name,
    batch_size=1000,
    staging_root="staging",
    dry_run=True,
    rollback_test=False,
):
    """
    Recovery-first orchestrator.

    dry_run=True:
        Inspect metadata only.

    rollback_test=True:
        Execute RECOVER_UPLOADED, then always ROLLBACK.

    Production write-mode is not enabled yet.
    """

    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table_name}")

    validate_positive_integer(batch_size, "batch_size")

    if not isinstance(dry_run, bool):
        raise ValueError("dry_run must be boolean")

    if not isinstance(rollback_test, bool):
        raise ValueError("rollback_test must be boolean")

    if dry_run and rollback_test:
        raise ValueError("dry_run and rollback_test cannot both be True")

    conn = get_db_connection()

    try:
        if dry_run:
            recoverable = get_recoverable_batches(conn, table_name)

            blocking = has_blocking_batch(conn, table_name)

            if has_failed_batch(conn, table_name):
                raise RuntimeError(f"FAILED batch requires investigation: {table_name}")

            action = select_ingestion_action(
                table_name,
                recoverable,
                blocking,
            )

            return {
                "table_name": table_name,
                "mode": "DRY_RUN",
                "action": action,
                "watermark": get_watermark(conn, table_name),
                "recoverable_batches": [item["batch_id"] for item in recoverable],
            }

        if rollback_test:
            # Lock first, then re-read batch state.
            client = get_minio_client()

            result = execute_ingestion_action_locked(
                conn=conn,
                client=client,
                table_name=table_name,
                batch_size=batch_size,
                staging_root=staging_root,
            )

            return {
                **result,
                "mode": "ROLLBACK_TEST",
                "persisted": False,
            }

        raise NotImplementedError("Production write-mode is not enabled yet")

    finally:
        conn.rollback()
        conn.close()


def execute_new_batch(
    conn,
    table_name,
    batch_size=1000,
    staging_root="staging",
):
    """
    Prepare a new incremental batch.

    Requires an active PostgreSQL transaction.

    Creates:
      - staged CSV file
      - PENDING batch metadata (uncommitted)

    Does NOT:
      - upload to MinIO
      - advance watermark
      - commit PostgreSQL
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table_name}")

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    # prepare_pending_batch() acquires the table advisory
    # transaction lock and checks for blocking batches.
    result = prepare_pending_batch(
        conn=conn,
        table_name=table_name,
        batch_size=batch_size,
        staging_root=staging_root,
    )

    if result is None:
        return {
            "action": "NEW_BATCH",
            "table_name": table_name,
            "status": "NO_DATA",
            "row_count": 0,
            "batch_id": None,
        }

    metadata = result["metadata"]

    if metadata["table_name"] != table_name:
        raise RuntimeError("Prepared batch table mismatch")

    if metadata["status"] != "PENDING":
        raise RuntimeError("Expected PENDING batch")

    return {
        "action": "NEW_BATCH",
        "table_name": table_name,
        "status": "PENDING",
        "batch_id": metadata["batch_id"],
        "row_count": metadata["row_count"],
        "object_key": metadata["object_key"],
        "staged_path": result["staged_path"],
    }


def execute_ingestion_action(
    conn,
    client,
    table_name,
    action,
    recoverable_batches,
    staging_root="staging",
):
    """Execute one selected recovery action in a caller-owned transaction."""
    require_transaction(conn)
    get_table_config(table_name)

    if action not in ("RECOVER_PENDING", "RECOVER_UPLOADED"):
        raise ValueError(f"Unsupported action: {action}")
    if len(recoverable_batches) != 1:
        raise RuntimeError("Expected exactly one recoverable batch")

    batch = recoverable_batches[0]
    if batch["table_name"] != table_name:
        raise RuntimeError("Recovery batch table mismatch")

    if action == "RECOVER_PENDING":
        if batch["status"] != "PENDING":
            raise ValueError("Expected PENDING batch")
        return execute_pending_recovery(
            conn, client, batch["batch_id"], staging_root=staging_root
        )

    if batch["status"] != "UPLOADED":
        raise ValueError("Expected UPLOADED batch")
    return execute_uploaded_recovery(conn, client, batch["batch_id"])


def execute_ingestion_action_locked(
    conn,
    client,
    table_name,
    batch_size=1000,
    staging_root="staging",
):
    """
    Re-select and execute ingestion action under
    a PostgreSQL table advisory transaction lock.

    Supports recovery of unfinished batches and creation of a new PENDING batch.

    Caller owns commit/rollback.
    """

    require_transaction(conn)
    get_table_config(table_name)

    # 1. Acquire table-level advisory transaction lock
    if not acquire_table_lock(conn, table_name):
        raise RuntimeError(f"Table ingestion lock is busy: {table_name}")

    # 2. Re-read batch state after acquiring lock
    recoverable = get_recoverable_batches(
        conn,
        table_name,
    )

    blocking = has_blocking_batch(
        conn,
        table_name,
    )

    # 3. Block ingestion if FAILED batches exist.
    if has_failed_batch(conn, table_name):
        raise RuntimeError(f"FAILED batch requires investigation: {table_name}")

    # 4. Select action while holding advisory lock
    action = select_ingestion_action(
        table_name,
        recoverable,
        blocking,
    )

    # 5. Execute while retaining the advisory lock.
    if action == "NEW_BATCH":
        return execute_new_batch(
            conn=conn,
            table_name=table_name,
            batch_size=batch_size,
            staging_root=staging_root,
        )

    return execute_ingestion_action(
        conn=conn,
        client=client,
        table_name=table_name,
        action=action,
        recoverable_batches=recoverable,
        staging_root=staging_root,
    )


def persist_new_pending_batch(
    table_name,
    batch_size=1000,
    staging_root="staging",
    allow_commit=False,
):
    """
    Prepare a new PENDING batch and optionally commit it.

    Default:
        allow_commit=False -> always rollback PostgreSQL.

    Explicit commit:
        allow_commit=True -> commit PENDING metadata only.

    Does not upload to MinIO or advance watermark.
    """

    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table_name}")

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    if not isinstance(allow_commit, bool):
        raise ValueError("allow_commit must be boolean")

    conn = get_db_connection()

    try:
        # Independent guard before any writes.
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            actual_database = cursor.fetchone()[0]

        if actual_database != "ecommerce_test":
            raise RuntimeError("Refusing to write outside ecommerce_test")

        result = execute_new_batch(
            conn=conn,
            table_name=table_name,
            batch_size=batch_size,
            staging_root=staging_root,
        )

        if result["status"] == "NO_DATA":
            conn.rollback()

            return {
                **result,
                "persisted": False,
            }

        if result["status"] != "PENDING":
            raise RuntimeError(f"Unexpected batch status: {result['status']}")

        # Re-check that the staged file is present.
        staged_path = Path(result["staged_path"])

        if not staged_path.is_file():
            raise RuntimeError("Staged CSV is missing")

        if allow_commit:
            conn.commit()
        else:
            conn.rollback()

        return {
            **result,
            "persisted": allow_commit,
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def _execute_two_transaction_ingestion(
    table_name,
    batch_size=1000,
    staging_root="staging",
):
    """
    Internal two-transaction ingestion orchestrator.

    Transaction 1:
        - Acquire lock and re-read state.
        - Recover an existing batch, OR
        - Prepare and commit a new PENDING batch.

    Transaction 2:
        - Recover the newly committed PENDING batch.
        - Commit the checkpoint.

    This function is not connected to the public
    write entrypoint yet.

    IMPORTANT:
        Must only be invoked through a guarded caller.
    """

    # ----------------------------------------------------
    # Transaction 1
    # ----------------------------------------------------
    conn = get_db_connection()

    try:
        if conn.autocommit:
            raise RuntimeError("Autocommit must be disabled")

        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database = cur.fetchone()[0]

        if database != "ecommerce_test":
            raise RuntimeError(f"Write blocked for database: {database}")

        client = get_minio_client()

        result = execute_ingestion_action_locked(
            conn=conn,
            client=client,
            table_name=table_name,
            batch_size=batch_size,
            staging_root=staging_root,
        )

        status = result["status"]

        if status == "NO_DATA":
            conn.rollback()

            return {
                **result,
                "persisted": False,
            }

        if status == "COMMITTED":
            if result.get("action") not in (
                "RECOVER_PENDING",
                "RECOVER_UPLOADED",
            ):
                raise RuntimeError("Unexpected Transaction 1 committed action")

            if result.get("table_name") != table_name:
                raise RuntimeError("Transaction 1 recovery table mismatch")

            if not result.get("batch_id"):
                raise RuntimeError("Transaction 1 recovery batch_id missing")

            conn.commit()

            return {
                **result,
                "persisted": True,
            }

        if status != "PENDING":
            raise RuntimeError(f"Unexpected transaction 1 status: {status}")

        if result.get("action") != "NEW_BATCH":
            raise RuntimeError("Transaction 1 PENDING action mismatch")

        if result.get("table_name") != table_name:
            raise RuntimeError("Transaction 1 PENDING table mismatch")

        batch_id = result.get("batch_id")

        if (
            not isinstance(batch_id, str)
            or len(batch_id) != 32
            or any(c not in "0123456789abcdef" for c in batch_id)
        ):
            raise RuntimeError("Transaction 1 PENDING batch_id invalid")

        staged_path = result.get("staged_path")

        if not isinstance(staged_path, (str, Path)):
            raise RuntimeError("Transaction 1 staged_path missing")

        if not Path(staged_path).is_file():
            raise RuntimeError("Transaction 1 staged CSV missing")

        # Persist PENDING metadata before recovery.
        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    # ----------------------------------------------------
    # Transaction 2
    # ----------------------------------------------------
    expected_batch_id = result["batch_id"]

    if not expected_batch_id:
        raise RuntimeError("Transaction 1 did not return a batch_id")

    conn = get_db_connection()

    try:
        if conn.autocommit:
            raise RuntimeError("Autocommit must be disabled")

        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database = cur.fetchone()[0]

        if database != "ecommerce_test":
            raise RuntimeError(f"Write blocked for database: {database}")

        # Acquire a new transaction-scoped advisory lock.
        if not acquire_table_lock(conn, table_name):
            raise RuntimeError("Table ingestion lock is busy")

        # Read exactly the batch created by Transaction 1.
        metadata = get_batch_metadata(
            conn,
            expected_batch_id,
        )

        if metadata is None:
            raise RuntimeError("Expected batch disappeared after commit")

        if metadata["table_name"] != table_name:
            raise RuntimeError("Transaction 2 batch table mismatch")

        if metadata["batch_id"] != expected_batch_id:
            raise RuntimeError("Transaction 2 batch ID mismatch")

        if metadata["status"] == "COMMITTED":
            # Another worker may have completed this batch.
            # Verify its checkpoint before reporting success.
            watermark = get_watermark(conn, table_name)

            if watermark["last_batch_id"] != expected_batch_id:
                raise RuntimeError("ALREADY_COMMITTED checkpoint mismatch")

            conn.rollback()

            return {
                "action": "ALREADY_COMMITTED",
                "table_name": table_name,
                "batch_id": expected_batch_id,
                "status": "COMMITTED",
                "persisted": True,
                "recovered": False,
            }

        if metadata["status"] not in RECOVERABLE_STATUSES:
            raise RuntimeError(
                "Unexpected Transaction 2 batch status: " f"{metadata['status']}"
            )

        # Ensure the expected batch is the only recoverable
        # batch and that no FAILED batch is present.
        recoverable = get_recoverable_batches(
            conn,
            table_name,
        )

        if (
            len(recoverable) != 1
            or recoverable[0]["batch_id"] != expected_batch_id
            or recoverable[0]["status"] != metadata["status"]
        ):
            raise RuntimeError("Transaction 2 recovery state mismatch")

        if has_failed_batch(conn, table_name):
            raise RuntimeError("FAILED batch requires investigation")

        action = (
            "RECOVER_PENDING" if metadata["status"] == "PENDING" else "RECOVER_UPLOADED"
        )

        recovery_result = execute_ingestion_action(
            conn=conn,
            client=client,
            table_name=table_name,
            action=action,
            recoverable_batches=recoverable,
            staging_root=staging_root,
        )

        if (
            recovery_result["batch_id"] != expected_batch_id
            or recovery_result["table_name"] != table_name
            or recovery_result["status"] != "COMMITTED"
        ):
            raise RuntimeError("Transaction 2 recovery result mismatch")

        conn.commit()

        return {
            **recovery_result,
            "persisted": True,
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def run_incremental_ingestion_write(
    table_name,
    batch_size=1000,
    staging_root="staging",
    allow_write=False,
):
    """
    Controlled incremental ingestion write entrypoint.

    Requires:
        - allow_write=True
        - INGESTION_WRITE_ENABLED=true
        - PostgreSQL database ecommerce_test
        - autocommit=False

    Executes the existing two-transaction ingestion workflow.
    """

    # 1. Validate arguments
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unsupported table: {table_name}")

    validate_positive_integer(batch_size, "batch_size")

    if not isinstance(staging_root, (str, Path)):
        raise ValueError("staging_root must be a path")

    if not str(staging_root).strip():
        raise ValueError("staging_root cannot be empty")

    if not isinstance(allow_write, bool):
        raise ValueError("allow_write must be boolean")

    # 2. Require explicit permission
    if not allow_write:
        return {
            "table_name": table_name,
            "mode": "WRITE_DISABLED",
            "persisted": False,
            "reason": "Explicit allow_write=True is required",
        }

    # 3. Require environment-level permission
    if os.getenv("INGESTION_WRITE_ENABLED", "").lower() != "true":
        raise RuntimeError(
            "Write-mode blocked: INGESTION_WRITE_ENABLED=true is required"
        )

    # 4. Validate database and transaction mode
    conn = get_db_connection()

    try:
        if conn.autocommit:
            raise RuntimeError("Write-mode requires autocommit=False")

        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database = cur.fetchone()[0]

        if database != EXPECTED_DB_NAME:
            raise RuntimeError(
                f"Write-mode blocked for database: {database}"
            )

    finally:
        conn.rollback()
        conn.close()

    # 5. Execute existing two-transaction workflow
    result = _execute_two_transaction_ingestion(
        table_name=table_name,
        batch_size=batch_size,
        staging_root=staging_root,
    )

    return {
        **result,
        "mode": "WRITE",
    }
