from unittest.mock import MagicMock, patch

import pytest

from ingestion.incremental import recovery


def test_retry_after_upload_success_and_database_failure():
    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    batch_id = "a" * 32

    metadata = {
        "batch_id": batch_id,
        "table_name": "products",
        "status": "PENDING",
    }

    recovered = {
        "batch_id": batch_id,
        "table_name": "products",
        "row_count": 5,
        "object_key": f"bronze_incremental/products/batch_id={batch_id}/data.csv",
        "checksum_sha256": "a" * 64,
        "csv_bytes": b"product_id,name\n1,test\n",
    }

    committed = {
        "batch_id": batch_id,
        "table_name": "products",
        "status": "COMMITTED",
        "last_timestamp": None,
        "last_id": 5,
    }

    # Simulated remote object state:
    # first attempt uploads successfully;
    # second attempt finds the same verified object.
    object_present = False

    def verify_object(*args):
        return object_present

    def upload_object(*args):
        nonlocal object_present
        object_present = True

    with (
        patch(
            "ingestion.incremental.batch_repository.get_batch_metadata",
            return_value=metadata,
        ),
        patch.object(recovery, "validate_recovery_metadata"),
        patch.object(
            recovery,
            "recover_staged_batch",
            return_value=recovered,
        ),
        patch(
            "ingestion.incremental.minio_storage.check_minio_bucket",
            return_value="ecommerce-data-lake",
        ),
        patch(
            "ingestion.incremental.minio_storage.verify_minio_object",
            side_effect=verify_object,
        ),
        patch(
            "ingestion.incremental.minio_storage.put_object_if_absent",
            side_effect=upload_object,
        ) as put,
        patch(
            "ingestion.incremental.batch_repository.mark_batch_uploaded",
            side_effect=[
                RuntimeError("Simulated PostgreSQL failure"),
                None,
            ],
        ) as mark,
        patch.object(
            recovery,
            "recover_uploaded_batch",
            return_value=committed,
        ) as checkpoint,
    ):
        # Attempt 1: upload succeeds, database step fails.
        with pytest.raises(
            RuntimeError, match="Simulated PostgreSQL failure"
        ):
            recovery.execute_pending_recovery(
                conn, client, batch_id
            )

        assert object_present is True
        assert put.call_count == 1
        checkpoint.assert_not_called()

        # Caller rolls back the failed PostgreSQL transaction.
        conn.rollback()

        # Attempt 2: object is reused without another upload.
        result = recovery.execute_pending_recovery(
            conn, client, batch_id
        )

    assert result["status"] == "COMMITTED"
    assert result["upload_result"]["status"] == "ALREADY_EXISTS"
    assert put.call_count == 1
    assert mark.call_count == 2
    checkpoint.assert_called_once_with(conn, client, batch_id)

    conn.commit.assert_not_called()

    print("MINIO UPLOAD REUSE: PASS")
    print("POSTGRESQL FAILURE SIMULATION: PASS")
    print("RECOVERY RETRY IDEMPOTENCY: PASS")
