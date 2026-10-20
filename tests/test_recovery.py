from unittest.mock import MagicMock, patch

import pytest

from ingestion.incremental import recovery


def test_execute_uploaded_recovery_success():
    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    metadata = {
        "batch_id": "test-batch-001",
        "status": "UPLOADED",
    }

    committed = {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "status": "COMMITTED",
        "last_timestamp": None,
        "last_id": 100,
        "recovered": True,
    }

    with (
        patch(
            "ingestion.incremental.database.require_transaction"
        ) as require_tx,
        patch(
            "ingestion.incremental.batch_repository.get_batch_metadata",
            return_value=metadata,
        ),
        patch.object(
            recovery,
            "recover_uploaded_batch",
            return_value=committed,
        ) as recover_mock,
    ):
        result = recovery.execute_uploaded_recovery(
            conn, client, "test-batch-001"
        )

    require_tx.assert_called_once_with(conn)
    recover_mock.assert_called_once_with(
        conn, client, "test-batch-001"
    )

    assert result["action"] == "RECOVER_UPLOADED"
    assert result["status"] == "COMMITTED"
    assert result["recovered"] is True
    assert result["last_id"] == 100

    conn.commit.assert_not_called()
    conn.rollback.assert_not_called()

def test_execute_pending_recovery_success():
    from unittest.mock import call

    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    metadata = {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "status": "PENDING",
    }

    upload_result = {
        "status": "UPLOADED",
        "object_key": "test/products/batch-001.csv",
    }

    committed = {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "status": "COMMITTED",
        "last_timestamp": None,
        "last_id": 100,
    }

    steps = MagicMock()

    with (
        patch(
            "ingestion.incremental.batch_repository.get_batch_metadata",
            return_value=metadata,
        ),
        patch.object(recovery, "validate_recovery_metadata"),
        patch.object(
            recovery, "recover_staged_batch"
        ) as staged,
        patch.object(
            recovery,
            "upload_recovered_batch",
            return_value=upload_result,
        ) as upload,
        patch(
            "ingestion.incremental.batch_repository.mark_batch_uploaded"
        ) as mark,
        patch.object(
            recovery,
            "recover_uploaded_batch",
            return_value=committed,
        ) as recover,
    ):
        steps.attach_mock(staged, "staged")
        steps.attach_mock(upload, "upload")
        steps.attach_mock(mark, "mark")
        steps.attach_mock(recover, "recover")

        result = recovery.execute_pending_recovery(
            conn, client, "test-batch-001"
        )

    assert steps.mock_calls == [
        call.staged(metadata, staging_root="staging"),
        call.upload(client, metadata, staging_root="staging"),
        call.mark(conn, client, "test-batch-001"),
        call.recover(conn, client, "test-batch-001"),
    ]

    assert result["action"] == "RECOVER_PENDING"
    assert result["status"] == "COMMITTED"
    assert result["recovered"] is True
    assert result["upload_result"] == upload_result
    assert result["last_id"] == 100

    conn.commit.assert_not_called()
    conn.rollback.assert_not_called()

def test_upload_recovered_batch_already_exists():
    conn_client = MagicMock()
    metadata = {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "status": "PENDING",
    }

    recovered = {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "row_count": 10,
        "object_key": "raw/products/test-batch-001.csv",
        "checksum_sha256": "a" * 64,
        "csv_bytes": b"id,name\n1,test\n",
    }

    with (
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
            return_value=True,
        ) as verify,
        patch(
            "ingestion.incremental.minio_storage.put_object_if_absent"
        ) as put,
    ):
        result = recovery.upload_recovered_batch(
            conn_client,
            metadata,
        )

    assert result["status"] == "ALREADY_EXISTS"
    assert result["object_key"] == recovered["object_key"]
    assert result["checksum_sha256"] == recovered["checksum_sha256"]

    verify.assert_called_once_with(
        conn_client,
        "ecommerce-data-lake",
        recovered["object_key"],
        recovered["checksum_sha256"],
    )
    put.assert_not_called()


def test_upload_recovered_batch_rejects_failed_post_upload_verification():
    client = MagicMock()
    metadata = {"batch_id": "test-batch-001"}

    recovered = {
        "object_key": "bronze_incremental/products/test.csv",
        "checksum_sha256": "a" * 64,
        "csv_bytes": b"id,name\n1,test\n",
    }

    with (
        patch.object(
            recovery, "recover_staged_batch", return_value=recovered
        ),
        patch(
            "ingestion.incremental.minio_storage.check_minio_bucket",
            return_value="ecommerce-data-lake",
        ),
        patch(
            "ingestion.incremental.minio_storage.verify_minio_object",
            side_effect=[False, False],
        ) as verify,
        patch(
            "ingestion.incremental.minio_storage.put_object_if_absent"
        ) as put,
    ):
        with pytest.raises(
            RuntimeError, match="Uploaded object could not be verified"
        ):
            recovery.upload_recovered_batch(client, metadata)

    put.assert_called_once_with(
        client,
        "ecommerce-data-lake",
        recovered["object_key"],
        recovered["csv_bytes"],
    )
    assert verify.call_count == 2


def test_upload_recovered_batch_rejects_existing_wrong_checksum():
    from minio.error import S3Error

    client = MagicMock()
    metadata = {"batch_id": "test-batch-001"}

    recovered = {
        "object_key": "bronze_incremental/products/test.csv",
        "checksum_sha256": "a" * 64,
        "csv_bytes": b"id,name\n1,test\n",
    }

    conflict = S3Error(
        code="PreconditionFailed",
        message="Object already exists",
        resource=recovered["object_key"],
        request_id="test-request",
        host_id="test-host",
        response=None,
    )

    with (
        patch.object(
            recovery, "recover_staged_batch", return_value=recovered
        ),
        patch(
            "ingestion.incremental.minio_storage.check_minio_bucket",
            return_value="ecommerce-data-lake",
        ),
        patch(
            "ingestion.incremental.minio_storage.verify_minio_object",
            return_value=False,
        ),
        patch(
            "ingestion.incremental.minio_storage.put_object_if_absent",
            side_effect=conflict,
        ) as put,
    ):
        with pytest.raises(S3Error) as exc_info:
            recovery.upload_recovered_batch(client, metadata)

    assert exc_info.value.code == "PreconditionFailed"
    put.assert_called_once()


def test_upload_recovered_batch_accepts_matching_concurrent_upload():
    from minio.error import S3Error

    client = MagicMock()
    metadata = {"batch_id": "test-batch-001"}

    recovered = {
        "object_key": "bronze_incremental/products/test.csv",
        "checksum_sha256": "a" * 64,
        "csv_bytes": b"id,name\n1,test\n",
    }

    conflict = S3Error(
        code="ConditionalRequestConflict",
        message="Concurrent object creation",
        resource=recovered["object_key"],
        request_id="test-request",
        host_id="test-host",
        response=None,
    )

    with (
        patch.object(
            recovery, "recover_staged_batch", return_value=recovered
        ),
        patch(
            "ingestion.incremental.minio_storage.check_minio_bucket",
            return_value="ecommerce-data-lake",
        ),
        patch(
            "ingestion.incremental.minio_storage.verify_minio_object",
            side_effect=[False, True],
        ),
        patch(
            "ingestion.incremental.minio_storage.put_object_if_absent",
            side_effect=conflict,
        ) as put,
    ):
        result = recovery.upload_recovered_batch(client, metadata)

    assert result["status"] == "ALREADY_EXISTS"
    put.assert_called_once()


def test_pending_recovery_stops_before_db_update_when_upload_fails():
    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    metadata = {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "status": "PENDING",
    }

    with (
        patch(
            "ingestion.incremental.batch_repository.get_batch_metadata",
            return_value=metadata,
        ),
        patch.object(recovery, "validate_recovery_metadata"),
        patch.object(recovery, "recover_staged_batch"),
        patch.object(
            recovery,
            "upload_recovered_batch",
            side_effect=RuntimeError("Simulated MinIO failure"),
        ),
        patch(
            "ingestion.incremental.batch_repository.mark_batch_uploaded"
        ) as mark,
        patch.object(recovery, "recover_uploaded_batch") as commit_checkpoint,
    ):
        with pytest.raises(RuntimeError, match="Simulated MinIO failure"):
            recovery.execute_pending_recovery(
                conn, client, "test-batch-001"
            )

    mark.assert_not_called()
    commit_checkpoint.assert_not_called()
    conn.commit.assert_not_called()
