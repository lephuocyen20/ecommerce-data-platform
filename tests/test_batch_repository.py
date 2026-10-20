from unittest.mock import MagicMock, patch

import pytest

from ingestion.incremental import batch_repository as repo


def make_metadata():
    return {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "watermark_type": "TIMESTAMP",
        "start_timestamp": None,
        "start_id": 0,
        "end_timestamp": None,
        "end_id": 100,
        "row_count": 10,
        "object_key": "test/products/batch-001.csv",
        "checksum_sha256": "a" * 64,
        "status": "PENDING",
    }


def test_register_pending_batch_success():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (
        "test-batch-001", "products", "PENDING"
    )

    with patch.object(
        repo,
        "TABLE_CONFIG",
        {"products": {}},
    ):
        result = repo.register_pending_batch(
            conn, make_metadata()
        )

    assert result == {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "status": "PENDING",
    }

    query, params = cursor.execute.call_args.args

    assert "INSERT INTO public.ingestion_batches" in query
    assert len(params) == 11
    assert params[-1] == "PENDING"

    conn.commit.assert_not_called()
    conn.rollback.assert_not_called()


def test_register_pending_batch_rejects_missing_metadata():
    conn = MagicMock()
    metadata = make_metadata()
    del metadata["checksum_sha256"]

    with pytest.raises(
        ValueError,
        match="Missing metadata field: checksum_sha256",
    ):
        repo.register_pending_batch(conn, metadata)

    conn.cursor.assert_not_called()


def test_register_pending_batch_rejects_invalid_status():
    conn = MagicMock()
    metadata = make_metadata()
    metadata["status"] = "COMMITTED"

    with pytest.raises(
        ValueError,
        match="Only PENDING batches can be registered",
    ):
        repo.register_pending_batch(conn, metadata)

    conn.cursor.assert_not_called()


def test_register_pending_batch_rejects_unsupported_table():
    conn = MagicMock()
    metadata = make_metadata()
    metadata["table_name"] = "unknown_table"

    with pytest.raises(
        ValueError,
        match="Unsupported table",
    ):
        repo.register_pending_batch(conn, metadata)

    conn.cursor.assert_not_called()

def test_mark_batch_uploaded_rejects_autocommit():
    conn = MagicMock()
    conn.autocommit = True

    client = MagicMock()

    with pytest.raises(
        ValueError,
        match="Autocommit must be disabled",
    ):
        repo.mark_batch_uploaded(
            conn,
            client,
            "test-batch-001",
        )

    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()
    client.assert_not_called()

def test_mark_batch_uploaded_rejects_failed_minio_verification():
    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    metadata = make_metadata()

    with (
        patch.object(repo, "lock_batch_metadata", return_value=metadata),
        patch(
            "ingestion.incremental.recovery.validate_recovery_metadata"
        ),
        patch(
            "ingestion.incremental.minio_storage.check_minio_bucket",
            return_value="ecommerce-data-lake",
        ),
        patch(
            "ingestion.incremental.minio_storage.verify_minio_object",
            return_value=False,
        ) as verify_mock,
    ):
        with pytest.raises(
            RuntimeError,
            match="MinIO object does not exist",
        ):
            repo.mark_batch_uploaded(
                conn,
                client,
                "test-batch-001",
            )

    verify_mock.assert_called_once_with(
        client,
        "ecommerce-data-lake",
        metadata["object_key"],
        metadata["checksum_sha256"],
    )

    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()

def test_mark_batch_uploaded_success():
    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    metadata = make_metadata()

    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ("test-batch-001",)

    with (
        patch.object(repo, "lock_batch_metadata", return_value=metadata),
        patch(
            "ingestion.incremental.recovery.validate_recovery_metadata"
        ),
        patch(
            "ingestion.incremental.minio_storage.check_minio_bucket",
            return_value="ecommerce-data-lake",
        ),
        patch(
            "ingestion.incremental.minio_storage.verify_minio_object",
            return_value=True,
        ) as verify_mock,
    ):
        result = repo.mark_batch_uploaded(
            conn,
            client,
            "test-batch-001",
        )

    assert result == {
        "batch_id": "test-batch-001",
        "status": "UPLOADED",
        "object_key": metadata["object_key"],
        "checksum_sha256": metadata["checksum_sha256"],
    }

    verify_mock.assert_called_once_with(
        client,
        "ecommerce-data-lake",
        metadata["object_key"],
        metadata["checksum_sha256"],
    )

    query, params = cursor.execute.call_args.args

    assert "SET status = 'UPLOADED'" in query
    assert "status IN ('PENDING', 'UPLOADED')" in query
    assert params == ("test-batch-001",)

    conn.commit.assert_not_called()
    conn.rollback.assert_not_called()
