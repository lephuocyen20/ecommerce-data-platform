from unittest.mock import MagicMock

import pytest

from ingestion.incremental import watermark as wm


def test_commit_uploaded_batch_rejects_autocommit():
    conn = MagicMock()
    conn.autocommit = True

    client = MagicMock()

    with pytest.raises(
        ValueError,
        match="Autocommit must be disabled",
    ):
        wm.commit_uploaded_batch(
            conn,
            client,
            "test-batch-001",
        )

    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()
    conn.rollback.assert_not_called()

def test_commit_uploaded_batch_rejects_missing_batch():
    from unittest.mock import patch

    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    with (
        patch(
            "ingestion.incremental.batch_repository.get_batch_metadata",
            return_value=None,
        ) as get_metadata,
        patch.object(wm, "get_locked_watermark") as get_watermark,
    ):
        with pytest.raises(
            ValueError,
            match="Batch not found: test-batch-001",
        ):
            wm.commit_uploaded_batch(
                conn,
                client,
                "test-batch-001",
            )

    get_metadata.assert_called_once_with(
        conn,
        "test-batch-001",
    )

    get_watermark.assert_not_called()
    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()
    client.assert_not_called()

def test_commit_uploaded_batch_rejects_pending_batch():
    from unittest.mock import patch

    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    metadata = {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "status": "PENDING",
        "watermark_type": "TIMESTAMP",
        "start_timestamp": None,
        "start_id": 0,
        "end_timestamp": None,
        "end_id": 100,
        "row_count": 10,
        "object_key": "test/products/batch-001.csv",
        "checksum_sha256": "a" * 64,
    }

    with (
        patch(
            "ingestion.incremental.batch_repository.get_batch_metadata",
            return_value=metadata,
        ),
        patch(
            "ingestion.incremental.batch_repository.lock_batch_metadata",
            return_value=metadata,
        ),
        patch(
            "ingestion.incremental.recovery.validate_recovery_metadata",
        ),
        patch.object(
            wm,
            "get_locked_watermark",
            return_value={
                "watermark_type": "TIMESTAMP",
                "last_timestamp": None,
                "last_id": 0,
            },
        ),
        patch(
            "ingestion.incremental.minio_storage.check_minio_bucket"
        ) as check_bucket,
    ):
        with pytest.raises(
            ValueError,
            match="Batch must be UPLOADED",
        ):
            wm.commit_uploaded_batch(
                conn,
                client,
                "test-batch-001",
            )

    check_bucket.assert_not_called()
    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()

def test_commit_rejects_watermark_mismatch():
    from unittest.mock import patch

    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    metadata = {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "status": "UPLOADED",
        "watermark_type": "TIMESTAMP",
        "start_timestamp": None,
        "start_id": 0,
        "end_timestamp": None,
        "end_id": 100,
        "row_count": 10,
        "object_key": "test/products/batch-001.csv",
        "checksum_sha256": "a" * 64,
    }

    with (
        patch(
            "ingestion.incremental.batch_repository.get_batch_metadata",
            return_value=metadata,
        ),
        patch(
            "ingestion.incremental.batch_repository.lock_batch_metadata",
            return_value=metadata,
        ),
        patch(
            "ingestion.incremental.recovery.validate_recovery_metadata",
        ),
        patch.object(
            wm,
            "get_locked_watermark",
            return_value={
                "watermark_type": "TIMESTAMP",
                "last_timestamp": None,
                "last_id": 999,
            },
        ),
        patch(
            "ingestion.incremental.minio_storage.check_minio_bucket"
        ) as check_bucket,
    ):
        with pytest.raises(
            RuntimeError,
            match="Watermark does not match batch start",
        ):
            wm.commit_uploaded_batch(
                conn, client, "test-batch-001"
            )

    check_bucket.assert_not_called()
    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()

def test_commit_rejects_failed_minio_verification():
    from unittest.mock import patch

    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    metadata = {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "status": "UPLOADED",
        "watermark_type": "TIMESTAMP",
        "start_timestamp": None,
        "start_id": 0,
        "end_timestamp": None,
        "end_id": 100,
        "row_count": 10,
        "object_key": "test/products/batch-001.csv",
        "checksum_sha256": "a" * 64,
    }

    watermark = {
        "watermark_type": "TIMESTAMP",
        "last_timestamp": None,
        "last_id": 0,
    }

    with (
        patch(
            "ingestion.incremental.batch_repository.get_batch_metadata",
            return_value=metadata,
        ),
        patch(
            "ingestion.incremental.batch_repository.lock_batch_metadata",
            return_value=metadata,
        ),
        patch(
            "ingestion.incremental.recovery.validate_recovery_metadata",
        ),
        patch.object(
            wm, "get_locked_watermark", return_value=watermark
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
            wm.commit_uploaded_batch(
                conn, client, "test-batch-001"
            )

    verify_mock.assert_called_once_with(
        client,
        "ecommerce-data-lake",
        metadata["object_key"],
        metadata["checksum_sha256"],
    )

    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()

def test_commit_uploaded_batch_success():
    from unittest.mock import patch

    conn = MagicMock()
    conn.autocommit = False
    client = MagicMock()

    metadata = {
        "batch_id": "test-batch-001",
        "table_name": "products",
        "status": "UPLOADED",
        "watermark_type": "TIMESTAMP",
        "start_timestamp": None,
        "start_id": 0,
        "end_timestamp": None,
        "end_id": 100,
        "row_count": 10,
        "object_key": "test/products/batch-001.csv",
        "checksum_sha256": "a" * 64,
    }

    watermark = {
        "watermark_type": "TIMESTAMP",
        "last_timestamp": None,
        "last_id": 0,
    }

    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ("test-batch-001",)

    with (
        patch(
            "ingestion.incremental.batch_repository.get_batch_metadata",
            return_value=metadata,
        ),
        patch(
            "ingestion.incremental.batch_repository.lock_batch_metadata",
            return_value=metadata,
        ),
        patch(
            "ingestion.incremental.recovery.validate_recovery_metadata",
        ),
        patch.object(
            wm, "get_locked_watermark", return_value=watermark
        ),
        patch(
            "ingestion.incremental.minio_storage.check_minio_bucket",
            return_value="ecommerce-data-lake",
        ),
        patch(
            "ingestion.incremental.minio_storage.verify_minio_object",
            return_value=True,
        ),
    ):
        result = wm.commit_uploaded_batch(
            conn, client, "test-batch-001"
        )

    assert result["status"] == "COMMITTED"
    assert result["last_id"] == 100

    assert cursor.execute.call_count == 2

    queries = [
        call.args[0]
        for call in cursor.execute.call_args_list
    ]

    assert "INSERT INTO public.ingestion_watermarks" in queries[0]
    assert "UPDATE public.ingestion_batches" in queries[1]
    assert "status = 'COMMITTED'" in queries[1]

    conn.commit.assert_not_called()
    conn.rollback.assert_not_called()
