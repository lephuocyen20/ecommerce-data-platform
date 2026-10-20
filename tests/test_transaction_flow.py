import pytest
from unittest.mock import MagicMock, patch

from ingestion.incremental import orchestrator


def test_tx1_failure_rolls_back_without_starting_tx2():
    conn = MagicMock()
    conn.autocommit = False

    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ("ecommerce_test",)

    with (
        patch.object(
            orchestrator,
            "get_db_connection",
            return_value=conn,
        ) as get_connection,
        patch.object(
            orchestrator,
            "get_minio_client",
            return_value=MagicMock(),
        ),
        patch.object(
            orchestrator,
            "execute_ingestion_action_locked",
            side_effect=RuntimeError("Simulated TX1 failure"),
        ),
    ):
        with pytest.raises(
            RuntimeError,
            match="Simulated TX1 failure",
        ):
            orchestrator._execute_two_transaction_ingestion(
                "products"
            )

    get_connection.assert_called_once()
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()

def test_tx2_failure_rolls_back_after_tx1_commit(tmp_path):
    batch_id = "a" * 32
    staged_file = tmp_path / "batch.csv"
    staged_file.write_text("id,name\n1,test\n", encoding="utf-8")

    tx1 = MagicMock()
    tx1.autocommit = False
    tx1_cursor = tx1.cursor.return_value.__enter__.return_value
    tx1_cursor.fetchone.return_value = ("ecommerce_test",)

    tx2 = MagicMock()
    tx2.autocommit = False
    tx2_cursor = tx2.cursor.return_value.__enter__.return_value
    tx2_cursor.fetchone.return_value = ("ecommerce_test",)

    pending_result = {
        "action": "NEW_BATCH",
        "table_name": "products",
        "status": "PENDING",
        "batch_id": batch_id,
        "staged_path": str(staged_file),
    }

    with (
        patch.object(
            orchestrator,
            "get_db_connection",
            side_effect=[tx1, tx2],
        ) as get_connection,
        patch.object(
            orchestrator,
            "get_minio_client",
            return_value=MagicMock(),
        ),
        patch.object(
            orchestrator,
            "execute_ingestion_action_locked",
            return_value=pending_result,
        ),
        patch.object(
            orchestrator,
            "acquire_table_lock",
            side_effect=RuntimeError("Simulated TX2 failure"),
        ),
    ):
        with pytest.raises(
            RuntimeError,
            match="Simulated TX2 failure",
        ):
            orchestrator._execute_two_transaction_ingestion(
                "products"
            )

    assert get_connection.call_count == 2

    tx1.commit.assert_called_once()
    tx1.rollback.assert_not_called()
    tx1.close.assert_called_once()

    tx2.commit.assert_not_called()
    tx2.rollback.assert_called_once()
    tx2.close.assert_called_once()

def test_tx1_no_data_rolls_back_without_starting_tx2():
    conn = MagicMock()
    conn.autocommit = False

    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ("ecommerce_test",)

    no_data_result = {
        "action": "NEW_BATCH",
        "table_name": "products",
        "status": "NO_DATA",
    }

    with (
        patch.object(
            orchestrator,
            "get_db_connection",
            return_value=conn,
        ) as get_connection,
        patch.object(
            orchestrator,
            "get_minio_client",
            return_value=MagicMock(),
        ),
        patch.object(
            orchestrator,
            "execute_ingestion_action_locked",
            return_value=no_data_result,
        ),
    ):
        result = orchestrator._execute_two_transaction_ingestion(
            "products"
        )

    assert result == {
        **no_data_result,
        "persisted": False,
    }

    get_connection.assert_called_once()
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()

@pytest.mark.parametrize(
    "action",
    ["RECOVER_PENDING", "RECOVER_UPLOADED"],
)
def test_tx1_recovery_commits_without_starting_tx2(action):
    conn = MagicMock()
    conn.autocommit = False

    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ("ecommerce_test",)

    recovery_result = {
        "action": action,
        "table_name": "products",
        "status": "COMMITTED",
        "batch_id": "a" * 32,
    }

    with (
        patch.object(
            orchestrator,
            "get_db_connection",
            return_value=conn,
        ) as get_connection,
        patch.object(
            orchestrator,
            "get_minio_client",
            return_value=MagicMock(),
        ),
        patch.object(
            orchestrator,
            "execute_ingestion_action_locked",
            return_value=recovery_result,
        ),
    ):
        result = orchestrator._execute_two_transaction_ingestion(
            "products"
        )

    assert result == {
        **recovery_result,
        "persisted": True,
    }

    get_connection.assert_called_once()
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()
    conn.close.assert_called_once()

@pytest.mark.parametrize(
    "recovery_status,expected_action",
    [
        ("PENDING", "RECOVER_PENDING"),
        ("UPLOADED", "RECOVER_UPLOADED"),
    ],
)
def test_tx2_success_commits_checkpoint(
    tmp_path,
    recovery_status,
    expected_action,
):
    batch_id = "a" * 32
    staged_file = tmp_path / "batch.csv"
    staged_file.write_text("id,name\n1,test\n", encoding="utf-8")

    tx1 = MagicMock()
    tx1.autocommit = False
    tx1.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "ecommerce_test",
    )

    tx2 = MagicMock()
    tx2.autocommit = False
    tx2.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "ecommerce_test",
    )

    pending_result = {
        "action": "NEW_BATCH",
        "table_name": "products",
        "status": "PENDING",
        "batch_id": batch_id,
        "staged_path": str(staged_file),
    }

    metadata = {
        "batch_id": batch_id,
        "table_name": "products",
        "status": recovery_status,
    }

    recovery_result = {
        "action": expected_action,
        "batch_id": batch_id,
        "table_name": "products",
        "status": "COMMITTED",
    }

    with (
        patch.object(
            orchestrator,
            "get_db_connection",
            side_effect=[tx1, tx2],
        ) as get_connection,
        patch.object(
            orchestrator,
            "get_minio_client",
            return_value=MagicMock(),
        ),
        patch.object(
            orchestrator,
            "execute_ingestion_action_locked",
            return_value=pending_result,
        ),
        patch.object(
            orchestrator,
            "acquire_table_lock",
            return_value=True,
        ),
        patch.object(
            orchestrator,
            "get_batch_metadata",
            return_value=metadata,
        ),
        patch.object(
            orchestrator,
            "get_recoverable_batches",
            return_value=[metadata],
        ),
        patch.object(
            orchestrator,
            "has_failed_batch",
            return_value=False,
        ),
        patch.object(
            orchestrator,
            "execute_ingestion_action",
            return_value=recovery_result,
        ) as execute_action,
    ):
        result = orchestrator._execute_two_transaction_ingestion(
            "products"
        )

    assert result == {
        **recovery_result,
        "persisted": True,
    }

    assert get_connection.call_count == 2

    tx1.commit.assert_called_once()
    tx1.rollback.assert_not_called()
    tx1.close.assert_called_once()

    tx2.commit.assert_called_once()
    tx2.rollback.assert_not_called()
    tx2.close.assert_called_once()

    execute_action.assert_called_once_with(
        conn=tx2,
        client=execute_action.call_args.kwargs["client"],
        table_name="products",
        action=expected_action,
        recoverable_batches=[metadata],
        staging_root="staging",
    )

def test_tx2_already_committed_verifies_checkpoint(tmp_path):
    batch_id = "a" * 32
    staged_file = tmp_path / "batch.csv"
    staged_file.write_text("id,name\n1,test\n", encoding="utf-8")

    tx1 = MagicMock()
    tx1.autocommit = False
    tx1.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "ecommerce_test",
    )

    tx2 = MagicMock()
    tx2.autocommit = False
    tx2.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "ecommerce_test",
    )

    pending_result = {
        "action": "NEW_BATCH",
        "table_name": "products",
        "status": "PENDING",
        "batch_id": batch_id,
        "staged_path": str(staged_file),
    }

    metadata = {
        "batch_id": batch_id,
        "table_name": "products",
        "status": "COMMITTED",
    }

    with (
        patch.object(
            orchestrator,
            "get_db_connection",
            side_effect=[tx1, tx2],
        ) as get_connection,
        patch.object(
            orchestrator,
            "get_minio_client",
            return_value=MagicMock(),
        ),
        patch.object(
            orchestrator,
            "execute_ingestion_action_locked",
            return_value=pending_result,
        ),
        patch.object(
            orchestrator,
            "acquire_table_lock",
            return_value=True,
        ),
        patch.object(
            orchestrator,
            "get_batch_metadata",
            return_value=metadata,
        ),
        patch.object(
            orchestrator,
            "get_watermark",
            return_value={"last_batch_id": batch_id},
        ),
        patch.object(
            orchestrator,
            "execute_ingestion_action",
        ) as execute_action,
    ):
        result = orchestrator._execute_two_transaction_ingestion(
            "products"
        )

    assert result == {
        "action": "ALREADY_COMMITTED",
        "table_name": "products",
        "batch_id": batch_id,
        "status": "COMMITTED",
        "persisted": True,
        "recovered": False,
    }

    assert get_connection.call_count == 2

    tx1.commit.assert_called_once()
    tx1.close.assert_called_once()

    tx2.commit.assert_not_called()
    tx2.rollback.assert_called_once()
    tx2.close.assert_called_once()

    execute_action.assert_not_called()

def test_tx2_already_committed_rejects_checkpoint_mismatch(tmp_path):
    batch_id = "a" * 32
    staged_file = tmp_path / "batch.csv"
    staged_file.write_text("id,name\n1,test\n", encoding="utf-8")

    tx1 = MagicMock()
    tx1.autocommit = False
    tx1.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "ecommerce_test",
    )

    tx2 = MagicMock()
    tx2.autocommit = False
    tx2.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "ecommerce_test",
    )

    pending_result = {
        "action": "NEW_BATCH",
        "table_name": "products",
        "status": "PENDING",
        "batch_id": batch_id,
        "staged_path": str(staged_file),
    }

    metadata = {
        "batch_id": batch_id,
        "table_name": "products",
        "status": "COMMITTED",
    }

    with (
        patch.object(
            orchestrator,
            "get_db_connection",
            side_effect=[tx1, tx2],
        ) as get_connection,
        patch.object(
            orchestrator,
            "get_minio_client",
            return_value=MagicMock(),
        ),
        patch.object(
            orchestrator,
            "execute_ingestion_action_locked",
            return_value=pending_result,
        ),
        patch.object(
            orchestrator,
            "acquire_table_lock",
            return_value=True,
        ),
        patch.object(
            orchestrator,
            "get_batch_metadata",
            return_value=metadata,
        ),
        patch.object(
            orchestrator,
            "get_watermark",
            return_value={"last_batch_id": "b" * 32},
        ),
        patch.object(
            orchestrator,
            "execute_ingestion_action",
        ) as execute_action,
    ):
        with pytest.raises(
            RuntimeError,
            match="ALREADY_COMMITTED checkpoint mismatch",
        ):
            orchestrator._execute_two_transaction_ingestion(
                "products"
            )

    assert get_connection.call_count == 2

    tx1.commit.assert_called_once()
    tx1.close.assert_called_once()

    tx2.commit.assert_not_called()
    tx2.rollback.assert_called_once()
    tx2.close.assert_called_once()

    execute_action.assert_not_called()

@pytest.mark.parametrize(
    "database",
    ["postgres", "ecommerce_prod"],
)
def test_tx1_rejects_unauthorized_database(database):
    conn = MagicMock()
    conn.autocommit = False

    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (database,)

    with (
        patch.object(
            orchestrator,
            "get_db_connection",
            return_value=conn,
        ) as get_connection,
        patch.object(
            orchestrator,
            "get_minio_client",
        ) as get_minio,
        patch.object(
            orchestrator,
            "execute_ingestion_action_locked",
        ) as execute_action,
    ):
        with pytest.raises(
            RuntimeError,
            match="Write blocked for database",
        ):
            orchestrator._execute_two_transaction_ingestion(
                "products"
            )

    get_connection.assert_called_once()
    get_minio.assert_not_called()
    execute_action.assert_not_called()

    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()
    conn.close.assert_called_once()
