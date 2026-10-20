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
