from unittest.mock import patch

import pytest

from ingestion.incremental import postgres_to_minio_incremental as m


def test_write_disabled_does_not_connect_to_database():
    func = m.run_incremental_ingestion_write

    with patch.dict(
        func.__globals__,
        {"get_db_connection": lambda: pytest.fail(
            "Database connection must not be opened"
        )},
    ):
        result = func("products")

    assert result["table_name"] == "products"
    assert result["mode"] == "WRITE_DISABLED"
    assert result["persisted"] is False


def test_write_disabled_is_default():
    func = m.run_incremental_ingestion_write

    with patch.dict(
        func.__globals__,
        {"get_db_connection": lambda: pytest.fail(
            "Database connection must not be opened"
        )},
    ):
        result = func("products")

    assert result["mode"] == "WRITE_DISABLED"


def test_invalid_write_permission_is_rejected():
    with pytest.raises(ValueError, match="allow_write must be boolean"):
        m.run_incremental_ingestion_write(
            "products",
            allow_write="True",
        )

def test_production_write_remains_disabled():
    from unittest.mock import MagicMock

    func = m.run_incremental_ingestion_write

    conn = MagicMock()
    conn.autocommit = False

    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ("ecommerce_test",)

    with patch.dict(
        func.__globals__,
        {"get_db_connection": lambda: conn},
    ):
        with pytest.raises(
            NotImplementedError,
            match="Production write-mode is not enabled yet",
        ):
            func("products", allow_write=True)

    cursor.execute.assert_called_once_with("SELECT current_database()")
    conn.rollback.assert_called_once()
    conn.close.assert_called_once()
