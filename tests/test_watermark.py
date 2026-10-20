from datetime import datetime, timezone

from ingestion.incremental.watermark import watermark_matches_batch_start


def test_timestamp_watermark_matches():
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

    watermark = {
        "watermark_type": "TIMESTAMP",
        "last_timestamp": ts,
        "last_id": 100,
    }

    metadata = {
        "watermark_type": "TIMESTAMP",
        "start_timestamp": ts,
        "start_id": 100,
    }

    assert watermark_matches_batch_start(watermark, metadata) is True


def test_timestamp_watermark_rejects_different_id():
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

    watermark = {
        "watermark_type": "TIMESTAMP",
        "last_timestamp": ts,
        "last_id": 100,
    }

    metadata = {
        "watermark_type": "TIMESTAMP",
        "start_timestamp": ts,
        "start_id": 101,
    }

    assert watermark_matches_batch_start(watermark, metadata) is False


def test_timestamp_watermark_rejects_different_timestamp():
    watermark = {
        "watermark_type": "TIMESTAMP",
        "last_timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "last_id": 100,
    }

    metadata = {
        "watermark_type": "TIMESTAMP",
        "start_timestamp": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "start_id": 100,
    }

    assert watermark_matches_batch_start(watermark, metadata) is False


def test_id_watermark_matches():
    watermark = {
        "watermark_type": "ID",
        "last_timestamp": None,
        "last_id": 100,
    }

    metadata = {
        "watermark_type": "ID",
        "start_timestamp": None,
        "start_id": 100,
    }

    assert watermark_matches_batch_start(watermark, metadata) is True


def test_watermark_rejects_type_mismatch():
    watermark = {
        "watermark_type": "ID",
        "last_timestamp": None,
        "last_id": 100,
    }

    metadata = {
        "watermark_type": "TIMESTAMP",
        "start_timestamp": None,
        "start_id": 100,
    }

    assert watermark_matches_batch_start(watermark, metadata) is False

from unittest.mock import MagicMock, patch
import pytest

from ingestion.incremental import watermark as wm


def test_get_watermark_initial_load():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = None

    with patch.object(
        wm,
        "get_table_config",
        return_value={"watermark_type": "TIMESTAMP"},
    ):
        result = wm.get_watermark(conn, "products")

    assert result["table_name"] == "products"
    assert result["watermark_type"] == "TIMESTAMP"
    assert result["last_timestamp"] is None
    assert result["last_id"] == 0
    assert result["last_batch_id"] is None
    assert result["is_initial_load"] is True

    cursor.execute.assert_called_once()


def test_get_watermark_existing_checkpoint():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value

    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    cursor.fetchone.return_value = (
        "TIMESTAMP", ts, 150, "batch-001"
    )

    with patch.object(
        wm,
        "get_table_config",
        return_value={"watermark_type": "TIMESTAMP"},
    ):
        result = wm.get_watermark(conn, "products")

    assert result["last_timestamp"] == ts
    assert result["last_id"] == 150
    assert result["last_batch_id"] == "batch-001"
    assert result["is_initial_load"] is False


def test_get_watermark_rejects_type_mismatch():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value

    cursor.fetchone.return_value = (
        "ID", None, 100, "batch-001"
    )

    with patch.object(
        wm,
        "get_table_config",
        return_value={"watermark_type": "TIMESTAMP"},
    ):
        with pytest.raises(RuntimeError, match="Watermark type mismatch"):
            wm.get_watermark(conn, "products")
