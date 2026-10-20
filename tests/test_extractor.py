from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.incremental import extractor


def make_connection():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.description = [("id",), ("updated_at",)]
    cursor.fetchall.return_value = [(101, "sample")]
    return conn, cursor


def test_timestamp_initial_load():
    conn, cursor = make_connection()

    watermark = {
        "watermark_type": "TIMESTAMP",
        "last_timestamp": None,
        "last_id": 0,
    }

    with patch.object(
        extractor,
        "get_table_config",
        return_value={
            "primary_key": "id",
            "watermark_column": "updated_at",
            "watermark_type": "TIMESTAMP",
        },
    ):
        result = extractor.extract_incremental(
            conn, "products", watermark, batch_size=100
        )

    assert result["row_count"] == 1
    assert result["columns"] == ["id", "updated_at"]

    args = cursor.execute.call_args.args
    assert args[1] == (100,)


def test_timestamp_incremental_uses_composite_watermark():
    conn, cursor = make_connection()
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

    watermark = {
        "watermark_type": "TIMESTAMP",
        "last_timestamp": ts,
        "last_id": 100,
    }

    with patch.object(
        extractor,
        "get_table_config",
        return_value={
            "primary_key": "id",
            "watermark_column": "updated_at",
            "watermark_type": "TIMESTAMP",
        },
    ):
        extractor.extract_incremental(
            conn, "products", watermark, batch_size=50
        )

    args = cursor.execute.call_args.args
    assert args[1] == (ts, 100, 50)


def test_id_incremental():
    conn, cursor = make_connection()

    watermark = {
        "watermark_type": "ID",
        "last_timestamp": None,
        "last_id": 200,
    }

    with patch.object(
        extractor,
        "get_table_config",
        return_value={
            "primary_key": "id",
            "watermark_column": "id",
            "watermark_type": "ID",
        },
    ):
        extractor.extract_incremental(
            conn, "orders", watermark, batch_size=25
        )

    args = cursor.execute.call_args.args
    assert args[1] == (200, 25)


def test_watermark_type_mismatch():
    conn, cursor = make_connection()

    watermark = {
        "watermark_type": "ID",
        "last_timestamp": None,
        "last_id": 0,
    }

    with patch.object(
        extractor,
        "get_table_config",
        return_value={
            "primary_key": "id",
            "watermark_column": "updated_at",
            "watermark_type": "TIMESTAMP",
        },
    ):
        with pytest.raises(ValueError, match="Watermark type mismatch"):
            extractor.extract_incremental(
                conn, "products", watermark
            )

    cursor.execute.assert_not_called()

def test_timestamp_incremental_sql_structure():
    conn, cursor = make_connection()
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

    watermark = {
        "watermark_type": "TIMESTAMP",
        "last_timestamp": ts,
        "last_id": 100,
    }

    with patch.object(
        extractor,
        "get_table_config",
        return_value={
            "primary_key": "id",
            "watermark_column": "updated_at",
            "watermark_type": "TIMESTAMP",
        },
    ):
        extractor.extract_incremental(
            conn, "products", watermark, batch_size=50
        )

    query, params = cursor.execute.call_args.args

    # Inspect the psycopg2 SQL composition without a real connection.
    from psycopg2 import sql

    assert isinstance(query, sql.Composed)

    sql_parts = [
        part.string
        for part in query
        if isinstance(part, sql.SQL)
    ]
    sql_text = " ".join(sql_parts)

    assert "WHERE (" in sql_text
    assert ") > (%s, %s)" in sql_text
    assert "ORDER BY" in sql_text
    assert "LIMIT %s" in sql_text
    assert params == (ts, 100, 50)
