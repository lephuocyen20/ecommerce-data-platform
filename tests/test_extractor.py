from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from psycopg2 import sql
from psycopg2.extensions import adapt

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


def test_late_overlap_uses_replay_window_and_monotonic_checkpoint(monkeypatch):
    conn, cursor = make_connection()
    monkeypatch.setenv("LATE_DATA_OVERLAP_ENABLED", "true")
    cursor.fetchone.return_value = (datetime(2026, 1, 3, tzinfo=timezone.utc), 101)
    cursor.description = [("product_id",), ("updated_at",)]
    cursor.fetchall.return_value = [(101, datetime(2026, 1, 2, tzinfo=timezone.utc))]
    result = extractor.extract_incremental(
        conn, "products",
        {"watermark_type": "TIMESTAMP", "last_timestamp": datetime(2026, 1, 2, tzinfo=timezone.utc),
         "last_id": 100, "replay_cursor_timestamp": None, "replay_cursor_id": 0,
         "replay_upper_timestamp": None},
    )
    assert result["checkpoint_timestamp"] == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert result["checkpoint_id"] == 101
    assert result["replay_upper_timestamp"] == datetime(2026, 1, 3, tzinfo=timezone.utc)
    assert result["replay_upper_id"] == 101
    assert len(cursor.execute.call_args_list) == 2


def test_late_overlap_replay_cursor_advances_across_batches_same_timestamp(monkeypatch):
    monkeypatch.setenv("LATE_DATA_OVERLAP_ENABLED", "true")
    conn, cursor = make_connection()
    cursor.description = [("product_id",), ("updated_at",)]
    cursor.fetchall.return_value = [(102, datetime(2026, 1, 2, tzinfo=timezone.utc))]
    first = extractor.extract_incremental(conn, "products", {
        "watermark_type": "TIMESTAMP", "last_timestamp": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "last_id": 100, "replay_cursor_timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc),
         "replay_cursor_id": 101, "replay_upper_timestamp": datetime(2026, 1, 2, tzinfo=timezone.utc), "replay_upper_id": 103,
    }, batch_size=1)
    assert first["replay_cursor_id"] == 102
    assert first["checkpoint_id"] == 102
    cursor.fetchall.return_value = [(103, datetime(2026, 1, 2, tzinfo=timezone.utc))]
    second = extractor.extract_incremental(conn, "products", {
        "watermark_type": "TIMESTAMP", "last_timestamp": first["checkpoint_timestamp"],
        "last_id": first["checkpoint_id"], "replay_cursor_timestamp": first["replay_cursor_timestamp"],
        "replay_cursor_id": first["replay_cursor_id"], "replay_upper_timestamp": first["replay_upper_timestamp"], "replay_upper_id": first["replay_upper_id"],
    }, batch_size=1)
    assert second["replay_cursor_id"] == 103
    assert second["replay_upper_timestamp"] is None

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


@pytest.mark.parametrize(
    ("table_name", "primary_key"),
    [("payments", "payment_id"), ("shipments", "shipment_id")],
    ids=["payments", "shipments"],
)
def test_child_incremental_binds_parent_scope_as_postgresql_array(table_name, primary_key):
    conn, cursor = make_connection()
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    cursor.description = [(name,) for name in (primary_key, "order_id", "updated_at", "_is_deleted", "deleted_at")]
    cursor.fetchall.return_value = [(101, 11, datetime(2026, 1, 2, tzinfo=timezone.utc), False, None)]
    watermark = {"watermark_type": "TIMESTAMP", "last_timestamp": ts, "last_id": 100}
    with patch.object(
        extractor,
        "get_table_config",
        return_value={"primary_key": primary_key, "watermark_column": "updated_at", "watermark_type": "TIMESTAMP"},
    ):
        extractor.extract_incremental(conn, table_name, watermark, batch_size=25, parent_order_ids=[11, 12])
    query, params = cursor.execute.call_args.args
    assert "ANY(%s)" in str(query)
    assert params == (ts, 100, [11, 12], 25)
    assert adapt(params[2]).getquoted() == b"ARRAY[11,12]"


def test_empty_parent_scope_returns_no_data_without_source_query():
    conn, cursor = make_connection()
    result = extractor.extract_incremental(
        conn,
        "shipments",
        {"watermark_type": "TIMESTAMP", "last_timestamp": None, "last_id": 0},
        parent_order_ids=[],
    )
    assert result["row_count"] == 0
    cursor.execute.assert_not_called()


@pytest.mark.parametrize(
    ("table_name", "primary_key"),
    [("payments", "payment_id"), ("shipments", "shipment_id")],
    ids=["payments", "shipments"],
)
def test_replay_child_scope_uses_array_any_and_exact_parameter_order(monkeypatch, table_name, primary_key):
    monkeypatch.setenv("LATE_DATA_OVERLAP_ENABLED", "true")
    conn, cursor = make_connection()
    last_timestamp = datetime(2026, 1, 2, tzinfo=timezone.utc)
    upper_timestamp = datetime(2026, 1, 3, tzinfo=timezone.utc)
    cursor.fetchone.return_value = (upper_timestamp, 101)
    cursor.description = [(name,) for name in (primary_key, "order_id", "updated_at", "_is_deleted", "deleted_at")]
    cursor.fetchall.return_value = [(101, 11, upper_timestamp, False, None)]

    result = extractor.extract_incremental(
        conn,
        table_name,
        {
            "watermark_type": "TIMESTAMP",
            "last_timestamp": last_timestamp,
            "last_id": 100,
            "replay_cursor_timestamp": last_timestamp,
            "replay_cursor_id": 0,
            "replay_upper_timestamp": None,
        },
        batch_size=25,
        parent_order_ids=[11, 12],
    )

    upper_query, upper_params = cursor.execute.call_args_list[0].args
    replay_query, replay_params = cursor.execute.call_args_list[1].args
    assert str(upper_query).count("ANY(%s)") == 2
    assert str(replay_query).count("ANY(%s)") == 1
    assert upper_params == ([11, 12], [11, 12])
    assert replay_params == (last_timestamp, 0, upper_timestamp, 101, [11, 12], 25)
    assert all(adapt(params).getquoted() == b"ARRAY[11,12]" for params in upper_params)
    assert adapt(replay_params[4]).getquoted() == b"ARRAY[11,12]"
    assert result["row_count"] == 1
