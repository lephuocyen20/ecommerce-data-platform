from unittest.mock import MagicMock

from ingestion.incremental.watermark import clear_replay_state


def test_empty_replay_clear_is_transactional_and_resets_all_cursor_fields():
    conn = MagicMock()
    conn.autocommit = False
    assert clear_replay_state(conn, "payments") is True
    query, params = conn.cursor.return_value.__enter__.return_value.execute.call_args.args
    assert "replay_cursor_timestamp = NULL" in query
    assert "replay_upper_id = 0" in query
    assert params == ("payments",)
    conn.commit.assert_not_called()
