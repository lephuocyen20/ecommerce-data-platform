import pytest

from ingestion.incremental.config import TABLE_CONFIG, late_data_overlap_enabled, validate_extraction_contract
from ingestion.incremental.tombstones import reject_physical_delete, validate_tombstone_columns, validate_tombstone_state


def test_mutable_tables_use_updated_at_timestamp_signal():
    for table in ("order_items", "payments"):
        assert TABLE_CONFIG[table]["watermark_type"] == "TIMESTAMP"
        assert TABLE_CONFIG[table]["watermark_column"] == "updated_at"


def test_overlap_is_disabled_by_default():
    assert late_data_overlap_enabled({}) is False
    assert validate_extraction_contract({})["late_data_overlap"] is False


def test_tombstone_contract_requires_source_columns():
    assert validate_tombstone_columns("payments", {"updated_at", "_is_deleted", "deleted_at"})
    with pytest.raises(ValueError, match="tombstone columns"):
        validate_tombstone_columns("order_items", {"order_item_id", "updated_at"})


def test_physical_delete_is_rejected():
    with pytest.raises(ValueError, match="Physical deletes"):
        reject_physical_delete(True)
    assert reject_physical_delete(False) is False


@pytest.mark.parametrize(
    ("is_deleted", "deleted_at"),
    [(True, None), (False, "2026-01-01T00:00:00Z")],
)
def test_tombstone_state_rejects_inconsistent_runtime_state(is_deleted, deleted_at):
    with pytest.raises(ValueError, match="tombstone"):
        validate_tombstone_state(is_deleted, deleted_at)


def test_null_updated_at_is_rejected_by_runtime_row_contract():
    with pytest.raises(ValueError, match="updated_at"):
        validate_tombstone_state(False, None, updated_at=None)
