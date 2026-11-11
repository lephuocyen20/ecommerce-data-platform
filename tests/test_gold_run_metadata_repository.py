from unittest.mock import MagicMock

from gold.implementation.run_metadata import build_runtime_dimensional_metadata
from gold.implementation.run_metadata_repository import persist_if_enabled, persist_gold_run_metadata


def metadata():
    return {
        "run_id": "run-1", "bronze_batch_ids": {"orders": "b1"},
        "silver_snapshots": {"orders": "snap_1"}, "iceberg_snapshots": {"orders": 1},
        "attestations": {"orders": "a1"}, "evidence": {"orders": "e1"},
        "gold_snapshot_id": "gold_snap_1", "status": "PASS", "code_sha": "abc1234",
        "started_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-01T00:01:00Z",
        "duration_seconds": 60, "rows_read": 1, "rows_written": 1, "rows_rejected": 0,
    }


def test_metadata_persistence_is_default_off():
    factory = MagicMock()
    result = persist_if_enabled("daily_sales", metadata(), factory, environ={})
    assert result["persisted"] is False
    factory.assert_not_called()


def test_metadata_repository_upsert_is_idempotent():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ("run-1", "daily_sales")
    result = persist_gold_run_metadata(conn, "daily_sales", metadata())
    assert result == {"run_id": "run-1", "dataset_name": "daily_sales", "persisted": True}
    assert "ON CONFLICT (run_id, dataset_name)" in cursor.execute.call_args.args[0]


def test_dimensional_metadata_is_built_from_completed_runtime_result():
    tables = ("customers", "products", "orders", "order_items", "payments", "shipments")
    lineage = {"sources": {
        table: {
            "snapshot_id": index,
            "snapshot_attestation_identity": f"{index:064x}",
            "quality_evidence": {"identity": f"{index + 10:064x}"},
            "row_count": 1,
            "bronze_batch_id": None,
        } for index, table in enumerate(tables, 1)
    }}
    result = {"status": "PUBLISHED", "graph": {"identity": "a" * 64}}
    built = build_runtime_dimensional_metadata(
        lineage=lineage, result=result, mode="WRITE",
        environment={"GOLD_RUN_ID": "airflow-run-1", "GOLD_CODE_SHA": "abc1234"},
    )
    assert built["run_id"] == "airflow-run-1"
    assert built["gold_snapshot_id"] == "gold_snap_" + "a" * 64
    assert set(built["silver_snapshots"]) == set(tables)
    assert set(built["bronze_batch_ids"].values()) == {None}
    assert built["rows_read"] == 6
