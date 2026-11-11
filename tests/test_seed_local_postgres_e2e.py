import pytest

from tests.seed_local_postgres_e2e import _fixture_rows, fixture_ids, seed_local_sources, validate_local_scope


def _local_env():
    return {
        "BRONZE_BUCKET": "ecommerce-quality-it",
        "AIRFLOW_LOCAL_E2E_MODE": "WRITE",
        "INGESTION_WRITE_ENABLED": "true",
    }


def test_seed_requires_bounded_local_profile():
    with pytest.raises(RuntimeError, match="ecommerce-quality-it"):
        validate_local_scope({"BRONZE_BUCKET": "ecommerce-data-lake", "AIRFLOW_LOCAL_E2E_MODE": "WRITE", "INGESTION_WRITE_ENABLED": "true"})


def test_seed_rejects_external_postgres_target():
    env = _local_env()
    env.update({"DB_HOST": "postgres.production.example", "DB_NAME": "analytics"})
    with pytest.raises(RuntimeError, match="PostgreSQL host"):
        validate_local_scope(env)


def test_fixture_rows_cover_all_six_sources_with_valid_fk_chain():
    rows = _fixture_rows(__import__("datetime").datetime(2026, 11, 10, 12, 0, 0))
    assert set(rows) == {"customers", "products", "orders", "order_items", "payments", "shipments"}
    assert rows["orders"][0][1] == rows["customers"][0][0]
    assert rows["order_items"][0][1:] [:2] == (rows["orders"][0][0], rows["products"][0][0])
    assert rows["payments"][0][1] == rows["orders"][0][0]
    assert rows["shipments"][0][1] == rows["orders"][0][0]


def test_run_ids_create_distinct_deltas_and_are_stable_on_retry():
    first = fixture_ids("run-a")
    retry = fixture_ids("run-a")
    second = fixture_ids("run-b")

    assert first == retry
    assert first != second
    assert _fixture_rows(__import__("datetime").datetime(2026, 11, 10), "run-a")["orders"] != _fixture_rows(__import__("datetime").datetime(2026, 11, 10), "run-b")["orders"]


def test_fixture_business_dates_stay_in_bounded_gold_window_while_watermark_can_advance():
    from datetime import datetime

    rows = _fixture_rows(
        datetime(2026, 11, 10, 13, 0, 0),
        "run-window",
        business_timestamp=datetime(2026, 1, 2, 12, 0, 0),
    )
    assert rows["orders"][0][3].date().isoformat() == "2026-01-02"
    assert rows["orders"][0][4].date().isoformat() == "2026-11-10"


def test_seed_is_idempotent_and_orders_parent_before_children(monkeypatch):
    monkeypatch.setattr("tests.seed_local_postgres_e2e.validate_local_scope", lambda: None)

    class Cursor:
        def __init__(self):
            self.tables = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query, values):
            if query.startswith("SELECT updated_at"):
                self.tables.append("customers_lookup")
            else:
                self.tables.append(query.split("public.", 1)[1].split(" ", 1)[0])

        def fetchone(self):
            return None

    class Connection:
        def __init__(self):
            self.cursor_obj = Cursor()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return self.cursor_obj

        def close(self):
            pass

    connection = Connection()
    result = seed_local_sources(lambda: connection)

    assert result == {table: 1 for table in ("customers", "products", "orders", "order_items", "payments", "shipments")}
    assert connection.cursor_obj.tables == ["customers_lookup", "customers", "products", "orders", "order_items", "payments", "shipments"]
