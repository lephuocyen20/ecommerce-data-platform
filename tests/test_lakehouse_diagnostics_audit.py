from types import SimpleNamespace

import pytest

from lakehouse.diagnostics import audit


pytestmark = pytest.mark.unit


class FakeCatalog:
    def __init__(self, *, exists=True, error=None):
        self.exists = exists
        self.error = error
        self.table_exists_calls = []

    def tableExists(self, table):
        self.table_exists_calls.append(table)
        if self.error:
            raise self.error
        return self.exists


class FakeSpark:
    def __init__(
        self,
        *,
        exists=True,
        catalog_error=None,
        snapshot=123,
        sql_error=None,
        table_error=None,
        count=7,
    ):
        self.catalog = FakeCatalog(exists=exists, error=catalog_error)
        self.snapshot = snapshot
        self.sql_error = sql_error
        self.table_error = table_error
        self.count = count

    def sql(self, query):
        if self.sql_error:
            raise self.sql_error
        return SimpleNamespace(
            first=lambda: (
                None
                if self.snapshot is None
                else SimpleNamespace(snapshot_id=self.snapshot)
            )
        )

    def table(self, table):
        if self.table_error:
            raise self.table_error
        return SimpleNamespace(count=lambda: self.count)


class SparkError(Exception):
    def __init__(self, error_class, message="spark error"):
        super().__init__(message)
        self.error_class = error_class

    def getErrorClass(self):
        return self.error_class


class FakeSession:
    def __init__(self):
        self.stopped = False

    def stop(self):
        self.stopped = True


class FakeBuilder:
    def __init__(self, session):
        self.session = session

    def appName(self, name):
        assert name == "LakehouseAudit"
        return self

    def getOrCreate(self):
        return self.session


@pytest.fixture
def spec():
    return SimpleNamespace(foreign_keys={"order_id": "orders"})


def test_audit_table_preserves_success_fields_and_uses_three_part_identifier(spec):
    spark = FakeSpark(snapshot=456, count=12)

    result = audit.audit_table(spark, "payments", spec)

    assert spark.catalog.table_exists_calls == ["lakehouse.silver.payments"]
    assert result == {
        "rows": 12,
        "snapshot_id": 456,
        "foreign_keys": {"order_id": "orders"},
        "status": audit.MIGRATED,
    }


def test_audit_table_marks_confirmed_catalog_absence_as_not_migrated(spec):
    result = audit.audit_table(FakeSpark(exists=False), "payments", spec)

    assert result["status"] == audit.NOT_MIGRATED
    assert result["rows"] is None
    assert result["snapshot_id"] is None


def test_missing_table_race_after_table_exists_is_not_migrated(spec):
    spark = FakeSpark(sql_error=SparkError("TABLE_OR_VIEW_NOT_FOUND"))

    assert audit.audit_table(spark, "payments", spec)["status"] == audit.NOT_MIGRATED


@pytest.mark.parametrize(
    ("operation", "error", "expected"),
    [
        ("catalog_error", SparkError("SCHEMA_NOT_FOUND"), audit.CATALOG_ERROR),
        ("catalog_error", SparkError("CATALOG_NOT_FOUND"), audit.CATALOG_ERROR),
        ("catalog_error", SparkError("PERMISSION_DENIED"), audit.PERMISSION_DENIED),
        ("sql_error", SparkError("SCHEMA_NOT_FOUND"), audit.CATALOG_ERROR),
        ("sql_error", SparkError("PERMISSION_DENIED"), audit.PERMISSION_DENIED),
        ("table_error", SparkError("ACCESS_DENIED"), audit.PERMISSION_DENIED),
        ("sql_error", RuntimeError("permission denied by catalog"), audit.QUERY_ERROR),
        ("table_error", RuntimeError("warehouse connection timed out"), audit.QUERY_ERROR),
    ],
)
def test_non_missing_errors_are_not_masked_as_not_migrated(spec, operation, error, expected):
    spark = FakeSpark(**{operation: error})

    assert audit.audit_table(spark, "payments", spec)["status"] == expected


def test_audit_table_marks_existing_table_without_snapshot_as_failure(spec):
    result = audit.audit_table(FakeSpark(snapshot=None), "payments", spec)

    assert result["status"] == audit.NO_SNAPSHOT


def test_exit_code_accepts_migration_states_and_rejects_failure_states():
    assert audit.exit_code({"payments": {"status": audit.NOT_MIGRATED}}) == 0
    assert audit.exit_code({"payments": {"status": audit.MIGRATED}}) == 0
    for state in audit.FAILURE_STATES:
        assert audit.exit_code({"payments": {"status": state}}) == 1


def test_schema_not_found_is_a_failing_catalog_state(spec):
    result = audit.audit_table(
        FakeSpark(catalog_error=SparkError("SCHEMA_NOT_FOUND")), "payments", spec
    )

    assert result["status"] == audit.CATALOG_ERROR
    assert audit.exit_code({"payments": result}) == 1


@pytest.mark.parametrize(
    "failure", [RuntimeError("configure failed"), RuntimeError("audit failed")]
)
def test_main_stops_spark_when_setup_or_audit_fails(monkeypatch, failure):
    session = FakeSession()
    monkeypatch.setattr(audit.SparkSession, "builder", FakeBuilder(session))

    if str(failure) == "configure failed":
        monkeypatch.setattr(
            audit,
            "configure_s3a",
            lambda spark: (_ for _ in ()).throw(failure),
        )
    else:
        monkeypatch.setattr(audit, "configure_s3a", lambda spark: None)
        monkeypatch.setattr(
            audit,
            "run_audit",
            lambda spark: (_ for _ in ()).throw(failure),
        )

    with pytest.raises(RuntimeError, match=str(failure)):
        audit.main()

    assert session.stopped is True
