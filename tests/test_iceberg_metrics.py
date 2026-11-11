import pytest

from lakehouse.diagnostics.iceberg_metrics import (
    IcebergMetricsError,
    collect_table_metrics,
    validate_read_only_statement,
)


class _Row:
    snapshot_id = 7
    committed_at = "2026-11-10T00:00:00Z"
    operation = "append"
    file_count = 2
    record_count = 12
    bytes = 2048


class _Result:
    def first(self):
        return _Row()


class _Spark:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        return _Result()


def test_collector_queries_only_snapshots_and_files_metadata():
    spark = _Spark()
    result = collect_table_metrics(spark, "lakehouse.silver.orders")
    assert result["snapshot_id"] == 7
    assert result["file_count"] == 2
    assert all("FROM lakehouse.silver.orders." in statement for statement in spark.statements)
    assert not any(token in " ".join(spark.statements).upper() for token in ("MERGE", "DELETE", "UPDATE", "DROP"))


@pytest.mark.parametrize("statement", ["DELETE FROM lakehouse.silver.orders", "ALTER TABLE x SET TBLPROPERTIES ('a'='b')", "VACUUM lakehouse.silver.orders"])
def test_mutation_tokens_are_rejected(statement):
    with pytest.raises(IcebergMetricsError, match="Mutation"):
        validate_read_only_statement(statement)


def test_invalid_table_identifier_is_rejected():
    with pytest.raises(IcebergMetricsError, match="identifier"):
        collect_table_metrics(_Spark(), "lakehouse.silver.orders; DROP TABLE x")
