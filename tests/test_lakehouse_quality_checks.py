"""Integration coverage for read-only Spark DataFrame quality checks."""

import pytest
from pyspark.sql import SparkSession

from lakehouse.quality.checks import evaluate_check
from lakehouse.quality.contracts import CheckDefinition, ERROR, FAIL, PASS


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("LakehouseQualityChecksTests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_not_null_pass_fail_sample_and_input_is_unchanged(spark):
    dataframe = spark.createDataFrame([(2, "b"), (1, None), (3, None)], ["id", "value"])
    before = dataframe.collect()

    failed = evaluate_check(dataframe, CheckDefinition("not_null:value", "NOT_NULL", "value", rule={"column": "value"}), "silver.example", 1)
    passed = evaluate_check(dataframe, CheckDefinition("not_null:id", "NOT_NULL", "id", rule={"column": "id"}), "silver.example")

    assert (failed.status, failed.evaluated_rows, failed.violation_count, failed.sample) == (FAIL, 3, 2, ({"id": 1, "value": None},))
    assert failed.error is None
    assert passed.status == PASS and passed.violation_count == 0 and passed.sample == ()
    assert dataframe.collect() == before


def test_unique_ignores_null_keys_and_counts_duplicate_rows(spark):
    dataframe = spark.createDataFrame([(2, "two"), (1, "one"), (None, "a"), (1, "one-again"), (None, "b")], ["key", "value"])
    result = evaluate_check(dataframe, CheckDefinition("unique:key", "UNIQUE", "key", rule={"columns": ["key"], "ignore_null": True}), "silver.example")

    assert (result.status, result.evaluated_rows, result.violation_count) == (FAIL, 5, 2)
    assert result.sample == ({"key": 1, "value": "one"}, {"key": 1, "value": "one-again"})


def test_unique_passes_when_non_null_keys_are_unique(spark):
    dataframe = spark.createDataFrame([(1,), (2,), (None,), (None,)], ["key"])
    result = evaluate_check(dataframe, CheckDefinition("unique:key", "UNIQUE", "key", rule={"columns": ["key"]}), "silver.example")

    assert (result.status, result.evaluated_rows, result.violation_count, result.sample) == (PASS, 4, 0, ())


def test_predicate_false_and_null_are_violations(spark):
    dataframe = spark.createDataFrame([(1, 4), (2, 0), (3, None)], ["id", "amount"])
    result = evaluate_check(dataframe, CheckDefinition("predicate:amount > 0", "PREDICATE", rule={"expression": "amount > 0"}), "silver.example")

    assert (result.status, result.evaluated_rows, result.violation_count) == (FAIL, 3, 2)
    assert result.sample == ({"id": 2, "amount": 0}, {"id": 3, "amount": None})


def test_predicate_passes_when_every_row_is_true(spark):
    dataframe = spark.createDataFrame([(1,), (2,)], ["amount"])
    result = evaluate_check(dataframe, CheckDefinition("predicate:amount > 0", "PREDICATE", rule={"expression": "amount > 0"}), "silver.example")

    assert (result.status, result.evaluated_rows, result.violation_count, result.sample) == (PASS, 2, 0, ())


@pytest.mark.parametrize(
    "check",
    [
        CheckDefinition("not_null:absent", "NOT_NULL", "absent", rule={"column": "absent"}),
        CheckDefinition("unique:absent", "UNIQUE", "absent", rule={"columns": ["absent"]}),
        CheckDefinition("predicate:bad", "PREDICATE", rule={"expression": "not valid sql !!!"}),
        CheckDefinition("predicate:missing", "PREDICATE", rule={"expression": "absent > 0"}),
    ],
)
def test_missing_columns_and_malformed_predicates_are_structured_errors(spark, check):
    result = evaluate_check(spark.createDataFrame([(1,)], ["id"]), check, "silver.example")

    assert result.status == ERROR
    assert result.error is not None
    assert result.evaluated_rows is None and result.violation_count is None and result.sample == ()


def test_spark_action_error_is_returned_as_structured_error(spark, monkeypatch):
    dataframe = spark.createDataFrame([(1,)], ["id"])
    check = CheckDefinition("not_null:id", "NOT_NULL", "id", rule={"column": "id"})

    def boom(_dataframe, _column):
        raise RuntimeError("controlled Spark action failure")

    monkeypatch.setattr("lakehouse.quality.checks._not_null", boom)
    result = evaluate_check(dataframe, check, "silver.example")

    assert result.status == ERROR
    assert result.error.error_class == "RuntimeError"
    assert "controlled Spark action failure" in result.error.message
