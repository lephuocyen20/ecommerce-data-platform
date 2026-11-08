"""Phase 3 integration tests for read-only Spark quality executors."""

from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from lakehouse.quality.checks import evaluate_check
from lakehouse.quality.contracts import CheckDefinition, ERROR, FAIL, PASS


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("LakehouseQualityAdvancedTests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_accepted_values_only_fails_invalid_non_null_values(spark):
    dataframe = spark.createDataFrame([(2, "PAID"), (1, "BAD"), (3, None)], ["id", "status"])
    check = CheckDefinition("accepted_values:status", "ACCEPTED_VALUES", "status", rule={"values": ["PAID", "PENDING"]})

    result = evaluate_check(dataframe, check, "silver.orders", sample_limit=1)

    assert (result.status, result.evaluated_rows, result.violation_count) == (FAIL, 3, 1)
    assert result.sample == ({"id": 1, "status": "BAD"},)


def test_accepted_values_errors_for_invalid_definition_and_missing_column(spark):
    dataframe = spark.createDataFrame([(1,)], ["id"])
    malformed = evaluate_check(dataframe, CheckDefinition("accepted", "ACCEPTED_VALUES", "id", rule={"values": []}), "t")
    missing = evaluate_check(dataframe, CheckDefinition("accepted", "ACCEPTED_VALUES", "status", rule={"values": ["OK"]}), "t")

    assert malformed.status == ERROR and malformed.error.error_class == "INVALID_CHECK_DEFINITION"
    assert missing.status == ERROR and missing.error.error_class == "MISSING_COLUMN"


def _schema_check(**overrides):
    rule = {
        "business_columns": ["id", "amount"],
        "casts": {"id": "bigint", "amount": "decimal(12,2)"},
        "managed_columns": {"_batch_id": "string", "_ingested_at": "timestamp"},
        "allow_extra_columns": False,
    }
    rule.update(overrides)
    return CheckDefinition("schema", "SCHEMA", rule=rule)


def test_schema_passes_and_is_contract_level_without_row_scan(spark):
    dataframe = spark.createDataFrame([(1, Decimal("10.25"), "b", None)], "id long, amount decimal(12,2), _batch_id string, _ingested_at timestamp")

    result = evaluate_check(dataframe, _schema_check(), "silver.payments")

    assert (result.status, result.evaluated_rows, result.violation_count, result.sample) == (PASS, 0, 0, ())


def test_schema_reports_sorted_missing_extra_and_type_mismatches(spark):
    dataframe = spark.createDataFrame([(1, "extra")], "id int, z_extra string")

    result = evaluate_check(dataframe, _schema_check(), "silver.payments")

    assert (result.status, result.evaluated_rows, result.violation_count) == (FAIL, 0, 5)
    assert result.sample == ({
        "missing_columns": ["_batch_id", "_ingested_at", "amount"],
        "extra_columns": ["z_extra"],
        "type_mismatches": {"id": {"expected": "bigint", "actual": "int"}},
    },)


def test_schema_allows_extras_when_explicitly_configured_and_rejects_bad_metadata(spark):
    dataframe = spark.createDataFrame([(1, Decimal("1.00"), "b", None, "ignored")],
                                      "id long, amount decimal(12,2), _batch_id string, _ingested_at timestamp, z string")
    allowed = evaluate_check(dataframe, _schema_check(allow_extra_columns=True), "t")
    malformed = evaluate_check(dataframe, _schema_check(casts={"id": "not_a_spark_type", "amount": "decimal(12,2)"}), "t")

    assert allowed.status == PASS
    assert malformed.status == ERROR and malformed.error.error_class == "INVALID_CHECK_DEFINITION"


def test_schema_requires_exact_decimal_precision_and_scale(spark):
    dataframe = spark.createDataFrame([(1, Decimal("1.00"), "b", None)],
                                      "id long, amount decimal(10,2), _batch_id string, _ingested_at timestamp")

    result = evaluate_check(dataframe, _schema_check(), "t")

    assert result.status == FAIL and result.violation_count == 1
    assert result.sample[0]["type_mismatches"] == {
        "amount": {"expected": "decimal(12,2)", "actual": "decimal(10,2)"}
    }


def test_foreign_key_anti_join_nulls_duplicates_and_inputs_remain_unchanged(spark):
    child = spark.createDataFrame([(3, 1), (1, 2), (2, None), (4, 9)], ["id", "customer_id"])
    parent = spark.createDataFrame([(1,), (1,), (2,), (None,)], ["customer_id"])
    before_child, before_parent = child.collect(), parent.collect()
    check = CheckDefinition("foreign_key:customer_id->customers.customer_id", "FOREIGN_KEY", "customer_id", "customers", {"parent_key": "customer_id"})

    result = evaluate_check(child, check, "silver.orders", parent_resolver=lambda name: parent)

    assert (result.status, result.evaluated_rows, result.violation_count) == (FAIL, 4, 1)
    assert result.sample == ({"id": 4, "customer_id": 9},)
    assert child.collect() == before_child and parent.collect() == before_parent


def test_foreign_key_error_paths_and_planner_metadata_error(spark):
    child = spark.createDataFrame([(1,)], ["customer_id"])
    check = CheckDefinition("fk", "FOREIGN_KEY", "customer_id", "customers", {"parent_key": "customer_id"})
    missing_resolver = evaluate_check(child, check, "t")
    absent_key = evaluate_check(child, check, "t", parent_resolver=lambda _: spark.createDataFrame([(1,)], ["other"]))
    metadata = CheckDefinition("fk", "FOREIGN_KEY", "customer_id", "customers", {
        "parent_key": None,
        "metadata_error": {"error_class": "UNKNOWN_PARENT_SPEC", "message": "no parent", "details": {"name": "customers"}},
    })
    planned_error = evaluate_check(child, metadata, "t", parent_resolver=lambda _: child)

    assert missing_resolver.status == ERROR and missing_resolver.error.error_class == "MISSING_PARENT_RESOLVER"
    assert absent_key.status == ERROR and absent_key.error.error_class == "MISSING_PARENT_COLUMN"
    assert planned_error.status == ERROR and planned_error.error.error_class == "UNKNOWN_PARENT_SPEC"

