"""Unit coverage for the pure-Python Lakehouse quality metadata contracts."""
import datetime
import json
from dataclasses import replace
import pytest

from lakehouse.models.spec import SPECS
from lakehouse.quality.contracts import (
    ERROR,
    FAIL,
    PASS,
    QualityError,
    QualityResult,
    TableQualityReport,
    aggregate_status,
    build_quality_plan,
)


pytestmark = pytest.mark.unit


def test_quality_result_serializes_structured_error_and_tuple_sample():
    result = QualityResult(
        check_id="predicate:amount > 0",
        check_type="PREDICATE",
        status=ERROR,
        table="lakehouse.silver.payments",
        rule={"expression": "amount > 0"},
        sample=({"payment_id": 1},),
        error=QualityError("AnalysisException", "unknown column", {"observed_at": datetime.date(2026, 1, 2)}),
    )

    assert result.as_dict() == {
        "check_id": "predicate:amount > 0", "check_type": "PREDICATE", "status": ERROR,
        "table": "lakehouse.silver.payments", "column": None, "reference_table": None,
        "rule": {"expression": "amount > 0"}, "evaluated_rows": None,
        "violation_count": None, "sample": [{"payment_id": 1}],
        "error": {"error_class": "AnalysisException", "message": "unknown column", "details": {"observed_at": "2026-01-02"}},
    }


def test_table_report_serializes_nested_check_results_and_access_error_as_json_safe():
    check = QualityResult("schema", "SCHEMA", PASS, "lakehouse.silver.customers")
    report = TableQualityReport("lakehouse.silver.customers", ERROR, 42, 3, (check,),
                                QualityError("TABLE_NOT_FOUND", "not found", {"attempts": (1, 2)}))

    assert report.as_dict()["checks"] == [check.as_dict()]
    assert report.as_dict()["snapshot_id"] == 42
    assert report.as_dict()["error"]["details"] == {"attempts": [1, 2]}
    json.dumps(report.as_dict())


def test_rule_plans_are_derived_from_every_spec_and_have_schema_check():
    planned_types = set()
    for name, spec in SPECS.items():
        plan = build_quality_plan(spec, SPECS)

        assert plan[-1].check_type == "SCHEMA"
        assert plan[-1].rule["business_columns"] == list(spec.columns)
        assert plan[-1].rule["casts"] == dict(spec.casts)
        assert plan[-1].rule["allow_extra_columns"] is False
        assert plan[-1].rule["managed_columns"].items() >= {
            "_batch_id": "string", "_ingested_at": "timestamp", "_record_hash": "string"
        }.items()
        assert [check.check_id for check in plan] == list(dict.fromkeys(check.check_id for check in plan))
        assert {check.check_type for check in plan} >= {"NOT_NULL", "UNIQUE", "SCHEMA"}
        planned_types.update(check.check_type for check in plan)
        assert any(check.check_id == f"unique:{spec.key}" for check in plan)
        for column in dict.fromkeys((spec.key,) + tuple(spec.required)):
            assert any(check.check_id == f"not_null:{column}" for check in plan), name
    assert planned_types == {"NOT_NULL", "UNIQUE", "PREDICATE", "ACCEPTED_VALUES", "FOREIGN_KEY", "SCHEMA"}


def test_rule_planner_maps_predicates_statuses_and_foreign_keys_from_tablespec():
    products = build_quality_plan(SPECS["products"], SPECS)
    assert {check.rule["expression"] for check in products if check.check_type == "PREDICATE"} == {
        "stock_quantity >= 0", "price > 0"
    }

    orders = build_quality_plan(SPECS["orders"], SPECS)
    status = next(check for check in orders if check.check_type == "ACCEPTED_VALUES")
    fk = next(check for check in orders if check.check_type == "FOREIGN_KEY")
    assert status.column == "status"
    assert status.rule == {"values": list(SPECS["orders"].allowed_statuses)}
    assert fk.column == "customer_id"
    assert fk.reference_table == "customers"
    assert fk.rule == {"parent_key": "customer_id"}


def test_rule_planner_deduplicates_predicates_in_declaration_order():
    spec = replace(SPECS["products"], predicates=("price > 0", "stock_quantity >= 0", "price > 0"))

    predicates = [check.rule["expression"] for check in build_quality_plan(spec, SPECS)
                  if check.check_type == "PREDICATE"]

    assert predicates == ["price > 0", "stock_quantity >= 0"]


def test_schema_provenance_derives_default_custom_and_absent_audit_columns():
    customers = build_quality_plan(SPECS["customers"], SPECS)[-1].rule["managed_columns"]
    payments = build_quality_plan(SPECS["payments"], SPECS)[-1].rule["managed_columns"]
    order_items = build_quality_plan(SPECS["order_items"], SPECS)[-1].rule["managed_columns"]

    assert customers["_source_updated_at"] == "timestamp"
    assert payments["_source_created_at"] == "timestamp"
    assert set(order_items) == {"_batch_id", "_ingested_at", "_record_hash"}


def test_unique_rule_explicitly_ignores_nulls_owned_by_not_null():
    unique = next(check for check in build_quality_plan(SPECS["customers"], SPECS) if check.check_type == "UNIQUE")
    assert unique.rule == {"columns": ["customer_id"], "ignore_null": True}


def test_foreign_key_plan_keeps_unknown_parent_as_metadata_error_for_future_runner():
    spec = SPECS["orders"]
    isolated_specs = {"orders": spec}

    fk = next(check for check in build_quality_plan(spec, isolated_specs) if check.check_type == "FOREIGN_KEY")

    assert fk.rule == {"parent_key": None, "metadata_error": {
        "error_class": "UNKNOWN_PARENT_SPEC", "message": "No TableSpec exists for parent 'customers'", "details": None,
    }}
    assert fk.check_id == "foreign_key:customer_id->customers.?"


def test_foreign_key_plan_marks_parent_key_missing_from_parent_contract():
    invalid_parent = replace(SPECS["customers"], key="not_a_customer_column")
    specs = {"orders": SPECS["orders"], "customers": invalid_parent}

    fk = next(check for check in build_quality_plan(SPECS["orders"], specs) if check.check_type == "FOREIGN_KEY")

    assert fk.rule["parent_key"] == "not_a_customer_column"
    assert fk.rule["metadata_error"]["error_class"] == "INVALID_PARENT_KEY"


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [((PASS,), PASS), ((PASS, FAIL), FAIL), ((PASS, FAIL, ERROR), ERROR)],
)
def test_aggregate_status_uses_error_then_fail_precedence(statuses, expected):
    checks = tuple(QualityResult("schema", "SCHEMA", status, "table") for status in statuses)
    assert aggregate_status(checks) == expected
