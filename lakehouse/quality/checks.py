"""Read-only Spark DataFrame executors for Lakehouse quality checks."""

from __future__ import annotations

from functools import reduce
import re
from typing import Iterable, Mapping

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

from .contracts import ERROR, FAIL, PASS, CheckDefinition, QualityError, QualityResult


_SUPPORTED_CHECK_TYPES = frozenset({"NOT_NULL", "UNIQUE", "PREDICATE", "ACCEPTED_VALUES", "SCHEMA", "FOREIGN_KEY"})
_SIMPLE_TYPES = frozenset({"string", "int", "bigint", "timestamp"})


def _result(check: CheckDefinition, table: str, status: str, **kwargs) -> QualityResult:
    """Preserve the immutable rule provenance on every result."""
    return QualityResult(
        check_id=check.check_id,
        check_type=check.check_type,
        status=status,
        table=table,
        column=check.column,
        reference_table=check.reference_table,
        rule=check.rule,
        **kwargs,
    )


def _error(check: CheckDefinition, table: str, error_class: str, message: str, details=None) -> QualityResult:
    return _result(
        check,
        table,
        ERROR,
        sample=(),
        error=QualityError(error_class, message, details),
    )


def _invalid(check: CheckDefinition, table: str, message: str) -> QualityResult:
    return _error(
        check,
        table,
        "INVALID_CHECK_DEFINITION",
        message,
        {"check_id": check.check_id, "check_type": check.check_type},
    )


def _validate_sample_limit(sample_limit: int) -> None:
    # bool is an int subclass but accepting it would be a surprising public API.
    if isinstance(sample_limit, bool) or not isinstance(sample_limit, int) or sample_limit < 0:
        raise ValueError("sample_limit must be a non-negative integer")


def _quoted(name: str):
    """Return a Column for an exact Spark field name, including dots/spaces."""
    return F.col("`" + name.replace("`", "``") + "`")


def _sample(dataframe, sample_limit: int) -> tuple:
    if not sample_limit:
        return ()
    # Ordering by all columns creates a deterministic total order for the scalar
    # data contracts this executor supports.  A Spark sorting error is surfaced as
    # ERROR by the public boundary rather than returning an arbitrary sample.
    ordered = dataframe.orderBy(*[_quoted(name).asc_nulls_first() for name in dataframe.columns])
    return tuple(row.asDict(recursive=True) for row in ordered.limit(sample_limit).collect())


def _missing_columns(dataframe, columns: Iterable[str]) -> list[str]:
    present = set(dataframe.columns)
    return [column for column in columns if column not in present]


def _not_null(dataframe, column: str):
    violation = _quoted(column).isNull()
    counts = dataframe.agg(
        F.count(F.lit(1)).alias("evaluated_rows"),
        F.sum(F.when(violation, F.lit(1)).otherwise(F.lit(0))).alias("violation_count"),
    ).first()
    return int(counts["evaluated_rows"]), int(counts["violation_count"] or 0), dataframe.filter(violation)


def _predicate(dataframe, expression: str):
    # Spark SQL WHERE filters NULL values out.  Coalescing first makes both FALSE
    # and NULL outcomes explicit violations, as required by the quality contract.
    violation = ~F.coalesce(F.expr(expression), F.lit(False))
    counts = dataframe.agg(
        F.count(F.lit(1)).alias("evaluated_rows"),
        F.sum(F.when(violation, F.lit(1)).otherwise(F.lit(0))).alias("violation_count"),
    ).first()
    return int(counts["evaluated_rows"]), int(counts["violation_count"] or 0), dataframe.filter(violation)


def _unique(dataframe, columns: list[str]):
    eligible = dataframe.filter(reduce(lambda left, right: left & right, (_quoted(column).isNotNull() for column in columns)))
    duplicate_keys = eligible.groupBy(*[_quoted(column) for column in columns]).count().filter(F.col("count") > 1)
    # Semi join retains exactly the original duplicate rows, so the count and
    # sample use the same row-level violation unit without a write or mutation.
    violations = eligible.join(duplicate_keys.select(*[_quoted(column) for column in columns]), columns, "left_semi")
    return int(dataframe.count()), int(violations.count()), violations


def _accepted_values(dataframe, column: str, values: list):
    # NULL ownership stays with NOT_NULL, matching UNIQUE and FOREIGN_KEY rules.
    violation = _quoted(column).isNotNull() & ~_quoted(column).isin(values)
    counts = dataframe.agg(
        F.count(F.lit(1)).alias("evaluated_rows"),
        F.sum(F.when(violation, F.lit(1)).otherwise(F.lit(0))).alias("violation_count"),
    ).first()
    return int(counts["evaluated_rows"]), int(counts["violation_count"] or 0), dataframe.filter(violation)


def _expected_type(cast: str) -> str:
    normalized = cast.strip().lower()
    if normalized in _SIMPLE_TYPES:
        return normalized
    decimal = re.fullmatch(r"decimal\((\d+),(\d+)\)", normalized)
    if decimal and int(decimal.group(1)) > 0 and int(decimal.group(2)) >= 0 and int(decimal.group(2)) <= int(decimal.group(1)):
        return normalized
    raise ValueError(f"Unsupported expected Spark type '{cast}'")


def _schema_result(dataframe, check: CheckDefinition, table: str, rule: Mapping) -> QualityResult:
    business_columns = rule.get("business_columns")
    casts = rule.get("casts")
    managed_columns = rule.get("managed_columns")
    allow_extra = rule.get("allow_extra_columns", False)
    if (not isinstance(business_columns, (list, tuple)) or not all(isinstance(name, str) and name for name in business_columns)
            or not isinstance(casts, Mapping) or not isinstance(managed_columns, Mapping) or not isinstance(allow_extra, bool)):
        return _invalid(check, table, "SCHEMA requires business_columns, casts, managed_columns, and boolean allow_extra_columns")
    expected = {}
    try:
        for name in business_columns:
            expected[name] = _expected_type(casts[name])
        for name, cast in managed_columns.items():
            if not isinstance(name, str) or not name:
                raise ValueError("SCHEMA column names must be non-empty strings")
            if name in expected:
                raise ValueError(f"SCHEMA column '{name}' is declared more than once")
            expected[name] = _expected_type(cast)
    except (KeyError, TypeError, ValueError) as exc:
        return _invalid(check, table, str(exc))
    try:
        actual = {field.name: field.dataType.simpleString().lower() for field in dataframe.schema.fields}
    except Exception as exc:
        return _error(check, table, type(exc).__name__, str(exc), {"check_id": check.check_id, "table": table})
    missing = sorted(set(expected) - set(actual))
    extras = [] if allow_extra else sorted(set(actual) - set(expected))
    mismatches = {name: {"expected": expected[name], "actual": actual[name]}
                  for name in sorted(set(expected) & set(actual)) if expected[name] != actual[name]}
    violation_count = len(missing) + len(extras) + len(mismatches)
    sample = () if not violation_count else ({"missing_columns": missing, "extra_columns": extras,
                                              "type_mismatches": mismatches},)
    return _result(check, table, PASS if not violation_count else FAIL, evaluated_rows=0,
                   violation_count=violation_count, sample=sample)


def _metadata_error(check: CheckDefinition, table: str, metadata_error) -> QualityResult | None:
    if metadata_error is None:
        return None
    if not isinstance(metadata_error, Mapping) or not isinstance(metadata_error.get("error_class"), str) or not isinstance(metadata_error.get("message"), str):
        return _invalid(check, table, "FOREIGN_KEY metadata_error must contain error_class and message")
    return _result(check, table, ERROR, sample=(), error=QualityError(
        metadata_error["error_class"], metadata_error["message"], metadata_error.get("details")))


def _foreign_key(dataframe, check: CheckDefinition, table: str, rule: Mapping, parent_resolver, sample_limit: int):
    metadata_result = _metadata_error(check, table, rule.get("metadata_error"))
    if metadata_result:
        return metadata_result
    child_key, parent_key = check.column, rule.get("parent_key")
    if not isinstance(child_key, str) or not child_key or not isinstance(parent_key, str) or not parent_key:
        return _invalid(check, table, "FOREIGN_KEY requires check.column and non-empty rule['parent_key']")
    if not isinstance(check.reference_table, str) or not check.reference_table:
        return _invalid(check, table, "FOREIGN_KEY requires reference_table")
    missing = _missing_columns(dataframe, [child_key])
    if missing:
        return _error(check, table, "MISSING_COLUMN", "DataFrame is missing required columns", {"columns": missing})
    if not callable(parent_resolver):
        return _error(check, table, "MISSING_PARENT_RESOLVER", "FOREIGN_KEY requires a callable parent_resolver")
    try:
        parent = parent_resolver(check.reference_table)
    except Exception as exc:
        return _error(check, table, type(exc).__name__, str(exc), {"reference_table": check.reference_table})
    if not isinstance(parent, DataFrame):
        return _error(check, table, "INVALID_PARENT_DATAFRAME", "parent_resolver must return a Spark DataFrame",
                      {"reference_table": check.reference_table})
    parent_missing = _missing_columns(parent, [parent_key])
    if parent_missing:
        return _error(check, table, "MISSING_PARENT_COLUMN", "Parent DataFrame is missing required key column",
                      {"columns": parent_missing, "reference_table": check.reference_table})
    try:
        # Rename the parent key so a child and parent may have different names safely.
        eligible = dataframe.filter(_quoted(child_key).isNotNull()).alias("child")
        parent_keys = parent.select(_quoted(parent_key).alias("__quality_parent_key")).filter(
            F.col("__quality_parent_key").isNotNull()).distinct().alias("parent")
        violations = eligible.join(parent_keys, F.col("child.`" + child_key.replace("`", "``") + "`") ==
                                   F.col("parent.__quality_parent_key"), "left_anti")
        evaluated_rows = int(dataframe.count())
        violation_count = int(violations.count())
        return _result(check, table, PASS if not violation_count else FAIL, evaluated_rows=evaluated_rows,
                       violation_count=violation_count, sample=_sample(violations, sample_limit) if violation_count else ())
    except Exception as exc:
        return _error(check, table, type(exc).__name__, str(exc), {"check_id": check.check_id, "table": table})


def evaluate_check(dataframe, check: CheckDefinition, table: str, sample_limit: int = 10, parent_resolver=None) -> QualityResult:
    """Evaluate one quality check against an in-memory Spark DataFrame.

    The function only builds Spark read transformations and actions; it never
    writes, caches, persists, or mutates the supplied DataFrame.
    """
    try:
        _validate_sample_limit(sample_limit)
    except ValueError as exc:
        return _invalid(check, table, str(exc))

    if check.check_type not in _SUPPORTED_CHECK_TYPES:
        return _invalid(check, table, f"Unsupported check type '{check.check_type}'")

    rule = check.rule or {}
    if not isinstance(rule, dict):
        return _invalid(check, table, "rule must be a mapping")

    if check.check_type == "SCHEMA":
        return _schema_result(dataframe, check, table, rule)
    if check.check_type == "FOREIGN_KEY":
        return _foreign_key(dataframe, check, table, rule, parent_resolver, sample_limit)
    if check.check_type == "NOT_NULL":
        column = check.column or rule.get("column")
        if not isinstance(column, str) or not column:
            return _invalid(check, table, "NOT_NULL requires a non-empty column")
        columns = [column]
    elif check.check_type == "UNIQUE":
        configured_columns = rule.get("columns")
        columns = configured_columns if configured_columns is not None else [check.column]
        if not isinstance(columns, (list, tuple)) or not columns or any(not isinstance(column, str) or not column for column in columns):
            return _invalid(check, table, "UNIQUE requires non-empty rule['columns'] or check.column")
        columns = list(columns)
    elif check.check_type == "PREDICATE":
        expression = rule.get("expression")
        if not isinstance(expression, str) or not expression.strip():
            return _invalid(check, table, "PREDICATE requires a non-empty rule['expression']")
        columns = []
    else:
        column = check.column
        values = rule.get("values")
        if not isinstance(column, str) or not column:
            return _invalid(check, table, "ACCEPTED_VALUES requires a non-empty column")
        if not isinstance(values, (list, tuple)) or not values or any(value is None for value in values):
            return _invalid(check, table, "ACCEPTED_VALUES requires non-empty rule['values'] without NULL")
        columns = [column]

    try:
        missing = _missing_columns(dataframe, columns)
    except Exception as exc:
        return _error(check, table, type(exc).__name__, str(exc), {"check_id": check.check_id, "table": table})
    if missing:
        return _error(check, table, "MISSING_COLUMN", "DataFrame is missing required columns", {"columns": missing})

    try:
        if check.check_type == "NOT_NULL":
            evaluated_rows, violation_count, violations = _not_null(dataframe, columns[0])
        elif check.check_type == "UNIQUE":
            evaluated_rows, violation_count, violations = _unique(dataframe, columns)
        elif check.check_type == "PREDICATE":
            evaluated_rows, violation_count, violations = _predicate(dataframe, expression)
        else:
            evaluated_rows, violation_count, violations = _accepted_values(dataframe, columns[0], list(values))
        sample = _sample(violations, sample_limit) if violation_count else ()
    except Exception as exc:
        return _error(
            check,
            table,
            type(exc).__name__,
            str(exc),
            {"check_id": check.check_id, "table": table},
        )

    return _result(
        check,
        table,
        PASS if violation_count == 0 else FAIL,
        evaluated_rows=evaluated_rows,
        violation_count=violation_count,
        sample=sample,
    )
