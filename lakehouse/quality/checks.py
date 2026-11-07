"""Read-only Spark DataFrame executors for the Phase 2 quality checks."""

from __future__ import annotations

from functools import reduce
from typing import Iterable

from pyspark.sql import functions as F

from .contracts import ERROR, FAIL, PASS, CheckDefinition, QualityError, QualityResult


_SUPPORTED_CHECK_TYPES = frozenset({"NOT_NULL", "UNIQUE", "PREDICATE"})


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


def evaluate_check(dataframe, check: CheckDefinition, table: str, sample_limit: int = 10) -> QualityResult:
    """Evaluate one Phase 2 check against an in-memory Spark DataFrame.

    The function only builds Spark read transformations and actions; it never
    writes, caches, persists, or mutates the supplied DataFrame.
    """
    try:
        _validate_sample_limit(sample_limit)
    except ValueError as exc:
        return _invalid(check, table, str(exc))

    if check.check_type not in _SUPPORTED_CHECK_TYPES:
        return _invalid(check, table, f"Unsupported Phase 2 check type '{check.check_type}'")

    rule = check.rule or {}
    if not isinstance(rule, dict):
        return _invalid(check, table, "rule must be a mapping")

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
    else:
        expression = rule.get("expression")
        if not isinstance(expression, str) or not expression.strip():
            return _invalid(check, table, "PREDICATE requires a non-empty rule['expression']")
        columns = []

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
        else:
            evaluated_rows, violation_count, violations = _predicate(dataframe, expression)
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
