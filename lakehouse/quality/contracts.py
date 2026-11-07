"""Pure-Python contracts for a future, read-only Silver quality runner.

The planner is deliberately the only executable behaviour in this module: it
derives all rules from :class:`TableSpec`, never opens a Spark session and never
touches a table.  A later runner must interpret SQL ``NULL`` as a NOT_NULL
violation and as a PREDICATE violation when the expression evaluates to NULL.
UNIQUE deliberately ignores NULL values; a configured NOT_NULL rule owns NULL
enforcement for the business key.
Foreign keys resolve their referenced column from the parent spec's ``key``;
null child keys are not orphans (a configured NOT_NULL rule owns that concern).
If a target or parent table cannot be read, the future runner must emit an
``ERROR`` result, not a data ``FAIL``.
"""
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Literal, Mapping, Optional


PASS, FAIL, ERROR = "PASS", "FAIL", "ERROR"
QualityStatus = Literal["PASS", "FAIL", "ERROR"]
CheckType = Literal["NOT_NULL", "UNIQUE", "PREDICATE", "ACCEPTED_VALUES", "FOREIGN_KEY", "SCHEMA"]
CHECK_TYPES = frozenset({"NOT_NULL", "UNIQUE", "PREDICATE", "ACCEPTED_VALUES", "FOREIGN_KEY", "SCHEMA"})


def _json_safe(value: Any):
    """Convert nested contract evidence into values accepted by ``json.dumps``."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_json_safe(item) for item in value]
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)


@dataclass(frozen=True)
class QualityError:
    """Structured reason an evaluator could not determine a quality outcome."""
    error_class: str
    message: str
    details: Optional[Mapping[str, Any]] = None

    def as_dict(self):
        return _json_safe(asdict(self))


@dataclass(frozen=True)
class CheckDefinition:
    check_id: str
    check_type: CheckType
    column: Optional[str] = None
    reference_table: Optional[str] = None
    rule: Optional[dict] = None


@dataclass(frozen=True)
class QualityResult:
    check_id: str
    check_type: CheckType
    status: QualityStatus
    table: str
    column: Optional[str] = None
    reference_table: Optional[str] = None
    rule: Optional[dict] = None
    evaluated_rows: Optional[int] = None
    violation_count: Optional[int] = None
    sample: tuple = ()
    error: Optional[QualityError] = None

    def as_dict(self):
        return _json_safe(asdict(self))


@dataclass(frozen=True)
class TableQualityReport:
    table: str
    status: QualityStatus
    snapshot_id: Optional[int]
    rows: Optional[int]
    checks: tuple
    error: Optional[QualityError] = None

    def as_dict(self):
        return _json_safe({"table": self.table, "status": self.status, "snapshot_id": self.snapshot_id,
                           "rows": self.rows, "checks": [check.as_dict() for check in self.checks],
                           "error": self.error})


def _managed_columns(spec):
    columns = {"_batch_id": "string", "_ingested_at": "timestamp", "_record_hash": "string"}
    if spec.watermark:
        columns[spec.audit_column or "_source_updated_at"] = "timestamp"
    return columns


def build_quality_plan(spec, specs: Mapping):
    """Build deterministic checks from the existing TableSpec only."""
    checks = []
    for column in dict.fromkeys((spec.key,) + tuple(spec.required)):
        checks.append(CheckDefinition(f"not_null:{column}", "NOT_NULL", column, rule={"column": column}))
    checks.append(CheckDefinition(f"unique:{spec.key}", "UNIQUE", spec.key,
                                  rule={"columns": [spec.key], "ignore_null": True}))
    for predicate in dict.fromkeys(spec.predicates):
        checks.append(CheckDefinition(f"predicate:{predicate}", "PREDICATE", rule={"expression": predicate}))
    if spec.status and spec.allowed_statuses:
        checks.append(CheckDefinition(f"accepted_values:{spec.status}", "ACCEPTED_VALUES", spec.status,
                                      rule={"values": list(spec.allowed_statuses)}))
    for column, parent_name in (spec.foreign_keys or {}).items():
        parent = specs.get(parent_name)
        parent_key = parent.key if parent else None
        metadata_error = None
        if parent is None:
            metadata_error = QualityError("UNKNOWN_PARENT_SPEC", f"No TableSpec exists for parent '{parent_name}'")
        elif parent_key not in parent.columns or parent_key not in parent.casts:
            metadata_error = QualityError("INVALID_PARENT_KEY", f"Parent '{parent_name}' key '{parent_key}' is not a declared column")
        rule = {"parent_key": parent_key}
        if metadata_error:
            rule["metadata_error"] = metadata_error.as_dict()
        checks.append(CheckDefinition(f"foreign_key:{column}->{parent_name}.{parent_key or '?'}", "FOREIGN_KEY", column,
                                      parent_name, rule))
    checks.append(CheckDefinition("schema", "SCHEMA", rule={"business_columns": list(spec.columns),
                                      "casts": dict(spec.casts), "managed_columns": _managed_columns(spec),
                                      "allow_extra_columns": False}))
    return tuple(checks)


def aggregate_status(checks) -> QualityStatus:
    statuses = {check.status for check in checks}
    return ERROR if ERROR in statuses else FAIL if FAIL in statuses else PASS
