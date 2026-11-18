"""Fail-closed helpers for bounded Airflow orchestration state updates.

These helpers deliberately do not import Airflow or open a database connection.
The caller supplies read/compare-and-set functions so tests and production
adapters can keep the metadata transaction boundary explicit.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Iterable
from uuid import uuid4


class OrchestrationStateError(RuntimeError):
    """Base class for rejected orchestration state operations."""


class IllegalStateTransition(OrchestrationStateError):
    """The requested state transition is not legal for the entity."""


class StaleStateError(OrchestrationStateError):
    """The state read by the caller no longer matches its expected state."""


class RunIdReuseError(OrchestrationStateError):
    """A run ID already exists and therefore cannot be reused."""


class DeadlockRetryExhausted(OrchestrationStateError):
    """A bounded deadlock retry budget was exhausted."""


class StateCasConflict(OrchestrationStateError):
    """The persistence boundary updated zero or multiple rows."""


# Airflow 3.1.7 task states accepted by the orchestration contract. ``None``
# is represented as Python None at the persistence boundary.
AIRFLOW_3_1_7_TASK_STATES = frozenset({
    "scheduled", "queued", "running", "success", "restarting", "failed",
    "skipped", "upstream_failed", "up_for_retry", "up_for_reschedule",
    "deferred", "removed",
})


@dataclass(frozen=True)
class StateUpdate:
    """Result of one idempotent compare-and-set state operation."""

    state: str | None
    changed: bool
    attempts: int


_DAG_RUN_TRANSITIONS = {
    None: frozenset({"queued", "running", "success", "failed", "canceled"}),
    "queued": frozenset({"running", "failed", "canceled"}),
    "running": frozenset({"success", "failed", "canceled"}),
    "success": frozenset({"success"}),
    "failed": frozenset({"failed"}),
    "canceled": frozenset({"canceled"}),
}

_TASK_TRANSITIONS = {
    None: frozenset({
        "scheduled", "queued", "running", "success", "failed", "skipped",
        "upstream_failed", "up_for_retry", "removed", "restarting",
        "up_for_reschedule", "deferred",
    }),
    "scheduled": frozenset({"queued", "running", "failed", "up_for_retry"}),
    "queued": frozenset({"running", "failed", "up_for_retry"}),
    "running": frozenset({"success", "failed", "up_for_retry", "up_for_reschedule", "deferred"}),
    "up_for_retry": frozenset({"queued", "running", "failed", "restarting"}),
    "up_for_reschedule": frozenset({"queued", "running", "failed"}),
    "deferred": frozenset({"queued", "running", "failed", "up_for_reschedule"}),
    "restarting": frozenset({"queued", "running", "failed"}),
    "success": frozenset({"success"}),
    "failed": frozenset({"failed"}),
    "skipped": frozenset({"skipped"}),
    "upstream_failed": frozenset({"upstream_failed"}),
    "removed": frozenset({"removed"}),
}

_CLEARABLE_TASK_STATES = frozenset({"failed", "up_for_retry", "upstream_failed"})


def validate_transition(entity: str, current: str | None, target: str) -> None:
    """Validate an Airflow-compatible transition before touching metadata."""
    if entity == "dag_run":
        transitions = _DAG_RUN_TRANSITIONS
    elif entity == "task_instance":
        transitions = _TASK_TRANSITIONS
    else:
        raise ValueError(f"Unsupported orchestration entity: {entity}")
    if target not in transitions.get(current, frozenset()):
        raise IllegalStateTransition(
            f"Illegal {entity} transition: {current!r} -> {target!r}"
        )


def is_postgres_deadlock(error: BaseException) -> bool:
    """Recognize PostgreSQL deadlocks without requiring psycopg at import time."""
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if getattr(current, "sqlstate", None) == "40P01":
            return True
        if getattr(current, "pgcode", None) == "40P01":
            return True
        if "deadlock detected" in str(current).lower():
            return True
        current = current.__cause__ or current.__context__
    return False


def compare_and_set_state(
    *,
    entity: str,
    expected_state: str | None,
    target_state: str,
    read_state: Callable[[], str | None],
    write_state: Callable[[str | None, str], int],
    max_deadlock_retries: int = 2,
    retry_delay_seconds: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
) -> StateUpdate:
    """Apply one legal, idempotent state change using caller-owned CAS.

    ``write_state`` must perform an atomic ``WHERE state = expected_state``
    update in its metadata transaction. A deadlock retries the whole read/CAS
    attempt; a stale read, illegal transition or exhausted retry fails closed.
    """
    if max_deadlock_retries < 0:
        raise ValueError("max_deadlock_retries must be non-negative")
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds must be non-negative")

    for attempt in range(1, max_deadlock_retries + 2):
        current = read_state()
        if current == target_state:
            return StateUpdate(state=current, changed=False, attempts=attempt)
        if current != expected_state:
            raise StaleStateError(
                f"Stale {entity} state: expected {expected_state!r}, found {current!r}"
            )
        validate_transition(entity, current, target_state)
        try:
            affected_rows = write_state(expected_state, target_state)
            if affected_rows != 1:
                raise StateCasConflict(
                    f"Expected one {entity} CAS row, got {affected_rows!r}"
                )
        except Exception as error:
            if not is_postgres_deadlock(error):
                raise
            if attempt > max_deadlock_retries:
                raise DeadlockRetryExhausted(
                    f"Deadlock retry budget exhausted for {entity}"
                ) from error
            if retry_delay_seconds:
                sleep(retry_delay_seconds * attempt)
            continue
        return StateUpdate(state=target_state, changed=True, attempts=attempt)

    raise AssertionError("unreachable")


def validate_clear_request(current_state: str | None) -> None:
    """Permit clear/requeue planning only for failed task instances."""
    if current_state not in _CLEARABLE_TASK_STATES:
        raise IllegalStateTransition(
            f"Task clear is not allowed from state {current_state!r}"
        )


def require_fresh_run_id(run_id: str, existing_run_ids: Iterable[str]) -> str:
    """Reject reuse of any existing run ID, including failed/stale runs."""
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a non-empty string")
    if run_id in set(existing_run_ids):
        raise RunIdReuseError(f"Run ID already exists and cannot be reused: {run_id}")
    return run_id


def reserve_fresh_run_id(run_id: str, insert_run: Callable[[str], int]) -> str:
    """Atomically reserve a run ID using one unique-key-protected INSERT."""
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a non-empty string")
    affected_rows = insert_run(run_id)
    if affected_rows != 1:
        raise RunIdReuseError(f"Run ID already exists and cannot be reserved: {run_id}")
    return run_id


def new_run_id(prefix: str = "manual") -> str:
    """Create an opaque fresh run ID; callers must still persist it once."""
    if not prefix or any(char.isspace() for char in prefix):
        raise ValueError("prefix must be non-empty and contain no whitespace")
    return f"{prefix}__{uuid4()}"

