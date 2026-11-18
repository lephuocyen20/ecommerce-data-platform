"""Production Airflow state boundary backed by an injected API client."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import quote

from common.airflow_orchestration import (
    StateUpdate,
    compare_and_set_state,
    reserve_fresh_run_id,
)
from common.http_json import LocalHttpJsonClient


class AirflowStateClient(Protocol):
    def read_state(self, *, entity: str, identity: dict[str, str]) -> str | None: ...

    def write_state(
        self,
        *,
        entity: str,
        identity: dict[str, str],
        expected_state: str | None,
        target_state: str,
    ) -> int: ...

    def reserve_run(self, *, run_id: str) -> int: ...


class AirflowStateApiTransport:
    """Airflow 3.1.x Stable REST API transport; never opens metadata DB access."""

    def __init__(self, *, base_url: str, token: str, timeout_seconds: float = 10.0,
                 http_client: LocalHttpJsonClient | None = None) -> None:
        if not base_url.strip():
            raise ValueError("Airflow state API URL must be non-empty")
        if not token.strip():
            raise ValueError("Airflow state API token must be non-empty")
        if timeout_seconds <= 0:
            raise ValueError("Airflow state API timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        self._timeout_seconds = timeout_seconds
        self._http = http_client or LocalHttpJsonClient()

    @staticmethod
    def _part(value: str) -> str:
        if not isinstance(value, str) or not value:
            raise ValueError("Airflow state identity values must be non-empty strings")
        return quote(value, safe="")

    def _task_url(self, identity: dict[str, str]) -> str:
        required = ("dag_id", "run_id", "task_id")
        if any(key not in identity for key in required):
            raise ValueError(f"task_instance identity requires {required}")
        return (
            f"{self._base_url}/api/v2/dags/{self._part(identity['dag_id'])}"
            f"/dagRuns/{self._part(identity['run_id'])}/taskInstances/"
            f"{self._part(identity['task_id'])}"
        )

    def read_state(self, *, entity: str, identity: dict[str, str]) -> str | None:
        if entity != "task_instance":
            raise ValueError("Airflow 3.1.7 API transport supports task_instance only")
        response = self._http.request_json(
            self._task_url(identity), headers=self._headers,
            timeout=self._timeout_seconds, error_context="Airflow task-instance GET",
        )
        state = response.get("state")
        if state is not None and not isinstance(state, str):
            raise RuntimeError("Airflow task-instance response has invalid state")
        return state

    def write_state(self, *, entity: str, identity: dict[str, str],
                    expected_state: str | None, target_state: str) -> int:
        if entity != "task_instance":
            raise ValueError("Airflow 3.1.7 API transport supports task_instance only")
        response = self._http.request_json(
            self._task_url(identity), method="PATCH",
            body={"new_state": target_state}, headers=self._headers,
            timeout=self._timeout_seconds, error_context="Airflow task-instance PATCH",
        )
        if response.get("state") != target_state:
            raise RuntimeError("Airflow task-instance API did not confirm target state")
        return 1

    def reserve_run(self, *, run_id: str) -> int:
        raise NotImplementedError("Run reservation is not exposed by Airflow Stable REST API")


def build_airflow_state_client_from_runtime(environ: dict[str, str] | None = None) -> AirflowStateClient:
    """Build the API client only from task-runtime config, never during DAG parsing."""
    values = os.environ if environ is None else environ
    base_url = values.get("AIRFLOW_STATE_API_URL", "").strip()
    token = values.get("AIRFLOW_STATE_API_TOKEN", "").strip()
    if not base_url or not token:
        raise RuntimeError(
            "Airflow task-state callback is unavailable: set "
            "AIRFLOW_STATE_API_URL and AIRFLOW_STATE_API_TOKEN"
        )
    try:
        timeout = float(values.get("AIRFLOW_STATE_API_TIMEOUT_SECONDS", "10"))
    except ValueError as exc:
        raise RuntimeError("AIRFLOW_STATE_API_TIMEOUT_SECONDS must be numeric") from exc
    return AirflowStateApiTransport(base_url=base_url, token=token, timeout_seconds=timeout)


@dataclass(frozen=True)
class AirflowStateAdapter:
    """Adapter used by the task/DagRun caller at the Airflow API boundary."""

    client: AirflowStateClient
    max_deadlock_retries: int = 2

    def transition(
        self,
        *,
        entity: str,
        identity: dict[str, str],
        expected_state: str | None,
        target_state: str,
    ) -> StateUpdate:
        return compare_and_set_state(
            entity=entity,
            expected_state=expected_state,
            target_state=target_state,
            read_state=lambda: self.client.read_state(entity=entity, identity=identity),
            write_state=lambda expected, target: self.client.write_state(
                entity=entity,
                identity=identity,
                expected_state=expected,
                target_state=target,
            ),
            max_deadlock_retries=self.max_deadlock_retries,
        )

    def reserve_run(self, run_id: str) -> str:
        return reserve_fresh_run_id(
            run_id,
            lambda value: self.client.reserve_run(run_id=value),
        )


def make_task_failure_callback(client: AirflowStateClient):
    """Create an explicit callback for production task-state API callers."""
    adapter = AirflowStateAdapter(client)

    def on_failure(context):
        ti = context["task_instance"]
        return adapter.transition(
            entity="task_instance",
            identity={"dag_id": ti.dag_id, "task_id": ti.task_id, "run_id": ti.run_id},
            expected_state="running",
            target_state="failed",
        )

    return on_failure


def make_runtime_task_failure_callback(client: AirflowStateClient | None = None):
    """Create a parse-safe callback; resolve the transport only inside failure handling."""
    def on_failure(context):
        runtime_client = client or build_airflow_state_client_from_runtime()
        return make_task_failure_callback(runtime_client)(context)

    return on_failure
