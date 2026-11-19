# Pipeline observability and controlled observation

## Scope

The critical local runtime path is:

```text
ecommerce_incremental_ingestion
  -> Bronze ingestion/validation
  -> Silver manifest/artifacts
  -> Gold dimensional WRITE
  -> table confirmation markers
```

`publish_gold_to_active` remains intentionally skipped while
`BI_ACTIVE_PUBLISH_APPLY=0`. Gold writes must not be described as BI cutover.

## Operational targets

These are initial local-runtime targets, not a production SLO certification:

| Signal | Target | Action |
|---|---:|---|
| DAG import errors | 0 | Stop and repair before triggering runs |
| Scheduled run state | 100% success over observation window | Inspect task log and dependency health |
| Run freshness | next run starts within 20 minutes of the 15-minute schedule | Check scheduler/dag processor |
| Gold readback | seven tables readable and manifest/hash consistent | Do not publish to BI |
| Lock state | no stale lock after a completed run | Use the lock recovery runbook |
| Active BI publish | `0` until separate cutover approval | Keep consumers on the certified Gold ref |

## Incremental outcome semantics

The following states are distinct operational signals. A no-op interval is not
an outage, but it must still have complete and internally consistent evidence.

| State | Meaning | Operator action |
|---|---|---|
| `NO_DATA` | An individual incremental source/Silver input had no rows eligible for the interval. No fabricated batch, manifest, watermark, checkpoint, or snapshot is implied. | Treat as expected only when the source watermark/window explains it; record the affected table and continue the contract checks. |
| `NO_CHANGE` | All required inputs resolved to no change, so Gold returns `GOLD_NO_CHANGE/NO_CHANGE` and skips Gold write, publication, and confirmations. | Count as a successful no-op when the DAG is `success` and the health checks pass; do not call it a failed write or a BI cutover. |
| `SUCCESS` | The DAG completed successfully. A full data-contract success additionally requires the applicable artifact, manifest/hash, readback, and lock evidence; the health script alone is only a DAG/runtime-health signal. This includes a valid `NO_CHANGE` no-op. | Record the run ID and evidence. Keep `BI_ACTIVE_PUBLISH_APPLY=0` unless a separate publication approval exists. |
| `FAILED` | A task, contract, checksum/manifest, readback, lock, service, or health invariant failed, or the DAG run is `failed`. Partial external state must be assumed possible until verified. | Preserve run ID/logs, inspect the existing recovery path, and do not blind-retry, clear state, or publish. |
| `STALE` | No valid run is observed inside the requested window, a run/lock is older than its freshness boundary, or required runtime evidence is not current. | Treat as an alert and fail closed; inspect scheduler/DAG processor and lock ownership before any remediation. |

`NO_DATA` is an input-level outcome; `NO_CHANGE` is the aggregate Gold
outcome; `SUCCESS` and `FAILED` are run/health outcomes; `STALE` is a
freshness/observation outcome. They must not be substituted for one another.

## Health check

From the repository root:

```powershell
.\scripts\check_pipeline_health.ps1
```

The command checks Compose health, Airflow import errors, DAG registration, and
recent incremental run history. A non-zero exit code is an alert condition. It
does not clear runs, trigger DAGs, alter Airflow metadata, or write lakehouse
data.

The monitor is fail-closed for the signals it actually checks. It fails when
required services are absent, not running/`Up`, or report a non-empty health
value that is not healthy; it also fails when Airflow reports import errors,
the DAG is not registered, the observation window has no valid run, a run
timestamp is invalid/outside the window, any recent run is `failed`, any run is
still `queued`/`running`, or the minimum successful-run threshold is not met.
Malformed or empty JSON from a probe is also an error; it is never interpreted
as a healthy empty result. The script does not require a health field from a
service that does not expose one, and it does not by itself validate Gold
manifest/hash, seven-table readback, lock state, or BI publication state.

Remediation is read-only first: capture the command output, run
`docker compose ps`, inspect Airflow import errors and the relevant task logs,
then verify lock ownership and existing recovery evidence. Do not make the
monitor self-heal by clearing runs, deleting locks, triggering a DAG, changing
watermarks/checkpoints, mutating MinIO/Nessie/PostgreSQL, or publishing BI.
Only an explicitly approved, guarded recovery procedure may change runtime
state, and it must be followed by a fresh health observation.

For a controlled observation window, run the command once per scheduled interval
for at least three consecutive intervals and record:

1. run ID and state;
2. whether the Silver artifacts were `PASS`, `NO_DATA`, or `NO_CHANGE`;
3. Gold writer status and seven-table readback;
4. candidate/stable manifest hash and row-count parity;
5. lock status after completion;
6. confirmation that `BI_ACTIVE_PUBLISH_APPLY` is still `0`.

Do not manually trigger a production write or clear a failed state as part of
this observation without a separate approval for the exact run and interval.

## Log retention

Airflow requires the host bind mount `airflow/logs` to exist. Never delete the
root directory while Compose is running. Use the safe dry-run rotation command:

```powershell
.\scripts\rotate_airflow_logs.ps1 -RetentionDays 14
```

The script enforces `MinimumAgeHours=24` as a safety floor, protecting the
active observation window even if a shorter retention value is passed.

After reviewing the candidate list, remove only expired log files with:

```powershell
.\scripts\rotate_airflow_logs.ps1 -RetentionDays 14 -Apply
```

The rotation script never deletes the log root and has no access to MinIO,
Nessie, PostgreSQL, or checkpoints.

## Alert response

### Import error or DAG missing

1. Run `docker compose ps`.
2. Run `docker compose exec -T airflow-scheduler airflow dags list-import-errors`.
3. Fix the DAG/import issue and rerun the static DagBag check.
4. Do not clear a production run before identifying the failure.

### Failed incremental run

1. Preserve the run ID and task logs.
2. Check MinIO, Nessie, Postgres and Docker daemon health.
3. Check whether Gold write produced a partial external state.
4. Follow the existing recovery/lock evidence before retrying; Gold write tasks
   intentionally have no blind retry.

### Stale lock

1. Confirm the owning run is not still active.
2. Capture lock identity, graph hash and run ID.
3. Use the existing guarded recovery path; never delete a lock object manually.

## BI publication gate

Active publication requires a separate approval and evidence bundle:

- seven-table readback;
- manifest/hash parity;
- expected stable-ref CAS precondition;
- consumer smoke query;
- rollback target and owner.

Until those are approved, `BI_ACTIVE_PUBLISH_APPLY=0` is the correct state.

## Current read-only evidence and E2E boundary

On 2026-11-19, the current incremental evidence recorded a successful
`manual__production_write_20261119_r05` no-op: all six source inputs were
`NO_DATA`, Gold returned `GOLD_NO_CHANGE/NO_CHANGE`, and Gold write,
publication, and confirmations were skipped. The current read-only Trino
check against `lakehouse.gold_run_20261112_gold_06` also read all seven Gold
tables successfully. This evidence does not certify a real source delta.

A real-delta E2E test is a separate mutating action and requires explicit
bounded approval. It must validate the immutable contract before creating any
client: `LAKEHOUSE_E2E_APPROVED=1`, bucket
`LAKEHOUSE_E2E_TEST_BUCKET=ecommerce-data-lake`, and the exact prefix
`LAKEHOUSE_E2E_PREFIX=quality-it/<run-id>/` for a valid immutable
`LAKEHOUSE_E2E_RUN_ID`. Without that approval and validation, keep the E2E
test deferred; never infer approval from a successful `NO_CHANGE` run.
