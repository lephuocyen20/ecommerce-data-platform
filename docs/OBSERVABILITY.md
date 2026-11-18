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

## Health check

From the repository root:

```powershell
.\scripts\check_pipeline_health.ps1
```

The command checks Compose health, Airflow import errors, DAG registration, and
recent incremental run history. A non-zero exit code is an alert condition. It
does not clear runs, trigger DAGs, alter Airflow metadata, or write lakehouse
data.

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
