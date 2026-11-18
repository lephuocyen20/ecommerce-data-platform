# System reliability hardening plan

## Scope

Implement review items 1-4 and 6 for the local Compose deployment. Item 5
(test/CI strategy) is explicitly out of scope. Do not change the test workspace,
do not enable `BI_ACTIVE_PUBLISH_APPLY`, and do not delete database, MinIO,
checkpoint, or Iceberg table data.

## Approved work

1. Add safe Airflow log retention/rotation guidance and an idempotent local
   maintenance command that never removes the active log root.
2. Add operational Airflow failure/health monitoring with actionable output and
   a runbook; use existing task failure callbacks where applicable.
3. Run a controlled observation window over 3-5 scheduled incremental intervals,
   checking run state, `NO_CHANGE`, Gold readback/parity, and lock state.
4. Record the BI publication decision and keep active publication disabled until
   a separate cutover approval and evidence gate.
5. Test/CI strategy is explicitly out of scope for this plan; no test workspace
   or test configuration changes are included.
6. Audit and clean only exact local operational leftovers in scope: duplicate
   `.env.backup-*` files and the known orphan Compose container. Nessie refs are
   audit/rollback state and are not deleted in this change.

## Acceptance criteria

- Airflow logs have a documented retention policy and a safe rotation command.
- Monitoring command exits non-zero on failed/stale DAG health and prints a
  remediation path; no credentials are printed.
- DagBag/import check remains clean.
- At least 3 scheduled incremental runs are observed, or the run is explicitly
  reported as incomplete because the schedule window has not elapsed.
- Gold writer/readback evidence is captured; `BI_ACTIVE_PUBLISH_APPLY` remains
  `0`.
- Only the exact duplicate local backups/orphan container are cleaned after
  read-only identity checks; no lakehouse data or checkpoints are removed.
- Independent QA reviews the diff and verification evidence before delivery.

## Risks and rollback

- Log rotation can remove historical diagnostics; default retention is
  conservative and excludes the current run window. Restore from the local
  backup/archive if needed.
- Removing an orphan container is reversible by recreating its Compose service.
- No Nessie/MinIO/Postgres/checkpoint cleanup is allowed in this phase.

## Verification commands

- `docker compose exec -T airflow-scheduler airflow dags list-import-errors`
- `docker compose ps`
- `docker compose exec -T airflow-scheduler airflow dags list`
- controlled monitoring/observation command documented in `docs/OBSERVABILITY.md`
- `git diff --check`

## Status

- Plan: approved by user in chat for items 1-4 and 6; item 5 excluded.
- Implementation: complete for the approved local scope.
- Runtime evidence:
  - `scheduled__2026-11-18T14:00:00+00:00`: success; Gold write success;
    `GOLD_NO_CHANGE`; publication and confirmations skipped.
  - `scheduled__2026-11-18T14:15:00+00:00`: success; Gold write success;
    `GOLD_NO_CHANGE`; publication and confirmations skipped.
  - `scheduled__2026-11-18T14:30:00+00:00`: success; Gold write success;
    `GOLD_NO_CHANGE`; publication and confirmations skipped.
  - Airflow import errors: empty; Compose services required by the monitor are
    running; `BI_ACTIVE_PUBLISH_APPLY=0` read from the scheduler.
  - Existing certified Gold readback/parity evidence for the seven tables and
    global fence identity remains the readback baseline; these three runs were
    no-op paths and therefore did not create a new external Gold snapshot.
  - Log rotation dry-run: 0 files eligible at 14-day retention.
  - Cleanup: one duplicate `.env.backup-*` removed after identical SHA-256
    verification; the known orphan `ecommerce-spark` container was stopped and
    removed; no Nessie refs, MinIO objects, PostgreSQL rows, checkpoints or
    volumes were touched.
- QA round 1: FAIL. Health parser/schema, stale/no-run detection, scope
  numbering, and evidence recording were corrected.
- QA round 2: FAIL. The health monitor did not fail closed for `queued` or
  `running` DAG runs in the observation window independently of
  `MinimumSuccessfulRuns`; the observability wording also overstated retention
  behavior by implying preservation of an older failed run.
- QA round 2 correction: queued/running states now fail closed, and retention
  wording is limited to the enforced `MinimumAgeHours=24` active-window guard.
- QA round 3: **PASS WITH WARNINGS**. Independent review found no Critical or
  High finding. PowerShell syntax/guard checks passed; the runtime health check
  passed against actual Compose JSON-lines and Airflow 3.1.7 output; three
  scheduled runs were successful no-op Gold runs with publication skipped.
- QA round 4 correction: the monitor now fails closed when a DAG run timestamp
  is more than 5 minutes in the future relative to the monitor's UTC clock.
- QA round 4: **PASS WITH WARNINGS**. Independent review found no code finding;
  mocked `+4 minutes` was accepted and `+6 minutes` was rejected; PowerShell
  syntax and safety scans passed.
- Remaining delivery warning: the new files are still untracked until the
  approved commit. The round-4 reviewer did not re-run live Airflow, while the
  earlier read-only health run passed against live Airflow.
- Status: complete for the approved local scope.
- Next step: user approval is required before committing or pushing these
  changes. Item 5 remains explicitly out of scope.
