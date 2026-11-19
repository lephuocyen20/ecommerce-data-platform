# Production incremental observability plan

## Scope

Strengthen the local operational contract for the already-approved continuous
incremental WRITE path. This phase is documentation and read-only monitoring
only: do not seed PostgreSQL, trigger a DAG, publish BI, mutate Nessie refs, or
write MinIO data.

## Approved work

1. Document the distinction between `NO_DATA`, `NO_CHANGE`, `SUCCESS`,
   `FAILED`, and `STALE` so a no-op incremental interval is not treated as an
   outage.
2. Document the existing health command as the operator entry point, including
   its fail-closed conditions and remediation paths.
3. Document read-only Trino/Metabase smoke checks and the BI publication gate.
4. Record the current live health evidence and the separate approval required
   for a bounded real-delta E2E test.

## Acceptance criteria

- `docs/OBSERVABILITY.md` defines actionable signal semantics and alert
  response for the incremental path.
- `docs/RUNBOOK.md` contains copy/paste-safe read-only Trino/Metabase checks
  and explicitly forbids source/lakehouse mutation in the smoke path.
- The plan records verification commands and does not claim a real delta was
  produced when only `NO_CHANGE` runs were observed.
- `BI_ACTIVE_PUBLISH_APPLY` remains `0`; no commit, push, or shared-data
  mutation is performed in this phase.
- Independent QA reviews the resulting diff before delivery.

## Risks and rollback

- Documentation-only changes have no runtime rollback surface; revert the
  documentation diff if wording is incorrect.
- A real-delta test remains a separate gated action because it mutates source
  PostgreSQL and the bounded E2E object-storage prefix.

## Verification commands

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\check_pipeline_health.ps1 -WindowMinutes 180 -MinimumSuccessfulRuns 1
docker compose exec -T airflow-scheduler airflow dags list-import-errors
docker compose config --quiet
git diff --check
```

## Status

- Plan: approved in chat for the safe implementation portion.
- Implementation: complete for the documentation-only scope after QA fixes.
- QA round 1: **FAIL**. Independent review found three documentation/process
  gaps: the fail-closed wording overstated the health script's service-health
  behavior and data-contract coverage; the Trino smoke command did not follow
  `nextUri` or assert seven rows; and this plan lacked the QA verdict.
- QA round 1 correction: the observability wording now distinguishes runtime
  health from full data-contract success, and the Trino smoke command follows
  pagination, fails on query errors, and asserts exactly seven rows.
- QA round 2: **PASS WITH WARNINGS**. Independent re-review confirmed the
  corrected fail-closed wording, Trino `nextUri`/error handling and exact
  seven-row assertion. Evidence: `git diff --check`, PowerShell parser check,
  static Trino snippet check, and `docker compose config --quiet` passed.
  Warning: runtime health and Trino smoke were not re-executed in this QA
  round; prior live health evidence remains recorded separately.
- Follow-up read-only runtime evidence: Trino pagination smoke returned all
  seven Gold table counts (`dim_date` 762, `dim_customer` 6236,
  `dim_product` 2691, `fact_orders` 26714, `fact_order_items` 80318,
  `fact_payments` 33768, `fact_shipments` 24596); Metabase
  `GET /api/health` returned HTTP 200 with `{"status":"ok"}`. No mutation was
  performed.
- Runtime delta test: explicitly deferred pending the exact bounded E2E
  authorization and target contract.
- Commit/push: not authorized by the repository delivery gate.
- Next step: user approval is required before staging/committing these
  documentation changes; a real-delta E2E remains a separate bounded runtime
  approval and is not part of this delivery.
