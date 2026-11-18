# Incremental DAG import fix plan

## Scope

Fix the Airflow 3 DAG import failure in
`ecommerce_incremental_ingestion` where the Silver operator factory returns
`None`, causing dependency construction to fail with
`AttributeError: 'NoneType' object has no attribute 'update_relative'`.

## Evidence

- `docker compose exec -T airflow-scheduler airflow version` returned **3.1.7**.
- `airflow dags list-import-errors` reports the failure in
  `ecommerce_incremental_ingestion.py` during `set_downstream`.
- `create_incremental_silver_operator()` in
  `airflow/dags/pipeline_operators.py` assigns `operator =
  create_spark_submit_operator(...)` but has no `return operator`.
- The DAG then executes `eligibility[table] >> [skip, transforms[table]]`,
  so `transforms[table]` is `None`.
- The historical DAG graph itself parses and shows all six expected stages;
  no historical runtime or shared data service will be touched by this fix.

## Approved implementation scope

1. Add `return operator` to `create_incremental_silver_operator()`.
2. Add a regression test asserting the factory returns an operator and that the
   incremental DAG can be imported without errors.
3. Do not change task dependencies, write guards, storage paths, or runtime
   configuration.

## Acceptance criteria

- `ecommerce_incremental_ingestion` imports with no DagBag/import errors.
- `transforms[table]` is a Docker/Spark operator for every supported table.
- Existing historical DAG graph remains unchanged.
- No Airflow task run, backfill, Spark job, MinIO/Nessie/PostgreSQL mutation,
  checkpoint reset, or cleanup is performed during verification.

## Verification

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_airflow_parse_imports.py
.\.venv\Scripts\python.exe -m compileall -q airflow/dags/pipeline_operators.py airflow/dags/ecommerce_incremental_ingestion.py
git diff --check
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

The runtime `airflow dags test` and any backfill remain outside this fix and
require a separate explicit approval.

## Follow-up implementation and QA result (2026-11-16)

- Removed only the duplicate parse-time production WRITE rejection from
  `create_gold_dimensional_operator()` in `airflow/dags/pipeline_operators.py`.
- Added `test_gold_write_factory_parses_without_forwarding_production_approval`
  to `tests/test_airflow_parse_imports.py`. The test constructs the factory in
  `WRITE` mode with the approval variable absent, while stubbing operator
  creation and storage validation; it does not create Airflow, Spark, Docker,
  MinIO, Nessie, or PostgreSQL clients.
- The authoritative runtime guard in
  `gold/implementation/dimensional/run_pipeline.py` was not changed.

Verification evidence:

- `\.venv\Scripts\python.exe -m pytest -q tests/test_airflow_parse_imports.py` — **25 passed**.
- `\.venv\Scripts\python.exe -m pytest -q tests/test_gold_orchestration.py` — **23 passed**, including the production WRITE denial test.
- Scoped `compileall` for `pipeline_operators.py` and
  `ecommerce_incremental_ingestion.py` — **passed**.
- `git diff --check` — **passed**; existing worktree line-ending warnings only.
- No Airflow task/backfill, external write, approval forwarding, checkpoint
  reset, or cleanup was performed.

Independent QA verdict: **PASS for the approved follow-up scope**. The
incremental DAG's parse-time duplicate rejection is removed, runtime safety is
preserved by the existing CLI guard and safety test, and task graph code was
not changed by this follow-up.

Next step: a separate read-only DagBag/import-error verification may be run in
the scheduler environment if explicitly authorized; task execution and
backfills remain out of scope.

## Risks and handoff

- The missing return is a parse-time blocker; further runtime issues may appear
  only after the DAG is accepted by DagBag.
- A separate latent issue remains in the historical DAG: Compose can inject
  blank `HISTORICAL_BACKFILL_RUN_ID`/`HISTORICAL_BACKFILL_CAPTURE_END` values.
  It is intentionally not bundled into this minimal fix.
- No commit, push, merge, or runtime data mutation is authorized by this plan.

## Gate

Implementation requires explicit user approval after review of this scope.

## Implementation and independent QA result (2026-11-16)

- Coder added the missing `return operator` in
  `airflow/dags/pipeline_operators.py` and a focused regression test.
- Focused test: `tests/test_airflow_parse_imports.py` — **24 passed**.
- Scoped `compileall` — **passed**.
- `git diff --check` — **passed** with existing LF/CRLF normalization warnings.
- Independent QA verdict: **FAIL / phase remains open**.

### QA findings

- The factory fix itself is correct, but the worktree contains substantial
  pre-existing changes in the same operator/test files, so the reviewer could
  not certify a clean isolated diff or claim that the full dependency graph is
  unchanged.
- Read-only Airflow verification progressed past the `None` failure, then found
  a second import-time error:
  `ValueError: Production Gold WRITE requires
  PRODUCTION_LAKEHOUSE_WRITE_APPROVED=1` from
  `create_gold_dimensional_operator()`.
- This second issue is outside the approved missing-return scope. It must not be
  bypassed by enabling production writes merely to make DagBag parse.

### Exact next step

Keep the missing-return change, leave the phase open, and prepare a separate
approved plan for the parse-time Gold WRITE configuration/guard issue. No task,
backfill, Spark job, MinIO/Nessie/PostgreSQL mutation, cleanup, reset, commit,
push, or merge has been performed for this phase.

## Approved follow-up scope: parse-time Gold guard (2026-11-16)

The user approved a follow-up fix after QA found that the incremental DAG still
failed DagBag import at `create_gold_dimensional_operator()` when the scheduler
was configured for production `WRITE` without the separate approval flag.

- Remove only the duplicate parse-time `PRODUCTION_LAKEHOUSE_WRITE_APPROVED`
  rejection from the Airflow operator factory.
- Preserve the authoritative runtime guard in
  `gold/implementation/dimensional/run_pipeline.py`; production WRITE must
  still fail closed inside the Spark task without approval.
- Add a regression test that the factory can be constructed without approval
  while the runtime CLI guard remains present.
- Do not set or forward production approval, do not run tasks/backfills, and do
  not touch Gold data or external services.

Follow-up acceptance criteria:

- `airflow dags list-import-errors` reports no import errors for the incremental
  DAG under the current scheduler environment.
- Existing production WRITE safety tests remain passing.
- Focused tests, compileall and diff check pass.

## Follow-up implementation QA (2026-11-16)

- Parse-time duplicate Gold WRITE rejection removed from the Airflow factory.
- Runtime production approval guard remains in
  `gold/implementation/dimensional/run_pipeline.py` and still fails closed.
- Focused tests: **48 passed** across
  `tests/test_airflow_parse_imports.py` and `tests/test_gold_orchestration.py`.
- Scoped `compileall`: **passed**.
- `git diff --check`: **passed**, with existing LF/CRLF normalization warnings.
- `docker compose exec -T airflow-scheduler airflow dags list-import-errors`:
  **No data found**.
- Independent QA verdict: **PASS WITH WARNINGS**. No Critical/High findings.

Warning: the worktree remains non-isolated and contains earlier task-graph
changes; this follow-up did not add new task-graph changes, but no commit,
push, merge, task execution, backfill, production approval, or runtime WRITE
was performed.

Next step: if delivery is desired, prepare an isolated reviewable diff and
request explicit approval before commit/push/merge. Otherwise the Airflow DAG
phase is complete and runtime WRITE remains separately guarded.

## Read-only DRY_RUN execution evidence (2026-11-16)

- An approved `airflow dags test ecommerce_incremental_ingestion` was started
  with `GOLD_DIMENSIONAL_MODE=DRY_RUN`; `.env` was not modified and no
  production approval was set.
- `validate_config` confirmed `mode=DRY_RUN` for all six source tables.
- The test runner then failed at `ingest_order_items` with an Airflow 3
  TaskInstance state/API error (`ServerResponseError`); dependent
  payments/shipments tasks remained `up_for_retry` and the DagRun ended
  `failed`.
- This was not a WRITE failure and no Gold/MinIO/Nessie/PostgreSQL mutation was
  authorized or observed. No retry, clear, checkpoint reset, or cleanup was
  performed.

Next step: diagnose the Airflow 3 `dags test` TaskInstance-state/concurrency
  behavior separately before attempting another runtime test. Do not retry this
  run automatically.

## Approved DRY_RUN contract follow-up (2026-11-16)

The failed DRY_RUN exposed two code-level contract mismatches to fix before a
new runtime test:

- Incremental child ingestion (`payments`, `shipments`) must accept a verified
  parent result in `DRY_RUN` even though no committed parent batch exists.
  `parent_orders_batch_id` must remain `None` and no write may occur.
- Gold dimensional artifact construction must map Silver
  `SKIPPED/DRY_RUN` to an explicit no-data artifact, just as it maps
  `SKIPPED/NO_DATA`, so the Gold writer can skip without creating clients or
  outputs.

Implementation scope is limited to the two contract branches and regression
tests. No WRITE behavior, task graph, watermark, checkpoint, or external data
service changes are included. A fresh DRY_RUN is required after QA; the failed
runs must not be retried or cleared.

## Approved DRY_RUN contract follow-up implementation (2026-11-16)

- Added `resolve_parent_orders_batch_id()` in
  `airflow/dags/pipeline_tasks/ingestion.py`. Child `payments`/`shipments`
  accept verified `SKIPPED/DRY_RUN` parent results and return no batch id;
  `COMMITTED` and `NO_DATA` handling remains unchanged, and only the WRITE
  path forwards a parent batch id.
- Updated `build_dimensional_parquet_artifact_task()` in
  `airflow/dags/pipeline_tasks/gold.py` to map `SKIPPED/DRY_RUN` to the
  explicit no-data artifact envelope without storage validation or client
  creation. Existing `SKIPPED/NO_DATA` mapping remains supported.
- Added `tests/test_incremental_dry_run_contract.py` and expanded the focused
  Gold artifact regression in `tests/test_gold_orchestration.py`.

Verification evidence:

- Focused contract tests: **4 passed**.
- Scoped `compileall` for the changed task modules and new regression test:
  **passed**.
- `git diff --check`: **passed**; existing worktree LF/CRLF warnings only.
- No Airflow runtime/backfill, failed-run retry/clear, external-service test,
  watermark/checkpoint operation, or data mutation was performed.

QA status: implementation is ready for independent reviewer handoff. A fresh
DRY_RUN remains required after QA and is not authorized in this session.

## DRY_RUN Gold preflight correction (2026-11-16)

The fresh DRY_RUN reached `gold_dimensional_write`, but its task log showed:
`ValueError: Malformed NO_DATA dimensional artifact: customers`. The artifact
producer intentionally emitted `status=NO_DATA, reason=DRY_RUN`, while the Gold
preflight accepted only `reason=NO_DATA`.

Approved correction was limited to accepting `reason=DRY_RUN` for the canonical
NO_DATA envelope. Malformed envelopes, unknown statuses, the NO_DATA path and
WRITE behavior remain fail-closed/unchanged. No retry or clear was performed on
the failed run.

Implementation and verification:

- Updated `airflow/dags/pipeline_operators.py` preflight validation.
- Added regression coverage in `tests/test_gold_orchestration.py`.
- Gold orchestration tests: **24 passed**.
- Scoped `compileall`: **passed**.
- `git diff --check`: **passed**, with existing LF/CRLF warnings.

Independent QA verdict: **PASS WITH WARNINGS**. No functional Critical/High
finding. Warning remains that the worktree is non-isolated and contains prior
changes outside this phase. A fresh DRY_RUN is the next verification step;
production WRITE, backfill, checkpoint mutation and external writes remain out
of scope.

## Fresh DRY_RUN runtime verification (2026-11-16)

- Command used `GOLD_DIMENSIONAL_MODE=DRY_RUN` with
  `airflow dags test ecommerce_incremental_ingestion ... --use-executor`.
- DagRun: `manual__2026-11-16T02:34:19.000204+00:00`.
- DagRun completed **successfully**.
- Read-only task-state verification: **37 success**, **21 skipped**, with no
  failed or `up_for_retry` task instances.
- No production approval was set; no WRITE/backfill/clear/retry of prior runs,
  checkpoint reset, Iceberg maintenance, or external data mutation was done.

Phase status: **complete for import and DRY_RUN execution**. Production WRITE
remains separately guarded and requires its own explicit approval/runtime gate.

## Production WRITE runtime attempt and result (2026-11-16)

The user explicitly approved a Production WRITE run. Before execution, QA
approved two safety corrections:

- Forward `PRODUCTION_LAKEHOUSE_WRITE_APPROVED` only when explicitly present;
  Compose defaults it to empty so the runtime remains fail-closed.
- Permit `+` in the validated runtime window ID because Airflow run IDs include
  UTC offsets such as `manual__...+00:00`.

The first trigger attempt was stopped before task execution because the
metadata trigger enum was written as lowercase `cli`; the exact DagRun row was
corrected to `CLI`, with no task mutation. That run then failed in Silver due
to the runtime-window validation issue and was not retried.

The subsequent approved run was:

- DagRun: `manual__2026-11-16T03:07:33.501195+00:00`
- State: **success**
- Task states: **42 success**, **16 skipped**, no failed or retrying tasks.
- Bronze WRITE committed/recovered batches for `products`, `orders`,
  `order_items`, `payments`, and `shipments`; `customers` returned `NO_DATA`.
- Silver produced verified Parquet/manifests for those five tables.
- `gold_dimensional_write` was **skipped** because the current run contained a
  `NO_DATA` artifact for `customers`; therefore no new Gold dimensional write
  or active publication occurred in this run.
- Approval was removed from scheduler/dag-processor immediately after the run.

No failed run was retried or cleared. The remaining product decision is whether
Gold should be allowed to reuse existing customer dimension state when one
incremental source has `NO_DATA`; that is a separate design change and was not
silently applied.
