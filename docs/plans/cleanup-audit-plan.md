# Cleanup Audit Plan

Date: 2026-11-11

## Scope

Remove ignored local artifacts that still describe the retired Warehouse/dbt
branch or generate references to the deleted `warehouse/` package. Preserve
runtime compatibility objects and active local landing data.

## Acceptance criteria

- Retired ignored docs, presentation assets, and stale helper scripts are gone.
- `source/database/migrations/003_add_iceberg_warehouse_lineage.sql`,
  `warehouse.loaded_batches`, and `staging/` are preserved.
- No tracked runtime file references the deleted aggregate Gold branch.
- Working tree and ignored-file inventory are rechecked after cleanup.

## Risks and decision

The legacy `loaded_batches` schema and migration remain because the current
lakehouse documentation explicitly reserves them for controlled backfill and
reconciliation. Removing them requires a separate database compatibility
decision and is outside this cleanup.

## Verification

- `git status --short`
- `rg` scan for retired Gold/dbt references
- Python compile check for tracked runtime sources

## Result

Completed. The ignored legacy docs, presentation bundle, and helper scripts
were removed. The compatibility migration, `warehouse.loaded_batches`, and
active `staging/` data were preserved. Runtime reference scan and Python
compile validation passed.

## Follow-up cleanup result

After the Silver Parquet → Gold Iceberg refactor, the following unsupported
main-path artifacts were removed:

- Silver Iceberg Airflow factories and the obsolete dependency helper.
- `lakehouse/customers_e2e.py` and `lakehouse/customers_manifest_e2e.py`, plus
  their retired E2E test cases.
- The unused Silver Iceberg cutover policy and configuration flags.
- The obsolete Gold artifact builder that consumed attestation/reconciliation/
  quality task payloads.
- The retired `silver/snapshots/` package, `silver/run_snapshot.py`, its
  snapshot-only tests, and the unused `silver_snapshot_paths` helper.

Legacy migration/attestation/quality modules remain only for explicit
compatibility/backfill coverage. Independent QA: **PASS**. Final evidence:
`337 passed, 76 deselected, 15 subtests passed`, compileall PASS, diff-check
PASS, and no stale supported-path references remain.

This follow-up cleanup retains the same compatibility boundary. The full
repository run is environment-limited in this workspace by Windows temp/Spark
permissions; the isolated regression suite remains the release evidence.

## Current-flow follow-up verification

- Removed the retired Silver snapshot package and snapshot-only tests.
- Removed `silver_snapshot_paths` and the obsolete snapshot test root from
  `pytest.ini`.
- Active-reference scan: **PASS** (`silver/run_snapshot.py`,
  `silver/snapshots/*`, `silver_snapshot_paths`: none).
- Focused regression: **44 passed, 1 skipped**.
- Python compileall: **PASS**.
- `git diff --check`: **PASS**.
- Independent QA verdict: **PASS**.

## Cleanup after continuous Production WRITE (2026-11-18)

- Removed only the two generated pytest temporary directories:
  `.pytest-tmp-candidate-qa/` and `.pytest-tmp-final-qa/`.
- Read-only Nessie inventory confirmed the active stable/candidate pair is
  `gold_prod_current` / `gold_prod_candidate_v2_cert_20261118_r04` at the
  same hash.
- Older candidate, backfill, and quality-it refs were retained for audit and
  rollback safety. No database, MinIO object, checkpoint, branch, or volume
  was deleted.
- Verification: no `pytest-tmp` entries remain in `git status`; runtime refs
  were inspected read-only.

## Whole-project audit (2026-11-18)

- `git diff --check`: **PASS**; Python `compileall`: **PASS**; Docker Compose
  config: **PASS**; Airflow DagBag import errors: **none**.
- Runtime DAG inventory contains only the incremental DAG and the dedicated
  historical backfill DAG. Incremental runs are succeeding; the historical
  DAG is unpaused but has `schedule=None`, so it does not self-schedule. Its
  prior manual backfill runs remain metadata/audit history.
- Effective production guards were inspected and match the approved mode:
  ingestion/Gold WRITE enabled, `NESSIE_REF=gold_prod_current`, and active BI
  publication disabled.
- Full pytest audit is **not green**: 111 tests pass before
  `tests/test_bounded_e2e_scope.py::test_scope_prefixes_silver_incremental_paths_without_changing_local_default`
  fails when the full suite runs. The test passes in isolation (9/9) and the
  relevant preceding subset passes (112/112), indicating order-dependent
  environment leakage around `BRONZE_BUCKET`; this remains open and was not
  changed during this read-only audit.
- Ignored local residue was inventoried (`.gatec2-*`, `.pytest_cache`,
  `staging/`, runtime logs, and `.env` backups). No runtime volume, database,
  MinIO object, Nessie ref, checkpoint, or source artifact was deleted.

## Test-isolation correction and independent QA (2026-11-18)

- Approved implementation changed only `tests/test_bounded_e2e_scope.py`: the
  test controls the leaked `BRONZE_BUCKET` value with `monkeypatch` and passes
  `environ={}` for the local-default assertion.
- Targeted verification: **19 passed** (`test_bounded_e2e_scope.py` and
  `test_storage_layout.py`); `git diff --check`: **PASS**.
- Full suite progressed beyond the original failure: **261 passed**, then
  stopped at the existing
  `test_gold_dimensional_recovery_contract.py::test_recovery_classifier_does_not_mutate_state`
  coverage-contract failure.
- Independent QA verdict: **PASS WITH WARNINGS** for the scoped test fix.
  Existing `common/paths.py` production storage-layout changes were excluded
  from this fix and require separate approval/review if delivered.
- Broader ignored-artifact cleanup remains pending until the full-suite
  failure is resolved or explicitly accepted; no runtime cleanup was done.

## Full-suite stabilization completion (2026-11-18)

- Corrected test-only environment/fixture isolation in:
  `tests/test_bounded_e2e_scope.py`,
  `tests/test_gold_dimensional_recovery_contract.py`,
  `tests/test_gold_orchestration.py`,
  `tests/test_quality_it_local_compose.py`, and
  `tests/test_silver_coverage_producer.py`.
- Final verification: **791 passed, 1 skipped, 1 warning**; targeted scoped
  tests: **102 passed**; `git diff --check`: **PASS**.
- Independent QA verdict: **PASS**. No production guard weakening or external
  runtime mutation was found in the scoped changes.
- Pytest directories generated during the audit were removed. Broader ignored
  residue (`staging/`, `.env.backup-*`, `.gatec2-*`, runtime logs) and runtime
  Nessie/MinIO/database state remain untouched pending a separately scoped
  cleanup approval.

## Approved local generated-artifact cleanup (2026-11-18)

- Removed the untracked generated QA directories `.gatec2-cp1-final-qa/`,
  `.gatec2-cp12-final/`, `.gatec2-cp12-unc-final/`.
- Removed `.pytest_cache/` and the generated `runtime_dry_run.stdout.log` /
  `runtime_dry_run.stderr.log` files.
- Verification: all six exact cleanup targets are absent; no tracked file,
  database, MinIO object, Nessie ref, checkpoint, volume, staging dataset, or
  environment backup was touched.

## Approved generated-artifact cleanup batch 2 (2026-11-18)

- Removed all workspace `__pycache__/` directories and the local
  `airflow/logs/` tree after explicit approval.
- Verification: `Remaining pycache=0` and `airflow_logs_exists=False`.
- Retained `staging/`, `.env.backup-*`, `infra/quality-it`, `.env`, and all
  external/runtime state. These remain separate cleanup candidates requiring
  their own confirmation.

## Approved legacy-source cleanup (2026-11-18)

- Removed `airflow/dags/pipeline_silver.py`; the active DAG path uses
  `airflow/dags/pipeline_tasks/silver.py` and `common.paths` instead.
- Removed `validation/validate_silver_optimized.py`; no active-code caller or
  DAG/test dependency remains.
- Verification: active-reference scan found no stale references, compileall
  passed, targeted import/storage tests passed (**60 passed**), and the full
  suite passed (**791 passed, 1 skipped, 1 warning**).
- Independent QA verdict: **PASS**. No database, MinIO, Nessie, checkpoint,
  volume, or runtime state was changed.
