# Gold Runtime Orchestration Boundary — Session Plan

Date: 2026-11-11
Status: IMPLEMENTED — QA PASS WITH WARNINGS

## Objective

Review and safely close the existing dirty runtime changes that consolidate
Gold dimensional orchestration around one atomic `gold_dimensional_write` task.
This is a runtime/data-path change, not a UI-only change.

## Proposed runtime scope

### DAG and orchestration

- `airflow/dags/ecommerce_incremental_ingestion.py`
- `airflow/dags/pipeline_operators.py`
- `airflow/dags/pipeline_tasks/gold.py`
- `airflow/dags/pipeline_tasks/silver.py`

### Gold runner and tests

- `gold/implementation/dimensional/run_pipeline.py`
- `tests/test_airflow_parse_imports.py`
- `tests/test_gold_orchestration.py`

### Documentation reconciliation

- `docs/plans/silver-parquet-gold-iceberg-plan.md`
- `docs/GOLD_LAKEHOUSE_PLAN.md`
- `docs/plans/airflow-gold-ui-layout-plan.md`

The untracked UI plan must be corrected or explicitly treated as a historical
note; it must not continue to claim that the current runtime changes are
UI-only.

## Runtime contract to verify

- Six verified Silver Parquet artifacts are required before Gold starts.
- The single Gold writer receives six validated artifact lineage payloads.
- Artifact input parsing is fail-closed for missing, duplicate, malformed or
  out-of-scope inputs.
- Legacy `--source-lineage` compatibility remains intentional and tested.
- `DRY_RUN` does not publish Gold output.
- `WRITE` reports success only after completed-graph verification.
- Bounded `ecommerce-data-lake` safety guards remain intact before creating
  Spark/MinIO clients.
- Seven confirmation tasks remain non-writing markers.
- No cutover, checkpoint reset, cleanup, rollback, compaction or external write
  is introduced by this session.

## Acceptance criteria

- Airflow DagBag/import has no errors.
- Task graph has six artifact upstreams, one Gold writer and seven downstream
  confirmation markers.
- Operator, XCom, CLI and lineage contracts have focused tests for valid and
  invalid inputs.
- Partial/retry behavior is documented and consistent with the existing
  atomic writer/recovery model.
- Full scoped compile/test/diff verification passes.
- Independent QA reviews the complete runtime diff.
- Documentation matches the actual runtime topology and does not claim
  UI-only behavior.

## Risks requiring explicit review

- Post-write verification failure may cause a retry of the atomic writer.
- Operator has a hard-coded six-table list that must stay aligned with the
  canonical table registry.
- Duplicate artifact keys must not silently overwrite earlier inputs.
- Confirmation trigger rules must remain correct for disabled/failed Gold.
- Existing dirty changes have no clean commit boundary and must not be
  silently mixed with unrelated work.

## Verification strategy

- Focused pytest for orchestration, artifact parsing, CLI compatibility and
  invalid-input fail-closed behavior.
- Airflow container DagBag/import and graph inspection.
- `python -m compileall` for runtime packages and tests.
- `git diff --check`.
- No external Gold `WRITE` or cutover unless separately approved under the
  bounded E2E exception.

## Delivery boundary

Implementation and QA must complete before proposing a runtime commit. Commit,
push and merge require separate user approval. Do not use `git reset`, delete
objects, or modify shared services.

## Session implementation evidence

Implementation is approved and the runtime boundary is now implemented.
`pipeline_operators.py`
consumes canonical `pipeline_contracts.TABLES` for all six Silver inputs.
`run_pipeline.py` rejects duplicate, empty, malformed and non-object artifacts;
legacy `--source-lineage` remains supported but is validated before Spark/MinIO
client creation. `WRITE` succeeds only when graph verification returns
`status=VERIFIED`; `DRY_RUN` does not publish. No external write was run.

## QA remediation verification

- Added mocked regression coverage in `tests/test_gold_orchestration.py` for
  DRY_RUN verification bypass, WRITE verification ordering, malformed-lineage
  client non-creation, and verification failure propagation.
- `python -m compileall -q gold airflow/dags tests/test_gold_orchestration.py`:
  PASS.
- `git diff --check`: PASS.
- Focused verification: `62 passed`.
- Airflow container DagBag/import verification previously returned `No data
  found` for import errors, and graph inspection passed for the six artifact
  upstreams, one Gold writer, and seven downstream confirmation markers.

## QA verdict and residual risks

Independent QA: **PASS WITH WARNINGS**. The implementation and documentation
match the current direct-dependency topology. Production `WRITE`, consumer
cutover, and production/disposable runtime certification remain unverified
and require separate approval. Re-run the current Airflow container DagBag if
the runtime diff changes or fresh container evidence is required.

## QA handoff

QA is complete for the scoped implementation and documentation re-review.
Do not mark runtime certification or Gold WRITE cutover complete.
