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

## Follow-up read-only quality-it evidence

- Dedicated `ecommerce-quality-it` Nessie catalog was queried without writes.
- All seven Gold tables were present.
- `$files` reported `PARQUET` for every table, with one inspected file per
  table and readable record/byte counts.
- Table properties reported `format=iceberg/parquet` and
  `format-version=2`; current snapshot IDs and Nessie commit metadata were
  present.
- No Gold `WRITE`, cleanup, compaction, rollback, checkpoint reset or cutover
  was executed. Production/disposable certification remains pending.

## Bounded Gold WRITE preflight (2026-11-11)

- User-approved bounded scope: bucket `ecommerce-data-lake`, exact prefix
  `quality-it/run-20261111-gold-01/`.
- Scope validation passed before any MinIO, PostgreSQL or Spark client was
  created; the exact prefix was confirmed empty.
- The normal ingestion watermark state has no new batch that can produce the
  required Silver Parquet artifacts for this run. Therefore the Gold WRITE
  path was not invoked and no object, table, checkpoint, or catalog state was
  changed.
- A fixture generator or source/checkpoint mutation would be a separate
  scope decision and is not part of this bounded Gold WRITE approval.
- Status: **BLOCKED — awaiting an approved in-scope Silver input fixture**.

## Bounded Gold E2E execution evidence (2026-11-11)

- Added a write-gated fixture runner at
  `tests/run_bounded_gold_parquet_fixture.py`; it validates the exact scope
  before creating Spark/MinIO clients and refuses non-empty prefixes or
  overwrite.
- Fixture WRITE completed for all six Silver inputs under the exact prefix;
  completion manifests and Parquet inventory were read back successfully.
- Gold `DRY_RUN` completed successfully with all seven target tables and all
  quality checks `PASS`.
- The first Gold `WRITE` attempt made no target-table publication because the
  required run-scoped Nessie ref did not exist, but it left the immutable graph
  fence for the first graph identity.
- After explicit approval, the exact Nessie branch was created from `main`.
- A retry with a new date-window graph identity created the scoped Gold
  namespace and an empty `dim_date` table, then failed before publishing table
  manifests. Read-only verification found only that table, zero rows, no
  files/snapshots, and no completed graph manifest.
- Independent QA verdict: **FAIL/BLOCKED** for runtime certification. Static
  fixture/lineage changes are **PASS WITH WARNINGS**; partial-state
  reconciliation and recovery require a separate decision. No further WRITE,
  cleanup, rollback, delete, compaction, or retry is permitted in this scope
  until that decision is approved.
- Status: **BLOCKED — operator-led reconciliation required**.

## Partial-state reconciliation (2026-11-11)

- Read-only inventory of `quality-it/run-20261111-gold-01/` found 27 objects;
  the Silver fixture remains present and the Gold area contains only two
  immutable graph fence objects.
- No Gold table completion manifest or completed graph manifest exists.
- Read-only Nessie verification found branch
  `quality_it_run_20261111_gold_01`; its head advanced from the original
  `main` hash because the retry created scoped namespace/table metadata.
- The scoped Gold namespace contains only `dim_date`; it has zero rows and no
  files/snapshots. No complete Gold output is resumable.
- No delete, overwrite, rollback, checkpoint reset, cleanup, or further WRITE
  was performed.
- Recovery status: **RECONCILED — current run is intentionally quarantined**.
  A new immutable bounded run prefix is the safe retry path; reusing this
  partial prefix would require destructive/operator recovery that remains
  disallowed.

## Gold writer diagnosis and remediation (2026-11-11)

- Local ephemeral Iceberg reproduction executed the real frame builder,
  `create_target_table`, and `merge_table` for all seven tables; all 7/7
  operations passed. This rules out the common local SQL/schema path as the
  immediate cause, but does not certify Nessie/MinIO runtime behavior.
- Writer error reporting now includes the failing table, root exception type
  and message, plus the already-published table list while preserving
  exception chaining.
- Regression coverage passed in the Spark image: **38 passed** across writer
  and lineage tests; compileall and diff check passed.
- Independent QA: diagnostic code fix **PASS WITH WARNINGS**; bounded runtime
  certification remains **BLOCKED**. No retry, cleanup, rollback, or mutation
  was performed on quarantined run-01/run-02 after diagnosis.
- Next step requires approval for a fresh explicit run ID (for example
  `run-20261111-gold-03`) and a corresponding allowlist update before one
  gated runtime probe.

## Bounded certified run-04 (2026-11-11)

- Approved fresh scope `quality-it/run-20261111-gold-04/` was validated and
  kept isolated from quarantined run-01/run-02/run-03.
- Six Silver Parquet fixtures/manifests were created and read back; the
  run-scoped Nessie branch was created from `main`.
- Gold `DRY_RUN` passed all seven quality checks.
- Gold `WRITE` returned `PUBLISHED`; completion verification returned
  `VERIFIED` for all seven tables.
- Independent read-back found seven Gold tables. Each table had readable
  Parquet files and one `append` snapshot; row/file evidence was:
  `dim_date 1/1`, `dim_customer 2/2`, `dim_product 2/2`, and each fact `1/1`
  (rows/files), all `PARQUET`.
- MinIO inventory found 70 objects in the prefix, including 9 Gold objects,
  with zero objects outside the exact prefix. Nessie ref
  `quality_it_run_20261111_gold_04` had a new commit hash.
- Final independent QA: **PASS WITH WARNINGS**. This certifies only the
  synthetic bounded run-04 scope; production WRITE, consumer cutover,
  maintenance, retry/idempotency proof, and performance validation remain
  pending. Quarantined earlier runs remain untouched.

## Bounded retry run-02 evidence (2026-11-11)

- Approved new scope `quality-it/run-20261111-gold-02/` was validated before
  client creation.
- Six Silver Parquet fixtures/manifests were created successfully; the exact
  prefix was read back.
- Nessie branch `quality_it_run_20261111_gold_02` was created from `main`.
- Gold `DRY_RUN` passed with all seven quality results `PASS`.
- Gold `WRITE` failed before publishing any table manifest. Read-only
  inventory found only one graph fence in the Gold area and no Gold table
  output. No further WRITE was attempted.
- Independent QA/runtime status remains **BLOCKED**. The remaining action is
  code-level diagnosis of the first-write failure and a new reviewed fix;
  do not retry either partial run or perform cleanup/rollback.
