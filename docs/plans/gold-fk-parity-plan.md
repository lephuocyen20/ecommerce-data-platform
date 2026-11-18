# Gold payments/orders referential parity

## Status

Generator parity implementation QA **PASS**. Runtime attempt
`demo20261114b` **FAILED**; artifact-selection fix is approved and pending
implementation and independent QA.

## Evidence

Bounded run `demo20261114a` produced an orders Silver artifact covering
`order_id` 5001..6000 and a payments artifact covering 4020..4822; a read-only
Spark anti-join found **780** payment order IDs absent from the orders artifact.
Gold correctly failed closed with:

`DimensionalQualityError: fact_payments has an unresolved order_id foreign key`

## Approved scope

- Make generator stage `all` retain the exact order IDs created in that batch.
- Generate payments and shipments only for that exact order-ID set.
- Preserve existing PostgreSQL FK and Gold quality gates; do not map orphan
  facts to `UNKNOWN_KEY`.
- Add regression tests for exact child coverage and artifact-level FK parity.
- Update roadmap evidence after independent QA.

## Non-goals

- No modification or cleanup of `demo20261114a` objects or locks.
- No retry of the failed run.
- No source database write, MinIO write, Nessie mutation, or Gold rerun during
  implementation verification.

## Acceptance criteria

- `--stage all` child records use only the exact order IDs generated in the
  same invocation.
- Payments and shipments cannot be generated from a broad `>= min_id` range.
- Existing `only_missing` behavior remains unchanged for standalone stages.
- Gold FK validation continues to reject orphan payments/shipments.
- Focused unit tests, compileall, and diff checks pass.

## Verification

```powershell
.\.venv\Scripts\python.exe -m pytest -q --basetemp .pytest-tmp tests/test_generator_all_stage.py tests/test_gold_dimensional_contracts.py tests/test_gold_dimensional_transformations.py tests/test_gold_dimensional_reader.py tests/test_gold_dimensional_writer.py
.\.venv\Scripts\python.exe -m compileall -q source/generator/v2 gold/implementation/dimensional tests
git diff --check
```

## QA gate

Independent reviewer must inspect the implementation diff against this plan.
Runtime certification requires a separate new run ID and approval after QA.

## Implementation evidence

- `generate_orders_with_items()` now returns the exact generated order IDs.
- `stage all` passes those IDs to payments and shipments using `= ANY(%s)`;
  it no longer uses a broad `>= min_id` selector for dependent records.
- Standalone `only_missing` and `order_id_min` compatibility remains in place.
- Focused verification: `6 passed` in
  `tests/test_generator_all_stage.py` and
  `tests/test_generator_exact_order_ids.py`.
- `compileall` and `git diff --check`: passed.
- Empty exact child selections are now treated as `NO_DATA` no-ops by
  `stage all`; standalone `insert_*` functions still reject empty records.
- The stage-all insertion helper now forwards `cursor` correctly to both
  non-empty child insertion functions.
- Focused regression suite after the reviewer finding and correction:
  `10 passed`, including empty and non-empty insertion paths.
- The full plan suite reached 31 passed and 5 environment failures in Spark
  tests because the Windows worker could not launch `python3`
  (`CreateProcess error=1920`); no implementation assertion failure was
  observed.
- No source database, MinIO, Nessie, Gold, or E2E runtime write was run.
- Independent reviewer verdict: **PASS**; no Critical/High/Medium findings
  remain.

## Next step

Implement the approved artifact-selection/parity-preflight fix. Do not retry
`demo20261114a` or `demo20261114b`; their artifacts and graph fences remain
preserved. A new immutable bounded run requires separate runtime approval
after implementation QA.

## Approved follow-up scope — artifact coverage parity

- Pin Gold to an explicit, same-runtime Silver manifest set instead of
  scanning the full table prefix independently.
- Preserve `batch_id`, manifest keys and coverage metadata in lineage.
- Add a fail-closed cross-table preflight for payments/shipments order IDs
  against orders before any Gold table merge.
- Keep the existing generator fix and `UNKNOWN_KEY` prohibition unchanged.

### Follow-up acceptance criteria

- Misaligned orders/payments fixtures fail with table, manifest/batch and
  orphan-count diagnostics before Gold writes.
- Aligned parent/child fixtures pass the preflight.
- Gold no longer silently combines artifacts with different coverage windows.
- Focused tests, compileall and `git diff --check` pass; no runtime writes
  occur during implementation or QA.

## Follow-up implementation evidence

- `verify_silver_manifest` now emits an explicit one-item `manifest_keys` set
  for the current runtime; Gold artifact selection consumes only that set and
  no longer performs an independent Silver prefix scan.
- Gold lineage preserves the selected manifest key, manifest identity,
  `batch_id`, data prefix and row count for the exact artifact.
- `validate_order_id_parity` runs after verified Silver reads and before any
  Gold target table create/merge. Failures are fail-closed and include a
  diagnostics table with child/parent tables, manifest keys, batch IDs and
  orphan counts.
- Regression fixtures cover stale/misaligned child coverage and aligned
  parent/child coverage in `tests/test_gold_artifact_parity.py`.
- Focused verification: `17 passed`; related Gold lineage/writer/contracts
  verification: `67 passed`. `compileall` and `git diff --check` passed.
- No generator, PostgreSQL, MinIO, Nessie, Iceberg, checkpoint, or runtime
  E2E state was modified.

## QA handoff

Implementation is ready for independent review against the approved
artifact-coverage acceptance criteria. Runtime certification and any new
bounded run remain out of scope and require separate approval.

## Reviewer correction evidence

- Added orchestration-path regression coverage in
  `tests/test_gold_dimensional_writer.py` for misaligned `payments` and
  `shipments` source order IDs through `run_dimensional_graph`.
- Each case asserts the parity error is raised before
  `create_target_table` or `merge_table`; both calls remain at zero.
- Focused verification: `21 passed` for the dimensional writer and artifact
  parity tests. `compileall` and `git diff --check` passed.
- No runtime write, generator change, or retry of an existing run was made.

## Correction QA handoff

Independent re-review verdict: **PASS WITH WARNINGS**, with no
Critical/High/Medium findings. The orchestration-path acceptance criterion is
verified. Remaining warnings are limited to the mocked-frame NULL-filter
limitation, Windows Spark worker configuration, and pre-existing mixed
working-tree changes. Runtime certification remains out of scope and requires
separate approval.

## Runtime attempt `demo20261114c`

- Bounded env was verified: bucket `ecommerce-data-lake`, prefix
  `quality-it/demo20261114c/`, `LAKEHOUSE_E2E_APPROVED=1`.
- The scheduler initially exited because the manually triggered run carried
  an invalid lowercase trigger enum; only that run metadata was corrected to
  the Airflow enum and the scheduler restarted.
- The DAG became `success`, but this run is **NOT CERTIFIED**: Gold
  dimensional write and active publish were both `skipped` because a no-data
  Silver table skipped its dimensional artifact task and the downstream
  writer trigger rule prevented execution.
- No Gold publish/cutover, lock cleanup, checkpoint reset or object cleanup
  occurred.

### Follow-up required

Plan a separate orchestration fix so NO_DATA Silver results produce explicit
artifact envelopes (or an equivalent safe trigger path) and do not silently
skip the Gold writer. Require a new implementation QA cycle and a new
immutable runtime run ID afterward.

## Runtime attempt `demo20261114d`

- Bounded env was verified with bucket `ecommerce-data-lake`, prefix
  `quality-it/demo20261114d/` and `LAKEHOUSE_E2E_APPROVED=1`.
- All six dimensional artifact tasks completed successfully through the new
  `silver_result`/`NO_DATA` orchestration path.
- Gold writer intentionally skipped because the container did not have
  `GOLD_DIMENSIONAL_ENABLED=true`; the compose default is `false`.
- This run is **NOT CERTIFIED**. No Gold publish/cutover occurred and no
  lock/checkpoint reset or object cleanup occurred. Do not retry this run.

### Runtime follow-up

A new immutable run must explicitly set `GOLD_DIMENSIONAL_ENABLED=true` and
retain `GOLD_DIMENSIONAL_MODE=WRITE` before Gold runtime certification.

## Runtime attempt `demo20261114e`

- Bounded env was verified with Gold enabled and `GOLD_DIMENSIONAL_MODE=WRITE`.
- All six dimensional artifact tasks completed successfully.
- Gold preflight intentionally raised `AirflowSkipException` because the
  `customers` artifact was explicit `NO_DATA`; the task log recorded the
  reason before Spark/lakehouse execution.
- This run is **NOT CERTIFIED**. No Gold publish/cutover or destructive
  operation occurred. Do not retry this run.

### Runtime follow-up

To exercise the full Gold `PASS` path, a separate approved bounded source
PostgreSQL delta is required so all required Silver tables produce data, then
a new immutable E2E run ID must be used.

## Approved orchestration follow-up

- Change the dimensional artifact dependency to consume `silver_result_*`,
  which completes for both `PASS` and `SKIPPED/NO_DATA`.
- Emit an explicit `NO_DATA` artifact envelope without fake `batch_id`,
  manifest key, Parquet path or historical fallback.
- Let the Gold writer detect `NO_DATA` and raise `AirflowSkipException` before
  Spark, MinIO, Iceberg, Nessie or active-publish work.
- Keep the strict lineage validator and full `PASS` path unchanged.
- Add graph, envelope, malformed-input and writer preflight regression tests.

### Orchestration acceptance criteria

- Every dimensional artifact task is downstream of its `silver_result_*` task.
- `NO_DATA` is a successful explicit artifact result, not an Airflow-upstream
  skip, and contains no fabricated source lineage.
- Gold writer skips intentionally and observably before any Gold write when
  any required artifact is `NO_DATA`.
- Full `PASS` artifacts still follow the existing dimensional write path.
- No runtime write or retry of `demo20261114c` occurs during implementation or
  QA.

## Orchestration implementation and QA evidence

- Artifact tasks now depend directly on `silver_result_*` and emit explicit
  `NO_DATA` envelopes without fabricated source lineage.
- Gold preflight validates every envelope before deciding to skip; malformed
  or unknown artifacts cannot be hidden behind an earlier valid `NO_DATA`.
- Direct operator regression confirms preflight runs before
  `super().execute()` and no lakehouse execution occurs on `NO_DATA`.
- Focused implementation tests: **56 passed**; correction/review suite:
  **37 passed**. `compileall` and `git diff --check` passed.
- Independent re-review verdict: **PASS WITH WARNINGS**, with no
  Critical/High/Medium findings. Warnings: local environment lacks Airflow 3
  `airflow.sdk` for an independent DAG import, and no separate direct PASS
  execute test. Runtime certification remains pending a new approved run.

## Orchestration implementation evidence

- Changed the dimensional artifact dependency from `verify_silver_*` to
  `silver_result_*`; the artifact task therefore runs for both PASS and
  SKIPPED/NO_DATA Silver outcomes.
- `build_dimensional_parquet_artifact_task` now emits exactly
  `{"table_name": <table>, "status": "NO_DATA", "reason": "NO_DATA"}`
  for the approved Silver no-data envelope. It does not create a MinIO client
  or add batch, manifest, path, or historical fallback fields. Unexpected and
  malformed statuses remain fail-closed.
- `GoldDimensionalDockerOperator` preflights artifact envelopes and raises
  `AirflowSkipException` for a valid NO_DATA artifact before lineage resolution
  or Docker/Spark execution. The preflight validates every envelope before
  raising, so malformed or unknown artifacts after a valid NO_DATA fail closed.
  Malformed NO_DATA envelopes raise instead of silently skipping. The strict
  lineage validator and PASS path are unchanged.
- Regression coverage was added for graph wiring, NO_DATA envelope shape,
  malformed statuses, writer preflight behavior, and direct operator execution
  ordering in `tests/test_gold_orchestration.py`.

## Orchestration verification evidence

- Focused pytest: `56 passed` across orchestration, Airflow parse, dimensional
  writer, artifact parity, and publish-XCom tests.
- Follow-up focused pytest: `31 passed` for orchestration and Airflow parse,
  including valid NO_DATA followed by an unknown artifact and direct
  `GoldDimensionalDockerOperator.execute()` ordering.
- `compileall` passed for the changed Airflow task/operator modules and tests.
- `git diff --check` passed.
- No runtime E2E, `demo20261114c` retry, MinIO/PostgreSQL/Nessie/checkpoint
  access, generator change, or external-service write was performed.

## Orchestration QA handoff

Implementation is ready for independent reviewer inspection against the
orchestration acceptance criteria. Runtime certification remains out of scope
and requires a separate immutable run ID plus explicit runtime approval.

## Runtime attempt `demo20261114f`

- A bounded source PostgreSQL delta generated 10 customers, 10 products, 100
  orders, 124 payments and 92 shipments.
- The approved run used `quality-it/demo20261114f/`, Gold enabled and WRITE;
  all six dimensional artifact tasks succeeded and Gold executed.
- Gold fail-closed parity reported **796** orphan payment order IDs and
  **564** orphan shipment order IDs. Orders, payments and shipments were
  emitted under independent batch IDs with inconsistent coverage, so the
  source delta alone did not create a common parent/child window.
- Retry hit the immutable graph fence as designed; the DAG ended `failed`.
  No active publish/cutover, lock deletion/reset or checkpoint reset occurred.

### Follow-up required

Plan a cross-table Silver coverage fix so the Gold source set is assembled
from a consistent parent/child runtime window, or fail earlier with a
deterministic coverage error before acquiring the Gold graph fence.

## Runtime attempt `demo20261114g`

- Approved Gold-enabled run with bounded source delta reached coverage
  preflight. Producer metadata included the runtime window and parent batch,
  but child `parent_orders_reference` lacked the current orders
  `manifest_identity`.
- Gold correctly failed before acquiring the fence with
  `parent_orders_reference_mismatch`; no Gold target write or publish occurred.
- Retry was not performed and no lock/checkpoint/object cleanup occurred.

### Correction scope

Update the Silver child producer to resolve and canonical-hash the current
orders completion manifest, include that identity in
`parent_orders_reference`, and validate parent table/batch/runtime coverage
before child Silver write. Keep Gold validation strict.

## Parent manifest identity correction QA

- Child Silver producers now resolve the current `orders` `COMPLETED`
  manifest, validate table/batch/runtime/coverage, canonical-hash it with the
  same JSON rules as Gold, and emit
  `parent_orders_reference.manifest_identity` before child write.
- Gold strict validation is unchanged; missing or tampered identity still
  fails closed before the fence.
- Focused verification: **17 passed**, `compileall` PASS and
  `git diff --check` PASS. No runtime/external-service write occurred.
- Independent QA verdict: **PASS WITH WARNINGS**, no Critical/High/Medium
  findings. Warning: no direct mocked `transform_batch()` publication-path
  test. Runtime certification requires a new approved immutable run.

## Coverage QA finding

Independent QA **FAILED** the first coverage implementation: no actual Silver
producer emitted `runtime_window_id`/parent coverage metadata, so runtime
artifacts could still silently use the legacy-compatible path. Required
correction: add producer metadata, narrow legacy compatibility to explicitly
identified legacy envelopes, standardize malformed diagnostics, and test
orders/payments/shipments propagation plus missing-source behavior. Runtime
remains closed until correction QA passes.

## Approved cross-table coverage scope

- Add a pure cross-table coverage contract with runtime-window and parent
  coverage identity metadata.
- Validate the complete orders/payments/shipments artifact set before any Gold
  global or graph fence acquisition; missing/mismatched metadata must fail
  closed with batch, manifest and coverage diagnostics.
- Preserve the existing data-level orphan parity check as defense in depth.
- Where existing ingestion contracts support it safely, assemble child Silver
  artifacts against the parent orders coverage; do not add historical fallback,
  `UNKNOWN_KEY`, schema migration or runtime mutation without a separately
  approved plan.
- Add pure contract, orchestration and writer tests proving no fence/no Gold
  write on coverage mismatch and deterministic success for aligned fixtures.

## Cross-table coverage implementation evidence

- Added the pure `gold.implementation.dimensional.coverage` contract. It
  validates `runtime_window_id`, `coverage_identity`, child
  `parent_coverage_identity` and optional immutable `parent_orders_reference`,
  with batch/manifest/window diagnostics.
- Coverage metadata is passed through the existing Silver completion manifest,
  Airflow `silver_result`, Parquet artifact and dimensional lineage fields.
  No table/schema migration, historical fallback or `UNKNOWN_KEY` behavior was
  added.
- `run_dimensional_graph` now validates cross-table coverage before reading
  completion state or acquiring either Gold fence. Existing order-ID orphan
  parity remains after verified source reads as defense in depth.
- Legacy PASS compatibility is preserved when none of the three cross-table
  sources carries coverage metadata; partial metadata is fail-closed.
- Added pure aligned/missing/mismatched/legacy coverage tests and a writer
  regression proving no fence and no Gold target write on coverage mismatch.
- Focused verification: `52 passed` across coverage, artifact parity, writer,
  Parquet source and orchestration tests. `compileall` and `git diff --check`
  passed.
- No PostgreSQL, MinIO, Nessie, Iceberg, checkpoint, source-generation or
  runtime E2E write was performed; `demo20261114f` was not retried.

## Cross-table coverage QA handoff

Implementation is ready for independent reviewer inspection against the
approved coverage acceptance criteria. Reviewer must inspect the shared-file
diffs for pre-existing worktree overlap and verify the pre-fence ordering.
Runtime certification remains out of scope and requires a separate approved
immutable run ID.

## Coverage QA correction implementation evidence

- Actual Silver producer path now receives the current Airflow `dag_run.run_id`
  as `runtime_window_id`; orders creates its coverage identity and payments/
  shipments receive the current orders batch through an explicit DAG dependency.
- Silver manifests now emit `coverage_identity`, child
  `parent_coverage_identity`, and `parent_orders_reference`; manifest reuse
  verifies the same current-runtime coverage before accepting the artifact.
- Silver verification rejects missing or stale runtime coverage before passing
  the artifact downstream. No schema migration, historical fallback,
  `UNKNOWN_KEY`, runtime write, or retry was added.
- Legacy compatibility now requires the explicit
  `legacy_coverage_compatibility=true` lineage marker. Unmarked artifacts fail
  closed.
- Coverage diagnostics are standardized for malformed metadata and missing
  sources with table, batch, manifest and coverage fields.
- Focused verification: `59 passed`; `compileall` passed; `git diff --check`
  passed. No external-service or bounded E2E test was executed.

## Correction QA handoff

Implementation is ready for independent reviewer inspection. Reviewer must
verify producer-to-manifest propagation for orders/payments/shipments,
explicit legacy marker enforcement, malformed/missing diagnostics, and that
coverage mismatch remains before any Gold fence or write. Runtime certification
remains out of scope and `demo20261114f` was not retried.

## Coverage correction QA result

- Silver producer now receives `dag_run.run_id` as `runtime_window_id` and
  emits coverage metadata for orders/payments/shipments, including child
  `parent_orders_reference`.
- Unmarked legacy artifacts fail closed; malformed/missing coverage diagnostics
  include table, batch, manifest and coverage fields. Validation remains before
  fence acquisition and Gold writes.
- Focused verification: **70 passed**, `compileall` PASS and
  `git diff --check` PASS. No runtime/external-service write occurred.
- Independent QA verdict: **PASS WITH WARNINGS**, with no
  Critical/High/Medium findings. Warning: no direct mocked `transform_batch()`
  integration test for final manifest emission. Runtime certification remains
  pending a new approved run.
## Runtime correction `demo20261114g` implementation evidence (2026-11-14)

- Approved scope implemented in `silver/incremental.py`: child payments/shipments
  resolve the deterministic current orders completion manifest and fail closed
  unless it is `COMPLETED`, identifies `orders` and the requested batch, and
  carries the exact current runtime coverage identity.
- Child `parent_orders_reference` now includes the canonical SHA-256 identity
  of the complete orders manifest using Gold's exact JSON canonicalization.
  `build_coverage_metadata` requires this validated parent manifest; no
  historical fallback, `UNKNOWN_KEY`, schema migration, runtime write, retry,
  or Gold validator relaxation was added.
- Regression tests cover correct producer identity propagation, invalid parent
  status/table/batch/runtime coverage, missing/wrong Gold parent identity, and
  aligned Gold coverage pass.
- Verification: `.venv\Scripts\python.exe -m pytest -q
  tests/test_silver_coverage_producer.py tests/test_gold_coverage.py` -> **17
  passed**; `compileall` -> **PASS**; `git diff --check` -> **PASS**. No
  external-service test or runtime write was executed.

### QA reviewer handoff

Reviewer must inspect the implementation diff against this correction scope,
especially canonical identity parity with `gold.implementation.dimensional.reader`,
pre-Spark child validation, and preservation of the strict Gold validator.
Runtime certification remains pending a separately approved immutable run; do
not retry `demo20261114g`.

## Runtime attempt `demo20261114h`

- The approved run used a bounded source delta and Gold WRITE, but child
  Silver transforms failed before artifact creation with
  `ValueError: Current orders completion manifest is required`.
- Orders completed after the child task timestamps. The DAG passed parent
  batch context but did not make child transforms depend on
  `verify_silver_orders`, so child producers could not resolve the current
  parent manifest.
- The DAG ended `failed`; no Gold write/publish, fence cleanup or checkpoint
  reset occurred. Do not retry this run.

### Follow-up required

Add a true parent-first dependency from orders Silver completion to child
payments/shipments Silver transforms while retaining fail-closed producer
validation.

## Parent-first DAG correction QA

- Payments and shipments Silver transforms now depend on
  `verify_silver_orders`, not only `validate_orders`; eligibility, child
  validation and producer fail-closed checks remain intact.
- Focused verification: **33 passed**, `compileall` PASS and
  `git diff --check` PASS. No runtime write occurred during implementation or
  QA.
- Independent QA verdict: **PASS WITH WARNINGS**, no Critical/High findings.
  Warning: local interpreter lacks Airflow 3 `airflow.sdk` for direct DAG
  instantiation; source-level regression tests pass.
- Runtime certification requires a new immutable run ID. Do not retry
  `demo20261114h`.

## Runtime attempt `demo20261114i`

- Parent-first ordering worked for orders/payments/shipments, but the Silver
  producer treated every non-orders table as an orders child.
- `customers`, `products` and `order_items` therefore failed with
  `Current orders completion manifest is required`, before Gold artifacts and
  Gold write. No publish/cutover or cleanup occurred.
- Required correction: parent-orders coverage is only for `payments` and
  `shipments`; dimensions and `order_items` retain independent runtime
  coverage metadata.

## Narrow correction QA after `demo20261114i`

- Implemented the approved restriction: parent-orders coverage and manifest
  resolution apply only to `payments` and `shipments`. `customers`, `products`
  and `order_items` use independent runtime coverage without an orders parent
  batch or manifest client.
- Focused verification: **40 passed**, `compileall` **PASS**, and
  `git diff --check` **PASS**. No runtime write, cleanup, deletion or retry of
  `demo20261114i` occurred.
- Independent QA verdict: **PASS WITH WARNINGS**, no Critical/High findings.
  Medium residual risk: Silver verification does not independently compare
  child parent coverage/manifest identity with the verified orders manifest;
  Gold remains the fail-closed boundary. Low risk: DAG evidence is
  source-level because local Airflow 3 `airflow.sdk` is unavailable.
- Runtime certification remains pending. Next step is to decide whether to
  harden Silver-side parent identity verification before requesting a new
  bounded immutable runtime run.

## Silver parent-identity hardening QA

- `verify_silver_manifest` now validates trusted orders XCom status/table,
  runtime window, parent coverage identity, parent batch, manifest key and
  canonical parent manifest identity before emitting child `PASS`.
- Independent tables remain free of orders XCom requirements. Gold validator,
  generator and runtime state were not changed.
- Independent QA verdict: **PASS WITH WARNINGS**, no Critical/High findings.
  Focused evidence: **42 passed**, compileall **PASS**, diff check **PASS**.
- Remaining warnings are test-environment/coverage limitations: no direct
  fake-XCom/MinIO behavioral test for every mismatch and no real DagBag parse
  because Airflow 3 `airflow.sdk` is unavailable locally. Runtime certification
  remains pending a separately approved immutable run.

## Runtime attempt `demo20261114j`

- Approved bounded source delta generated **10 customers, 10 products, 100
  orders, 296 order items, 131 payments and 92 shipments**. The Airflow run
  used exact scope `ecommerce-data-lake/quality-it/demo20261114j/` with Gold
  `WRITE` enabled.
- Airflow import initially exposed a missing project `PYTHONPATH`; Compose was
  corrected to expose `/opt/airflow/project:/opt/airflow/dags`, then the run was
  triggered successfully with `DagRunTriggeredByType.CLI`.
- All six Silver transforms, Silver verification tasks and dimensional Parquet
  artifact tasks succeeded, including the new parent identity verification.
- Gold failed closed on existing cross-table parity: **794** orphan payment
  order IDs and **780** orphan shipment order IDs relative to the current
  orders artifact. No Gold table write, active publish or confirmation marker
  succeeded. The configured one-minute retry also failed with the same parity
  diagnostics.
- No lock/object deletion, checkpoint reset, rollback, compaction, cutover or
  retry of an old run occurred. `demo20261114j` is not runtime-certified.
- Next step is a new approved investigation/fix for incremental child source
  selection and coverage parity; do not retry `demo20261114j`.

## Approved-plan proposal — exact child order scope

### Root cause

`orders`, `payments` and `shipments` are extracted independently using
table-local watermarks. The stage-all generator path can select child rows by
an order-ID range rather than the exact order IDs created in the same
invocation. This lets historical child order IDs enter the current artifact;
Gold parity then correctly rejects them before any Gold write.

### Proposed implementation scope (awaiting user approval)

- `source/generator/v2/orders.py`: expose the exact generated order-ID set.
- `source/generator/v2/runner.py`: propagate that set through stage-all.
- `source/generator/v2/payments.py` and `shipments.py`: use exact `ANY(%s)`
  selection for stage-all; preserve standalone selectors and empty NO_DATA.
- Add regression tests for exact selection, historical exclusion, empty child
  selection, and standalone compatibility.
- Do not change Gold parity/coverage gates, Silver transforms, old runtime
  artifacts, locks, checkpoints or source data cleanup.

### Acceptance and verification

- Stage-all child order IDs are a subset of the exact current orders set.
- No broad `order_id >= min_id` predicate remains on the stage-all child path.
- Focused generator/extractor/Silver/Gold tests pass; compileall and diff check
  pass; independent QA reviews the implementation diff.
- Only after QA and a separate approval, run a new immutable bounded E2E ID;
  never retry `demo20261114j`.

### Risks and rollback

- Existing historical child rows are not modified; only future generated
  deltas are corrected.
- Runtime source mounts/images must include the corrected generator code.
- Rollback is code-only; no object deletion, checkpoint reset, lock cleanup or
  database rollback is permitted.

## Exact child-order implementation and QA

- Implemented the approved generator fix across orders, runner, payments and
  shipments. Stage-all now propagates the exact generated order-ID set,
  selects children with parameterized `ANY(%s)`, and rejects child IDs outside
  that set before insert. Standalone `order_id_min`, `only_missing` and empty
  `NO_DATA` behavior remain supported.
- Changed tests cover exact selection, historical exclusion, empty child
  selection, subset guard and selector compatibility.
- Verification: `.venv` focused pytest **12 passed**, generator compileall
  **PASS**, and `git diff --check` **PASS**.
- Independent QA verdict: **PASS WITH WARNINGS**, no Critical/High/Medium
  findings. Warnings: no live PostgreSQL integration test in QA and the
  repository contains unrelated pre-existing worktree changes. No runtime
  write or retry of `demo20261114j` occurred.
- Next step requires separate approval for a new immutable bounded runtime
  run after ensuring the Airflow/Spark runtime mounts the corrected generator.

## Runtime attempt `demo20261114k`

- The approved generator produced **100 orders, 124 payments and 96
  shipments** for the new delta, and all Silver plus six Parquet artifact
  tasks succeeded under `quality-it/demo20261114k/`.
- The runtime still failed Gold parity with **807** orphan payment order IDs
  and **828** orphan shipment order IDs. This proves the generator fix is
  effective for newly generated rows but is not sufficient to constrain the
  table-local incremental extractor: historical child rows can still enter the
  current child artifact while the orders artifact contains only the current
  orders scope.
- Gold remained fail-closed; no publish/confirmation completed. No old run was
  retried and no lock, object, checkpoint or database cleanup was performed.
- The next fix must be planned at ingestion/Silver coverage selection (filter
  current child artifact by the verified orders key set or establish an
  equivalent cross-table extraction contract). Do not weaken Gold parity and
  do not retry `demo20261114k`.

## Proposed plan — parent-scoped child ingestion (awaiting approval)

### Root cause refinement

The generator now creates exact child rows, but table-local child watermarks
still allow historical payments/shipments into a new child Bronze batch. Silver
does not currently validate the child key set against the current orders
artifact; Gold is the first semantic anti-join and correctly fails.

### Scope

- Add optional parent `order_id` scope to normal and replay child extraction,
  using parameterized `order_id = ANY(%s)` together with the existing watermark.
- Pass the verified current orders scope to `payments`/`shipments` only, with a
  parent-first ingestion dependency. Preserve recovery of staged batches
  without re-querying source.
- Add Silver fail-closed child key-set validation before publishing a child
  manifest; do not silently filter bad rows. Bind parent manifest identity and
  deterministic key-set evidence.
- Keep Gold parity strict and leave all old runtime artifacts/checkpoints/locks
  untouched. Generator is not changed again.

### Acceptance

- New and replay child Bronze batches contain only IDs in the current orders
  scope; empty scope becomes `NO_DATA`.
- Recovery uses the existing staged payload and does not re-extract.
- Silver rejects out-of-scope child IDs before manifest publication.
- Focused extractor/orchestration/Silver/Gold tests, compileall and diff check
  pass; independent QA passes before any new runtime.

### Risks and rollback

- Large parent ID sets may make XCom unsuitable; this is recorded as a future
  scalability risk, not solved by weakening the contract.
- Historical late child rows require a separate reconciliation/backfill.
- Rollback is code-only; no source/database/object/checkpoint/lock cleanup is
  permitted.

## Parent-scoped ingestion implementation and QA

- Child extraction for `payments`/`shipments` now combines table-local
  watermark/replay predicates with parameterized exact parent `order_id` scope.
  Orders-first orchestration supplies the committed orders batch;
  `PENDING`/`UPLOADED` recovery reuses staged payloads without source re-query.
- Silver rejects child IDs outside the verified orders scope before Parquet or
  completion-manifest publication and validates the canonical parent orders
  data prefix before reading it. Gold parity remains unchanged and strict.
- Verification: focused suites **41 passed** plus transaction-flow **26
  passed**; compileall **PASS** and `git diff --check` **PASS**.
- Independent QA round 2: **PASS WITH WARNINGS**, no Critical/High/Medium
  findings. Warnings are limited to no full external integration test and the
  need to keep this correction isolated from pre-existing Gold/generator/config
  changes when committing. No runtime write or retry of `demo20261114j/k`.
- Next step: request approval for one new immutable bounded runtime run to
  verify Bronze/Silver child key subsets and Gold parity.

## Session W — Gold writer XCom contract correction

- Runtime `demo20261114n` proved the parent-scoped ingestion correction: ingest,
  validation, Silver verification, dimensional artifact creation and
  `gold_dimensional_write` all passed. The writer emitted a certified
  `GOLD_DIMENSIONAL_RESULT`, but `publish_gold_to_active` received `None`.
- Root cause is Airflow DockerOperator behavior: `do_xcom_push=True` with the
  default `xcom_all=False` returns only the last stdout line, while the
  canonical attestation is emitted earlier in Spark output.
- Approved narrow fix: Gold writer uses `xcom_all=True`; the custom operator
  normalizes captured output to the canonical structured result before XCom
  persistence. Publish authorization, CAS promotion and bounded lakehouse
  gates are unchanged.
- Verification: focused XCom/orchestration/DAG contract tests **42 passed**;
  compileall **PASS**; Airflow container `dags list-import-errors` returned
  `No data found`.
- Independent QA round 1: **FAIL for scope provenance**, not for the XCom
  logic. The worktree contains pre-existing changes from earlier approved
  ingestion/Gold sessions, so the reviewer could not certify the entire dirty
  tree as this hotfix. The XCom path itself passed its acceptance checks.
- Required next step: re-review only the approved XCom hunks, preserving all
  pre-existing user changes; then rerun a new bounded runtime only after QA
  passes. No cleanup, rollback, checkpoint reset or commit was performed.

## Scoped QA verdict — Gold writer XCom correction

- Independent QA re-review of only the approved XCom hunks: **PASS**.
- Evidence: `42 passed`, compileall **PASS**, `git diff --check` **PASS**;
  canonical output was verified when Spark shutdown logging followed it.
- No Critical/High/Medium/Low findings. Fail-closed writer evidence and active
  publish authorization remain unchanged.
- Next step requires user approval: run a fresh bounded WRITE runtime with a
  new immutable `quality-it/<run-id>/` prefix to verify XCom delivery and CAS
  publication end-to-end. No commit, push or merge is authorized yet.

## Runtime attempt `demo20261114o` and Airflow 3 XComArg correction

- The fresh bounded run used branch `quality_it_demo20261114o` and prefix
  `quality-it/demo20261114o/`. Ingest, validation, Silver, dimensional Gold
  write, certification and readback all passed. The writer XCom row contained
  a structured `dict`, but `publish_gold_to_active` still received `None`.
- Runtime DAG inspection showed `publish_gold_to_active` had
  `op_args=(GoldDimensionalDockerOperator object,)`, not an `XComArg`; passing
  a classic operator directly to an Airflow 3 TaskFlow task does not resolve
  its return value. The run failed closed before stable-branch promotion.
- Approved correction: pass `dimensional_writer.output` to the publish task,
  while retaining the explicit `dimensional_writer >> active_gold_publish`
  dependency and all publication gates.
- Focused verification: **43 passed** across the relevant suites,
  compileall **PASS**, `git diff --check` **PASS**. Independent QA:
  **PASS WITH WARNINGS**, no code findings; warning is only that the local
  `.venv` lacks `airflow.sdk`, so runtime parse must be checked in the
  Airflow container.
- No retry of `demo20261114o`, no cleanup, rollback, checkpoint reset or
  deletion was performed. Next step requires approval for a new immutable
  bounded WRITE run after the XComArg correction.

## Runtime attempt `demo20261114p` — XComArg verified

- Airflow container parsed the publish task as `PlainXComArg` targeting
  `gold_dimensional_write.return_value`; this confirms the wiring correction
  in the real runtime, not only source tests.
- Ingest, validation, Silver verification, Gold write, certification and
  readback all passed for the bounded prefix
  `quality-it/demo20261114p/` on branch `quality_it_demo20261114p`.
- `publish_gold_to_active` received the structured writer result and advanced
  to the next fail-closed gate. It stopped because
  `BI_ACTIVE_PUBLISH_APPLY=1` is not enabled. No stable-branch promotion or
  cutover was performed.
- This is expected authorization behavior, not an XCom or data-quality
  failure. No retry, cleanup, rollback, checkpoint reset or deletion was
  performed.
- Next step requires a separate explicit approval to enable active Gold
  promotion and provide/verify `BI_GOLD_CURRENT_EXPECTED_HASH`; do not infer
  that approval from bounded Gold WRITE approval.

## Runtime certification — `demo20261114p` active promotion complete

- After explicit approval, `BI_ACTIVE_PUBLISH_APPLY=1` was enabled with CAS
  expected hash `2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d`.
- The Nessie adapter was corrected to unwrap the v2 `reference` response
  envelope; scoped QA was **PASS WITH WARNINGS** with no required code fix.
- Only `publish_gold_to_active` and the seven confirmation markers were
  rerun; `gold_dimensional_write` was not rerun. All eight tasks succeeded.
- Read-only Nessie verification confirms `bi_gold_current` and
  `quality_it_demo20261114p` both point to
  `9106977b619f7f740457683f0ce75f9567583f76e03a26088c2996e20f19d362`.
- Runtime certification is **COMPLETE** for the bounded run: Gold write,
  seven-table readback, CAS active promotion and confirmations all passed.
- No commit, push or merge was performed. Remaining delivery step is user
  approval before any Git commit/push/merge.

## Runtime attempt `demo20261114l`

- The bounded run used the corrected generator and parent-scoped ingestion
  configuration. Orders and independent tables passed, but
  `ingest_payments` and `ingest_shipments` failed before child Silver because
  PostgreSQL received a nested tuple for the parent scope and rejected
  `ANY((...))` with `WrongObjectType: op ANY/ALL (array) requires array on
  right side`.
- No child artifact or Gold write was produced; no retry, cleanup, lock/object
  deletion or checkpoint reset occurred.
- Required narrow correction: bind the parent order IDs as a PostgreSQL array
  compatible with `order_id = ANY(%s)`, add a live-style parameter adaptation
  regression test, then repeat QA. Do not retry `demo20261114l`.
