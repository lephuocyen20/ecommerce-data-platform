# Gold Incremental Snapshot Reuse Plan

## Objective

Allow the incremental Gold pipeline to continue when one Silver source has
`NO_DATA`, while reusing only a verified, pinned existing Gold snapshot for the
unchanged dimension and rebuilding the affected Gold graph from new Silver
deltas.

Example: `customers=NO_DATA` with new `products`, `orders`, `order_items`,
`payments`, or `shipments` must not block a valid Gold update.

## Approved design direction

- Keep `NO_DATA` distinct from `PASS`; never fabricate a new source batch.
- Resolve each Gold input explicitly as one of:
  - `SILVER_DELTA`
  - `GOLD_SNAPSHOT_REUSE`
  - `REBUILD_FROM_DELTA_PLUS_REUSED_DIM`
- Reuse only a verified and pinned Gold snapshot/ref with identity and schema
  evidence.
- Preserve existing fact grain, dimension keys, SCD behavior, FK checks and
  measure additivity.
- Record reused snapshot identity, Silver manifests, target action and readback
  evidence in lineage/manifest metadata.
- Same input resolution must be idempotent; rerun must not duplicate facts.
- Fail closed for failed/stale snapshots, schema mismatch, missing manifests or
  unverifiable readback.
- Rollback is a feature/config gate only; no delete, checkpoint reset, snapshot
  rollback, compaction or cutover is included.

## Expected implementation scope

- `gold/implementation/dimensional/lineage.py`
- `gold/implementation/dimensional/reader.py`
- `gold/implementation/dimensional/writer.py`
- `gold/implementation/dimensional/manifest.py`
- `gold/implementation/run_metadata.py`
- `airflow/dags/pipeline_tasks/gold.py`
- `airflow/dags/ecommerce_incremental_ingestion.py` only if orchestration
  wiring must expose the new resolution contract.
- Focused Gold orchestration, lineage, writer, recovery and dimensional tests.
- Update `docs/GOLD_LAKEHOUSE_PLAN.md` only after independent QA.

## Acceptance criteria

- `customers=NO_DATA` with valid Silver deltas for other selected tables can
  produce and verify a Gold candidate.
- Existing customer/product rows are resolved from a pinned snapshot rather
  than converted to `UNKNOWN`.
- Fact grain, FK coverage, totals and idempotency checks pass.
- Lineage identifies both Silver deltas and reused Gold inputs.
- Invalid or stale reuse inputs fail before external write/client mutation.
- Unit/contract tests pass; runtime certification is separate and cannot be
  claimed from static evidence alone.

## Risks and verification

- Main risk is mixing a stale dimension snapshot with a newer fact delta.
- Verify snapshot/ref identity, schema compatibility, source coverage and
  readback before publication.
- Use a feature flag disabled by default until QA and a controlled DRY_RUN pass.
- No Production WRITE or Gold cutover during implementation without a separate
  explicit approval.

## Implementation evidence (2026-11-16)

- Added `gold/implementation/dimensional/snapshot_reuse.py` as a pure,
  fail-closed input-resolution boundary. It defines `SILVER_DELTA`,
  `GOLD_SNAPSHOT_REUSE` and `REBUILD_FROM_DELTA_PLUS_REUSED_DIM`, validates
  pinned Gold catalog/namespace/ref/table, snapshot/manifest/schema identities,
  and independent PASS readback evidence, and hashes the resolved contract for
  idempotency/audit use.
- Added `tests/test_gold_snapshot_reuse_contract.py` covering the default-off
  flag, explicit enabled resolution, dependent rebuild classification, invalid
  snapshot/readback/schema rejection, and deterministic contract identity.
- The existing reader/transformations do not yet expose a safe API to read a
  verified Gold dimension snapshot and combine it with Silver deltas. The
  implementation therefore stops at the contract boundary; it does not alter
  orchestration, create clients, read external state, merge facts, or enable
  cutover.
- Verification evidence: `.venv\Scripts\python.exe -m pytest
  tests/test_gold_snapshot_reuse_contract.py
  tests/test_gold_dimensional_lineage.py tests/test_gold_dimensional_writer.py -q`
  => **54 passed in 0.16s**; `.venv\Scripts\python.exe -m compileall -q
  gold/implementation/dimensional/snapshot_reuse.py
  tests/test_gold_snapshot_reuse_contract.py` => **exit 0**; `git diff
  --check` => **exit 0** (existing line-ending warnings only).
- Independent QA verdict: **BOUNDED CONTRACT PASS / RUNTIME NOT CERTIFIED**.
  Runtime reuse, Production WRITE, external-service mutation and cutover were
  not run. The next step requires a separately approved runtime adapter and
  independent QA before enabling the feature flag.

## Workflow gate

This is Phase 1 planning. Phase 2 coding starts only after explicit user
approval of this scope and acceptance criteria. After implementation, run
independent QA and update this plan with verdict, evidence and next step.

## Phase 2 bounded implementation (2026-11-16)

Implemented a contract-only boundary in
`gold/implementation/dimensional/snapshot_reuse.py` with focused tests in
`tests/test_gold_snapshot_reuse_contract.py`.

- Feature flag `GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED` defaults to disabled.
- Explicit input resolutions are represented:
  `SILVER_DELTA`, `GOLD_SNAPSHOT_REUSE`, and
  `REBUILD_FROM_DELTA_PLUS_REUSED_DIM`.
- Structural validation is fail-closed for pinned catalog/namespace/ref/table,
  snapshot/manifest/schema identities, readback status/counts and identity
  consistency.
- Contract identity is deterministic for audit/idempotency boundaries.
- No Spark, MinIO, Nessie or PostgreSQL client is created; no runtime merge,
  write, cutover, checkpoint or external mutation was performed.

Verification evidence:

- Focused Gold tests: **54 passed**.
- Scoped `compileall`: **passed**.
- `git diff --check`: **passed**, with existing LF/CRLF warnings.

## Independent QA verdict (2026-11-16)

**PASS WITH WARNINGS — BOUNDED CONTRACT ONLY**.

No Critical or High functional finding was reported. QA identified that the
current contract validates structure, not semantic freshness or actual
manifest/schema content. Runtime snapshot reading, delta merge, fact-grain/FK/
SCD verification and duplicate prevention remain **not implemented and not
certified**.

Before enabling the feature flag, the next phase must add a runtime adapter that
performs semantic snapshot/manifest/schema/readback checks, isolates the default
off test from ambient environment, adds stronger canonical-identity/idempotency
tests, and passes a controlled DRY_RUN plus independent QA. Production WRITE or
Gold cutover remains prohibited for this feature until those gates pass.

Phase status: **bounded contract complete; runtime snapshot reuse open**.

## Runtime adapter and merge-plan phase (2026-11-16)

- Added an injected `SnapshotReuseReader` runtime adapter in
  `gold/implementation/dimensional/snapshot_reuse.py`. It uses only the
  existing Spark Iceberg read seam (`format("iceberg")`, `option("snapshot-id")`,
  `load`) and requires an injected catalog resolver for pinned
  `catalog/namespace/ref/table` coordinates; no Nessie client API is assumed.
- The adapter is fail-closed on future/stale timestamps, resolver identity,
  table schema, row count, file count/bytes and content-addressed readback
  evidence. The default reader still raises `NotImplementedError` and creates
  no external client.
- Added pure `build_incremental_merge_plan()` resolution. It combines PASS
  Silver deltas with verified reused dimensions in a deterministic target plan,
  keeps the declared fact grain/merge keys and dimension inputs, and rejects
  missing/unverified reused dimensions. It does not join or write data and is
  not wired into the writer or Airflow DAG.
- Added fake Spark/catalog tests for pinned reads, schema/readback failures,
  future timestamps, missing reuse data, and no duplicate/fanout in the
  fact-order plan. The feature flag remains disabled by default.

Verification evidence:

- `.venv\\Scripts\\python.exe -m pytest -q
  tests/test_gold_snapshot_reuse_contract.py
  tests/test_gold_dimensional_reader.py tests/test_gold_dimensional_contracts.py`
  => **28 passed in 0.09s**.
- Scoped `compileall` => **passed**.
- `git diff --check` => **passed**; existing dirty-worktree LF/CRLF warnings
  only.
- No MinIO/Nessie/Postgres/Spark external runtime, write, DAG execution,
  checkpoint mutation, Iceberg maintenance, cutover or feature enablement was
  performed.

Bounded QA verdict: **PASS WITH WARNINGS — adapter/plan only; runtime merge not
certified**. The adapter is available only through injected seams and remains
unwired. A future phase still requires an explicitly approved writer
integration, controlled DRY_RUN and independent QA before enabling the flag.

## QA round 3 and phase handoff (2026-11-16)

The stale-clock and table-name validation findings were corrected, and the
Airflow `@task` boundary was removed so the contract remains pure Python until
a real Iceberg reader/merge adapter exists.

Independent QA round 3: **PASS WITH WARNINGS**.

- Focused tests: **40 passed**.
- `compileall`: **passed**.
- `git diff --check`: **passed**, with existing LF/CRLF warnings.
- No snapshot-reuse DAG wiring, external client creation, runtime execution or
  external write was performed.
- Feature flag remains disabled by default.

Residual LOW hardening note: future-dated `captured_at` is not yet rejected.
This does not block the current disabled bounded contract, but must be fixed
before any runtime adapter or feature enablement.

Handoff: **bounded contract phase complete**. The next phase requires a separate
approval for a real catalog-backed Iceberg snapshot reader and delta-merge
implementation, followed by controlled DRY_RUN and independent QA. Production
WRITE/cutover remains prohibited for this feature.

## Catalog-backed adapter phase (2026-11-16)

Implemented a bounded, injected runtime seam without enabling or wiring it into
the DAG/writer:

- `SnapshotReuseReader` pins catalog/namespace/ref/table/snapshot ID and reads
  the table and `.files` metadata through injected Spark/catalog seams.
- Schema, row count, file count/bytes, readback identity, stale and future
  timestamps are verified before a snapshot can be considered reusable.
- `build_incremental_merge_plan()` deterministically describes Silver deltas and
  reused dimensions while preserving declared fact inputs/merge keys; it does
  not join, write or mutate state.
- Missing runtime seams fail closed. The feature flag remains disabled.

Verification evidence:

- Focused adapter tests: **28 passed**.
- `compileall`: **passed**.
- `git diff --check`: **passed**, with existing LF/CRLF warnings.
- No Spark, MinIO, Nessie, PostgreSQL runtime or write was executed.

Independent QA verdict: **PASS WITH WARNINGS**.

Before feature enablement, the direct reader should require a fully validated
adapter result rather than allowing callers to bypass manifest/schema/graph
contract validation. Additional required gates are explicit snapshot/ref read
path assertions and runtime writer tests for deduplication, fan-out, FK/grain,
idempotency and readback. Controlled DRY_RUN and a new independent QA round are
still required. Production WRITE/cutover remains prohibited.

## Adapter hardening phase (2026-11-16)

Completed the remaining bounded-contract hardening without wiring runtime
execution:

- Reader accepts only a resolved reuse contract and revalidates the full
  manifest/schema/graph/readback evidence before reading.
- Fake catalog/Spark tests assert the pinned table identifier, `snapshot-id`
  option and `.files` metadata path.
- Pure merge-plan validation covers duplicate/fan-out, grain, FK and input
  mapping constraints, with deterministic idempotency identity.
- Naive datetimes are rejected/normalized through `SnapshotReuseContractError`.
- Feature flag remains disabled; no DAG/writer wiring or external runtime/write
  was performed.

Verification evidence:

- Focused hardening tests: **23 passed**.
- `compileall`: **passed**.
- `git diff --check`: **passed**, with existing LF/CRLF warnings.

Independent QA verdict: **PASS WITH WARNINGS — BOUNDED CONTRACT ONLY**. No
Critical/High/Medium findings. Remaining warnings are explicitly about missing
runtime writer integration and real Iceberg data verification, not this pure
contract boundary.

Phase status: **bounded adapter contract complete; runtime writer integration
open**. Next phase requires an explicit approval, controlled DRY_RUN, and a new
independent QA before any feature enablement or Production WRITE/cutover.

## Controlled catalog-backed DRY_RUN (2026-11-16)

With explicit user approval, a read-only Spark/Nessie verification was executed
against `gold_prod_current` using the injected `SnapshotReuseReader` seam.

- `dim_customer`: snapshot `2207301212624688080`, **6,226 rows**, 6 columns.
- `dim_product`: snapshot `6280965936243979810`, **2,681 rows**, 7 columns.
- Both table reads were pinned by Iceberg `snapshot-id`; the corresponding
  `.files` metadata was read and verified for row/file counts and identity.
- Catalog/ref/namespace/table coordinates were resolved as
  `lakehouse.gold.<table>` on Nessie ref `gold_prod_current`.
- No MERGE, table creation, manifest publication, ref update, MinIO write,
  PostgreSQL mutation or checkpoint operation occurred.

Runtime read-only result: **PASS** for the adapter seam. This does not certify
the full incremental Gold merge because the DAG is still not wired to construct
or consume snapshot-reuse plans in production.

Next gate: independent QA of this runtime evidence, then a separately approved
writer/DAG integration DRY_RUN before any feature enablement or Production
WRITE/cutover.

## Runtime snapshot reuse adapter boundary (2026-11-16)

- Added `GoldSnapshotReuseAdapter` and `SnapshotReuseReader` in
  `gold/implementation/dimensional/snapshot_reuse.py`.
- Reuse resolution now verifies semantic catalog/ref/namespace/table identity,
  content-addressed manifest/schema/readback identities, and a timezone-aware
  stale policy. Canonical hashing is key-order independent.
- `SnapshotReuseReader.read_verified_snapshot()` is an explicit fail-closed
  `NotImplementedError` seam because the current Gold reader/writer has no safe
  verified Gold-snapshot read plus delta-merge API. No unsupported Iceberg API
  was invented.
- QA round 2 removed the orchestration-visible
  `resolve_gold_snapshot_reuse_task` boundary from
  `airflow/dags/pipeline_tasks/gold.py`. Snapshot reuse remains callable only
  through the pure-Python resolver in
  `gold/implementation/dimensional/snapshot_reuse.py`; no DAG wiring, client
  creation, external read/write, or feature-flag enablement was added.
- Tests cover ambient-environment isolation for default-off behavior,
  reordered-key canonical identity, semantic evidence mismatch, stale snapshot
  rejection, and the fail-closed reader seam.

Verification evidence:

- Focused Gold suite: **86 passed**.
- Scoped `compileall`: **passed**.
- `git diff --check`: **passed**; only existing LF/CRLF warnings were emitted.
- No external-service runtime execution or writes were performed.

QA handoff: independent reviewer must inspect this diff against the approved
scope, verify the semantic evidence contract, and keep runtime reuse uncertified
until a separately approved implementation provides a real read/merge adapter
and controlled DRY_RUN evidence.

## QA round 2 remediation (2026-11-16)

- Removed the residual Airflow `@task` wrapper
  `resolve_gold_snapshot_reuse_task` from `airflow/dags/pipeline_tasks/gold.py`.
- Added a static import/parse regression guard proving that the resolver and
  task ID are absent from the orchestration module.
- Verification evidence: `.venv\\Scripts\\python.exe -m pytest
  tests/test_gold_snapshot_reuse_contract.py tests/test_airflow_parse_imports.py
  tests/test_gold_orchestration.py -q` => **65 passed**;
  `.venv\\Scripts\\python.exe -m compileall -q
  airflow/dags/pipeline_tasks/gold.py
  gold/implementation/dimensional/snapshot_reuse.py
  tests/test_airflow_parse_imports.py
  tests/test_gold_snapshot_reuse_contract.py` => **exit 0**;
  `git diff --check` => **exit 0** (existing line-ending warnings only).
- Runtime Iceberg read/merge, DAG triggering, external writes, and
  feature-flag enablement remain out of scope.
- Next step: independent QA review of this remediation diff; keep the phase
  open until that review passes.

## Approved QA hardening: snapshot reuse boundary (2026-11-16)

Implemented only in `gold/implementation/dimensional/snapshot_reuse.py` and
`tests/test_gold_snapshot_reuse_contract.py`:

- `SnapshotReuseReader` now accepts only a resolved adapter result and
  revalidates the complete manifest/schema/graph/readback contract before any
  Spark or catalog read.
- The reader pins the resolved table identifier, `snapshot-id` option and
  `.files` metadata path; fake Spark tests assert all three boundary values.
- Pure merge-graph validation rejects duplicate inputs/keys, fan-out input
  mappings, invalid grain/FK mappings and mismatched target/source mappings.
- Merge-plan identity is deterministic and excludes dataframe object identity,
  preserving rerun idempotency checks.
- Naive `datetime` values are rejected with `SnapshotReuseContractError`; aware
  values are normalized to UTC.
- Feature flag remains disabled; no DAG/writer wiring, external runtime, data
  service, checkpoint or Iceberg mutation was performed.

Verification evidence:

- `.venv\\Scripts\\python.exe -m pytest -q
  tests/test_gold_snapshot_reuse_contract.py` => **23 passed in 0.07s**.
- Scoped `compileall` => **exit 0**.
- Scoped `git diff --check` => **exit 0**.

QA status: implementation evidence is ready for independent reviewer
handoff. Runtime Spark/catalog certification and feature enablement remain
out of scope and require separate approval.

## Approved runtime writer integration (2026-11-16)

- The dimensional writer now accepts an explicit `merge_plan` boundary and
  revalidates its deterministic identity, complete source set, source kinds,
  reused snapshot IDs, target mappings, declared grain/FK graph and existing
  `TABLE_MERGE_KEYS` before frame construction.
- Reused dimensions are consumed directly from the verified plan DataFrame;
  the writer does not infer or create a snapshot reader. Missing plan data,
  tampered actions, unavailable seams or a disabled
  `GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED` flag fail closed before Gold
  target reads/merges. Existing transformations, quality gates, Iceberg MERGE
  keys, fences and pinned readback remain the write path.
- No DAG wiring, flag enablement, Spark/MinIO/Nessie/Postgres external runtime,
  checkpoint mutation, Iceberg maintenance or external write was performed.

Verification evidence:

- Focused tests: `.venv\\Scripts\\python.exe -m pytest -q
  tests/test_gold_snapshot_reuse_contract.py
  tests/test_gold_dimensional_writer.py` => **47 passed**.
- Scoped `compileall` => **exit 0**.
- `git diff --check` => **exit 0**; existing dirty-worktree line-ending
  warnings only.

Bounded QA verdict: **PASS WITH WARNINGS — writer boundary only**. Pure/fake
coverage includes plan consumption, default-off fail-closed behavior,
deterministic rerun identity, duplicate/fanout and action/merge-key contract
rejection, plus existing FK/grain/readback regression checks. Runtime snapshot
read/merge certification remains pending and the feature must stay disabled.

QA reviewer handoff: inspect the writer/plan diff against this approved scope;
verify that a future runtime caller supplies DataFrames only from the validated
`SnapshotReuseReader` seam, then require a separately approved controlled
`DRY_RUN` before any flag enablement or WRITE/cutover.

## Independent QA verdict: controlled catalog-backed adapter DRY_RUN (2026-11-16)

The approved read-only runtime check used Spark/Nessie against the existing
`gold_prod_current` ref and did not create or mutate any MinIO, Nessie,
PostgreSQL, Spark checkpoint, Iceberg table, or Gold publication.

Runtime evidence:

- `dim_customer`: pinned snapshot `2207301212624688080`, read back 6,226 rows
  and 6 columns; `.files` and schema/readback identity checks passed.
- `dim_product`: pinned snapshot `6280965936243979810`, read back 2,681 rows
  and 7 columns; `.files` and schema/readback identity checks passed.
- Corrected adapter harness result: both inputs returned `PASS` through the
  catalog-backed reader seam with a fixed aware UTC clock.

Independent reviewer verdict: **PASS WITH WARNINGS — adapter-only / controlled
catalog-backed DRY_RUN**. The reviewer confirmed that snapshot/ref/table
pinning and readback are evidenced, but no new tests or runtime writes were
performed by QA.

Boundary and unresolved risk:

- Full Silver-delta + reused-dimension Gold merge is **NOT CERTIFIED**.
- DAG/orchestration creation and propagation of a real `merge_plan` is **NOT
  CERTIFIED**.
- Runtime fact grain, FK, SCD, measure parity, replay idempotency and
  readback/reconciliation across all Gold outputs remain open.
- The feature flag remains disabled and Production WRITE/cutover is not
  approved by this verdict.

Required next gate: obtain separate approval for a controlled writer/DAG
`DRY_RUN` that builds a real merge plan from Silver deltas, consumes only
validated reused snapshots, proves grain/FK/SCD/totals/parity and idempotent
replay, then reads back and reconciles all Gold outputs before another
independent QA round.

## Approved implementation gate: controlled writer/DAG DRY_RUN

Approval received on 2026-11-16. Scope is limited to a non-mutating runtime
path that constructs a real merge plan from representative Silver deltas and
validated reused dimension snapshots, passes it through the existing writer
boundary, and verifies the resulting plan/graph contracts. The feature flag,
Production WRITE, Iceberg MERGE, Nessie ref update, checkpoint mutation and
Gold publication must remain disabled.

Acceptance criteria:

- DAG/orchestration creates and propagates a real `merge_plan` through the
  approved writer seam.
- Reused dimensions originate only from `SnapshotReuseReader` validation;
  Silver delta inputs are explicit and deterministic.
- DRY_RUN proves target/source mapping, grain, FK coverage, SCD behavior,
  totals/parity checks and deterministic replay identity for the representative
  graph.
- No external write or state mutation occurs.
- Focused tests and an independent QA review are completed before any next
  phase.

Risks: the DRY_RUN may expose missing orchestration seams or mismatches between
the real Silver artifact contracts and the writer merge graph. Any such issue
keeps this gate open; it does not authorize enabling the feature or falling
back to Production WRITE.

## Independent QA verdict: controlled writer/DAG DRY_RUN round 1 (2026-11-16)

Verdict: **FAIL**. Focused tests passed (`75 passed`), compileall passed and
`git diff --check` passed, but the implementation does not yet satisfy the
approved orchestration gate.

Blocking findings:

- **HIGH:** snapshot provenance accepts an arbitrary callable and does not
  enforce `SnapshotReuseReader`/`GoldSnapshotReuseAdapter` validation.
- **HIGH:** the helper is only invoked by tests; it is not wired into an actual
  Airflow task/DAG path that creates and propagates the merge plan.
- **MEDIUM:** no representative DataFrame writer DRY_RUN proves SCD behavior,
  totals/parity, FK/grain and replay identity together.

Required remediation: enforce the reader provenance boundary, add safe actual
Airflow task wiring plus import/task regression coverage, and add representative
injected DataFrame DRY_RUN tests through the writer. Keep the feature flag and
all external writes disabled, then repeat independent QA.

## Remediation implemented after QA round 1 (2026-11-16)

- `build_controlled_dry_run_merge_plan()` now rejects arbitrary callable
  readers and accepts only `SnapshotReuseReader` or
  `GoldSnapshotReuseAdapter`, ensuring reused snapshot provenance crosses the
  verified reader boundary.
- Added a disabled-by-default `controlled_incremental_snapshot_reuse_dry_run_task`
  and wired it into the actual incremental DAG dependency graph without
  creating clients or mutating state.
- Added provenance rejection, verified-reader acceptance and DAG wiring tests.
- Added a representative injected DataFrame writer DRY_RUN regression covering
  SCD, totals/parity, FK/grain and replay identity.

Verification evidence:

- `.venv\\Scripts\\python.exe -m pytest -q
  tests/test_gold_snapshot_reuse_contract.py tests/test_gold_orchestration.py
  tests/test_gold_dimensional_writer.py` => **78 passed in 0.26s**.
- Scoped `compileall` => **exit 0**.
- `git diff --check` => **exit 0**, with existing LF/CRLF warnings only.
- The representative Spark DRY_RUN was attempted but terminated after local
  Spark on Windows exceeded the safe runtime window; no pass is claimed and no
  external write occurred.

Status: ready for independent QA round 2. Feature flag, Production WRITE,
Iceberg MERGE, Nessie ref update and checkpoint mutation remain disabled.

## Independent QA verdict: controlled writer/DAG DRY_RUN round 2 (2026-11-16)

Verdict: **FAIL**. Provenance enforcement passed and the task dependency is
present in the DAG, but the task is still only a placeholder: it skips when the
flag is disabled and raises when enabled without constructing or propagating a
`merge_plan` through `run_controlled_incremental_dry_run()`.

The focused non-Spark suite remained green (`78 passed`), compileall and
`git diff --check` passed, and no external write occurred. The representative
Spark DRY_RUN did not complete within the safe local Windows runtime window and
is **NOT CERTIFIED**.

Required remediation before the next QA round:

- Make the actual Airflow task invoke the controlled orchestration seam with
  explicit task inputs and verified reader/writer seams, then return/push the
  DRY_RUN evidence through the task graph.
- Add a direct task-level regression test for disabled safety and enabled
  controlled propagation of the same `merge_plan`.
- Keep all feature flags and Production WRITE disabled outside the controlled
  test seam; do not treat Spark timeout as a pass.

## Controlled writer/DAG DRY_RUN implementation evidence (2026-11-16)

Implemented only the approved non-mutating orchestration seam:

- `build_controlled_dry_run_merge_plan()` requires an injected callable
  `snapshot_reader`, reads every reused dimension through that seam, and then
  delegates to the existing deterministic `build_incremental_merge_plan()`.
  Missing reader, missing resolved artifact or missing reader result fails
  closed.
- `run_controlled_incremental_dry_run()` is the orchestration-to-writer seam:
  it constructs one real plan from explicit Silver delta objects and validated
  reused-dimension results, passes the same plan object to the injected writer
  with `mode="DRY_RUN"`, and requires `DRY_RUN` evidence in the writer result.
- The dimensional writer accepts an explicit merge plan in `DRY_RUN` while the
  snapshot-reuse feature flag remains disabled; `WRITE` with a merge plan still
  requires `GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED=true` and the existing
  separate write guards remain unchanged.
- Regression coverage exercises deterministic source mapping, dependent
  rebuild classification, grain/merge-key/FK graph validation, reused
  snapshot provenance, fail-closed missing seams, and replay-stable plan
  identity. Existing writer quality tests continue to cover FK/grain/totals
  and parity validation at the injected-frame boundary.

Verification evidence:

- `.venv\\Scripts\\python.exe -m pytest -q
  tests/test_gold_snapshot_reuse_contract.py
  tests/test_gold_dimensional_writer.py tests/test_gold_orchestration.py`
  => **75 passed in 0.28s**.
- Scoped `.venv\\Scripts\\python.exe -m compileall -q` over the changed
  Python modules/tests => **exit 0**.
- `git diff --check` => **exit 0**; only existing LF/CRLF conversion warnings.
- No Spark, MinIO, Nessie or PostgreSQL runtime was started. No Iceberg MERGE,
  Nessie ref update, checkpoint mutation, Gold publication, feature-flag
  enablement or Production WRITE was performed.

Changed files for this gate:

- `gold/implementation/dimensional/snapshot_reuse.py`
- `gold/implementation/dimensional/writer.py`
- `airflow/dags/pipeline_tasks/gold.py`
- `tests/test_gold_snapshot_reuse_contract.py`
- `tests/test_gold_dimensional_writer.py`
- `tests/test_gold_orchestration.py`

QA status: **implementation evidence ready for independent QA; runtime
external certification remains open**. QA must inspect the scoped diff against
this gate, verify the injected reader/writer seams and confirm that no DAG
deployment or external runtime write is implied. Next step requires QA verdict;
feature enablement and Production WRITE remain prohibited.

## QA round 1 remediation (2026-11-16)

Scope is limited to the controlled writer/DAG `DRY_RUN` boundary. The feature
flag remains disabled by default; no WRITE, Iceberg MERGE, Nessie ref update,
checkpoint mutation, Gold publication or external-service write was performed.

- `build_controlled_dry_run_merge_plan()` now rejects arbitrary callables,
  including a fake callable returning `status=PASS`. It accepts only the
  runtime-branded `SnapshotReuseReader` or `GoldSnapshotReuseAdapter` seam.
- Added `controlled_incremental_snapshot_reuse_dry_run_task` to the actual
  Airflow DAG path. With the flag disabled it returns skip evidence; when
  enabled without explicitly injected reader/writer seams it fails closed.
  It creates no clients and writes no state.
- Added DAG wiring regression coverage and a representative injected Spark
  DataFrame writer DRY_RUN regression covering latest-row SCD behavior, order
  totals, cross-table parity, FK/grain quality gates and deterministic replay
  identity.

Verification status: strict provenance and DAG regression tests passed in the
focused run: **78 passed in 0.25s**. Scoped `compileall` passed and scoped
`git diff --check` passed with only existing LF/CRLF warnings. The
representative Spark test reached local Spark execution but remained running
beyond the local test window and was terminated; it is **not claimed as
passed**. The fixture issues found in the first run (unverified callable
expectations, incomplete lineage, missing `product_id`, and Windows Spark
defaulting to `python3`) were corrected. Remaining blocker is local Spark
runtime completion on this Windows workspace.

QA handoff: review the strict reader provenance check, Airflow task dependency,
disabled flag behavior and final local Spark DRY_RUN evidence. Runtime
certification and feature enablement remain prohibited.

## QA round 2 remediation (2026-11-16)

Scope remains limited to the approved controlled writer/DAG `DRY_RUN` boundary.
No Spark, MinIO, Nessie or PostgreSQL client was created, and no Iceberg MERGE,
checkpoint mutation, publication, feature enablement or Production WRITE was
performed.

- Added `_execute_controlled_incremental_snapshot_reuse_dry_run()` as the
  task-level execution seam. It accepts explicit `resolved_inputs` and
  `silver_deltas`, requires an injected verified reader and writer when
  enabled, calls `run_controlled_incremental_dry_run()`, and returns the same
  real `merge_plan` evidence that is passed to the writer.
- The Airflow task now exposes that explicit boundary. The production DAG
  invokes it with no controlled inputs, so the default-disabled path is a
  no-op and an accidentally enabled path fails closed before any client or
  write. Spark DataFrames/callables are not placed in XCom; only a separately
  controlled direct task wrapper may inject them.
- Added direct task-level regressions for disabled no-op/no clients and enabled
  propagation of the identical `merge_plan` object to the DRY_RUN writer.

Verification evidence:

- `.venv\\Scripts\\python.exe -m pytest -q
  tests/test_gold_orchestration.py tests/test_gold_snapshot_reuse_contract.py
  tests/test_gold_dimensional_writer.py` => **80 passed in 0.28s**.
- Scoped `.venv\\Scripts\\python.exe -m compileall -q` over
  `airflow/dags/pipeline_tasks/gold.py`,
  `airflow/dags/ecommerce_incremental_ingestion.py` and
  `tests/test_gold_orchestration.py` => **exit 0**.
- Scoped `git diff --check` => **exit 0**; only existing LF/CRLF conversion
  warnings were emitted.

QA status: remediation evidence is ready for an independent QA round 3.
External runtime certification, feature enablement and Production WRITE remain
prohibited. Next step is reviewer validation of the task-level seam and its
fail-closed production DAG behavior.

## Independent QA verdict: controlled writer/DAG DRY_RUN round 3 (2026-11-16)

Verdict: **FAIL**. The private task-level seam correctly calls the controlled
runner when explicit injected inputs are supplied, and provenance/safety checks
pass. However, the deployed DAG invokes the task without those inputs, so an
enabled deployed task still fails closed before constructing or propagating a
real `merge_plan`.

The focused suite passed (`80 passed`), compileall and `git diff --check`
passed, and no external mutation occurred. The Spark representative DRY_RUN is
**NOT CERTIFIED** because the local Windows Spark test timed out.

Required remediation:

- Add a runtime-safe, serializable controlled DRY_RUN spec/manifest path to the
  actual DAG task. The task must construct verified reader/writer seams locally
  and invoke `run_controlled_incremental_dry_run()`; no DataFrame or arbitrary
  callable may cross XCom.
- Add an actual task/DAG regression proving enabled execution with that spec,
  same-plan propagation and disabled safety.
- Keep this path explicitly controlled/default-off; Production WRITE, Iceberg
  MERGE, ref update and checkpoint mutation remain disabled.

## QA round 3 remediation (2026-11-16)

Approved scope: close only the actual DAG wiring blocker above. No external
writes, Spark runtime, MinIO/Nessie/PostgreSQL clients, Iceberg MERGE, ref or
checkpoint mutation, feature enablement, or Gold publication was performed.

Implementation:

- `validate_incremental_run_config()` preserves the explicitly named
  `controlled_dry_run_spec` as a serializable config field.
- The actual DAG passes only `config` to
  `controlled_incremental_snapshot_reuse_dry_run_task`; no reader, writer,
  callable, DataFrame, or runtime object crosses XCom.
- The task skips when the feature flag is off or the spec is absent. When the
  flag is explicitly enabled and a spec is present, it validates strict JSON
  shape/version and reference maps, then constructs a branded local
  `SnapshotReuseReader` and DRY_RUN-only writer adapter and calls
  `run_controlled_incremental_dry_run()`.
- Malformed/non-serializable specs fail closed. Local artifact references are
  opaque handles used only for deterministic plan construction/evidence; no
  external client or write is created.

Files changed in this remediation:

- `airflow/dags/pipeline_tasks/gold.py`
- `airflow/dags/ecommerce_incremental_ingestion.py`
- `airflow/dags/pipeline_config.py`
- `tests/test_gold_orchestration.py`

Verification evidence:

- Focused controlled suite: **83 passed**:
  `tests/test_gold_orchestration.py`,
  `tests/test_gold_snapshot_reuse_contract.py`,
  `tests/test_gold_dimensional_writer.py`.
- `compileall` on touched runtime/test files: **PASS**.
- `git diff --check` on touched files: **PASS** (only existing LF/CRLF
  conversion warnings).
- `tests/test_quality_it_local_compose.py tests/test_airflow_parse_imports.py`:
  **1 pre-existing/incompatible assertion failure** because the historical
  test rejects any `snapshot_reuse` substring in `gold.py`; the approved
  actual task necessarily contains that runtime boundary. Other tests in that
  command passed (**37 passed, 1 failed**). This is not runtime certification.
- Spark representative runtime: **NOT CERTIFIED**; not run in this remediation.

QA handoff: independently review the serializable spec contract, actual DAG
call edge, absent/disabled no-op, malformed fail-closed behavior, and exact
merge-plan identity returned by the local DRY_RUN adapter. Keep production
feature flags, Iceberg MERGE, Nessie/ref/checkpoint mutation and Gold
publication disabled.

## Independent QA verdict: controlled writer/DAG DRY_RUN round 4 (2026-11-16)

Verdict: **PASS WITH WARNINGS**.

Independent QA confirmed:

- The DAG passes only serializable `controlled_dry_run_spec` configuration;
  no DataFrame, reader or callable crosses XCom.
- The task constructs the branded `SnapshotReuseReader` and DRY_RUN-only
  writer locally, calls `run_controlled_incremental_dry_run()`, and propagates
  the same `merge_plan` object to the writer seam.
- Absent, disabled and malformed specs fail closed; old resolver/task-boundary
  and parse-time Spark/reader imports remain blocked.
- Feature and WRITE guards remain explicit and no external mutation occurred.

QA evidence: `109 passed in 0.40s` for the focused parse/orchestration,
snapshot-reuse contract and dimensional-writer suites; compileall passed and
`git diff --check` passed with only existing line-ending warnings.

Warnings and boundary:

- Spark/Nessie/MinIO/PostgreSQL runtime certification and full incremental Gold
  merge certification remain **NOT CERTIFIED**.
- SCD, FK/grain, totals/parity, readback and replay are covered by unit/fake
  seams, not a completed lakehouse runtime run.
- Direct malformed/non-serializable spec tests are recommended as a low-risk
  follow-up, but are not blocking this controlled unit/static verdict.

Phase status: controlled writer/DAG DRY_RUN implementation is complete for
unit/static evidence. Next phase requires separate approval for a controlled
Spark/Nessie runtime DRY_RUN; feature enablement and Production WRITE remain
disabled.

## QA round 3 remediation follow-up (2026-11-16)

The serializable controlled spec path is implemented and the focused controlled
suite is green (`83 passed`). One legacy static guard still fails because it
forbids every `snapshot_reuse` substring in `gold.py`, which conflicts with the
approved controlled task. The guard must be narrowed to forbid the old resolver
and unapproved task boundary while allowing the explicitly controlled DRY_RUN
task. Spark runtime remains **NOT CERTIFIED**.

## Static guard remediation (2026-11-16)

Updated `tests/test_airflow_parse_imports.py` so the Gold parse-import guard no
longer rejects every `snapshot_reuse` substring. It still rejects the legacy
`resolve_gold_snapshot_reuse_task` boundary and
`task_id="resolve_gold_snapshot_reuse"`, checks for the approved controlled
task and `controlled_dry_run_spec`, and inspects only module-level imports to
keep Spark and dimensional reader dependencies out of the Airflow parse graph.

Verification evidence:

- `.venv\\Scripts\\python.exe -m pytest -q tests/test_airflow_parse_imports.py
  tests/test_quality_it_local_compose.py tests/test_gold_orchestration.py
  tests/test_gold_snapshot_reuse_contract.py tests/test_gold_dimensional_writer.py`
  => **121 passed in 0.37s**.
- `.venv\\Scripts\\python.exe -m compileall -q airflow gold common tests` =>
  **exit 0**.
- `git diff --check` => **exit 0**; only existing LF/CRLF conversion warnings.
- No Spark, MinIO, Nessie or PostgreSQL runtime was started and no external
  write, Iceberg MERGE, checkpoint mutation, feature enablement or Production
  WRITE was performed.

QA handoff: review the narrowed AST-based parse-import guard and confirm that
the approved serializable controlled DRY_RUN boundary remains covered while
the legacy resolver and parse-time dependency boundaries remain prohibited.
Spark runtime certification remains open.

## Approved controlled Spark/Nessie runtime DRY_RUN (2026-11-16)

User approval received to execute the next gate. Scope is read-only and limited
to the existing `gold_prod_current` catalog/ref and already pinned Gold
snapshots. The run may create Spark readers and perform Iceberg snapshot reads,
manifest/schema/readback and merge-plan/quality validation, but must not perform
Iceberg MERGE, Nessie ref update, MinIO/PostgreSQL mutation, checkpoint or
watermark mutation, compaction/expiration/cleanup, publication or Production
WRITE.

Acceptance criteria:

- Resolve approved snapshots on `gold_prod_current` through the verified reader
  seam and read them by pinned snapshot ID.
- Build the controlled merge plan from explicit Silver artifact references and
  reused dimensions, then exercise the DRY_RUN writer/quality path.
- Verify target mapping, SCD behavior, FK/grain, totals/parity and deterministic
  replay identity where the runtime fixtures support it.
- Capture exact snapshot/schema/row/file evidence and explicitly report any
  runtime limitation; no timeout may be treated as a pass.
- Keep all feature and WRITE flags disabled after completion and run an
  independent QA review of the runtime evidence.

## Controlled Spark/Nessie runtime attempt (2026-11-16)

Approved scope was executed read-only. No source code, data contract, harness
semantics, database, object storage, checkpoint, watermark, Nessie ref, or
Iceberg table was mutated.

Environment safety observed for the attempt:

- `GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED=false`
- `GOLD_DIMENSIONAL_WRITE_ENABLED=false`
- `PRODUCTION_LAKEHOUSE_WRITE_APPROVED` unset/empty
- `INGESTION_WRITE_ENABLED=false`
- `AIRFLOW_LOCAL_E2E_MODE` unset/empty
- `NESSIE_REF=gold_prod_current`

Read-only service checks:

- Nessie `GET http://127.0.0.1:19120/api/v2/trees` returned HTTP 200.
- `gold_prod_current` was present with Nessie hash
  `adb060d0499e2cfd0fd38d28b3c6c3b85ef5c1aedd54238fbe3a7d652f0e196e`.
- TCP connectivity was observed for Nessie `19120`, MinIO `9000`, and
  PostgreSQL `5432`; connectivity is not data/runtime certification.

Runtime command attempted with a 45-second safe timeout:

```text
.venv\Scripts\python.exe -m pytest -q tests/test_gold_snapshot_reuse_dry_run.py
```

The local Spark process produced no test output and did not complete within the
45-second window. The Spark/Python process tree started by this attempt was
stopped. Result: **NOT CERTIFIED (timeout)**. The timeout is not treated as a
pass, and no Spark/Nessie Iceberg snapshot read was certified.

Exact runtime snapshot/schema/row/file evidence:

- Current `gold_prod_current` snapshot IDs: **NOT OBSERVED** because the Spark
  Iceberg reader did not complete.
- Current schema, row counts, file counts and file bytes: **NOT OBSERVED** for
  the same reason.
- The contract still declares `dim_customer` as six columns and `dim_product`
  as seven columns, but these are expected schemas, not runtime observations.
- Existing repository evidence in `gold_table_registry.py` is for the separate
  `quality_it_run_20261112_gold_06` ref and was not substituted for the
  approved `gold_prod_current` evidence.

Controlled non-Spark validation executed separately:

```text
.venv\Scripts\python.exe -m pytest -q tests/test_gold_orchestration.py tests/test_gold_snapshot_reuse_contract.py tests/test_gold_dimensional_writer.py
=> 83 passed in 0.33s
```

This verifies the bounded contracts and DRY_RUN guards only. It does not
certify Spark reads, explicit Silver artifact reads, SCD/FK/grain/totals
quality against current lakehouse data, replay identity over runtime data, or
the writer against a live catalog.

Limitations and next step:

- Runtime gate status remains **NOT CERTIFIED** due to the safe timeout.
- No Iceberg MERGE, Nessie ref update, MinIO/PostgreSQL mutation, publication,
  compaction, cleanup, checkpoint/watermark mutation, or Production WRITE was
  performed.
- Independent QA must review this evidence and retain the NOT CERTIFIED
  verdict. A future runtime attempt needs a bounded Spark execution environment
  that completes within the approved timeout before any feature enablement or
  Production WRITE discussion.

## Independent QA verdict: controlled Spark/Nessie runtime attempt (2026-11-16)

Verdict: **PASS WITH WARNINGS — evidence handling only; runtime NOT
CERTIFIED**.

QA confirmed that the attempt is reported conservatively: Nessie
`gold_prod_current` was reachable with HTTP 200 and the expected ref hash, but
Spark DRY_RUN timed out at the bounded 45-second limit and was stopped safely.
No pinned Iceberg snapshot ID, schema, row count, file count/bytes, Silver
artifact read, quality result or replay evidence was observed.

No Iceberg MERGE, Nessie ref update, MinIO/PostgreSQL mutation, checkpoint or
watermark mutation, cleanup, publication or Production WRITE occurred. Feature
and WRITE gates remained disabled.

The runtime gate remains open and **NOT CERTIFIED**. The Nessie ref hash is not
table-level snapshot/data validation. Before another attempt, prepare a bounded
Spark environment that can complete within the approved timeout, obtain
separate approval for that retry, capture the required runtime evidence, and
run independent QA again.

## Approved bounded Spark retry (2026-11-16)

Approval received for one remediation/retry cycle. Scope remains strictly
read-only: diagnose and correct local Spark execution configuration, then run
the controlled DRY_RUN against `gold_prod_current` with all write/ref/checkpoint
flags disabled. No Iceberg MERGE, Nessie ref update, MinIO/PostgreSQL mutation,
publication, cleanup or Production WRITE is authorized.

Required evidence remains pinned snapshot ID, schema, row/file counts, Silver
artifact references, SCD/FK/grain, totals/parity and replay identity. A bounded
timeout remains **NOT CERTIFIED**, not a pass.

## Bounded Spark retry remediation result (2026-11-16)

The approved remediation was limited to the local Windows test harness in
`tests/test_gold_snapshot_reuse_dry_run.py`. No Gold transformation, writer,
Iceberg catalog, data contract, or runtime feature semantics were changed.

Exact harness changes:

- Replace `setdefault` for `PYSPARK_PYTHON` and
  `PYSPARK_DRIVER_PYTHON` with the normalized absolute `sys.executable` path,
  so an inherited Unix-style `python3` cannot be selected on Windows.
- Set local-only defaults `SPARK_LOCAL_IP=127.0.0.1` and
  `SPARK_LOCAL_HOSTNAME=localhost`.
- Pin the test Spark driver to loopback with
  `spark.driver.host=127.0.0.1` and
  `spark.driver.bindAddress=127.0.0.1`.
- Keep `local[2]`, `spark.ui.enabled=false`, one shuffle partition, and enable
  Python worker reuse. These are harness settings only.

Diagnostic evidence:

- Java: OpenJDK 17.0.21; PySpark: 4.2.0; interpreter:
  `.venv\\Scripts\\python.exe`.
- A minimal Spark `local[2]` smoke command with the explicit loopback/Python
  settings reached `session`, completed `range(1).count() == 1`, and stopped;
  this does not certify Iceberg or Gold runtime behavior.
- The focused pytest command still produced no output and was stopped after
  the bounded window.

Controlled retry command and flags:

```text
$env:PYSPARK_PYTHON=(Resolve-Path '.venv\\Scripts\\python.exe').Path
$env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON
$env:SPARK_LOCAL_IP='127.0.0.1'
$env:SPARK_LOCAL_HOSTNAME='localhost'
& .venv\\Scripts\\python.exe -m pytest -q tests/test_gold_snapshot_reuse_dry_run.py
```

The retry was bounded to approximately 60 seconds. It produced no pytest
output and did not complete; Ctrl-C stopped the process safely with exit code
1. Therefore the controlled DRY_RUN against `gold_prod_current` did not yield
runtime evidence. Exact snapshot IDs, schemas, row counts, file counts/bytes,
Silver artifact references, SCD/FK/grain, totals/parity and replay identity are
all **NOT OBSERVED** in this retry. No value from repository fixtures or prior
runs is substituted as runtime evidence.

Safety result:

- `GOLD_DIMENSIONAL_WRITE_ENABLED` and production write/cutover paths were not
  enabled; no Iceberg MERGE, Nessie ref update, MinIO/PostgreSQL mutation,
  checkpoint/watermark mutation, publication or cleanup occurred.
- Non-Spark regression checks: `.venv\\Scripts\\python.exe -m pytest -q
  tests/test_gold_snapshot_reuse_contract.py tests/test_gold_orchestration.py`
  => **60 passed in 0.27s**.
- Scoped `compileall` => **passed**; `git diff --check` for the changed harness
  and plan => **passed**.

Runtime gate verdict: **NOT CERTIFIED (bounded timeout)**. The next step is an
independently reviewed runtime attempt in a Spark environment that completes
within the safety timeout; no feature enablement or Production WRITE is
authorized from this evidence.

## Independent QA verdict: bounded Spark retry (2026-11-16)

Verdict: **PASS WITH WARNINGS — evidence handling only; runtime NOT
CERTIFIED**.

QA confirmed the retry changed only the local Spark harness: normalized virtualenv
Python executables, loopback defaults, `local[2]`, UI disabled, one shuffle
partition and worker reuse. No Gold transformation, writer semantics, catalog
contract or write flag was changed.

Minimal Spark smoke completed, but the Iceberg/Nessie-focused DRY_RUN still
timed out and was stopped safely. Snapshot ID, schema, row/file counts, Silver
references, SCD/FK/grain, totals/parity and replay identity remain **NOT
OBSERVED**. No Iceberg MERGE, Nessie ref update, MinIO/PostgreSQL mutation,
checkpoint/watermark mutation, cleanup, publication or Production WRITE
occurred.

The retry cycle is closed without runtime certification. Further progress
requires a materially different bounded Spark execution environment and a new
explicit approval; blind retries are not authorized. Feature enablement and
Production WRITE remain disabled.

## Versioned pinned Gold snapshot metadata contract (2026-11-17)

Implemented the approved static/unit scope across the dimensional contract,
manifest, writer, run metadata, verification and snapshot-reuse boundaries.

- v2 reusable PASS metadata carries deterministic `schema_identity`,
  `graph_identity`, `manifest_identity`, positive `snapshot_id`,
  `catalog`/`namespace`/`ref`, `stale_policy`, and `readback_identity`.
- Canonical identities are JSON-key-order independent and mismatches fail
  closed. Legacy v1 table manifests and legacy reuse envelopes remain readable
  but are not implicitly upgraded to reusable v2 metadata.
- `DRY_RUN` returns `reusable=false` and the snapshot artifact builder rejects
  attempts to issue reusable PASS metadata in DRY_RUN.
- Focused static verification: `63 passed`; scoped `compileall`: passed;
  `git diff --check`: passed (existing line-ending warnings only).
- No runtime, Spark, Docker, MinIO, Nessie, PostgreSQL write, migration,
  checkpoint mutation or commit was performed. Independent reviewer QA and
  runtime certification remain separate gates.

## QA correction cycle (2026-11-17)

Applied the QA-required corrections directly in the working tree:

- v2 table manifests now emit top-level `status: PASS` and canonical
  `manifest_identity`; the producer-to-reuse-builder-to-validator path is
  covered by regression tests.
- Verification recomputes canonical readback identity from payload content.
- Current embedded manifests must agree with snapshot catalog, namespace, ref,
  schema identity, readback identity and snapshot ID.
- `ALREADY_PUBLISHED` recovery returns existing table manifests so run metadata
  retains Gold snapshot metadata.
- Reusable manifest creation requires explicit `captured_at` or stale policy;
  it no longer calls `datetime.now()` implicitly.

Correction verification: **102 focused tests passed**, scoped `compileall`
passed, and `git diff --check` passed. No runtime or external write was run.

## QA round 2 correction (2026-11-17)

`dimensional/verification.py` now requires reusable manifests to have
`status=PASS`, the expected table, and a snapshot ID matching physical
readback. Stale policy validation requires timezone-aware `captured_at`,
rejects future timestamps, and rejects expired snapshots against an explicit
validation clock. Regression coverage includes status/table/snapshot mismatch,
future, expired and naive timestamps.

Verification: **108 focused tests passed**; scoped `compileall` passed;
`git diff --check` passed. Static-only; no runtime or external write/commit.

## Final QA identity/timestamp correction (2026-11-17)

The verifier now rejects a manifest whose legacy top-level `identity` alias
does not equal `manifest_identity`, after canonical recomputation. The
manifest producer parses and normalizes `captured_at` and rejects invalid or
naive timestamps at the producer boundary.

Final static verification: **111 focused tests passed**; scoped `compileall`
passed; `git diff --check` passed. No runtime, external write or commit.

## Final QA shared Silver identity correction (2026-11-17)

`airflow/dags/pipeline_tasks/silver.py` now imports and uses
`common.manifest.canonical_manifest_identity`; the redundant local `hashlib`
producer implementation was removed. The shared identity regression and the
focused Gold metadata/reuse/verification/writer/lineage/orchestration suites
passed: **142 tests**. Scoped `compileall` and `git diff --check` passed.
Static-only; no runtime, external write or commit.

## Session: registry-only NO_DATA regression coverage (2026-11-17)

Approved scope: close the LOW coverage gap only. Add a unit regression proving
that preflight resolves both `customers` and `products` from the validated
registry when both Silver artifacts are `NO_DATA` and neither artifact carries
inline `snapshot_reuse`. The test must also prove the preflight path does not
invoke the downstream Docker/runtime seam. No production code, DAG execution,
feature enablement, external client, or snapshot ID is introduced.

Acceptance criteria:

- Both dimension artifacts are `NO_DATA` and contain no inline reuse payload.
- Both effective resolutions are `GOLD_SNAPSHOT_REUSE` and match the registry
  artifact content.
- No downstream Docker/runtime call occurs during preflight.
- Existing focused tests, `compileall`, and `git diff --check` pass.

Implementation and verification:

- Added `test_gold_writer_preflight_resolves_both_no_data_dimensions_from_registry_only`
  to `tests/test_gold_orchestration.py`.
- Test execution evidence and independent QA verdict are recorded below after
  the scoped verification commands complete.

Verification evidence:

- `.venv\\Scripts\\python.exe -m pytest -q tests/test_gold_orchestration.py
  tests/test_gold_snapshot_reuse_contract.py tests/test_gold_snapshot_reuse_registry.py`
  => **88 passed in 0.29s**.
- `.venv\\Scripts\\python.exe -m compileall -q airflow gold common tests`
  => **exit 0**.
- `git diff --check` => **exit 0**; only pre-existing line-ending warnings were
  emitted for unrelated working-tree files.
- No DAG/runtime was triggered and no MinIO, Nessie, PostgreSQL or Spark client
  was created. No feature flag, production snapshot ID, checkpoint, watermark,
  object, table or ref was changed.

Independent QA verdict for this session: **PASS**. The regression constructs
both dimension inputs as `NO_DATA` without inline reuse, supplies only the
validated registry bundle, verifies both `GOLD_SNAPSHOT_REUSE` resolutions
match the registry, and verifies the downstream Docker seam is untouched.
Existing unrelated working-tree modifications remain preserved. Next step:
user approval is required before any broader suite or runtime gate is run.
