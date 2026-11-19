# Historical production backfill plan

## Status

- Implementation vertical slice in progress; no runtime write executed.
- User scope confirmed: backfill all source history from `2026-09-26` through a fixed `capture_end`, then promote a verified candidate to `gold_prod_current`.
- No production bucket/object/ref has been created or written by this plan.

## Scope

- Source tables: `customers`, `products`, `orders`, `order_items`, `payments`, `shipments`.
- Production bucket: `ecommerce-lakehouse-prod`.
- Canonical layout: `bronze/<table>/...`, `silver/<table>/...`, `gold/<table>/...`.
- Candidate ref: `gold_prod_backfill_<run-id>`; stable ref: `gold_prod_current`.
- Backfill must not read or mutate incremental watermarks/checkpoints and must not reset or delete any existing data.

## Implementation steps

1. Add an isolated historical extraction/orchestration package with fixed `capture_end`, keyset pagination, new batch IDs, and separate metadata.
2. Add Bronze validation and Silver backfill transforms/manifests, including deterministic dedupe and parent-first FK checks.
3. Add a separate Airflow historical-backfill DAG with production guards and DRY_RUN support.
4. Add a production-only candidate/ref promotion contract using Nessie CAS and fail-closed conflict handling.
5. Add focused unit/contract tests, run compile/import/static checks, and perform independent QA.
6. Only after QA passes, execute the approved production backfill write, read back Bronze/Silver/Gold, verify the candidate, and CAS-promote the stable ref.

## Acceptance criteria

- Six source tables are fully covered for the fixed source window; source row counts and lake row counts reconcile.
- No `ingestion_watermarks` or incremental checkpoints change.
- Bronze/Silver/Gold objects use the production bucket and canonical paths.
- Silver manifests, primary keys, dedupe, and `payments`/`shipments` FK parity pass.
- All seven Gold tables are verified with pinned snapshots and read-back evidence.
- Candidate verification passes before promotion; stable ref advances only by CAS to the verified candidate hash.
- CAS conflict, validation failure, or partial write fails closed without cleanup/deletion.

## Risks and controls

- Source mutation: capture a fixed end timestamp and use repeatable-read/snapshot-consistent extraction where supported.
- Hard deletes: document that this is a current-state backfill; CDC/tombstones remain separate.
- Retry duplication: immutable run/batch IDs and create-if-absent semantics.
- Wrong target: validate production environment before creating clients.
- Stable ref race: require expected hash and fail closed on conflict.

## Required verification

- Focused historical-backfill tests.
- `compileall` for `common`, `ingestion`, `silver`, `gold`, and `airflow`.
- Airflow import/DAG contract test.
- Production storage guard and watermark immutability tests.
- Candidate read-back and CAS promotion tests.
- Independent reviewer verdict recorded here before runtime write.

## Current evidence

- Read-only source inventory completed: all six tables have `updated_at`; source ranges begin at `2026-09-26` or later.
- Production bucket is currently absent; existing incremental metadata contains test-run watermarks.
- Planner review completed; isolated backfill is required before any production write.

## Session implementation checkpoint (2026-11-14)

Changed files owned by this session:

- `ingestion/historical/{__init__,config,extractor,staging,repository,orchestrator}.py`
- `source/database/migrations/006_create_historical_backfill_metadata.sql`
- `airflow/dags/historical_production_backfill.py`
- `gold/implementation/historical_ref_promotion.py`
- `tests/test_historical_backfill_contracts.py`

The implementation is intentionally isolated from `ingestion/incremental`,
uses `public.historical_backfill_batches`, emits canonical
`bronze/<table>`, `silver/<table>` paths, and keeps promotion as a separate
CAS contract for `gold_prod_current`. No database, MinIO, Nessie, Spark,
watermark, checkpoint or object-storage client was created or called.

Verification executed:

- `python -m compileall -q ingestion/historical gold/implementation/historical_ref_promotion.py airflow/dags/historical_production_backfill.py` — passed.
- `python -m pytest -q tests/test_historical_backfill_contracts.py` — not executed: current interpreter lacks `pytest` (`No module named pytest`).

Unresolved risks:

- Airflow import/DagBag and PostgreSQL/Nessie/MinIO read-back have not been
  runtime-certified in this session.
- The migration is version-controlled only; it was not applied.
- The remaining Silver-to-Gold writer integration, candidate read-back and
  production CAS execution remain gated on independent QA and an explicit
  runtime-write approval.

## Next gate

Run the focused suite in an environment with project test dependencies,
perform independent QA against this diff, then request a separate explicit
runtime-write/promotion approval. Do not mark runtime gates complete from
compile/static evidence alone.

## QA round 2 correction checkpoint (2026-11-14, safely paused)

- Historical candidate evidence validation is now strict for all seven Gold
  tables: positive `snapshot_id`, `readback_status=PASS`, non-negative
  `row_count`, positive `file_count`/`file_bytes`, SHA-256
  `manifest_identity`/`readback_identity` equality, and
  `consistency_status=PASS`. Missing-field and identity-mismatch regressions
  are covered.
- Historical metadata registration now uses atomic
  `ON CONFLICT (run_id, table_name, window_start, window_end) DO NOTHING`
  create-or-verify semantics. State transitions use CAS predicates on
  `batch_id`, expected `status`, and expected `attempt`, with retry attempt
  increment for `FAILED -> PENDING`; the migration adds the window-order
  constraint.
- Gold recovery regression fixed by classifying completed/partial immutable
  recovery state before new-run cross-table coverage validation. Partial
  recovery still returns without source read, target create, MERGE, or
  evidence relaxation; new runs remain coverage-gated before fences/writes.
- Historical DAG remains fail-closed with
  `HISTORICAL_GOLD_WRITER_UNAVAILABLE`; no `VERIFIED` result is fabricated.
  Physical historical Gold writer/readback remains intentionally unavailable,
  so runtime write and promotion remain blocked.

Verification at pause:

- Focused correction/recovery/promotion suite: **44 passed**.
- Gold suite attempt: **156 passed, 5 failed** under the default Windows
  Spark worker configuration (`python3`/`CreateProcess error=1920`); these
  are environment launch failures, not assertion failures. A rerun with
  explicit `.venv` Spark interpreter was started but intentionally stopped by
  the user before a final result.
- `compileall`, `git diff --check`, and independent QA verdict for this
  correction are still pending; no runtime/external-service write, bucket
  creation, delete/reset, Iceberg cutover, or promotion occurred.

Next exact step: rerun the Gold suite with the pinned local Spark interpreter,
then run compileall and `git diff --check`; perform independent QA before any
runtime approval.

## Session plan: historical Gold candidate branch bootstrap (2026-11-15)

### Runtime finding

Runtime evidence shows date coverage is now correct and the Gold Spark path
reaches Nessie, but it fails before publish because the run-scoped candidate
branch does not exist (`gold_prod_backfill_prodbackfill20261115j`). The
read-only Nessie `/trees` check confirms that the configured base branch
`main` exists. Run `j` must not be retried.

### Implemented scope

- Add a small, injected Nessie v2 branch-bootstrap operation to the historical
  Gold writer boundary.
- Resolve the base ref from `HISTORICAL_GOLD_BASE_REF`, defaulting to `main`;
  read the base reference and capture its exact hash before any create request.
- Read the candidate first. If it is absent, issue only `POST /trees` for the
  run-scoped candidate branch, with the candidate branch name and base hash in
  the v2 reference payload/query shape required by the adapter. Never create,
  assign, update, or delete `gold_prod_current`.
- If the candidate already exists, require exact `name`, `type=BRANCH`, and
  verified hash identity; do not mutate it or recreate it.
- Read the candidate after bootstrap and fail closed unless the readback has
  the exact candidate name, `BRANCH` type, and expected base hash. The result
  must be validated before Spark/MinIO runtime construction and before Gold
  table publish.
- Add only mocked adapter/HTTP unit regressions for missing-candidate create,
  existing exact candidate, wrong type/name/hash, base-ref failure, and stable
  ref non-mutation. No external runtime test and no retry of run `j`.

### Acceptance criteria

- Missing candidate is bootstrapped from the configured base ref and verified
  by readback before Gold writer clients are created.
- Existing candidate is accepted only when its identity is exactly a branch;
  any mismatch fails closed without a write.
- The stable ref is never present in a create/update/delete request from this
  path.
- Base-ref and candidate hash/name/type mismatches fail closed.
- Focused unit tests, `compileall`, and `git diff --check` pass; no external
  service, object store, database, checkpoint, watermark, Iceberg cutover, or
  runtime retry is executed.

### Files implemented in this session

- `gold/implementation/historical_ref_promotion.py`
- `gold/implementation/historical_gold_writer.py`
- `tests/test_historical_backfill_contracts.py` and/or a focused Nessie adapter
  test module
- This plan and, after independent QA, `docs/GOLD_LAKEHOUSE_PLAN.md`

### Risks and controls

- Nessie API shape risk: isolate the POST payload/path in the adapter and test
  the exact request; do not guess through fallback mutations.
- Concurrent creator race: after a create conflict, read back the candidate
  and accept only the exact branch/hash identity; otherwise fail closed.
- Wrong base branch: require the configured base ref to read as an existing
  reference and pin its hash before creation.
- Stable-ref blast radius: keep bootstrap API limited to candidate names and
  do not reuse the promotion CAS method.

### Verification and QA gate

- Focused mocked Nessie/historical writer tests only.
- Scoped Python `compileall` and `git diff --check`.
- Independent QA against this approved scope and the implementation diff.
- Runtime certification remains open; do not retry run `j`.

### Implementation evidence (2026-11-15)

- `NessieRestAdapter` now distinguishes HTTP 404 as
  `NessieReferenceNotFound`, creates a candidate branch with the Nessie v2
  detached `{type, hash}` payload, and validates response identity.
- `ensure_candidate_branch` reads the candidate first, creates only a missing
  run-scoped candidate from the configured base hash, then performs readback
  validation. Existing candidates must be exact `BRANCH` identities and are
  never mutated. `gold_prod_current` is rejected as a bootstrap target.
- Historical Gold now performs candidate bootstrap before importing or
  constructing Spark/MinIO clients in `_default_runtime_factory`.
- Added mocked adapter/HTTP regressions for missing-candidate creation,
  existing-branch no-op, non-branch mismatch, stable-ref rejection, and exact
  POST payload.

Verification executed:

- `.venv\Scripts\python.exe -m pytest -q --basetemp
  .pytest-tmp\candidate-bootstrap tests/test_historical_backfill_contracts.py`
  — **57 passed**.
- `.venv\Scripts\python.exe -m pytest -q --basetemp
  .pytest-tmp\candidate-bootstrap-parse tests/test_airflow_parse_imports.py
  tests/test_historical_backfill_contracts.py` — **80 passed**.
- Scoped `compileall` — **passed**.
- Scoped `git diff --check` — **passed**.

No external runtime, Nessie/MinIO/PostgreSQL/Spark client, object mutation,
watermark/checkpoint change, run `j` retry, promotion, cutover, or destructive
operation was performed.

Independent QA verdict: **pending**. The implementation phase is complete,
but runtime certification and the roadmap phase status remain open until an
independent reviewer checks this diff against the approved plan.

### QA FAIL and correction (2026-11-15)

Independent QA found that the first implementation reread the base ref inside
`NessieRestAdapter.create_branch()` and sent a non-contract nested `reference`
payload to `POST /trees`. This violated the exact captured-hash/no-race
acceptance criterion and the then-assumed create-reference contract.

Correction scope:

- Use `POST /api/v2/trees?name=<candidate_ref>&type=BRANCH` with the detached
  body `{type=DETACHED, hash=<captured-base-hash>}`.
- Pass the hash captured by `ensure_candidate_branch()` directly into the POST;
  do not reread the base ref in the adapter.
- Add HTTP 404 translation coverage and assert bootstrap precedes Spark/MinIO
  construction.

No runtime or external mutation is permitted. Focused tests, `compileall`, and
`git diff --check` are required before independent QA reruns.

Correction verification:

- `.venv\\Scripts\\python.exe -m pytest -q --basetemp
  .pytest-tmp\\nessie-bootstrap-correction tests/test_historical_backfill_contracts.py
  tests/test_airflow_parse_imports.py` — **82 passed**.
- Scoped `compileall` for the changed Python modules — **passed**.
- `git diff --check` for tracked changes — **passed**. The Nessie/historical
  implementation and plan files remain untracked workspace files inherited from
  the prior session, so ordinary Git diff output does not include them; their
  contents were checked directly and no whitespace errors were emitted.

Independent QA must rerun against the corrected untracked implementation files
and confirm the exact v2 query/body, captured-hash preservation, HTTP 404
translation, and bootstrap-before-client ordering. The earlier v1 contract
notes in this historical section are superseded by the v2 correction recorded
below.

### QA FAIL: terminal create errors must propagate (2026-11-15)

Independent QA found that `ensure_candidate_branch()` caught every
`PromotionError` from candidate creation and then performed a candidate
readback. This incorrectly converted terminal HTTP 400 responses into a
concurrent-create fallback. The v2 detached create payload is
`{type=DETACHED, hash=<captured-hash>}`; only HTTP 409 is a valid
concurrent-create signal.

Correction scope:

- Add an explicit `NessieReferenceConflict` for HTTP 409 and catch only that
  exception in `ensure_candidate_branch()`.
- Add a full-path regression proving HTTP 400 performs one POST and no
  fallback GET of the candidate.
- Correct the contradictory wording in `docs/GOLD_LAKEHOUSE_PLAN.md` to the
  actual v2 detached create payload.

Verification required: focused historical backfill tests, scoped `compileall`,
and `git diff --check`. No external runtime, retry, or data/catalog mutation.

Correction verification (2026-11-15):

- `.venv\\Scripts\\python.exe -m pytest -q --basetemp
  .pytest-tmp\\nessie-bootstrap-terminal
  tests/test_historical_backfill_contracts.py` — **62 passed**.
- Scoped `compileall` for the changed Python modules — **passed**.
- `git diff --check` — **passed**; direct whitespace checks also covered the
  untracked implementation/test files without reporting whitespace errors.
- No external runtime, retry, client construction, data/catalog mutation,
  checkpoint/watermark change, cutover, or destructive operation occurred.

Independent QA verdict: **pending**. QA must verify that only HTTP 409 raises
`NessieReferenceConflict` and that the HTTP 400 full-path regression has no
candidate fallback GET before this historical phase advances.

## Session plan: historical Silver canonical cross-table coverage (2026-11-15)

### Runtime finding

The historical Silver producer emitted canonical Parquet lineage but omitted
the Gold cross-table `coverage` metadata for `orders`, `payments` and
`shipments`. This caused the Gold coverage preflight to reject the six-artifact
historical candidate despite the artifact envelopes being otherwise verified.

### Approved implementation scope

- Set the deterministic historical `runtime_window_id` to the validated
  historical `run_id`.
- Emit `coverage_identity` as
  `sha256(runtime_window_id:table:batch_id)` in nested `parquet_lineage`.
- Keep `orders` free of parent coverage fields.
- Emit `parent_coverage_identity` and the exact
  `parent_orders_reference` (`table`, `batch_id`, `manifest_key`,
  `manifest_identity`) for `payments` and `shipments`.
- Add positive six-artifact coverage validation and mismatched-parent
  regression coverage.
- Run only focused unit tests, Python compilation and diff checks.

No external rerun, service write, object mutation, overwrite, promotion,
checkpoint/watermark change or Iceberg cutover is in scope.

### Implementation evidence

- `ingestion/historical/pipeline.py` now builds coverage after the verified
  manifest identity is available and passes it through
  `build_parquet_lineage_artifact`.
- `tests/test_historical_pipeline.py` validates the actual six mocked Silver
  artifacts with `validate_cross_table_coverage`, checks deterministic and
  parent metadata, and rejects a mismatched payment parent batch.
- Focused command:
  `.venv\Scripts\python.exe -m pytest -q --basetemp
  .pytest-tmp\historical-coverage-fix tests/test_historical_pipeline.py
  tests/test_gold_coverage.py` — **23 passed**.
- Compile command:
  `.venv\Scripts\python.exe -m compileall -q ingestion/historical
  gold/implementation/dimensional tests/test_historical_pipeline.py
  tests/test_gold_coverage.py` — **passed**.
- `git diff --check` — **passed**.

No external-service test, runtime rerun, object mutation, overwrite,
promotion, checkpoint/watermark reset or Iceberg cutover was executed.

### QA Reviewer Handoff

Review only the scoped historical coverage diff. Confirm that the six
artifact path reaches the existing Gold coverage validator, that orders has no
parent fields, and that payments/shipments reference the exact orders batch
and manifest identity. Runtime certification and promotion remain separate
gates.

## Independent QA verdict (2026-11-14)

- Verdict: **FAIL**.
- Verified: `tests/test_historical_backfill_contracts.py` passed 8 tests;
  compileall for `common`, `ingestion`, `silver`, `gold`, and `airflow`
  passed; `tests/test_airflow_parse_imports.py` passed 19 static contract
  tests; `git diff --check` passed.
- Blocking findings: the historical DAG only validates scope and returns
  `DRY_RUN`; it does not execute extraction, Bronze/Silver processing,
  reconciliation, Gold read-back, or candidate verification. Extraction is
  not snapshot-consistent and excludes rows exactly at `START_AT` in the
  implemented query. Candidate promotion is an injected adapter contract,
  not a runtime Nessie/read-back integration. Production guard validation is
  bypassed for candidate refs, and historical metadata lacks complete retry
  state/idempotency semantics.
- Runtime limitation: actual Airflow import/DagBag could not be certified in
  the available environment because `airflow.sdk`/`airflow.models` are absent.
- Required next step: keep the phase open, implement the missing vertical
  integration and guard/idempotency fixes, then repeat independent QA before
  any production write or promotion approval.

## Correction checkpoint (2026-11-14, post-FAIL)

### Correction scope

- Historical-only files were corrected; incremental ingestion and existing
  Silver runtime code were not broadened.
- The DAG now exposes the complete dependency graph:
  `extract_historical_snapshot -> bronze_validate_upload_metadata ->
  silver_transform_manifest -> gold_integration_verification_gate ->
  candidate_verification`.
- Gold write mode fails closed with
  `HISTORICAL_GOLD_WRITER_UNAVAILABLE`; dry-run outputs remain PLANNED or
  BLOCKED_PLANNED and cannot be treated as candidate verification.
- Source extraction uses an inclusive first page (`updated_at >=
  window_start`), fixed `capture_end`, keyset continuation, and an explicit
  `REPEATABLE READ READ ONLY` lifecycle.
- Production storage validation now applies to candidate refs before any
  external client could be created; quality/E2E flags remain forbidden.
- Metadata create-or-verify now checks run/table/window identity and exposes
  guarded state transitions. `order_items` validates both order and product
  parent keys.
- Historical promotion now has a Nessie REST adapter, candidate ref/hash
  readback, seven-table evidence validation, and stable-branch CAS. The
  legacy `bi_ref_promotion` run-06 path is not used by this DAG.

### Correction evidence

- `.venv\Scripts\python.exe -m pytest -q --basetemp .pytest-tmp\qa-correction-5 tests/test_historical_backfill_contracts.py tests/test_airflow_parse_imports.py tests/test_lakehouse_safety_guards.py` — **37 passed**.
- `.venv\Scripts\python.exe -m pytest -q --basetemp .pytest-tmp\qa-correction-2 tests/test_bi_ref_promotion.py -k "not cli"` — **18 passed, 4 deselected** (the deselected CLI tests were blocked by the host temp-directory permission error, not by an assertion).
- `.venv\Scripts\python.exe -m compileall -q common ingestion silver gold airflow` — **passed**.
- `git diff --check` — **passed** (only existing LF/CRLF warnings were emitted).

### Remaining limitations / gate status

- **Partial correction only; phase remains OPEN and runtime gates are NOT
  complete.** The production Gold writer, seven-table physical read-back and
  candidate artifact verification are not safely wired to runtime clients;
  the DAG intentionally fails closed at that boundary.
- Airflow 3 `airflow.sdk`/DagBag runtime certification is unavailable in the
  current local interpreter; only static graph/import contracts were run.
- PostgreSQL snapshot extraction, MinIO upload, Spark Silver execution,
  Nessie REST calls and production CAS were not executed. No production write,
  bucket/ref creation, delete, reset, checkpoint mutation or cutover occurred.
- The metadata migration is version-controlled but not applied. A follow-up
  implementation must reconcile existing deployments before any write gate.
- The CLI-focused promotion tests were not fully executed because the host
  temp root returned `WinError 5`; rerun them in a writable test environment.

### QA handoff

Independent QA must review this correction diff against the failed findings,
then certify the static contracts again. A subsequent implementation session
still requires explicit approval to build/connect a safe Gold writer and to run
any bounded runtime verification. Until then, do not approve production
backfill or promotion.

## Historical Spark Gold flag propagation correction (2026-11-15)

### Approved scope

- Forward the non-secret Airflow environment flags
  `GOLD_DIMENSIONAL_MODE`, `GOLD_DIMENSIONAL_ENABLED`, and
  `GOLD_DIMENSIONAL_WRITE_ENABLED` from `create_historical_spark_operator` to
  the pinned Spark container.
- Preserve `HISTORICAL_BACKFILL_WRITE_APPROVED` and all existing fail-closed
  defaults/guards.
- Add a regression test for the actual operator `private_environment`.
- Run focused unit tests, Python compilation, and `git diff --check` only.

### Implementation evidence

- `airflow/dags/pipeline_operators.py` now forwards all three flags with
  fail-closed defaults (`DRY_RUN`/`false`/`false`).
- `tests/test_lakehouse_safety_guards.py` verifies the historical Gold
  operator forwards the configured values together with the historical write
  approval flag.
- Focused command:
  `.venv\Scripts\python.exe -m pytest -q --basetemp
  .pytest-tmp\historical-env-flags-3
  tests/test_lakehouse_safety_guards.py::test_historical_spark_factory_forwards_gold_dimensional_flags
  tests/test_historical_backfill_contracts.py tests/test_airflow_parse_imports.py`
  — **75 passed, 1 warning**, exit code 0. The warning is the existing
  PySpark/pandas compatibility warning; Windows emitted a post-test temp
  cleanup `ERROR: Access denied` after the successful summary.
- `.venv\Scripts\python.exe -m compileall -q airflow/dags/pipeline_operators.py
  tests/test_lakehouse_safety_guards.py tests/test_historical_backfill_contracts.py
  tests/test_airflow_parse_imports.py` — **passed**.
- `git diff --check` — **passed**; only pre-existing LF/CRLF normalization
  warnings were emitted for unrelated dirty files.

### Runtime and QA status

- No runtime retry, Spark/MinIO/Nessie/PostgreSQL client, object/ref mutation,
  checkpoint/watermark change, promotion, cutover, or destructive operation
  was performed.
- Independent QA is still required against this implementation diff before
  advancing the historical phase. Runtime certification remains a separate
  approval gate.

## QA round 3 narrow correction (2026-11-14)

- Corrected `gold/implementation/historical_ref_promotion.py` so boolean values
  are rejected explicitly for numeric evidence fields `snapshot_id`,
  `row_count`, `file_count`, and `file_bytes` (Python `bool` is an `int`
  subclass).
- Added focused regression coverage for both `True` and `False` for each of
  the four fields in `tests/test_historical_backfill_contracts.py`.
- Verification evidence: `.venv\\Scripts\\python.exe -m pytest -q
  --basetemp .pytest-tmp\\qa-round-3
  tests/test_historical_backfill_contracts.py -k "candidate"` — **18 passed,
  10 deselected**; `.venv\\Scripts\\python.exe -m compileall -q common
  ingestion silver gold airflow` — **passed**; `git diff --check` — **passed**
  (pre-existing LF/CRLF warnings only).
- Runtime gate remains **NOT CERTIFIED**. No production write, bucket/ref
  creation, delete/reset, checkpoint mutation, Iceberg cutover, or promotion
  was performed.

## Safe Gold writer + physical read-back integration (2026-11-14)

Approved scope implemented:

- Added `gold/implementation/historical_gold_writer.py` as a historical-only
  adapter from six verified Silver lineage artifacts to the existing native
  Gold writer.
- The adapter validates production identity before constructing Spark, MinIO or
  Nessie clients, requires `gold_prod_backfill_<run-id>`, and rejects
  `gold_prod_current`.
- WRITE returns only after the existing writer's pinned physical read-back and
  completed graph verification pass for exactly seven tables. Evidence includes
  `snapshot_id`, `row_count`, `file_count`, `file_bytes`, manifest/readback
  identities, and `readback_status=PASS`; candidate hash is read back from the
  candidate ref only.
- The historical DAG now consumes explicit verified Silver artifact XCom and
  calls this adapter in WRITE mode. Runtime construction remains fail-closed;
  DRY_RUN cannot produce `VERIFIED` or promote anything.

Changed files:

- `gold/implementation/historical_gold_writer.py`
- `airflow/dags/historical_production_backfill.py`
- `tests/test_historical_backfill_contracts.py` (focused regressions pending)

Evidence/status:

- No Spark/MinIO/Nessie client was constructed by this session.
- No production bucket/object/ref was created or written; no stable ref,
  watermark, checkpoint, delete/reset, or Iceberg cutover was touched.
- Physical runtime read-back is **NOT CERTIFIED** until an explicitly approved
  bounded runtime execution is performed.

Unresolved runtime risks:

- Airflow/DagBag and Spark/Nessie/MinIO integration remain environment-gated.
- Historical Silver task must be supplied with six verified artifact envelopes
  by the upstream historical Silver implementation before WRITE can proceed.
- Candidate ref hash read-back requires `NESSIE_URI`; absence fails closed.

Verification checkpoint after integration:

- `.venv\Scripts\python.exe -m pytest -q --basetemp
  .pytest-tmp\historical-gold-writer-final
  tests/test_historical_backfill_contracts.py tests/test_airflow_parse_imports.py`
  — **50 passed, exit code 0**, with one existing PySpark pandas compatibility
  warning. Windows printed `ERROR: Access denied` during temp cleanup after the
  passing summary; no test assertion failed.
- `.venv\Scripts\python.exe -m compileall -q common ingestion silver gold
  airflow` — **passed**.
- `git diff --check` for the changed integration/plan files — **passed**.

Current gate: **static/unit integration complete; physical runtime and
independent QA remain open**. No production runtime command was run.

## Independent QA review — safe Gold writer/readback integration (2026-11-14)

- Verdict: **FAIL** (static/unit evidence passes; runtime certification remains
  **NOT CERTIFIED**).
- Verified positive contracts: candidate-only environment/ref guard before the
  default runtime factory, stable-ref rejection, fail-closed unavailable
  runtime, explicit six-artifact lineage input, seven-table physical output
  coverage, candidate Nessie hash readback, strict promotion evidence
  validation, and no fabricated `VERIFIED` result on DRY_RUN.
- Blocking findings: the historical DAG hard-codes `dry_run=True`, so its
  WRITE branch and Gold adapter are unreachable through Airflow; its Bronze and
  Silver tasks do not produce the six verified Silver artifact envelopes that
  the Gold XCom contract requires. The adapter also labels
  `readback_identity` with the manifest identity rather than an independently
  read-back identity, weakening evidence independence.
- Test evidence: `.venv\\Scripts\\python.exe -m pytest -q
  tests/test_historical_backfill_contracts.py` — **31 passed**;
  `.venv\\Scripts\\python.exe -m compileall -q common ingestion silver gold
  airflow` — **passed**; `git diff --check` — **passed**. A PySpark pandas
  compatibility warning and post-test Windows temp cleanup `ERROR: Access
  denied` were observed without failed assertions.
- No Airflow runtime/DagBag, Spark, MinIO, Nessie, PostgreSQL, production
  write, bucket/ref creation, delete/reset, checkpoint mutation, cutover or
  promotion was executed. Static QA must not be promoted to runtime
  certification.
- Required next step: correct the DAG's executable WRITE/XCom integration and
  independently source/validate readback identity, then repeat independent QA
  before any runtime-write approval.

## QA round 4 correction implementation (2026-11-14)

- Replaced the historical DAG's hard-coded dry-run construction with explicit
  `HISTORICAL_BACKFILL_MODE` resolution. The default is `DRY_RUN`; `WRITE`
  requires `HISTORICAL_BACKFILL_WRITE_APPROVED=1`, production storage identity,
  the run-scoped candidate ref, and no E2E/quality flags.
- Added a strict six-table Silver artifact boundary. Missing, placeholder, or
  non-`PASS` envelopes fail closed before Gold; no `VERIFIED` artifact is
  fabricated or copied through XCom.
- Moved the `NESSIE_URI` check ahead of the historical writer runtime factory,
  so Spark, MinIO, and Nessie clients are not constructed without catalog
  configuration.
- Historical Gold output now requires `readback_identity` from the physical
  readback payload. The adapter never copies `manifest_identity`; if the
  existing native writer does not provide an independent identity, it fails
  closed and cannot claim `VERIFIED`.

Verification executed:

- `.venv\\Scripts\\python.exe -m pytest -q --basetemp
  .pytest-tmp\\historical-qa-fix tests/test_historical_backfill_contracts.py
  tests/test_airflow_parse_imports.py` — **53 passed**, one known PySpark
  pandas compatibility warning; post-test Windows temp cleanup emitted
  `ERROR: Access denied` after the passing summary.
- `.venv\\Scripts\\python.exe -m compileall -q common ingestion silver gold
  airflow tests` — **passed**.
- `git diff --check` — **passed**; only pre-existing LF/CRLF warnings.

Safety evidence: no Spark/MinIO/Nessie/PostgreSQL client was created by this
session; no production bucket/ref/object was created or written, no delete,
reset, checkpoint mutation, Iceberg cutover, or stable-ref promotion occurred.

Current gate: implementation correction complete; independent reviewer QA is
required against this diff before any runtime-write approval. Runtime physical
readback remains **NOT CERTIFIED** until an explicitly approved bounded run.

## QA round latest correction (2026-11-14)

Implemented only the requested QA fixes:

- Historical Silver validation now requires exactly six real envelopes with
  source table, `status=PASS`, non-empty artifact identity, object/path or
  positive snapshot identity, batch/backfill identity, checksum/manifest
  identity, positive non-boolean `row_count`, and rejects placeholder markers.
- Native `read_table_readback` now derives a deterministic SHA-256 identity
  from the verified physical readback evidence (`table`, snapshot, file and
  row counts, status). Historical candidate evidence carries this identity;
  candidate validation rejects copying `manifest_identity` into it.
- The historical DAG is explicitly contract-only at its current boundary.
  It emits `BLOCKED_REAL_SILVER_REQUIRED` until upstream supplies all six real
  envelopes; it never fabricates `PLANNED`/`VERIFIED` Silver artifacts. WRITE
  fails closed when those envelopes are absent.

Verification evidence:

- `.venv\Scripts\python.exe -m pytest -q --basetemp
  .pytest-tmp\qa-round-latest-2 tests/test_historical_backfill_contracts.py
  tests/test_gold_dimensional_writer.py` — **56 passed**, one known PySpark
  pandas compatibility warning; Windows emitted post-test temp cleanup
  `ERROR: Access denied` after the passing summary.
- `.venv\Scripts\python.exe -m compileall -q common ingestion silver gold
  airflow tests` — **passed**.
- `git diff --check` — **passed**; only pre-existing LF/CRLF warnings were
  emitted.
- Independent QA review against the implementation diff remains pending.

Exact limitation: no historical extractor/Bronze/Silver runtime is wired into
this DAG, and no Spark/MinIO/Nessie/PostgreSQL runtime or physical readback was
executed. Therefore this is static/unit contract evidence only, not full E2E or
runtime certification. No production write, bucket creation, delete/reset,
checkpoint mutation, Iceberg cutover, or stable promotion occurred.

### QA Reviewer Handoff

Review the latest diff against this plan. Confirm the six-envelope validator
cannot accept placeholders or boolean/zero row counts; confirm native readback
identity is generated after pinned physical reads and differs from manifest
identity; and confirm the DAG cannot complete WRITE without six real upstream
Silver envelopes. Keep runtime gate and promotion approval **OPEN**.

Current checkpoint status: **PARTIAL COMPLETE — STATIC/UNIT VERIFIED, QA OPEN**.

## Session plan: historical Silver canonical lineage correction (2026-11-15)

### Runtime finding

The bounded runtime Gold candidate exposed a contract mismatch: historical
`run_historical_silver` emitted `parquet_lineage.status` inside the nested
lineage source. Gold dimensional lineage requires the exact canonical Parquet
source fields and rejects that extra field, while the artifact envelope must
retain top-level `status=PASS`.

### Approved correction scope

- Build the nested lineage through the existing
  `build_parquet_lineage_artifact` canonical adapter.
- Preserve the historical artifact envelope's top-level `status=PASS` and
  existing manifest/checksum/row-count metadata.
- Add regression coverage proving the actual six historical Silver artifacts
  build a dimensional lineage envelope and that extra nested lineage fields
  are rejected.
- Run only focused local tests, `compileall`, and `git diff --check`.
- Do not rerun or write external services, mutate objects, overwrite runs,
  promote refs, reset checkpoints/watermarks, or enable Iceberg cutover.

### Planned files and acceptance criteria

- `ingestion/historical/pipeline.py`
- `tests/test_historical_pipeline.py`
- `tests/test_gold_dimensional_lineage.py`
- This plan, updated with verification and QA handoff.
- The six artifacts from the mocked historical Silver path pass
  `build_dimensional_lineage_from_artifacts`.
- Nested `parquet_lineage` contains no `status`; top-level artifact status is
  still `PASS`.
- An extra nested lineage field fails closed with `DimensionalLineageError`.

### Implementation and verification evidence

- `run_historical_silver` now uses `build_parquet_lineage_artifact` for the
  nested canonical lineage source and preserves the envelope-level `PASS`.
- The actual six mocked Silver artifacts are passed through
  `build_dimensional_lineage_from_artifacts` in regression coverage.
- `.venv\Scripts\python.exe -m pytest -q --basetemp
  .pytest-tmp\historical-lineage-contract tests/test_historical_pipeline.py
  tests/test_gold_dimensional_lineage.py` — **41 passed**.
- `.venv\Scripts\python.exe -m compileall -q ingestion/historical
  gold/implementation/dimensional tests/test_historical_pipeline.py
  tests/test_gold_dimensional_lineage.py` — **passed**.
- `git diff --check -- ingestion/historical/pipeline.py
  tests/test_historical_pipeline.py tests/test_gold_dimensional_lineage.py
  docs/plans/historical-production-backfill-plan.md` — **passed**.
- No external-service test, runtime rerun, object mutation, overwrite,
  promotion, checkpoint/watermark reset, or Iceberg cutover was executed.

### QA Reviewer Handoff

Independent QA should review the scoped diff and confirm that the historical
Silver producer delegates nested lineage construction to the canonical helper,
that top-level `status=PASS` remains intact, and that the six-artifact positive
path plus extra-field rejection regressions cover the Gold validator boundary.
Runtime certification and promotion remain separate gates.

### Independent QA verdict — historical Silver canonical lineage correction (2026-11-15)

- Verdict: **PASS WITH WARNINGS** for the scoped static/unit review.
- Verified `ingestion/historical/pipeline.py:263-280` delegates nested lineage
  construction to `build_parquet_lineage_artifact`, preserves envelope-level
  `status=PASS`, and returns exactly six validated artifacts.
- Verified `tests/test_historical_pipeline.py:213-226` sends the actual six
  mocked Silver envelopes through `build_dimensional_lineage_from_artifacts`
  and asserts that nested sources contain no `status`.
- Verified `tests/test_gold_dimensional_lineage.py:371-387` rejects an extra
  nested lineage field with `DimensionalLineageError`.
- Evidence: focused suite **41 passed**; scoped `compileall` **PASS**; scoped
  `git diff --check` **PASS**.
- Warning: the worktree contains unrelated pre-existing changes, including
  modified `ingestion/incremental/*`; attribution of those changes to this
  correction is not possible from the current dirty worktree. The reviewed
  historical pipeline itself has no incremental import or watermark reference.
- No runtime Gold rerun, external-service write, object mutation, bucket
  creation, delete/reset, checkpoint/watermark reset, promotion, or Iceberg
  cutover was executed. Runtime Gold certification remains a separate gate.

## Historical extractor -> Bronze -> Silver wiring (2026-11-14)

Approved scope implemented in historical-only code:

- Added `ingestion/historical/pipeline.py` with guarded `run_historical_bronze`
  and `run_historical_silver` callables.
- The historical DAG now calls those real callables across its extract,
  Bronze validation/upload and Silver transform/manifest task boundaries.
- Default `DRY_RUN` returns before PostgreSQL, MinIO or Spark client creation.
  `WRITE` validates the production bucket/layout, run-scoped candidate ref and
  `HISTORICAL_BACKFILL_WRITE_APPROVED=1` before clients are constructed.
- Source extraction is one fixed `REPEATABLE READ READ ONLY` snapshot with
  inclusive `window_start`, fixed `capture_end`, keyset continuation and
  parent-first order. No incremental watermark/checkpoint module is imported
  or mutated.
- Bronze uses canonical `bronze/<table>/run_id=<run-id>/` objects with
  create-if-absent upload and checksum/row-count manifests. FK parity is
  checked before publication.
- Silver reads the verified Bronze objects with Spark, writes Parquet using
  `errorifexists`, validates row counts and non-empty output, publishes a
  manifest identity, and produces exactly six `parquet_lineage` envelopes.
- The existing historical Gold adapter receives only those six envelopes in
  WRITE mode. It remains candidate-only and fail-closed when runtime/catalog
  dependencies are unavailable; no stable promotion is wired or executed.

Verification evidence:

- `.venv\\Scripts\\python.exe -m pytest -q --basetemp
  .pytest-tmp\\historical-final-4 tests/test_historical_pipeline.py
  tests/test_historical_backfill_contracts.py tests/test_airflow_parse_imports.py`
  — **61 passed**, one known PySpark pandas compatibility warning. Windows
  emitted post-test temp cleanup `ERROR: Access denied` after the passing
  summary; no assertion failed.
- `.venv\\Scripts\\python.exe -m compileall -q common ingestion silver gold
  airflow tests` — **passed**.
- `git diff --check` — **passed**; only pre-existing LF/CRLF warnings.

Runtime limitations and gate status:

- No Airflow `DagBag` runtime, PostgreSQL snapshot, MinIO upload, Spark Silver
  write, Nessie readback, Gold candidate write, bucket/ref creation, delete,
  reset, checkpoint mutation, Iceberg cutover or stable promotion was run.
- The Silver runtime requires actual Spark/MinIO availability and fails closed
  otherwise. The production WRITE branch is contract-tested only.
- The worktree contains unrelated pre-existing changes; no incremental source
  files were intentionally modified by this implementation.

### QA Reviewer Handoff — latest wiring

Review `ingestion/historical/pipeline.py`,
`airflow/dags/historical_production_backfill.py`, and
`tests/test_historical_pipeline.py` against the approved scope. Confirm that
DRY_RUN cannot construct clients, WRITE guards run before factories, source
snapshot and parent/FK checks are preserved, and exactly six real Silver
lineage envelopes reach the existing Gold adapter. Keep physical runtime and
promotion gates **OPEN** until an explicitly approved bounded runtime test is
performed.

### Independent QA — actual historical extractor -> Bronze -> Silver wiring (2026-11-14)

- Verdict: **FAIL (static)**; physical runtime gate remains **NOT CERTIFIED**.
- Verified: focused historical suite **42 passed**; `.venv\\Scripts\\python.exe -m compileall -q common ingestion silver gold airflow` passed; `git diff --check` passed.
- Verified positive contracts: DRY_RUN returns before client factories; WRITE guard resolution precedes factories; fixed `capture_end`, `REPEATABLE READ READ ONLY`, inclusive start boundary, keyset continuation, parent-first extraction and FK checks; Silver uses `errorifexists`; DAG failure paths propagate and Gold receives a six-table artifact map in the WRITE path.
- Blocking findings: Bronze validates the uploaded data checksum but does not read back and validate the persisted manifest or independently count rows from the persisted object; Silver validates only pre-write Spark count plus non-empty object listing and does not read back persisted Parquet rows/checksum/manifest identity. These leave the requested Bronze/Silver physical integrity acceptance criteria unproven.
- Tests are contract/unit evidence only. No PostgreSQL, MinIO, Spark, Nessie, Airflow DagBag/runtime, production write, bucket/ref creation, delete/reset, checkpoint mutation, Iceberg cutover or promotion was executed.
- Required next step: add focused mocked read-back/failure tests and implementation coverage for Bronze manifest/data/row-count reconciliation and Silver persisted-output read-back, then repeat independent QA before any runtime-write approval.

### Independent QA final — persisted Bronze/Silver read-back correction (2026-11-14)

- Verdict: **PASS WITH WARNINGS** for static/unit QA; physical runtime gate
  remains **NOT CERTIFIED** and separate.
- Verified Bronze persisted data read-back with recomputed canonical object key,
  byte checksum, CSV row count, and persisted manifest field/status
  reconciliation. Verified Silver Spark Parquet read-back, row-count
  reconciliation, persisted-object checksums, exact manifest/identity
  reconciliation, and retained `errorifexists`.
- Verified Spark initialization cleanup attempts `stop()` for both
  `configure_s3a` and MinIO-client failures while preserving the original
  exception as the cause. DRY_RUN returns before PostgreSQL/MinIO/Spark
  construction, and Silver returns exactly six verified envelopes.
- Evidence: requested pytest command **48 passed, 1 warning**; compileall for
  `common ingestion silver gold airflow tests` **passed**; scoped and full
  `git diff --check` **passed**. Pytest emitted Windows post-summary `ERROR:
  Access denied` during temp cleanup without assertion failures.
- Warning: mocked coverage does not independently exercise missing/extra
  Silver object-set drift or every manifest payload-field mismatch; no live
  MinIO/Spark/PostgreSQL/Airflow runtime was executed. These do not block the
  static verdict but keep physical read-back and runtime certification open.
- Safety: no external/production write, bucket creation, deletion, reset,
  checkpoint mutation, Iceberg cutover or promotion was performed.

Next step: obtain separate explicit approval for a bounded non-production
runtime read-back test, if desired; do not treat this static verdict as runtime
certification or consumer cutover approval.

## Session plan: Silver readback manifest-prefix regression (2026-11-14)

### Runtime finding

The historical Silver persisted readback currently calls `spark.read.parquet(target)`
where `target` is the run data prefix. That prefix also contains `manifest.json`,
so a real Spark read can fail with `CANNOT_READ_FILE_FOOTER` when it attempts to
interpret the JSON manifest as a Parquet object. The existing object checksum
helper filters to `.parquet`, but the Spark readback path does not.

### Proposed approved scope — pending user approval

- Change only `_verify_silver_readback` so Spark reads the verified Parquet object
  paths (or an equivalent data-only prefix) and never `manifest.json`.
- Preserve the existing object-set, per-object checksum, manifest identity and
  row-count checks; reject an empty or mismatched verified object set.
- Add mocked regression coverage proving a manifest in the run prefix is ignored,
  Parquet readback remains validated, and empty/mismatched object sets fail closed.
- Do not modify incremental ingestion, external services, production objects,
  checkpoints, watermarks, Iceberg cutover or runtime promotion.

### Acceptance criteria

- A prefix containing valid Parquet plus `manifest.json` passes Silver readback,
  and the Spark call receives only verified Parquet paths/data scope.
- Empty or mismatched Parquet object sets fail before a verified artifact is
  returned; checksum/manifest/row-count mismatch failures remain preserved.
- Focused tests, `compileall`, and `git diff --check` pass.
- Independent QA reviews the implementation diff; runtime promotion remains
  blocked until QA and any separate runtime approval are complete.

### Planned files and verification

- `ingestion/historical/pipeline.py`
- `tests/test_historical_pipeline.py`
- This plan, updated with implementation evidence and independent QA verdict
  after the approved change.
- Planned commands: `.venv\\Scripts\\python.exe -m pytest -q
  tests/test_historical_pipeline.py`; `.venv\\Scripts\\python.exe -m
  compileall -q ingestion tests`; `git diff --check`.

### Risk and QA gate

The main risk is accidentally weakening the verified object-set/checksum
contract while excluding the manifest. The implementation must keep readback
bounded to the manifest-declared Parquet objects and fail closed on empty or
unexpected objects. No external-service test or production rerun is authorized
in this session. Current status: **PLAN READY — USER APPROVAL REQUIRED;
RUNTIME PROMOTION BLOCKED**.

## Implementation update: Silver readback manifest-prefix correction (2026-11-14)

- `_verify_silver_readback` now lists persisted `.parquet` objects under the
  bounded data prefix, requires a non-empty exact match with the
  manifest-declared object set, and passes only those object paths to
  `spark.read.parquet`. `manifest.json` is never included in the Spark read.
- Added regression coverage for a manifest-containing prefix and for empty or
  mismatched persisted Parquet object sets.
- Incremental ingestion, checkpoints, watermarks, production objects and
  external runtime state were not modified or rerun.

Verification evidence and independent QA verdict are recorded below. Runtime
promotion remains blocked.

### Implementation verification

- `.venv\\Scripts\\python.exe -m pytest -q --basetemp
  .pytest-tmp\\historical-silver-readback-fix-2
  tests/test_historical_pipeline.py` — **13 passed**.
- `.venv\\Scripts\\python.exe -m compileall -q ingestion tests` — **passed**.
- `git diff --check -- ingestion/historical/pipeline.py
  tests/test_historical_pipeline.py
  docs/plans/historical-production-backfill-plan.md` — **passed**.
- No external-service test, production rerun, object mutation, checkpoint reset,
  Iceberg cutover or promotion was executed.

### QA Reviewer Handoff — pending independent review

Review only the bounded Silver readback correction and its focused tests. Confirm
that Spark receives only manifest-declared `.parquet` object paths, a prefix
manifest is ignored, and empty/extra object sets fail closed before artifact
verification. Keep runtime promotion **BLOCKED** until independent QA signs off.

## Session plan: historical shipments CSV multiline regression (2026-11-14)

### Runtime finding

The historical shipments Bronze CSV contains quoted fields with embedded newlines.
With Spark CSV's default `multiLine=false`, the observed Silver read count was
49,170 instead of the Bronze manifest's 24,587 records. This finding is scoped
to `run_historical_silver`; incremental ingestion remains unchanged.

### Approved implementation and acceptance criteria

- Add `.option("multiLine", "true")` to the historical Spark CSV reader while
  preserving `header=true` and `inferSchema=true`.
- Add regression coverage proving the option is passed and a row-count mismatch
  fails closed before a verified Silver artifact is returned.
- Do not alter Bronze data, delete/overwrite existing runs, rerun runtime,
  mutate incremental watermarks/checkpoints, enable Iceberg cutover, or run any
  external-service test.

### Implementation evidence

- Changed `ingestion/historical/pipeline.py` only in the historical CSV reader;
  `multiLine=true` is now passed alongside the existing header/schema options.
- Changed `tests/test_historical_pipeline.py` to record CSV options and cover both
  the multiline option contract and a mismatched CSV row count.
- No incremental source, Bronze object, existing run, checkpoint, watermark, or
  external service was modified or rerun.

Verification executed:

- `.venv\\Scripts\\python.exe -m pytest -q --basetemp
  .pytest-tmp\\historical-multiline-fix tests/test_historical_pipeline.py` —
  **14 passed**.
- `.venv\\Scripts\\python.exe -m compileall -q ingestion tests` — **passed**.
- `git diff --check -- ingestion/historical/pipeline.py
  tests/test_historical_pipeline.py` — **passed**.

### QA Reviewer Handoff

Review the implementation diff against this session scope. Confirm that
`run_historical_silver` passes `multiLine=true` without changing `header` or
`inferSchema`, that row-count mismatch remains fail-closed, and that no
incremental/runtime state was touched. Runtime rerun, Bronze mutation, existing
run overwrite, checkpoint/watermark changes, Iceberg cutover and promotion
remain **BLOCKED**. Independent QA verdict is required before advancing the
historical phase.

## Session plan: Gold child parent-coverage completeness correction (2026-11-15)

### Approved scope

- Correct `gold/implementation/dimensional/coverage.py` so `payments` and
  `shipments` require both `parent_coverage_identity` and
  `parent_orders_reference`.
- Require the complete four-field parent reference and compare it exactly with
  the verified orders source reference (`table`, `batch_id`, `manifest_key`,
  `manifest_identity`).
- Add regression coverage for each child missing each required parent field;
  preserve aligned positive coverage and existing mismatch behavior.
- Run only focused unit tests, compilation and diff checks. No external
  runtime write/delete/promotion is permitted.

### Implementation evidence

- Child metadata validation is now fail-closed when either parent field is
  absent. Incomplete nested parent references remain classified as
  `parent_orders_reference_mismatch` to preserve the existing mismatch
  diagnostic contract.
- Cross-table validation now requires exact equality for the full parent
  orders reference; absent orders reference values are no longer ignored.
- Added four regressions in `tests/test_gold_coverage.py` covering
  `payments`/`shipments` × missing `parent_coverage_identity`/
  `parent_orders_reference`.
- Focused command:
  `.venv\Scripts\python.exe -m pytest -q --basetemp
  .pytest-tmp\coverage-high-fix-2 tests/test_gold_coverage.py
  tests/test_historical_pipeline.py` — **27 passed**.
- Compile command:
  `.venv\Scripts\python.exe -m compileall -q gold/implementation/dimensional
  tests/test_gold_coverage.py tests/test_historical_pipeline.py` — **passed**.
- `git diff --check` — **passed**, with only pre-existing LF/CRLF warnings.

No external-service test, runtime write/delete, object mutation, promotion,
checkpoint/watermark change or Iceberg cutover was executed.

### QA Reviewer Handoff

Independent QA must review only the scoped coverage validator/test/plan hunks
against the HIGH finding. Confirm both child parent fields are mandatory, the
full four-field reference is exact against orders, positive and mismatch tests
remain intact, and no runtime boundary was expanded. Historical runtime
promotion remains separately gated and is not certified by this static/unit
evidence.

## Session plan: Silver/Gold manifest contract correction (2026-11-15)

### Runtime finding

Candidate run `prodbackfill20261114g` passed Bronze and Silver, then Gold failed closed
because the persisted Silver manifest used legacy field names while the Gold Parquet reader
requires `status=COMPLETED`, `table_name`, `data_prefix`, and `parquet_objects`. A second
adjacent finding showed Gold read the whole prefix and could include `manifest.json`.

### Approved implementation and acceptance criteria

- Persist the Gold-compatible Silver manifest fields while retaining existing readback and
  lineage fields; keep manifest identity computed over the persisted payload.
- Make Gold read only manifest-declared Parquet object paths after exact inventory validation.
- Add focused regression tests; do not mutate existing immutable runs, watermarks/checkpoints,
  or stable Nessie ref.

### Implementation and QA evidence

- Silver correction: `ingestion/historical/pipeline.py` and
  `tests/test_historical_pipeline.py`; focused tests **16 passed**.
- Independent QA: **PASS WITH WARNINGS**; manifest identity, object shape, coverage and
  non-destructive guards passed. Warning: no direct external reader integration test.
- Gold reader correction: `gold/implementation/dimensional/reader.py` and
  `tests/test_gold_parquet_source.py`; focused tests **6 passed**, lineage/verification
  regressions **31 passed**, compileall and diff check passed.
- Independent QA: **PASS WITH WARNINGS**, no blocking defect; broader writer subset had
  one unrelated coverage-preflight failure.

Existing runtime candidate `g` remains immutable and is not reused. Next step is a fresh
immutable run after these corrections, followed by independent runtime evidence review.
Stable ref promotion remains **BLOCKED** pending explicit approval after candidate QA.

### Runtime evidence — candidate `h`

- Bronze: **PASS**, all six source tables; capture end fixed at
  `2026-11-14T07:20:18`; incremental watermarks/checkpoints unchanged.
- Silver: **PASS**, all six persisted Parquet outputs and manifest readbacks.
- Candidate branch `gold_prod_backfill_prodbackfill20261114h`: created from `main`.
- Gold first failed in the local harness before any Gold write because the harness looked
  for a non-persisted `manifest_identity`; corrected harness identity to the canonical hash
  of the persisted manifest.
- Gold then reached the writer but failed closed on the existing global fence:
  `gold/dimensional/locks/v1/global.json`, `PreconditionFailed`.
- Read-only inspection found the global fence plus one graph-fence object, but no completed
  graph/table manifests. This is an operator-recovery state. No lock/object was deleted or
  overwritten, and no stable ref was changed.

### Current gate

Candidate Gold certification and stable promotion remain **BLOCKED** until the stale global
fence is recovered through the approved operator procedure. Do not remove or overwrite the
fence from this session without separate explicit recovery authorization and evidence.

### Implementation evidence — global-fence recovery contract

- Updated `gold/implementation/dimensional/manifest.py` so a conditional global-fence
  conflict fails closed with `code=GLOBAL_FENCE_CONFLICT`,
  `recovery_state=GLOBAL_FENCE_RECOVERY_REQUIRED`, the deterministic `lock_key`,
  requested `graph_identity`, and read-only `lock_metadata` for operator diagnosis.
- The conflict path never calls `remove_object`, releases, overwrites, or retries the
  existing fence. Unexpected lock metadata remains a hard recovery conflict.
- Added regressions in `tests/test_gold_dimensional_recovery_contract.py` and
  `tests/test_gold_dimensional_writer.py` for conflict classification, recovery state,
  lock metadata, non-release, and fail-closed behavior.
- Corrected the coverage-preflight regression fixture with orders `batch_id`,
  `manifest_key`, and `manifest_identity`, plus exact `parent_orders_reference`
  values for payments and shipments; payments retains `runtime_window_id=window-2`
  so the test asserts the intended `runtime_window_mismatch` diagnostic.
- Focused related command:
  `.venv\Scripts\python.exe -m pytest -q tests/test_gold_dimensional_recovery_contract.py
  tests/test_gold_dimensional_writer.py -k "global_fence or graph_fence or
  recovery_classifier or partial_preflight"` — **10 passed, 17 deselected** (prior
  related run; superseded by the full scoped result below).
- Full scoped command:
  `.venv\Scripts\python.exe -m pytest -q tests/test_gold_dimensional_recovery_contract.py
  tests/test_gold_dimensional_writer.py` — **27 passed** after the QA fixture correction.
- Compile command:
  `.venv\Scripts\python.exe -m compileall -q gold/implementation/dimensional/manifest.py
  tests/test_gold_dimensional_recovery_contract.py tests/test_gold_dimensional_writer.py`
  — **passed**.
- `git diff --check` — **passed** (only pre-existing LF/CRLF warnings).

No runtime WRITE, MinIO/Nessie/PostgreSQL/Iceberg mutation, candidate retry,
promotion/cutover, fence release, checkpoint/watermark change, or commit was performed.

## Implementation update: historical Spark boundary correction (2026-11-15)

Approved scope implemented:

- Removed module-level historical Gold writer imports from the Airflow DAG.
- Historical Silver and Gold tasks now use `HistoricalSparkResultOperator` and
  `spark-submit` in `ecommerce-spark:3.5.6`; Airflow XCom carries only compact
  plan, Bronze identity and verified Silver artifact JSON envelopes.
- Added `gold/implementation/historical_gold_cli.py`. It validates JSON shape,
  parent-first six-table scope, candidate ref, production bucket and
  `HISTORICAL_BACKFILL_WRITE_APPROVED=1` before lazy Spark/MinIO/Nessie or
  dimensional-writer imports. Stable `gold_prod_current` is rejected. Successful
  Gold output is candidate-only `VERIFIED` with exactly seven physical outputs
  and `readback_status=PASS`; no stable ref promotion is performed.
- `PRODUCTION_LAKEHOUSE_WRITE_APPROVED`, incremental DAG behavior, watermarks,
  checkpoints and existing runtime guards were not changed.

Verification for this correction:

- Focused historical/Airflow contract tests, `compileall`, and `git diff --check`
  are the only permitted checks for this session. No Docker runtime backfill or
  external-service mutation was executed.

QA handoff: independently review the DAG-to-Spark operator boundary, compact
XCom payloads, CLI preflight ordering, stable-ref rejection and seven-output
readback contract. Runtime certification and promotion remain blocked.

Correction evidence (2026-11-15): updated historical DAG contract assertions to
the Spark-boundary contract and moved the lazy historical writer import below
candidate, write-approval, production-storage and exact-six-artifact validation.
Focused pytest: **59 passed, 1 warning**; compileall: **passed**;
`git diff --check`: **passed**. The warning is the existing PySpark/pandas
compatibility warning; no external runtime or write was executed.

Independent QA correction evidence (2026-11-15): common historical Silver/Gold
WRITE preflight now validates candidate production environment and historical
write approval before lazy runtime imports; CLI independently validates all
seven Gold physical readback envelopes; generic Spark operator environment no
longer forwards `PRODUCTION_LAKEHOUSE_WRITE_APPROVED`. Focused pytest:
**68 passed, 1 warning**; compileall: **passed**; `git diff --check`: **passed**.
No Docker/runtime write or external mutation was executed.

Runtime XCom boundary correction evidence (2026-11-15): historical Spark
operators now receive Airflow task-output `XComArg` values and resolve them in
the worker before compact `json.dumps`; Jinja `ti.xcom_pull` indexing was
removed from the command and environment templates. Focused pytest:
**69 passed, 1 warning**; compileall: **passed**; `git diff --check`: **passed**.
The fresh runtime candidate was not retried and no runtime write was executed.

Airflow 3 XComArg type correction evidence (2026-11-15): historical DAG keeps
direct PlainXComArg indexing for TaskFlow Bronze (`bronze_result["source"]`)
and uses `.output["artifacts"]` for classic
`HistoricalSparkResultOperator` Silver output. Regression coverage forbids
Jinja `ti.xcom_pull` in the historical DAG.
Focused pytest: **69 passed, 1 warning**; compileall: **passed**;
`git diff --check`: **passed**. No runtime retry or write was executed.

Runtime f mount correction evidence (2026-11-15): Spark CLI rejected before
Gold processing because the `lakehouse` Python package was absent from the
container mount set (`No module named 'lakehouse'`). Added the read-only
`project_path\\lakehouse` -> `/opt/spark/work-dir/lakehouse` mount to
`create_spark_submit_operator` with regression coverage. Runtime f was not
retried and no write/mutation was executed.

QA mount-test correction evidence (2026-11-15): strengthened the regression
test to isolate the actual lakehouse `Mount` configuration and assert source,
target, `type="bind"`, and `read_only=True`; implementation is unchanged.
Focused pytest: **88 passed, 1 warning**; compileall: **passed**;
`git diff --check`: **passed**. No runtime retry or write was executed.

Historical Bronze XCom boundary correction evidence (2026-11-15): `bronze()`
now returns the compact CLI payload directly (`status`, `run_id`, `bronze`),
and the Silver Spark operator receives the whole TaskFlow XComArg without
`source`-key indexing or a wrapper mismatch. Focused pytest: **72 passed, 1
warning**; compileall: **passed**; `git diff --check`: **passed**. No runtime
retry or write was executed.

Runtime f mount correction evidence (2026-11-15): Spark CLI rejected before
Gold processing because the `lakehouse` Python package was absent from the
container mount set (`No module named 'lakehouse'`). Added the read-only
`project_path\\lakehouse` -> `/opt/spark/work-dir/lakehouse` mount to
`create_spark_submit_operator` with regression coverage. Focused pytest:
**88 passed, 1 warning**; compileall: **passed**; `git diff --check`:
**passed**. Runtime f was not retried and no write/mutation was executed.

Historical DAG regression correction evidence (2026-11-15): updated the stale
pipeline contract test to assert Spark operator wiring with the whole compact
`bronze_result` XComArg and no `source`-key wrapper/indexing. Focused pytest:
**88 passed, 1 warning**; compileall: **passed**; `git diff --check`:
**passed**. No runtime retry or write was executed.

Historical flag semantics correction evidence (2026-11-15): orchestration now
recognizes only `1`, `true`, `yes`, and `on` as enabled forbidden flags;
`0`, `false`, and empty values are disabled. `production_environment` guards
remain unchanged. Focused pytest: **71 passed, 1 warning**; compileall:
**passed**; `git diff --check`: **passed**. No runtime retry or write was
executed.

Historical extract XCom compaction evidence (2026-11-15):
`extract_historical_snapshot` now returns only JSON-safe Bronze metadata
(`status`, `run_id`, table, object and manifest identities, row count and
checksum); raw `records` are excluded before XCom serialization. Focused
pytest: **72 passed, 1 warning**; compileall: **passed**;
`git diff --check`: **passed**. Existing run artifacts were not modified and
no runtime retry/write was executed.

Historical Gold XCom output correction evidence (2026-11-15): candidate
verification now consumes `gold_result.output` from the classic
`HistoricalSparkResultOperator`; Silver retry behavior is unchanged. Focused
pytest: **72 passed, 1 warning**; compileall: **passed**;
`git diff --check`: **passed**. No runtime retry or write was executed.

Runtime g Gold XCom boundary correction (2026-11-15): runtime evidence showed
Silver succeeded but Gold failed before Spark with `XComNotFound key='artifacts'`.
The Airflow 3 classic `HistoricalSparkResultOperator` returns its complete
structured result under `return_value`, so the DAG now passes
`silver_result.output` to Gold. `historical_gold_cli.py` unwraps the accepted
Silver envelope (`status=PASS`, `artifacts={...}`) before exact six-artifact
validation and preserves direct artifact-map compatibility.

Approved verification scope: add regression coverage for whole-output classic
operator wiring and both Silver input shapes; run focused local tests,
`compileall`, and `git diff --check`. Runtime g was not retried. No MinIO,
Nessie, PostgreSQL, Spark, checkpoint, watermark, object, candidate, or stable
ref mutation was performed.

Implementation verification for runtime g boundary fix: focused command
`.venv\Scripts\python.exe -m pytest -q tests/test_historical_backfill_contracts.py
tests/test_historical_pipeline.py` — **67 passed, 1 warning**, exit code 0.
The existing PySpark/pandas compatibility `FutureWarning` remains; pytest also
prints a post-run `ERROR: Access denied` after the passing summary, but the
process exits successfully and no test failure is reported. Scoped
`.venv\Scripts\python.exe -m compileall -q
airflow/dags/historical_production_backfill.py
gold/implementation/historical_gold_cli.py
tests/test_historical_backfill_contracts.py` — **passed**.
`git diff --check` — **passed**; only pre-existing LF/CRLF normalization
warnings for unrelated dirty files were emitted. QA handoff: independently
review the classic Airflow 3 `return_value` wiring, envelope unwrapping and
direct artifact compatibility; runtime g remains not retried and runtime
certification/promotion remain blocked.

Historical Airflow parse assertion correction evidence (2026-11-15): fixed the
stale regression assertion in `tests/test_airflow_parse_imports.py` so it now
requires the complete `input_value=silver_result.output` XComArg and explicitly
forbids indexed `silver_result.output["artifacts"]` wiring. The historical DAG
was not changed; no runtime retry or write was executed.

Approved verification scope: run only
`tests/test_historical_backfill_contracts.py` and
`tests/test_airflow_parse_imports.py`, `compileall`, and `git diff --check`.
Verification evidence: `.venv\Scripts\python.exe -m pytest -q
tests/test_historical_backfill_contracts.py tests/test_airflow_parse_imports.py`
— **74 passed, 1 warning**, exit code 0. The existing PySpark/pandas
compatibility `FutureWarning` remains; pytest also prints a post-run
`ERROR: Access denied` after the passing summary, but no test failure is
reported. Scoped `compileall` — **passed**; `git diff --check` — **passed**.
No runtime retry or write was executed. Independent QA must review this
test-only correction against the historical DAG wiring before the phase advances.

## Historical Gold auto-expand date coverage (2026-11-15)

### Approved scope implemented

- Preserved the existing `updated_at` Silver extraction contract.
- Added `gold/implementation/dimensional/date_coverage.py` to validate
  `orders.order_date`, derive observed UTC calendar-date min/max from verified
  Silver, and expand configured capture bounds inclusively.
- Null/invalid dates fail with deterministic JSON diagnostics containing column,
  null/invalid counts and a stable order-id-ordered sample.
- Historical Gold performs Silver verification and date preflight before target
  creation or `MERGE`; effective bounds feed `dim_date` generation.
- Effective, configured and observed bounds are included in the deterministic
  graph manifest body and therefore graph identity.
- Existing non-historical recovery/fence ordering remains backward-compatible;
  expanded preflight is enabled only when the historical writer supplies
  explicit capture bounds.

### Focused verification

- `.venv\Scripts\python.exe -m pytest -q --basetemp
  .pytest-tmp\date-coverage-4 tests/test_historical_gold_date_coverage.py
  tests/test_gold_dimensional_writer.py tests/test_historical_backfill_contracts.py`
  — **82 passed, 1 existing PySpark/pandas warning**. The pinned
  `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` environment was required on Windows;
  the first unpinned attempt had one Spark worker launch failure
  (`python3`, `CreateProcess error=1920`) and was not an assertion failure.
- Coverage includes inclusive and out-of-range expansion, null/invalid
  diagnostics, UTC boundary conversion, customer FK/unknown-member resolution,
  and preflight-before-target/merge ordering.
- No runtime/external service test, object/ref mutation, checkpoint/watermark
  change, promotion, cutover or destructive operation was performed.

### Remaining risks and QA handoff

- Independent reviewer must inspect the scoped date-coverage diff and confirm
  the historical path consumes verified Silver before creating targets, while
  non-historical recovery semantics remain unchanged.
- Runtime Spark/MinIO/Nessie/PostgreSQL certification remains a separate gate
  and is not approved or complete.

## Nessie create-reference contract correction (2026-11-15)

Runtime evidence from the existing BI adapter and tests establishes the Nessie
0.108 contract as `POST /api/v2/trees?name=<ref>&type=BRANCH` with the detached
body `{type: DETACHED, hash: <captured-base-hash>}`. The historical adapter now
uses this exact v2 query/body shape; `get_ref` and `update_branch` remain on v2.
The captured base hash is preserved verbatim after validation.

Only HTTP 409 is translated to `NessieReferenceConflict` and is eligible for
the existing read-back fallback. HTTP 400 and all other failures propagate
without retry or fallback mutation. No candidate `k` retry or runtime write was
performed.

Focused local verification is limited to the historical contract tests,
`compileall`, and `git diff --check`. No Nessie, MinIO, PostgreSQL, Spark,
candidate, Gold, watermark, checkpoint, promotion, cutover, or cleanup
operation is permitted.

Verification executed:

- `.venv\Scripts\python.exe -m pytest -q --basetemp .pytest-tmp\nessie-v2-create-reference tests/test_historical_backfill_contracts.py tests/test_airflow_parse_imports.py` — **86 passed**.
- `.venv\Scripts\python.exe -m compileall -q ingestion/historical gold/implementation/historical_ref_promotion.py tests/test_historical_backfill_contracts.py airflow/dags/historical_production_backfill.py` — **passed**.
- `git diff --check` — **passed**; unrelated pre-existing dirty files emitted only
  Git LF/CRLF normalization warnings.
- `.venv\Scripts\python.exe -m compileall -q ingestion/historical gold/implementation/historical_ref_promotion.py tests/test_historical_backfill_contracts.py airflow/dags/historical_production_backfill.py` — **passed**.
- `git diff --check` — **passed**; only pre-existing Git LF/CRLF normalization warnings were emitted for unrelated dirty files.

QA handoff for the correction was completed before the runtime session:
independently review the v2 create/v2 read-update separation, exact
`name`/`type` query and `DETACHED` body, captured-hash preservation, HTTP 400
no-retry regression, HTTP 409-only conflict signal, and scoped diff. The later
runtime certification is recorded below; `demo20261114k` remains unretried.

## Runtime certification and stable initialization (2026-11-16)

- Immutable candidate run `prodbackfill20261115m` completed successfully after
  the Nessie v2 reference-contract correction.
- Gold integration verification returned `VERIFIED`; all seven physical Gold
  outputs passed read-back and consistency checks: `dim_date`, `dim_customer`,
  `dim_product`, `fact_orders`, `fact_order_items`, `fact_payments`, and
  `fact_shipments`.
- Candidate ref
  `gold_prod_backfill_prodbackfill20261115m` is a `BRANCH` at hash
  `adb060d0499e2cfd0fd38d28b3c6c3b85ef5c1aedd54238fbe3a7d652f0e196e`.
- With explicit user approval, the previously absent `gold_prod_current` ref
  was initialized from that verified candidate hash. Read-back confirms both
  refs are `BRANCH` refs with the same hash.
- This was guarded initialization, not CAS promotion: stable absence was
  asserted first, the candidate type/hash were asserted, and the stable ref was
  read back after creation. No candidate `n` was triggered.

### Remaining risks and next handoff

- The runtime is certified for this bounded candidate, but no commit, push,
  merge, consumer cutover, checkpoint reset, cleanup, or destructive
  maintenance was performed.
- The local `.env` still contains the prepared-but-unused `n` candidate
  settings; an operator must reset those values to the intended stable/current
  configuration before the next operational run.
- Credentials were exposed in earlier local diagnostics; rotate them outside
  this task and do not record replacement values in repository documentation.

## Runtime certification — full production-bucket candidate (2026-11-19)

- Corrected historical parent closure in `ingestion/historical/extractor.py` and
  `ingestion/historical/pipeline.py`: `customers` and `products` are selected
  by `created_at <= capture_end`, while orders and child tables retain the
  bounded `updated_at` window. This prevents FK loss when a parent is updated
  after the requested snapshot or was created before the ingestion start date.
- Added a regression for the parent-closure query. Focused verification:
  `tests/test_historical_backfill_contracts.py` and
  `tests/test_historical_pipeline.py` — **80 passed**; historical compileall
  passed; no incremental watermark/checkpoint mutation occurred.
- Approved run `prodbackfill20261119full01` with capture end
  `2026-11-19T02:00:00` completed successfully:
  `manual__prodbackfill20261119full01` — validate, Bronze, Silver, Gold and
  candidate verification all **success**.
- Candidate ref
  `gold_prod_backfill_prodbackfill20261119full01` is a `BRANCH` at hash
  `0a2ae6cb10d44b1508d8f212dceccbbe9f1a70fe8b2e269c58de26c427eb74bd`.
  Gold report is candidate-only (`stable_ref=null`), seven outputs have
  `readback_status=PASS` and `consistency_status=PASS`; row counts include
  `fact_orders=26714`, `fact_order_items=80318`, `fact_payments=33768`, and
  `fact_shipments=24596`.
- Production location evidence is present under the MinIO prefix
  `lakehouse/gold_run_20261112_gold_06/`; no stable publish/cutover was done.
- Restored runtime configuration: `NESSIE_REF=gold_prod_current`,
  `HISTORICAL_BACKFILL_WRITE_APPROVED=0`, `HISTORICAL_BACKFILL_MODE=DRY_RUN`.

### QA gate

Independent QA round 1 returned FAIL for two HIGH ordering/default issues.
Those were corrected: the operator default base ref is now `main`, and the
production namespace is validated before Nessie branch bootstrap/client
creation. Round 2 independent QA returned **PASS** with focused tests
`72 passed`, compileall and diff-check passed. The `/tests/` ignored finding is
accepted because the user explicitly chose not to commit the test folder.

Stable ref remains at the rollback baseline hash
`761c847646be22d5096fc62a71f9bfe6d9c44c6de7389cfc87ba88538803a4ad`; the next
action after QA is a separate explicit approval gate for any stable promotion.

## Stable promotion — approved (2026-11-19)

- User explicitly approved promotion after independent QA PASS.
- Guarded CAS promotion executed only after verifying stable hash
  `761c847646be22d5096fc62a71f9bfe6d9c44c6de7389cfc87ba88538803a4ad` and
  candidate hash `0a2ae6cb10d44b1508d8f212dceccbbe9f1a70fe8b2e269c58de26c427eb74bd`.
- Nessie result: `APPLY_CAS`; `gold_prod_current` read-back equals
  `0a2ae6cb10d44b1508d8f212dceccbbe9f1a70fe8b2e269c58de26c427eb74bd`.
- No object deletion, checkpoint reset, rollback, cleanup, commit or push was
  performed. The next operational step is normal incremental Production Write
  using the newly promoted stable ref.
