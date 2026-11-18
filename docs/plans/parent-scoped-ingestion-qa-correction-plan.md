# Parent-scoped ingestion QA correction

## PostgreSQL ARRAY binding correction after `demo20261114l`

- Root cause: tuple-shaped parent IDs were adapted as nested SQL tuple syntax,
  producing `ANY((...))` and PostgreSQL `WrongObjectType`.
- Correction: normalize `parent_order_ids` to a list before binding while
  preserving parameterized `order_id = ANY(%s)` and normal/replay ordering.
- Regression evidence: extractor tests **12 passed**, compileall **PASS**, and
  `git diff --check` **PASS**. Independent QA: **PASS WITH WARNINGS**, no
  correctness findings; warning is limited to unrelated worktree provenance.
- No runtime write or retry of `demo20261114l` occurred. A new immutable run
  is required to verify live PostgreSQL behavior.

## Approved task and scope

- Cover replay child extraction with executable assertions for `order_id = ANY(%s)` and exact parameter ordering.
- Prove `PENDING` and `UPLOADED` recovery reuses staged/Uploaded state without `extract_incremental` or a source query.
- Validate the parent orders manifest `data_prefix` against the canonical orders Silver batch/run path before Silver anti-join/read.
- Do not change Gold strictness, runtime artifacts, locks, checkpoints, or retry demos `demo20261114j`/`demo20261114k`.

## Acceptance and verification

- Focused extractor/recovery/Silver tests pass.
- Transaction-flow recovery tests pass.
- `.venv` `compileall` passes.
- `git diff --check` passes.
- No external-service or runtime write was performed.

## Session evidence

- Focused suite: `41 passed`.
- Transaction-flow suite: `26 passed` with workspace-local `--basetemp`.
- `compileall`: passed.
- `git diff --check`: passed; existing line-ending warnings only.

## QA handoff

- Implementation is ready for independent reviewer inspection against this scope.
- Runtime certification remains out of scope and still requires separate bounded-run approval.

## Session update: PostgreSQL array binding fix (2026-11-14)

### Approved narrow scope

- Convert `parent_order_ids` to a psycopg2 PostgreSQL-array-compatible binding
  while preserving parameterized `order_id = ANY(%s)` SQL.
- Preserve normal/replay upper-bound parameter ordering, empty `NO_DATA`, and
  non-child table behavior.
- Add regression coverage for both `payments` and `shipments`.
- Do not retry `demo20261114l`; do not modify Gold, generator, DAG ordering, or
  runtime artifacts.

### Verification evidence

- `.venv\Scripts\python.exe -m pytest tests/test_extractor.py -q`: `12 passed`.
- `.venv\Scripts\python.exe -m compileall -q ingestion tests`: passed.
- `git diff --check`: passed; only pre-existing LF/CRLF warnings were reported.
- Regression assertions verify psycopg2 adapts `[11, 12]` as `ARRAY[11,12]`
  and exact normal/replay parameter tuples for both child tables.

### QA handoff

- Review only `ingestion/incremental/extractor.py` and the focused additions in
  `tests/test_extractor.py` against this narrow scope.
- No external-service or runtime write was performed; independent reviewer
  sign-off remains the next gate.

## Session update: historical Bronze/Silver persisted read-back corrections (2026-11-14)

### Approved task and scope

- Verify Bronze persisted data and manifest after WRITE, including independently
  recomputed object key, checksum and row count.
- Verify Silver persisted Parquet by reading it back with Spark, reconciling row
  count, per-object checksums and manifest identity, while preserving
  `errorifexists`.
- Stop Spark on `configure_s3a` and MinIO client initialization failures without
  masking the original exception.
- Add mocked WRITE-path regression tests, six-envelope coverage, mismatch
  failures, DRY_RUN client isolation and historical/incremental isolation.
- No production/external runtime write, bucket creation, delete, reset, promote,
  cutover or checkpoint mutation.

### Implementation evidence

- `ingestion/historical/pipeline.py` now reads back Bronze data and JSON
  manifest before returning a verified Bronze result.
- Silver now reads persisted Parquet, computes checksums from persisted object
  bytes, validates the persisted manifest, and returns exactly six verified
  envelopes through `require_verified_silver_artifacts`.
- Spark initialization cleanup suppresses only cleanup failures, preserving the
  original initialization exception as the cause.
- `tests/test_historical_pipeline.py` adds mocked Bronze/Silver WRITE-path,
  mismatch, corrupt-readback, six-envelope and Spark cleanup coverage.

### Verification evidence

- `.venv\Scripts\python.exe -m pytest tests/test_historical_pipeline.py -q`:
  **11 passed**.
- `.venv\Scripts\python.exe -m pytest tests/test_historical_pipeline.py tests/test_historical_backfill_contracts.py -q`:
  **48 passed, 1 warning** (third-party PySpark/pandas compatibility warning).
- `.venv\Scripts\python.exe -m compileall -q ingestion tests`: passed.
- `git diff --check -- ingestion/historical/pipeline.py tests/test_historical_pipeline.py docs/plans/parent-scoped-ingestion-qa-correction-plan.md`: passed.
- No external-service test or runtime write was executed.

### QA handoff

- Independent reviewer should inspect only the historical pipeline read-back,
  checksum/manifest contract and focused tests in this session.
- Runtime certification remains out of scope; no claim is made for production
  MinIO/Spark behavior beyond mocked contract coverage.
- Next step: independent QA against this diff; phase remains open until QA
  verdict is recorded.
