# Gold graph lock recovery handling

## Status

Implementation complete; independent QA **PASS**; runtime recovery remains separately gated.

## Problem

The bounded E2E Gold writer receives MinIO `PreconditionFailed` when the
content-addressed graph fence already exists. The fence is intentionally
create-once and is not auto-released after failure, so a retry currently
surfaces a low-level S3 exception without a clear operator-safe recovery
classification.

## Approved scope

- Preserve create-if-absent locking and never delete or overwrite an existing
  lock automatically.
- Classify an existing matching graph fence as an explicit recovery state with
  the lock key and graph identity, while keeping writes blocked.
- Preserve conflict detection for a mismatched lock payload and the existing
  global-fence behavior.
- Add unit tests for matching stale lock, mismatched lock, and no source read
  before lock acquisition.
- Update roadmap evidence after independent QA.

## Non-goals

- No MinIO object deletion or overwrite.
- No retry of the same immutable run ID.
- No database, Nessie, Iceberg, Spark, or Metabase runtime write.

## Acceptance criteria

- Existing matching graph fence yields a deterministic operator-facing
  recovery error/state, not an unclassified raw S3 traceback.
- Mismatched graph fence remains a hard conflict.
- No source read or table merge occurs after lock acquisition fails.
- Existing completed/partial recovery behavior remains unchanged.
- Focused tests, compileall, and diff checks pass.

## Verification

```powershell
.\.venv\Scripts\python.exe -m pytest -q --basetemp .pytest-tmp tests/test_gold_dimensional_writer.py
.\.venv\Scripts\python.exe -m compileall -q gold common tests
git diff --check
```

Runtime lock inspection/deletion and a new bounded E2E run require a separate
explicit runtime approval.

## QA gate

Independent reviewer must inspect the implementation diff against this plan
before the phase can close.

## Implementation and QA evidence (2026-11-14)

- Matching graph fences now raise `GRAPH_FENCE_ALREADY_HELD` with
  `GRAPH_FENCE_RECOVERY_REQUIRED`, lock key and graph identity.
- Mismatched payloads remain `GRAPH_FENCE_CONFLICT` hard failures.
- No lock deletion or overwrite was added; writer ordering still blocks source
  reads before fence acquisition.
- Focused writer suite: **17 passed**.
- Combined writer/XCom suite: **23 passed**.
- `compileall -q gold common tests`: **PASS**.
- `git diff --check`: **PASS**.
- Independent reviewer verdict: **PASS**, no Critical/High/Medium findings.
- Runtime MinIO inspection, lock deletion, and bounded E2E write were not run.

## Next step

Operator must inspect the existing lock and active runs read-only. A new
immutable bounded run requires separate runtime approval; the existing lock
must not be deleted or overwritten automatically.

## Runtime attempt evidence (2026-11-14)

- Approved bounded run: `manual__demo20261114a`.
- Boundary validation passed for bucket `ecommerce-data-lake` and prefix
  `quality-it/demo20261114a/` before execution.
- Bronze ingestion, Silver transforms, Silver verification, and six lineage
  artifact tasks completed successfully within the bounded scope.
- Gold writer failed before table merge with
  `DimensionalQualityError: fact_payments has an unresolved order_id foreign key`.
- Airflow retry then correctly stopped on the newly created immutable graph
  fence with `GRAPH_FENCE_ALREADY_HELD`; final DAG state: `failed`.
- No active publication, Nessie branch update, Metabase cutover, lock deletion,
  checkpoint reset, or cleanup was performed.
- Next step: fix the payments/orders referential-parity issue, then use a new
  immutable run ID after a separate runtime approval. Do not retry this run.
