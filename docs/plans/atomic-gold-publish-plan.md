# Atomic Gold publish flow

## Status

**Implementation complete — runtime gate pending — 2026-11-13**

## User outcome

One approved Airflow `WRITE` run must read the current PostgreSQL source,
produce the Bronze/Silver/Gold graph, verify the complete Gold result, and make
that verified snapshot visible through the existing active Metabase connection
(`lakehouse`, schema `gold_run_20261112_gold_06`, Nessie ref
`bi_gold_current`). A partial or unverified run must never become visible to
the active BI consumer.

## Current gap

The DAG's `WRITE` configuration controls ingestion, while Gold mode is supplied
by Airflow environment. Shared-bucket bounded runs write to an immutable
run-scoped warehouse/ref/schema. The existing promotion contract accepts only
the certified run-06 candidate and does not connect a new candidate to the
active BI schema. Therefore a successful candidate write alone does not update
Metabase.

## Approved scope for implementation

- Add a generic, fail-closed publish contract for a verified Gold candidate.
- Keep active Trino/Metabase catalog and schema stable; publish the candidate
  behind `bi_gold_current` only after manifest, table-set, snapshot, row-count,
  schema and lineage checks pass.
- Add an Airflow publish task after Gold verification and before confirmation
  tasks. It must be idempotent, single-writer/CAS guarded, retry-safe, and
  return `ALREADY_PUBLISHED` for an identical verified candidate.
- Propagate a run identity and candidate manifest through XCom using bounded
  metadata only; never pass DataFrames or credentials.
- Preserve existing dry-run behavior, bounded `ecommerce-data-lake` scope,
  read-only validation path, rollback/time-travel evidence, and active/fallback
  boundaries.
- Add unit/contract tests for candidate validation, CAS mismatch, failed
  verification, duplicate retry, stable schema identity and DAG dependency.
- Update plan/roadmap evidence after implementation QA.

## Explicit non-goals

- No runtime execution, database write, MinIO write, Nessie promotion, schema
  migration, cleanup, snapshot expiry, orphan removal or cutover in this
  implementation session.
- No change to Compose credentials or active Trino access policy.
- No automatic promotion from an unverified or arbitrary run ID.

## Implementation evidence

- `NessieRestAdapter.update_branch` adds expected-hash CAS advancement for the
  stable `bi_gold_current` branch.
- `publish_gold_to_active_task` is inserted between Gold writer verification
  and table confirmation tasks. It requires certified `WRITE` evidence,
  `BI_ACTIVE_PUBLISH_APPLY=1`, and `BI_GOLD_CURRENT_EXPECTED_HASH`.
- Shared-bucket candidates use the stable active namespace
  `gold_run_20261112_gold_06` while retaining run-scoped warehouse/ref
  isolation; quality-it runs never publish to the active branch.
- Focused implementation suite: **51 passed**; `compileall` and
  `git diff --check`: **PASS**.
- No runtime write, Nessie branch update, MinIO mutation, database mutation or
  Metabase cutover was executed.

## Proposed design

1. `validate_config` canonicalizes `WRITE`, selected tables and immutable
   `run_id`; invalid or missing shared-scope authorization fails before clients
   are created.
2. Existing ingestion/Silver/Gold writer produces a candidate manifest in the
   bounded run scope.
3. A read-only verification task validates exact seven-table identity, schema,
   snapshot IDs, row counts, file evidence, lineage and candidate hash.
4. A guarded publish adapter performs a Nessie CAS check and advances the
   stable BI branch `bi_gold_current` to the verified candidate. The operation
   is no-op on an identical already-published candidate and rejects a changed
   current hash. The stable ref is a branch, not an immutable tag, because the
   active BI view must advance for each verified Gold run.
5. Only publish success or verified idempotent no-op unlocks the seven BI
   confirmation tasks.

## Acceptance criteria

- A `WRITE` run cannot reach active publication without `VERIFIED` evidence.
- Active Metabase requires no datasource edit after publication.
- A failed/partial/retried run cannot point `bi_gold_current` at an invalid
  snapshot or create duplicate publication state.
- Candidate and stable ref/schema identity are explicit in manifests and logs.
- Dry-run and current read-only flows remain behaviorally unchanged.
- Focused tests and DAG import/dependency checks pass; no runtime mutation is
  performed by static verification.

## Required runtime gate after implementation

Runtime execution requires a separate bounded approval specifying an immutable
`LAKEHOUSE_E2E_RUN_ID`, exact prefix
`quality-it/<run-id>/`, expected current `bi_gold_current` hash, and the
systems written (PostgreSQL source read, MinIO Gold objects, Nessie metadata,
and active BI ref). The runtime gate must record before/after hashes and
read-back parity before claiming Metabase visibility.
