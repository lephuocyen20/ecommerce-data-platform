# Plan: Gold semantic contract and runtime certification

Status: IMPLEMENTED — independent QA PASS WITH WARNINGS

## Scope

This session covers exactly two workstreams:

1. Gold semantic contract/business parity for the seven dimensional outputs.
2. A bounded runtime certification gate for the existing Bronze → Silver Parquet → Gold Iceberg path.

Out of scope: observability/SLO expansion, CI redesign, Iceberg maintenance/cleanup,
consumer cutover, production writes, checkpoint reset, object deletion, and merge/push.

## Workstream 1 — Semantic contract

### Intended changes

- Make the existing Gold contracts explicit for grain, natural/warehouse keys,
  measures, additivity, lifecycle signs, UTC/date semantics, unknown members,
  and Type-1 dimension policy.
- Add deterministic golden fixtures and reconciliation assertions for orders,
  order items, payments, and shipments.
- Fail closed on grain violations, fan-out, invalid lifecycle semantics,
  foreign-key gaps, and schema/contract drift.
- Document unresolved business decisions instead of silently choosing semantics.

### Candidate files

- `gold/implementation/dimensional/contracts.py`
- `gold/implementation/dimensional/transformations.py`
- `gold/implementation/dimensional/quality.py`
- `gold/implementation/semantic_reconciliation.py`
- `docs/GOLD_DIMENSIONAL_MODEL.md`
- focused `tests/test_gold_dimensional_*.py` and semantic reconciliation tests

### Acceptance criteria

- Every Gold table has one declared business grain and merge key.
- Measures are labelled additive/non-additive and are not duplicated by joins.
- `fact_orders` remains order-grain; shipment fees are not copied to order items.
- Refund/cancel/delete behavior is explicit and covered by expected-value fixtures.
- UTC date-key behavior, unknown key `-1`, FK coverage, duplicate grain and
  schema drift are tested.
- Semantic reconciliation fails closed on missing, extra, duplicate, or mismatched rows.
- Documentation identifies the required business/analytics sign-off; code tests
  do not claim business approval.

## Workstream 2 — Bounded runtime certification gate

### Intended changes

- Add or tighten a read-only certification/preflight report around existing
  runtime evidence and writer/verification outputs.
- Verify DAG import/dependency topology, deterministic lineage, seven-output graph
  manifest, snapshot/file/row read-back, and retry/recovery classification.
- Ensure all external clients are created only after exact scope validation.
- Keep `DRY_RUN` default and keep Gold WRITE/cutover disabled unless separately approved.

### Candidate files

- `gold/implementation/dimensional/run_pipeline.py`
- `gold/implementation/dimensional/writer.py`
- `gold/implementation/dimensional/manifest.py`
- `gold/implementation/dimensional/verification.py`
- `common/bounded_e2e_scope.py`
- `tests/run_phase_c_local_runtime.py`
- `tests/run_phase_c_local_stack.py`
- focused runtime/safety/orchestration tests
- `docs/QUALITY_IT_LOCAL.md`, `docs/RUNBOOK.md`, `docs/GOLD_LAKEHOUSE_PLAN.md`

### Acceptance criteria

- `DRY_RUN` returns `can_write=false` and does not mutate MinIO/Nessie/PostgreSQL.
- A bounded report records run ID, git/config identity, lineage identities,
  bucket/prefix/ref, table outputs, snapshot IDs, row counts and verification status.
- The report proves all seven Gold outputs are covered by one completed graph manifest.
- Same lineage is classified as `ALREADY_PUBLISHED`; partial output is classified
  as `PARTIAL_REQUIRES_RECONCILIATION`; no automatic cleanup or rollback occurs.
- Wrong bucket/prefix/ref/run ID is rejected before any external client is created.
- No production/shared path, unbounded write, destructive maintenance, or cutover is enabled.

## Verification commands

Safe/static checks before any bounded runtime approval:

```powershell
.\.venv\Scripts\python.exe -m pytest -q -m "unit or integration" -p no:cacheprovider
.\.venv\Scripts\python.exe -m compileall gold common lakehouse airflow tests
git diff --check
```

Focused suites will be added/updated in the approved implementation plan.
Any bounded external write requires a separate explicit approval and the exact
environment/scope rules in `AGENTS.md`.

## Risks

- Existing contracts may encode technical assumptions that still need business confirmation.
- Runtime local evidence may not prove production behavior; roadmap status must remain
  `local/runtime verified`, not `production certified`.
- Runtime certification must not accidentally create clients or write outside the
  approved quality-it/bounded scope.

## Approval gate

Do not delegate implementation to coder until the user explicitly approves this
scope. After implementation, run independent reviewer QA against this plan and
update this file with changed files, test evidence, verdict, unresolved risks and next step.

## Implementation and QA closure (2026-11-12)

### Changed files

- Gold semantic contracts, quality, reconciliation and transformations:
  `gold/implementation/dimensional/contracts.py`,
  `gold/implementation/dimensional/quality.py`,
  `gold/implementation/dimensional/reconciliation.py`,
  `gold/implementation/dimensional/writer.py`.
- Runtime certification and verification:
  `gold/implementation/dimensional/run_pipeline.py`,
  `gold/implementation/dimensional/verification.py`,
  `gold/implementation/semantic_reconciliation.py`.
- Documentation:
  `docs/GOLD_DIMENSIONAL_MODEL.md`, `docs/QUALITY_IT_LOCAL.md`.
- Tests:
  `tests/test_semantic_runtime_certification.py`,
  `tests/test_gold_dimensional_writer.py`,
  `tests/test_gold_dimensional_contracts.py`,
  `tests/test_gold_orchestration.py`,
  `tests/test_session_g_followup_contracts.py`.

### Verification evidence

- Independent QA verdict: **PASS WITH WARNINGS**.
- Focused suite: `54 passed, 1 warning`.
- Spark transformation suite: `7 passed, 1 warning` with the project Python
  configured for Spark workers.
- `compileall -q gold common tests`: PASS.
- `git diff --check`: PASS.
- Fixed and tested: actual `ALREADY_PUBLISHED`/partial recovery handling,
  DRY_RUN PostgreSQL persistence guard, seven-output certification coverage,
  snapshot-pinned row/file evidence, strict `fact_orders` schema checks,
  payments/shipments FK contracts, and customer-key alias conflicts.

### QA warnings and unresolved risks

- No real Docker/Iceberg/Nessie/MinIO runtime was executed in this session;
  snapshot reader API compatibility remains to be proven in bounded runtime.
- Business/analytics sign-off for metric ownership and order lifecycle revenue
  semantics remains pending.
- This work does not certify production, authorize Gold WRITE, consumer cutover,
  cleanup, rollback, maintenance, or shared-service mutation.

### Exact next step

Obtain a separate explicit approval for a bounded quality-it runtime verification
using the exact scope rules in `AGENTS.md`. Keep production routing and Gold WRITE
disabled until that runtime evidence and business/analytics sign-off are complete.

## Implementation evidence (2026-11-12)

- Scope 1: dimensional contracts now declare merge keys, measure additivity,
  lifecycle policy, UTC/unknown-member semantics, and business sign-off status;
  quality validates declared dimensional foreign keys and payment lifecycle
  signs; strict parity helpers cover orders, order items, payments, and shipments.
- Scope 1 decision: `fact_orders` remains order-grain and does not gain
  `signed_revenue`; order cancellation/deletion sign ownership is documented as
  unresolved pending business/analytics approval.
- Scope 2: a read-only `build_certification_report` records identity, lineage,
  scope, seven outputs, snapshots/row counts, verification and recovery status;
  the runner emits it and keeps `DRY_RUN` `can_write=false`.
- No Docker runtime, E2E marker, external client run, Gold write, MinIO/Nessie/
  PostgreSQL mutation, cleanup, checkpoint reset, or cutover was performed.
- Verification evidence: focused safe suite `34 passed` covering the new
  semantic/runtime tests plus dimensional contracts, semantic reconciliation,
  verification, and writer regression tests; `compileall -q gold common tests`
  passed; `git diff --check` passed. No external runtime or E2E marker was run.
- Coder self-review verdict: PASS WITH WARNINGS. Scope is limited to the two
  approved workstreams; independent reviewer QA remains the next gate. Runtime
  certification is not claimed as production certified.
