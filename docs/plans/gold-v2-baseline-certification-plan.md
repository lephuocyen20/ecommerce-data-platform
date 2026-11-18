# Plan: Gold v2 baseline certification

## Continuous Gold WRITE cadence implementation (2026-11-17)

- Approved code scope sets the incremental DAG schedule to
  `*/15 * * * *`, preserving existing timezone handling, `catchup=False`, and
  `max_active_runs=1`.
- The stateful `gold_dimensional_write` operator is explicitly non-retrying
  (`retries=0`) because a failed external write may leave partial state;
  existing execution and Docker API timeouts remain configured. Publication
  and confirmation boundaries remain non-retrying and require verified writer
  evidence.
- Compose and `.env.example` remain fail-closed: Gold is disabled/`DRY_RUN`
  by default and production approval is blank/unset unless explicitly
  supplied. No runtime flags were changed.
- Focused tests, `compileall`, and `git diff --check` are code verification
  only. No DAG trigger, runtime write/cutover/BI action, or external-service
  test was performed.
- Continuous runtime PASS is **not claimed**. Independent QA and separately
  approved runtime certification remain open.

Status: **IMPLEMENTATION BLOCKED — approved write cannot start until candidate wiring is closed**

## Objective

Produce one new, immutable Gold dimensional baseline whose seven table
manifests and graph manifest persist the complete v2 reuse contract, then
rerun the read-only Gate C preflight. This is required because the existing
`gold_prod_current` v1 manifests cannot be retroactively certified from
snapshot readback alone.

## Scope

- Use the existing dimensional writer and v2 manifest builders; do not add a
  second writer or synthesize metadata.
- Bind the run to one explicit approved Silver run and its stored completion
  manifests, quality evidence, reconciliation evidence, and lineage.
- Ensure all seven outputs are present: `dim_date`, `dim_customer`,
  `dim_product`, `fact_orders`, `fact_order_items`, `fact_payments`, and
  `fact_shipments`.
- Persist, and read back, schema identity, graph identity, manifest identity,
  pinned snapshot ID, catalog/namespace/ref, stale policy, and physical
  readback identity for every table.
- Persist graph-level lineage, quality/reconciliation binding, and the exact
  source Silver run identity.
- Run Gate C read-only after the write; candidate branch creation and
  publication/cutover remain separate gates.

## Acceptance criteria

- Preflight rejects any missing/non-PASS fact artifact, stale dimension
  artifact, wrong ref/namespace, or incomplete seven-table set before write.
- The new baseline has seven immutable table manifests plus one graph manifest.
- Every table manifest passes the v2 validator and its pinned snapshot/file/
  row readback matches the persisted identity.
- Graph readback references exactly the seven table manifests and the bound
  Silver lineage/quality/reconciliation evidence.
- Gate C read-only returns `PASS` for artifact completeness and ref-bound
  readback. Runtime certification still requires independent QA evidence.
- Stable-ref publication, consumer cutover, cleanup, rollback, checkpoint and
  watermark mutation are not performed.

## Required approval gate

Before execution, obtain explicit approval for one bounded runtime write that
names the exact source Silver run, target Gold ref/namespace, time limit, and
the systems that may be mutated (Iceberg/MinIO metadata and data only). This
approval does not include stable publication, cutover, cleanup, or deletion.

## Verification commands

```powershell
.venv\Scripts\python.exe -m pytest -q tests/test_gold_snapshot_metadata_contract.py tests/test_gold_snapshot_reuse_contract.py tests/test_gold_dimensional_verification.py tests/test_gold_candidate_artifact_bundle.py tests/test_gold_candidate_readback.py tests/test_airflow_parse_imports.py
.venv\Scripts\python.exe -m compileall -q gold airflow tests
git diff --check
```

Runtime evidence must include the exact run identity, target ref/namespace,
seven pinned snapshot IDs, manifest keys and identities, graph identity,
readback counts/files/bytes, and a no-publication/no-cutover assertion. Do not
claim certification from unit tests or injected-reader DRY_RUN alone.

## Risks and rollback

The primary risks are routing to `gold_prod_current`, partial output, and
lineage drift. The run must use a new immutable target identity and fail
closed on partial output. Rollback is configuration-only: stop the run and
leave the immutable evidence for review; do not delete objects, reset
checkpoints, expire snapshots, or roll back refs.

## Current blocker

The existing v1 `dim_customer` and `dim_product` manifests contain usable
snapshot/physical readback evidence but lack the complete v2 persisted
contract. No candidate ref or Gold write has been created.

## Preflight finding after approval

The approval was received, but the runtime boundary is not yet safe to invoke:

- `gold_candidate_orchestrator.py` contains the guarded candidate contract but
  is not wired to a production CLI/Airflow task that loads the approved Silver
  bundle and constructs the candidate-bound Spark/MinIO/Nessie runtime.
- `common/storage_layout.py` accepts `gold_prod_backfill_*` production refs,
  but not the candidate ref format required by
  `GoldCandidatePlan` (`gold_prod_candidate_*`).
- Directly invoking `gold/implementation/dimensional/run_pipeline.py` with the
  current production layout can resolve the stable namespace/ref instead of
  the explicit candidate namespace, so it is rejected for this approved run.

Required correction before runtime write: add and test one candidate-only CLI
or operator boundary that validates the target ref/namespace before creating
clients, binds the four approved Silver facts plus two pinned dimensions, and
invokes only `ProductionGoldCandidateAdapter`. After that correction, run
static QA, then execute the already-approved bounded write and independent QA.

## Wiring implementation evidence

- Added `gold/implementation/gold_candidate_cli.py`; it validates the plan and
  six-artifact bundle before importing Spark, constructs an explicit candidate
  scope, bootstraps only the candidate Nessie branch, and invokes
  `ProductionGoldCandidateAdapter`.
- Updated `common/storage_layout.py` to accept the explicit
  `gold_prod_candidate_*` production-ref pattern.
- Added `tests/test_gold_candidate_cli.py`.
- Verification: **71 tests passed**, compileall passed, and `git diff --check`
  passed.

The runtime write was not started because the approved Silver run has no
current dimension PASS artifacts and the existing dimension manifests are v1;
the candidate contract correctly rejects them as a reusable v2 bundle. A
fresh runtime-verified v2 dimension bundle is still required.

## Source preflight refresh and full-Silver baseline correction (2026-11-18)

- Read-only Airflow XCom preflight for
  `manual__gold_v2_silver_recovery_current_20261117` found all six Silver
  source artifacts in `PASS`: `customers`, `products`, `orders`,
  `order_items`, `payments`, and `shipments`.
- The run carries deterministic Silver manifest keys and identities for all
  six tables. This is the first source run in the current environment that is
  eligible for a full-Silver baseline; task success alone was not used.
- The current candidate implementation still hardcodes dimensions as
  `NO_DATA + pinned Gold snapshot reuse`. Existing Gold snapshot metadata is
  stale/incomplete, so the approved full baseline cannot safely use that path.
- Approved correction scope: extend the candidate artifact contract,
  dimensional lineage and candidate writer path to accept verified Silver
  `customers/products` artifacts, while retaining the existing pinned-snapshot
  path for incremental reuse. Add static/unit QA and independent review before
  any candidate WRITE.
- No runtime write, candidate ref creation, MinIO/Nessie mutation, checkpoint
  change or stable-ref publication occurred during this preflight.

### Full-Silver correction initial QA — FAIL (2026-11-18)

- Independent review found three **High** defects before runtime: the CLI still
  required `gold_snapshots` for a valid full-Silver bundle; candidate/bundle
  validation happened after Spark/MinIO/Nessie client construction; and a
  bundle containing `gold_snapshots` could route PASS Silver dimensions into
  synthetic `NO_DATA` snapshot reuse.
- Medium findings: embedded-manifest checksum binding was incomplete and the
  legacy flat fact contract had weaker lineage/coverage checks.
- Runtime WRITE, candidate ref creation and stable publication remain blocked
  until these corrections are fixed and independently re-reviewed.

### Full-Silver correction re-review — FAIL (2026-11-18)

- Re-review confirmed the three prior High findings were corrected:
  full-Silver routing, pre-client validation ordering, and no synthetic
  `NO_DATA` dimensions.
- A new **High** finding remains in the legacy flat compatibility path:
  embedded manifest tampering is not bound to canonical `manifest_identity`,
  and facts can carry mixed `source_run_identity` values.
- Required correction: canonical manifest identity validation and one shared
  source-run identity across legacy facts, with regression tests. Runtime WRITE
  remains blocked until independent QA passes.

### Full-Silver correction final QA closure (2026-11-18)

- Independent re-review: **PASS WITH WARNINGS**; no Critical/High/Medium
  finding and no required code fix.
- Full-Silver routing, pre-client validation, envelope propagation, checksum
  binding, legacy manifest tamper rejection, shared fact source identity and
  stable-ref/cutover guards all passed review.
- Scoped pure-contract suite: **168 passed in 0.36s**; compileall and
  `git diff --check` passed. No external client, candidate ref or Iceberg
  mutation was created.
- Warning boundary: candidate runtime WRITE/readback remains **NOT CERTIFIED**.
  It requires a separate bounded runtime approval with exact candidate ref,
  source run, timeout and allowed Iceberg/MinIO/Nessie data/metadata scope.

## Candidate runtime evidence — WRITE/READBACK PASS

- Source run: `production_silver_v2_cert_20261117`.
- Candidate ref: `gold_prod_candidate_v2_cert_20261117`.
- Candidate namespace: `gold_candidate_v2_cert_20261117`.
- Nessie read-back: exact `BRANCH`, hash
  `a4274ec5b703fc37a34ecb8efab17931773aa82987e37ca324a7c798cbfb59e7`.
- Graph identity:
  `0859025fd0fec3226fa75c2e1cb78cd46d3a9286d1f3a985ba1a85df197d6641`.
- Native writer returned `PUBLISHED`; completed-graph verification returned
  `VERIFIED` for exactly seven tables. Every table manifest is `PASS`, bound
  to the candidate ref/namespace, and has v2 schema/manifest/readback
  identities.
- Snapshot/readback summary: `dim_customer=2 rows`, `dim_date=5844`,
  `dim_product=1001`, `fact_orders=1`, `fact_order_items=1000`,
  `fact_payments=1`, `fact_shipments=1`; all seven have positive file counts
  and `readback.status=PASS`.
- No stable-ref publication, consumer cutover, cleanup, delete, rollback,
  checkpoint or watermark mutation was performed.

## Final regression evidence

- Candidate/manifest/Airflow regression suite: **135 passed**.
- `compileall -q gold common airflow tests`: PASS.
- `git diff --check`: PASS (only existing LF/CRLF normalization warnings).
- Independent reviewer was dispatched after the runtime evidence and remains
  pending; until that verdict arrives, delivery status is runtime PASS but not
  independently certified.

## Independent QA correction — PASS

- Reviewer initially found three HIGH routing gaps: candidate ref loss in
  `StorageLayout`, missing production bucket validation in the CLI, and
  non-deterministic candidate namespace acceptance.
- Corrected all three and added executable regression coverage.
- Final focused regression after corrections: **146 passed**; compileall and
  diff-check passed.
- Raw runtime evidence is now persisted at
  `docs/evidence/gold-candidate-v2-runtime-20261117.json`.
- Final reviewer re-review confirmed **PASS**: all seven manifest keys are
  present and bind to the same graph identity; no Critical/High/Medium finding
  remains. Stable publication remains a separate approval gate.

## Approved Silver rebuild evidence

- Triggered exactly one run: `production_silver_v2_20261117`.
- DagRun completed `success`; Gold dimensional write and publication were
  skipped because Gold remained disabled.
- `customers` and `products` still returned `NO_DATA`; therefore no fresh
  dimension PASS artifacts were produced. The run cannot yet supply the v2
  dimension bundle required by the candidate CLI.
- No PostgreSQL source seed, checkpoint reset, watermark mutation, Gold write,
  Nessie candidate branch, cleanup or cutover was performed.

## Approved source-delta follow-up evidence

- Created one idempotent PostgreSQL source delta: `customer_id=6226` and
  `product_id=2681`, after read-only MAX checks returned `6225` and `2680`.
- Triggered exactly one follow-up run:
  `production_silver_v2_dimensions_20261117`; DagRun completed `success`.
- Silver results: `customers=PASS (1 row)`, `products=PASS (1 row)`,
  `order_items=PASS (1000 rows)`, while `orders`, `payments`, and `shipments`
  remained `NO_DATA`.
- Gold remained disabled and skipped. The candidate bundle was not built and
  no Nessie candidate branch or Gold write was attempted.
- The candidate gate now needs a current PASS artifact for all four facts;
  additional source data for `orders`, `payments`, and `shipments` requires a
  separate approval because it extends the approved source-delta scope.

## Stable promotion evidence — PASS

- Explicit stable-promotion approval was received and Nessie CAS was applied
  with expected stable hash
  `adb060d0499e2cfd0fd38d28b3c6c3b85ef5c1aedd54238fbe3a7d652f0e196e`.
- `gold_prod_current` now reads back as a `BRANCH` at candidate hash
  `a4274ec5b703fc37a34ecb8efab17931773aa82987e37ca324a7c798cbfb59e7`.
- Post-promotion read-only Spark verification passed for all seven tables and
  pinned snapshots. Candidate branch was retained; no cleanup or deletion was
  performed.

## Bounded production WRITE attempt — BLOCKED SAFELY

- Triggered exactly one approved run:
  `manual__gold_v2_production_write_20261117`, with explicit six-table
  `WRITE` config and process-scoped write guards.
- Gold preflight failed closed before Spark/MinIO/Nessie Gold client creation:
  current Silver `customers` was `NO_DATA`, producing
  `Gold dimensional facts require current Silver PASS: customers`.
- Airflow exhausted configured retries; no second DagRun was created. Gold
  write/publication/stable-ref mutation did not occur. `products` and
  `order_items` Silver tasks completed before the Gold gate.
- Write flags were restored and verified disabled/DRY_RUN on scheduler and
  dag-processor. Snapshot reuse cannot be used as a workaround because its
  runtime adapter is not yet implemented/certified.
- Exact next step: obtain separate approval for an idempotent customers/products
  source delta, or implement and independently certify runtime snapshot reuse,
  then rerun the controlled production gate.

## Production Gold WRITE runtime evidence — PASS

- Final bounded DagRun:
  `manual__gold_v2_silver_recovery_full_20261117`.
- Six dimensional artifact tasks were rebuilt after the canonical manifest
  identity fix, then the same DagRun's Gold task was rerun with process-scoped
  WRITE guards.
- `gold_dimensional_write`, `publish_gold_to_active`, and all seven Gold
  confirmations completed `success`. Writer status was `PUBLISHED` and graph
  verification was `VERIFIED`.
- Readback passed for all seven outputs: `dim_date=5844`, `dim_customer=2`,
  `dim_product=2`, `fact_orders=1`, `fact_order_items=1000`,
  `fact_payments=1`, `fact_shipments=1`.
- Write guards were restored to disabled/DRY_RUN. No cleanup, delete, rollback,
  checkpoint reset, or watermark reset occurred. Independent QA is pending.
- Sanitized task-level evidence is persisted at
  `docs/evidence/gold-production-write-runtime-20261117.json`; reviewer
  re-review closed the prior evidence-gap warning with an independent
  **PASS** verdict.

## Production Gold WRITE closure — independent QA PASS

- Independent reviewer re-reviewed the implementation fixes and the raw
  task-level runtime evidence. Verdict: **PASS**; no remaining Critical,
  High, or Medium finding applies to this scope.
- Structural evidence validation passed for the DagRun, six artifact tasks,
  writer/publication tasks, seven confirmation tasks, seven unique table
  readbacks, graph/lineage identities, and safety flags.
- Final regression evidence: **129 passed** in the scoped Gold/Airflow suite;
  compileall passed and `git diff --check` passed with only existing
  LF/CRLF normalization warnings.
- Approved scope is closed at runtime **PASS**. Write flags remain disabled/
  `DRY_RUN`; commit, push, merge, consumer cutover, cleanup, rollback,
  checkpoint reset, and watermark reset were not performed.

## Continuous Gold WRITE implementation QA handoff — FAIL (2026-11-17)

- Approved implementation scope: 15-minute DAG schedule, Gold stateful writer
  `retries=0`, and regression coverage; no runtime enablement or DAG trigger.
- Coder evidence: focused suite **82 passed**, compileall passed, and
  `git diff --check` passed.
- Independent QA initially returned **FAIL** because the worktree contains
  broad pre-existing uncommitted changes and the reviewer could not isolate
  the session delta from the existing snapshot-reuse orchestration. The local
  quality profile's Gold DRY_RUN opt-in was also flagged for clarification.
- Runtime continuous WRITE remains disabled and not certified. Re-review is
  required against the session-owned delta and the approved scope before any
  scheduler/runtime flag change.

## Continuous Gold WRITE implementation QA closure — PASS WITH WARNINGS (2026-11-17)

- Scoped independent re-review returned **PASS WITH WARNINGS**. No required
  code fix remains for the session-owned delta: 15-minute schedule,
  `retries=0`, timeout preservation, fail-closed defaults, and unchanged
  pre-existing dependency graph.
- Direct focused verification in the repository `.venv` passed: **68 passed**
  for DAG parse/import, orchestration, and pipeline-config tests.
- A broader selected suite returned **120 passed, 1 failed**. The failure is
  the pre-existing recovery fixture
  `test_recovery_classifier_does_not_mutate_state`, which supplies empty
  lineage and now reaches the existing cross-table coverage contract; it is
  outside the session-owned cadence/retry delta and was not changed here.
- `compileall` and `git diff --check` passed. Continuous runtime WRITE remains
  **NOT CERTIFIED / DISABLED**; runtime enablement requires a separate
  controlled certification run and evidence.

## Continuous Gold WRITE certification attempt — BLOCKED SAFELY (2026-11-17)

- With explicit approval, one controlled DagRun was triggered:
  `manual__gold_v2_continuous_cert_20261117`.
- Process-scoped flags were verified before trigger: ingestion/write enabled,
  Gold `WRITE` enabled, production approval `1`, lakehouse write enabled;
  BI publication apply remained `0` and snapshot reuse remained disabled.
- DagRun failed closed at Gold preflight with:
  `Gold dimensional facts require current Silver PASS: customers`.
  `gold_dimensional_write` created no Spark/MinIO/Nessie Gold client and no
  Gold mutation was observed. No blind retry, reset, cleanup, rollback, or
  watermark/checkpoint mutation was performed.
- Root cause is a stale/no-current Silver dimension artifact (`customers`),
  not a WRITE authorization or scheduling failure. Continuous WRITE cannot be
  enabled until the next approved source/Silver recovery produces current
  PASS artifacts for all six inputs.
- Scheduler and dag-processor were recreated afterward and verified back to
  `INGESTION_WRITE_ENABLED=false`, Gold `DRY_RUN`/write-disabled,
  `PRODUCTION_LAKEHOUSE_WRITE_APPROVED` empty, and
  `LAKEHOUSE_WRITE_ENABLED=false`.

## Approved Silver recovery after certification block — PASS (2026-11-17)

- Read-only MAX checks found the previous dimension watermark at
  `2026-11-17 08:40:00`; one FK-valid source delta was inserted for all six
  tables with a later `updated_at` and no deletes or updates to existing rows.
- Recovery DagRun
  `manual__gold_v2_silver_recovery_current_20261117` completed **success**.
- All six dimensional artifact tasks completed **success**:
  `customers`, `products`, `orders`, `order_items`, `payments`, and
  `shipments`. Gold write and publication were skipped because Gold remained
  disabled.
- Runtime flags were restored and verified disabled/`DRY_RUN` after recovery.
- Exact next step: run one new controlled Gold WRITE certification against
  this fresh six-artifact PASS bundle; do not enable continuous schedule until
  that certification and independent QA pass.

## Controlled Gold WRITE certification after recovery — PASS (2026-11-17)

- Gold was rerun in the same recovery DagRun after clearing only the 9 Gold
  writer/publication/confirmation task states; the six Silver artifact PASS
  results were preserved. A scheduler race was avoided by stopping the
  scheduler before metadata clear and restarting it with verified WRITE flags.
- DagRun `manual__gold_v2_silver_recovery_current_20261117` completed
  **success**. `gold_dimensional_write`, `publish_gold_to_active`, and all
  seven confirmation tasks completed **success**.
- Writer returned `PUBLISHED`; graph verification returned `VERIFIED`; all
  seven readbacks returned `PASS`. Snapshot/row/file evidence is persisted at
  `docs/evidence/gold-continuous-write-certification-20261117.json`.
- Process-scoped WRITE flags were restored to disabled/`DRY_RUN` immediately
  afterward. No cleanup, rollback, checkpoint reset, watermark reset, BI
  cutover, or Iceberg cutover occurred.
- Independent QA is still required before enabling the 15-minute schedule.

## Continuous 15-minute Gold WRITE enablement — ACTIVE (2026-11-17)

- Independent QA returned **PASS WITH WARNINGS**, with no Critical/High/Medium
  findings and no required code fixes. The warnings are limited to pytest not
  being independently runnable in the reviewer environment and the absence
  of long-running HA certification.
- After the controlled certification PASS, only `airflow-scheduler` and
  `airflow-dag-processor` were recreated with process-scoped WRITE guards.
- Runtime verification: DAG is unpaused; `GOLD_DIMENSIONAL_MODE=WRITE`, Gold
  and ingestion writes are enabled, and `PRODUCTION_LAKEHOUSE_WRITE_APPROVED=1`.
  All six Iceberg cutover flags and `BI_ACTIVE_PUBLISH_APPLY` remain false.
- DAG schedule is `*/15 * * * *`, with `catchup=False` and
  `max_active_runs=1`. The first observed scheduled run before the final
  enablement was `scheduled__2026-11-17T13:00:00+00:00` and completed
  `success`; post-enablement long-running stability is not yet certified.
- This is an operational enablement, not a claim of HA/long-running
  certification. Monitor the first several intervals and pause/revert flags
  on any partial-state or verification failure.

## First post-enable scheduled run — FAIL-CLOSED / PAUSED (2026-11-17)

- The first observed post-enable scheduled run,
  `scheduled__2026-11-17T13:15:00+00:00`, failed at Gold preflight with
  `Gold dimensional facts require current Silver PASS: customers`.
- Six dimensional artifact tasks completed, but the current scheduled window
  had no new `customers` Silver PASS artifact. Gold writer did not create a
  Spark/MinIO/Nessie client and performed no Gold mutation.
- Per safety policy, the DAG was paused and scheduler/dag-processor were
  recreated with all write flags disabled/`DRY_RUN`. Final verification:
  `is_paused=true`, ingestion/write false, Gold disabled, production approval
  empty, and lakehouse write false.
- Continuous WRITE is **PAUSED / NOT ACTIVE** pending a source/Silver
  freshness strategy. Do not unpause or enable WRITE until scheduled runs can
  receive current PASS artifacts for all six inputs (or a separately certified
  snapshot-reuse path is approved).

## Airflow production DRY_RUN evidence — PASS

- Triggered exactly one run: `ecommerce_incremental_ingestion` /
  `manual__gold_v2_dry_run_20261117`, with explicit six-table `DRY_RUN` config.
- DagRun completed `success`; validation and dimensional artifact tasks that
  executed completed successfully.
- `gold_incremental_snapshot_reuse_dry_run` completed `success`;
  `gold_dimensional_write`, `publish_gold_to_active`, and all seven Gold
  confirmation tasks were skipped by the write-disabled gate.
- No Gold write, publication, cutover, cleanup, checkpoint reset, or
  watermark mutation occurred. Exact next step: separately approve one
  bounded production `WRITE` run, then run independent QA.

## Full-Silver candidate correction implementation — 2026-11-18

Status: **IMPLEMENTED — scoped regression evidence PASS; independent QA pending**

The QA FAIL correction was limited to the candidate CLI/artifact contract:

- Full-Silver input now selects the six verified Silver artifacts even when
  `gold_snapshots` is present; the legacy reuse envelope is selected only when
  the full-Silver dimensions are absent and the snapshot bundle is validated.
- Candidate environment and bundle preflight now runs before SparkSession,
  MinIO client, or Nessie adapter construction. The exact selected envelope is
  passed to `run_gold_candidate` for both routes.
- An embedded manifest checksum, when present, must equal the verified artifact
  checksum. Legacy flat fact validation now rejects mismatched optional
  manifest/source lineage fields.

Changed files: `gold/implementation/gold_candidate_cli.py`,
`gold/implementation/gold_candidate_artifacts.py`,
`gold/implementation/gold_candidate_contract.py`,
`tests/test_gold_candidate_cli.py`, and
`tests/test_gold_candidate_artifact_bundle.py`.

Verification evidence:

- Candidate/reuse/lineage/orchestration scoped pytest suite: **155 passed**.
- `.venv\\Scripts\\python.exe -m compileall -q gold common airflow tests`:
  **exit 0**.
- `git diff --check`: **exit 0**; only pre-existing LF/CRLF warnings were
  emitted.
- No runtime/DAG, candidate ref, external client, WRITE, flag enablement,
  checkpoint/watermark mutation, or stable-ref operation was performed.

Required next step: independent QA review this exact correction diff against
the FAIL findings before any runtime candidate gate is considered.
