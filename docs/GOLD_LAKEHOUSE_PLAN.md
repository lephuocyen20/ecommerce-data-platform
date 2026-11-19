# Gold Lakehouse Development Plan

## Stable promotion and continuous incremental WRITE enabled (2026-11-18)

- Read-only Silver-derived parity passed for all seven r04 Gold tables.
- Approved Nessie CAS advanced `gold_prod_current` to the verified r04 hash;
  post-promotion stable readback passed for all seven tables.
- Continuous incremental WRITE is enabled and the incremental DAG is unpaused
  on the `*/15 * * * *` schedule with `max_active_runs=1`.
- `BI_ACTIVE_PUBLISH_APPLY=0` remains set, so the DAG does not perform another
  automatic stable-ref promotion or BI cutover.

## Session 2026-11-19: production incremental WRITE event — PASS

- Seeded the approved local PostgreSQL delta under immutable run ID
  `prodwrite20261119r06`: 5 customers, 8 products, 20 orders, 40 order items,
  20 payments, and 20 shipments.
- Read-back verified exact row counts and FK relationships; replay with the
  same run ID passed without duplication.
- Airflow run `manual__prodwrite20261119r06` completed successfully. Gold
  dimensional WRITE, active publish, and all seven Gold confirmations passed.
- Nessie stable and configured candidate refs read back the new hash
  `7fe1816313bf5cb6964a786c6a09a6f94dac16c9d07e9f8da67293315c91ef54`.
- No destructive maintenance, rollback, checkpoint reset, or cleanup was run.

## Gold production candidate r04 runtime certification - PASS WITH WARNINGS (2026-11-18)

- The approved full-Silver candidate r04 completed a seven-table Gold graph
  on `gold_prod_candidate_v2_cert_20261118_r04` and namespace
  `gold_candidate_v2_cert_20261118_r04`.
- Independent Spark/Nessie/MinIO read-only verification passed for all seven
  pinned snapshots with positive file, byte and row evidence.
- Stable ref `gold_prod_current` remained unchanged. No stable publication,
  BI consumer cutover, cleanup, rollback, checkpoint or watermark mutation was
  performed.
- Independent QA verdict: **PASS WITH WARNINGS**. The only warning is missing
  captured `GOLD_CANDIDATE_RESULT` stdout; persisted read-only evidence remains
  complete. Stable publish/cutover are not certified by this result.

## Continuous Gold WRITE cadence implementation (2026-11-17)

- Approved code scope is implemented: `ecommerce_incremental_ingestion` now
  uses `*/15 * * * *`, with existing timezone handling, `catchup=False`, and
  `max_active_runs=1` preserved.
- `gold_dimensional_write` is explicitly non-retrying (`retries=0`) because a
  failed external write may have partial state; timeout behavior remains in
  place. Gold publication and confirmation tasks remain non-retrying and gate
  on verified writer evidence.
- Compose and `.env.example` remain fail-closed (`GOLD_DIMENSIONAL_ENABLED`
  false, `GOLD_DIMENSIONAL_MODE=DRY_RUN`, write flag false); production
  approval is blank/unset by default and only wired through when explicitly
  supplied. No runtime configuration was enabled.
- This is static/unit implementation evidence only. No DAG trigger, runtime
  write, BI publish, cutover, checkpoint/watermark mutation, or external
  service test was performed. Continuous runtime PASS is **not claimed**;
  independent QA and separately approved runtime certification remain open.

## Session 2026-11-17: Gold v2 baseline certification plan — WAITING FOR APPROVAL

- The live read-only probe confirmed the existing `dim_customer` and
  `dim_product` snapshots are physically readable, but their v1 manifests do
  not contain the complete v2 persisted contract required for Gate C.
- Added `docs/plans/gold-v2-baseline-certification-plan.md` describing the
  bounded next step: one new seven-table Gold baseline with v2 metadata,
  followed by read-only Gate C and independent QA.
- No Nessie ref, Iceberg table, MinIO object, PostgreSQL row, checkpoint,
  watermark, publication or cutover was created or modified in this session.
- Required next gate: explicit approval naming the bounded runtime write,
  source Silver run, target ref/namespace and allowed systems. Stable publish,
  consumer cutover and cleanup are excluded.

## Session 2026-11-17: approved write preflight — BLOCKED ON CANDIDATE WIRING

- Approval was received for the bounded Gold write using the approved Silver
  run and an immutable candidate target.
- Preflight found that the guarded `gold_candidate_orchestrator` is not yet
  connected to a production CLI/Airflow runtime boundary, and the production
  storage layout does not accept the required `gold_prod_candidate_*` ref.
- Direct invocation of the native writer is rejected because it can resolve
  the stable namespace/ref rather than the explicit candidate target.
- No Nessie branch, Iceberg table, MinIO object, PostgreSQL row, checkpoint,
  watermark, publication, cutover, cleanup or delete was performed.
- Next exact step: implement and statically QA candidate-only runtime wiring;
  then execute the already-approved bounded write and independent QA.

## Session 2026-11-17: candidate runtime wiring — STATIC PASS; WRITE BLOCKED

- Added the candidate-only CLI and explicit production candidate-ref support.
- The CLI validates the plan and six-artifact bundle before creating Spark,
  MinIO or Nessie clients, binds the target namespace/ref explicitly, and calls
  only `ProductionGoldCandidateAdapter`.
- Verification: **71 tests passed**, compileall PASS, `git diff --check` PASS.
- The approved write was not started because the source run has
  `customers/products=NO_DATA` and the available Gold dimension manifests are
  v1, so the v2 candidate bundle validator correctly rejects them. No false
  PASS and no runtime mutation occurred.
- Next exact step: obtain a fresh runtime-verified v2 dimension bundle (or an
  explicitly approved six-table Silver/Gold rebuild), then invoke the new CLI.

## Session 2026-11-17: approved Silver rebuild — PASS; dimension data still absent

- Triggered exactly one run: `production_silver_v2_20261117` with all six
  tables selected and `mode=WRITE`.
- DagRun completed `success`; Gold dimensional write and active publication
  were skipped because Gold remained disabled.
- `customers` and `products` still produced `NO_DATA`, so the run did not
  create the fresh dimension PASS artifacts required for the v2 candidate
  bundle. This is a source-data absence, not an Airflow failure.
- No PostgreSQL seed, checkpoint reset, watermark mutation, Gold write,
  Nessie candidate branch, cleanup or cutover was performed.
- Next gate: explicit approval to create a bounded source delta for
  `customers` and `products`, then rerun the six-table Silver flow once.

## Session 2026-11-17: dimension source delta — PASS; fact bundle still incomplete

- Created one idempotent PostgreSQL source delta for `customer_id=6226` and
  `product_id=2681` after read-only MAX checks.
- Follow-up run `production_silver_v2_dimensions_20261117` completed
  `success`. Silver produced `customers=PASS (1)`, `products=PASS (1)`, and
  `order_items=PASS (1000)`.
- `orders`, `payments`, and `shipments` remained `NO_DATA`; therefore the
  candidate contract still rejects the bundle because all four current facts
  must be PASS. Gold remained disabled and no candidate branch/write occurred.
- Next gate requires separate approval for bounded source deltas covering
  `orders`, `payments`, and `shipments` (with FK-valid data), followed by one
  more six-table Silver run.

## Session 2026-11-17: bounded Gold candidate runtime — WRITE/READBACK PASS

- Source run: `production_silver_v2_cert_20261117` with all six Silver
  artifacts PASS.
- Created and used only candidate ref
  `gold_prod_candidate_v2_cert_20261117` and namespace
  `gold_candidate_v2_cert_20261117`; stable `gold_prod_current` was not
  advanced.
- Nessie candidate read-back is an exact BRANCH at hash
  `a4274ec5b703fc37a34ecb8efab17931773aa82987e37ca324a7c798cbfb59e7`.
- Native Gold writer returned `PUBLISHED`; completed graph verification
  returned `VERIFIED` with graph identity
  `0859025fd0fec3226fa75c2e1cb78cd46d3a9286d1f3a985ba1a85df197d6641` and
  exactly seven PASS table manifests/readbacks.
- Readback rows: `dim_customer=2`, `dim_date=5844`, `dim_product=1001`,
  `fact_orders=1`, `fact_order_items=1000`, `fact_payments=1`,
  `fact_shipments=1`; all have positive files and PASS readback identities.
- No stable publication, cutover, cleanup, delete, rollback, checkpoint or
  watermark mutation occurred. Independent QA review remains the final
  delivery gate; no commit/push/merge was made.

## Session 2026-11-17: final regression gate — PASS; independent QA pending

- Candidate/manifest/Airflow regression suite: **135 passed**; compileall and
  `git diff --check` passed.
- Runtime candidate evidence remains `WRITE/READBACK PASS` on the immutable
  candidate ref. Stable ref and consumers were not changed.
- Reviewer was dispatched against the implementation diff and evidence. Until
  the reviewer returns, status is **runtime PASS / not independently
  certified**. No commit, push or merge was made.

## Session 2026-11-17: candidate isolation QA corrections — STATIC PASS; re-review pending

- Reviewer found three HIGH issues: candidate ref loss in `StorageLayout`,
  missing production bucket validation in the candidate CLI, and
  non-deterministic candidate namespace acceptance.
- Fixed all three: storage layout preserves the requested candidate ref, the
  CLI validates production bucket/layout before clients, and namespace is
  deterministic from the candidate ref.
- Added raw runtime evidence:
  `docs/evidence/gold-candidate-v2-runtime-20261117.json`.
- Final regression after corrections: **146 passed**, compileall PASS,
  `git diff --check` PASS.
- Reviewer re-review is pending. Until it returns, do not advance
  `gold_prod_current`; runtime remains candidate-only PASS WITH WARNINGS.

## Session 2026-11-17: candidate certification — QA PASS

- Added all seven Gold `manifest_key` values to the raw runtime evidence.
- Structural evidence validation: PASS.
- Independent reviewer re-review: **PASS**; no Critical/High/Medium finding
  remains.
- Candidate isolation, seven-table metadata/readback, deterministic namespace,
  production bucket/ref routing, and no-publication/cutover guarantees are
  accepted.
- `gold_prod_current` remains unchanged. Stable publication/cutover requires a
  separate explicit approval.

## Session 2026-11-17: approved stable CAS promotion — PASS

- Explicit approval received for stable promotion.
- Read-back before CAS: candidate branch hash
  `a4274ec5b703fc37a34ecb8efab17931773aa82987e37ca324a7c798cbfb59e7`;
  stable expected hash
  `adb060d0499e2cfd0fd38d28b3c6c3b85ef5c1aedd54238fbe3a7d652f0e196e`.
- Applied Nessie CAS update to `gold_prod_current`; stable read-back equals
  candidate hash and remains a `BRANCH`.
- Post-promotion Spark read-only verification passed for all seven pinned
  snapshots, with row/file/byte evidence matching candidate manifests.
- No cleanup, delete, rollback, checkpoint reset or watermark mutation was
  performed. No commit, push or merge was made.

## Session 2026-11-17: live read-only Iceberg baseline probe — NOT CERTIFIED

- Both pinned snapshots loaded successfully from `gold_prod_current` in the
  Spark container and matched physical readback: `dim_customer` 6226 rows / 2
  files / 131997 bytes; `dim_product` 2681 rows / 2 files / 49680 bytes.
- Independent QA still classified full runtime certification **FAIL / NOT
  CERTIFIED** because the existing v1 baseline does not persist the complete
  v2 contract: schema/graph/stale/ref-bound identities, complete seven-table
  graph evidence, and Silver lineage binding.
- Sanitized audit record:
  `docs/evidence/gold-prod-current-readonly-baseline-20261117.json`.

## Session 2026-11-17: current stable-ref closure — PASS

- The historical v1 baseline probe above is retained as historical evidence;
  it does not supersede the approved v2 CAS promotion recorded earlier.
- Current `gold_prod_current` is the promoted v2 candidate hash
  `a4274ec5b703fc37a34ecb8efab17931773aa82987e37ca324a7c798cbfb59e7`.
- The post-promotion seven-table pinned-snapshot read-only verification is the
  current runtime gate evidence and passed. Candidate branch retention is
  intentional; no cleanup, delete, rollback, checkpoint reset, or watermark
  mutation was performed.
- Delivery status: **PASS for the approved Gold v2 promotion scope**. No Git
  commit, push, or merge was performed.

## Session 2026-11-17: bounded production WRITE attempt — BLOCKED SAFELY

- Triggered exactly one approved run:
  `manual__gold_v2_production_write_20261117` with explicit six-table
  `WRITE` config and process-scoped write guards.
- Gold write failed closed in preflight before any Spark/MinIO/Nessie Gold
  client was created: current Silver `customers=NO_DATA` caused
  `Gold dimensional facts require current Silver PASS: customers`.
- Airflow exhausted the task's configured retries; no additional run was
  triggered. Gold publication, stable-ref mutation, cleanup, rollback,
  checkpoint reset, and watermark mutation did not occur. Some approved
  Silver tasks (`products` and `order_items`) completed before the Gold gate.
- Temporary write guards were reverted by recreating only scheduler and
  dag-processor; verified state is `GOLD_DIMENSIONAL_ENABLED=false`,
  `GOLD_DIMENSIONAL_MODE=DRY_RUN`,
  `GOLD_DIMENSIONAL_WRITE_ENABLED=false`,
  `PRODUCTION_LAKEHOUSE_WRITE_APPROVED` unset, and
  `LAKEHOUSE_WRITE_ENABLED=false`.
- Next step requires either a separately approved idempotent source delta that
  produces current PASS artifacts for `customers` and `products`, or
  implementation/certification of the runtime snapshot-reuse adapter.

## Session 2026-11-17: production Gold WRITE runtime — PASS

- Approved source deltas produced a complete FK-valid Silver recovery bundle;
  the final bounded DagRun was
  `manual__gold_v2_silver_recovery_full_20261117`.
- Rebuilt six read-only dimensional artifact tasks after correcting the
  canonical manifest identity and reran the Gold task in the same DagRun.
- Runtime result: `gold_dimensional_write=success`,
  `publish_gold_to_active=success`, and all seven Gold confirmations
  `success`. Writer returned `status=PUBLISHED` and
  `verification.status=VERIFIED`.
- Seven-table readback passed: `dim_date=5844`, `dim_customer=2`,
  `dim_product=2`, `fact_orders=1`, `fact_order_items=1000`,
  `fact_payments=1`, `fact_shipments=1`; every output had positive files and
  `readback_status=PASS`.
- Write flags were restored and verified disabled/DRY_RUN after completion.
  No cleanup, delete, rollback, checkpoint reset, or watermark reset was
  performed.
- Sanitized raw task-level evidence is persisted at
  `docs/evidence/gold-production-write-runtime-20261117.json`.
- Independent QA review: **PASS WITH WARNINGS** initially; the evidence-gap
  warning is addressed by the raw task-level record and requires re-review.

## Session 2026-11-17: Airflow production DRY_RUN — PASS

- Triggered exactly one run of `ecommerce_incremental_ingestion` with run ID
  `manual__gold_v2_dry_run_20261117` and explicit six-table `DRY_RUN` config.
- DagRun state: **success**. All executed validation/artifact tasks succeeded;
  `gold_incremental_snapshot_reuse_dry_run` succeeded.
- `gold_dimensional_write`, `publish_gold_to_active`, and all seven Gold
  confirmation tasks were **skipped** as required by the write-disabled
  `DRY_RUN` gate.
- No new Gold write, publication, cutover, cleanup, checkpoint reset, or
  watermark mutation occurred. The next gate is a separately approved single
  production `WRITE` run followed by independent QA.

## Session 2026-11-17: Gold v2 pinned snapshot metadata contract — QA PASS WITH WARNINGS

- Implemented versioned Gold metadata for new dimensional writes, including
  schema/graph/manifest identities, pinned snapshot ID, catalog coordinates,
  explicit stale policy and readback identity. Producer and snapshot-reuse
  validator now share canonical manifest identity semantics.
- Added fail-closed checks for status/table/snapshot mismatches, readback hash
  tampering, stale/future/naive timestamps, coordinate mismatches and
  `ALREADY_PUBLISHED` metadata preservation. DRY_RUN does not emit reusable
  PASS metadata.
- Independent final QA verdict: **PASS WITH WARNINGS**. Evidence: 200
  focused tests passed, AST parse passed for 213 Python files, and
  `git diff --check` passed. No Critical/High/Medium/Low defect remains.
- Runtime Spark snapshot-reuse dry-run on Windows did not complete because of
  the local Spark/Hadoop environment; runtime certification remains
  **NOT CERTIFIED**. No MinIO/Nessie/PostgreSQL object or state mutation,
  candidate branch, production Gold WRITE, cutover, cleanup, checkpoint or
  watermark action was performed.
- Next step: obtain separate approval for a bounded Linux/container Spark
  runtime dry-run, then separately gate any candidate branch creation and
  production candidate WRITE. Do not claim production runtime PASS from this
  static/unit verdict.

- Approved bounded Linux/container command completed
  `tests/test_gold_snapshot_reuse_dry_run.py`: **1 passed in 13.38s** with
  WRITE/publication/cutover flags disabled. Independent QA classified this as
  **PASS WITH WARNINGS — bounded test evidence only**: the test uses injected
  readers and monkeypatched storage, so it does not certify live
  Iceberg/Nessie/MinIO snapshot reads or production runtime. Runtime gate stays
  **NOT CERTIFIED**.

## Session 2026-11-17: Gate C artifact-bundle QA and production preflight — BLOCKED

- Independent QA verdict for the read-only artifact bundle implementation:
  **PASS WITH WARNINGS**; 32 focused tests passed, compileall and
  `git diff --check` passed, with no Critical/High/Medium findings.
- Read-only production evidence was rechecked for the approved Silver run
  `production_silver__20261117T031723Z__3d525ed68c967f928f85538a3f7091db`.
  The four Silver manifests contain checksum, lineage evidence and producer
  `run_id`; the Airflow `silver_result_*` XCom envelopes do not expose all of
  those fields directly, so the bundle must be built from the stored manifests.
- Gate C remains **BLOCKED before candidate creation** because the existing
  Gold `dim_customer`/`dim_product` metadata has snapshot/readback evidence but
  lacks the complete pinned-reuse contract: canonical schema and graph
  evidence, stale policy, and ref-bound readback. No synthetic evidence was
  accepted, and no Nessie ref, Spark job, Gold WRITE, checkpoint, watermark,
  publication, or cleanup mutation was performed.
- Next exact step: produce a runtime-verified Gold baseline manifest carrying
  the full pinned-reuse contract (or obtain a fresh approved dimension build),
  then rerun Gate C read-only. Candidate branch creation remains a separate
  explicit approval gate.

- Additional read-only Spark evidence: `dim_customer` and `dim_product` both
  loaded successfully from `gold_prod_current`; observed columns match the
  declared contract, with readback `6226 rows / 2 files / 131997 bytes` and
  `2681 rows / 2 files / 49680 bytes` respectively. This closes the schema and
  physical readback check only; it does not manufacture the missing persisted
  graph/stale-policy/ref-bound evidence, so Gate C remains blocked.

## Session 2026-11-17: bounded Gold snapshot-reuse runtime attempt — not certified

- Separately approved read-only Spark `DRY_RUN` attempted with write,
  publication, cutover, checkpoint, and feature-enable flags disabled.
- Local Spark produced no output within the safe 50-second bound and was
  stopped safely; exit code `1`.
- Independent QA verdict: **FAIL — runtime certification gate; NOT CERTIFIED
  (timeout)**. No false PASS and no observed external mutation.
- Do not retry in the same environment. A materially different bounded Spark
  environment and fresh explicit approval are required before another attempt.

## Session 2026-11-17: Gold snapshot reuse implementation — static QA complete

- Implemented mixed Silver/Gold resolution: unchanged `customers/products`
  dimensions may reuse pinned verified Gold snapshots; facts require current
  Silver `PASS` artifacts and fact `NO_DATA` fails closed.
- Added mixed lineage, deterministic resolution identity, pre-client
  validation, JSON-safe XCom boundary checks, and regression coverage.
- Independent QA verdict: **PASS WITH WARNINGS**. Focused QA tests passed
  (`74 passed`), compileall and diff-check passed; no Critical/High/Medium
  finding remains.
- Spark/Iceberg/Nessie/MinIO runtime behavior is **not certified**. The feature
  flag remains disabled by default; no production Gold WRITE, publication,
  cutover, or runtime mutation was performed.

## Session 2026-11-17: Gold snapshot reuse explicit-gate correction — static evidence

- Runtime merge-plan construction now honors the existing
  `GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED` helper and cannot implicitly enable
  pinned dimension reuse when called outside the preflight path.
- Regression coverage confirms mixed dimension `NO_DATA` with a valid pinned
  artifact fails closed before snapshot/Silver reads when the flag is absent.
- Focused suite: **174 passed**; compileall exit `0`; `git diff --check` exit
  `0` with existing LF/CRLF conversion warnings.
- This is implementation evidence only. No DAG trigger, external runtime
  client, production flag enablement, Gold write, cutover, or runtime
  certification is claimed.

## Session 2026-11-17: canonical Silver identity re-verification — blocker closed

- Fresh candidate `production_silver__20261117T031723Z__3d525ed68c967f928f85538a3f7091db`
  completed `success`: 59 tasks, 42 `success`, 17 `skipped`, 0 failed.
- `customers` and `products` were `SKIPPED / NO_DATA`, confirmed as expected
  no delta at the effective checkpoint; no retry, backfill, reset, cleanup, or
  mutation was performed for them.
- For `orders`, `order_items`, `payments`, and `shipments`, exact XCom
  `manifest_key` read-back matched MinIO stored manifests, and each satisfied
  `XCom identity = stored identity = canonical identity`.
- Independent QA verdict: **PASS WITH WARNINGS**; no Critical/High/Medium
  finding. The Silver manifest-identity blocker is closed for the four
  committed tables. Full six-table data coverage is still not claimed because
  two tables had no delta.
- Gold runtime, consumer cutover, and production publication remain open and
  require a separate explicit approval gate.

## Session 2026-11-17: fresh production Silver run - six-table gate remains open

- Exactly one new candidate was triggered:
  `production_silver____dee17da68cca499299c18eb45cbbabcf`, logical date
  `2026-11-17T02:45:56Z`; the Airflow POST returned HTTP `200`.
- DagRun completed `success` with 59 task instances: 42 `success`, 17
  `skipped`, 0 failed. No retry, reset, cleanup, commit, or Gold launch was
  performed. JWT handling was memory-only.
- Production guards were correct: bucket `ecommerce-lakehouse-prod`, ref
  `gold_prod_current`, ingestion enabled; E2E/quality-it unset; warehouse,
  lakehouse, Gold, publish, cutover, backfill, checkpoint/watermark and cleanup
  disabled.
- Read-back found committed Silver artifacts for `orders`, `order_items`,
  `payments`, and `shipments`; each had Bronze input, one Silver Parquet file,
  completion manifest, quality evidence, reconciliation evidence, and exact
  prefix containment. `customers` and `products` were `NO_DATA` and therefore
  have no six-table manifest/evidence in this run.
- Stored MinIO manifest identities differed from the corresponding Silver
  XCom identities for all four committed tables. Quality/reconciliation
  evidence reported `PASS`, but this mismatch blocks immutable six-table
  runtime certification.
- Verdict: **PASS** for safe single production Silver execution and bounded
  read-back; **OPEN / NOT CERTIFIED** for the Gold runtime gate. Do not retry,
  reset, clean up, or launch Gold. Independent QA must review this evidence
  before any next action.

## Session 2026-11-17: production config reconcile and stale/import recovery

- Recreated only `airflow-scheduler` and `airflow-dag-processor` with
  process-scoped production layout (`ecommerce-lakehouse-prod`,
  `gold_prod_current`). `.env` was not edited; API server and other services
  were not restarted; no DAG was triggered.
- Both processes are `running`, `oom=false`, with
  `INGESTION_WRITE_ENABLED=true`; warehouse/lakehouse/Gold,
  cutover/publish/backfill/checkpoint/watermark/cleanup paths remain disabled.
- `AIRFLOW_LOCAL_E2E_MODE`, all `LAKEHOUSE_E2E_*`, and all `QUALITY_IT_*` keys
  are unset in both recreated processes.
- Authenticated GET-only verification passed: health 200 with scheduler, dag
  processor, and metadatabase healthy; import-errors 200 with
  `total_entries=0`; DAG details 200 with `is_stale=false` and
  `has_import_errors=false`.
- Verdict: **PASS** for production config reconciliation and import/stale
  recovery; **OPEN / NOT CERTIFIED** for end-to-end runtime certification.
  No DAG trigger, data-plane operation, or destructive maintenance occurred.
- QA handoff: independently verify sanitized Docker evidence and GET-only
  results before advancing the Gold runtime gate. No commit was made.

## Session 2026-11-17: independent QA - latest bounded trigger hard-stop

- Verdict: **PASS** for safe hard-stop; **FAIL / BLOCKED** for runtime
  certification.
- Fresh candidate `qualityit__20261117T__` used logical date
  `2026-11-17T02:01:00Z`; its single exact OpenAPI-path Airflow POST returned
  HTTP `404`. Evidence records a memory-only JWT and no DagRun, task, or data
  mutation; no retry, reset, cleanup, Docker, DAG, or Gold action is authorized.
- Current blocker classification: **POST route/contract-specific**. The
  read-only GET diagnosis resolved the Airflow route and exact DAG metadata,
  so the prior classification is superseded for current status; the earlier
  **endpoint/DAG discovery** wording remains preserved in the historical QA
  entry below. The evidence does not identify which POST contract detail
  caused the `404`, and does not indicate an ingestion or data-plane failure.
  Runtime certification remains **OPEN / NOT CERTIFIED**.
- QA documentation correction: updated only this current classification and
  the related semantic-plan probe-count wording; prior endpoint/DAG discovery
  history remains unchanged. `git diff --check`: **PASS**. No source,
  configuration, `.env`, POST/DAG, restart, or data-store action was performed.

## Session 2026-11-16: independent QA — process-scoped E2E injection

- Reviewed the latest evidence in
  `docs/plans/semantic-runtime-certification-plan.md`.
- Acceptance result: **PASS WITH WARNINGS**. Only
  `airflow-scheduler` and `airflow-dag-processor` were recreated with
  `--no-deps --force-recreate`; both are recorded as `running`, image
  `ecommerce-airflow:3.1.7`, `oom=false`.
- The exact bounded contract is recorded as approved: `LAKEHOUSE_E2E_APPROVED=1`,
  bucket `ecommerce-data-lake`, run ID
  `qualityit_20261116232139_0a5f832a06fc`, and prefix
  `quality-it/qualityit_20261116232139_0a5f832a06fc/`. `INGESTION_WRITE_ENABLED`
  is `true`; Gold, lakehouse, warehouse, historical backfill, publication and
  all Iceberg cutover guards remain disabled.
- No DAG, token/API, task, or data-plane operation was performed or authorized
  by this QA review. Runtime Silver/Gold certification remains **OPEN / NOT
  CERTIFIED** because no trigger/read-back/manifests/checksums/reconciliation
  evidence exists.
- Exact next step: obtain separate explicit approval for one fresh bounded
  trigger and its runtime verification. Do not trigger, retry, reset, clean up,
  or access data stores from this evidence.

## Session 2026-11-16: independent QA — Silver preflight hard-stop

- Reviewed the latest three-service host-inspection evidence recorded in
  `docs/plans/semantic-runtime-certification-plan.md`.
- QA verdict: **PASS** for safe hard-stop handling. `INGESTION_WRITE_ENABLED`
  is `false`; E2E approval is `0` with bucket/prefix/run-id unset; Gold,
  warehouse, lakehouse, historical backfill, and all six cutover guards are
  disabled. The operation stopped before trigger, with no DagRun or data
  mutation claimed.
- This verdict covers preflight safety only; Silver lineage/runtime
  certification remains **OPEN/BLOCKED**. No Docker or DAG was run for this QA.
- Exact next step: after separate explicit approval, recreate only the needed
  scheduler/worker service with command-scoped
  `INGESTION_WRITE_ENABLED=true`, then host-inspect the effective flags before
  any trigger. Stop on any mismatch.

## Session 2026-11-16: Airflow write-disabled service recreate

- Approved scope: recreate only `airflow-scheduler`, `airflow-dag-processor`,
  and `airflow-api-server` after applying the already-approved
  `LAKEHOUSE_WRITE_ENABLED=false`. No `.env` edit, DAG trigger, data-store
  access, retry/reset/cleanup, commit, push, merge, or other service action.
- Exact command executed:
  `docker compose up -d --no-deps --force-recreate airflow-scheduler airflow-dag-processor airflow-api-server`.
- Compose result: all three named containers were `Recreated` and `Started`.
  Docker reported an existing `ecommerce-spark` orphan warning; no
  `--remove-orphans` was used and `ecommerce-spark` was not touched.
- Exact post-recreate host-side allowlisted inspect evidence (no `docker exec`,
  no full environment/secrets output):
  - `ecommerce-data-platform-airflow-scheduler-1`: `status=running`,
    `running=true`, `oom=false`, `image=ecommerce-airflow:3.1.7`,
    `LAKEHOUSE_WRITE_ENABLED=false`.
  - `ecommerce-data-platform-airflow-dag-processor-1`: `status=running`,
    `running=true`, `oom=false`, `image=ecommerce-airflow:3.1.7`,
    `LAKEHOUSE_WRITE_ENABLED=false`.
  - `ecommerce-data-platform-airflow-api-server-1`: `status=running`,
    `running=true`, `oom=false`, `image=ecommerce-airflow:3.1.7`,
    `LAKEHOUSE_WRITE_ENABLED=false`.
- Verification verdict: **PASS** for the requested service recreate and
  write-disabled runtime parity. No DAG or data operation was triggered.
- QA handoff: independently review this exact operational evidence and confirm
  no scope expansion; no further runtime action is authorized by this entry.

## Session 2026-11-16: independent QA — lakehouse write deny-by-default

- Review scope: `docker-compose.yml`, `.env.example`,
  `.env.quality-it.local.example`, `tests/test_quality_it_local_compose.py`,
  and this plan entry. No Airflow service, DAG, database, MinIO object,
  Nessie ref, checkpoint, or runtime state was modified.
- Acceptance results: `docker-compose.yml:14` uses
  `"${LAKEHOUSE_WRITE_ENABLED:-false}"`; both committed templates contain the
  literal `LAKEHOUSE_WRITE_ENABLED=false`; rendered Compose with the variable
  absent resolves `false` for `airflow-init`, `airflow-api-server`,
  `airflow-scheduler`, and `airflow-dag-processor`.
- Verification: `.venv\Scripts\python.exe -m pytest
  tests/test_quality_it_local_compose.py -q` — **14 passed**;
  scoped `compileall` — **PASS**; `docker compose config --quiet` — **PASS**;
  rendered Compose inspection — **PASS**; `git diff --check` — **PASS**.
  Docker emitted only inaccessible local config/missing host-variable warnings;
  no service was started or restarted and no DAG was triggered.
- `.env` has no Git diff and was not edited. Its existing
  `LAKEHOUSE_WRITE_ENABLED=false` line is unchanged.
- Independent QA verdict: **PASS WITH WARNINGS** for this scoped flag change.
  The working tree contains many unrelated modifications, including additional
  changes in some reviewed files; this verdict is not approval of those changes
  or of the whole tree.
- Required fix: none for the scoped deny-by-default flag. Before delivery,
  isolate/clean unrelated changes or review them separately; runtime
  certification and any write/cutover operation remain separately gated.

## Session 2026-11-16: explicit lakehouse write deny-by-default configuration

- Approved scope: add only the `LAKEHOUSE_WRITE_ENABLED` Compose default, literal
  `false` values to the two committed env templates, the matching static
  regression assertions, and this plan evidence. `.env`, backups, runtime code,
  DAGs, checkpoints and data stores remain untouched.
- Implementation: root `docker-compose.yml` now injects
  `LAKEHOUSE_WRITE_ENABLED: "${LAKEHOUSE_WRITE_ENABLED:-false}"`; both
  `.env.example` and `.env.quality-it.local.example` declare
  `LAKEHOUSE_WRITE_ENABLED=false`.
- Verification evidence: `docker compose config --quiet` **PASS**; sanitized
  rendered Compose inspection resolved `LAKEHOUSE_WRITE_ENABLED=false` for
  `airflow-init`, `airflow-api-server`, `airflow-scheduler`, and
  `airflow-dag-processor`; focused
  `tests/test_quality_it_local_compose.py` **14 passed**; scoped source/test
  `compileall` **PASS**; `git diff --check` **PASS**. Docker emitted only the
  pre-existing inaccessible local config warning. No Compose service was
  started or restarted and no DAG was triggered.
- QA handoff: independently review the scoped diff and confirm the rendered
  value is `false` when the variable is absent. Runtime certification remains
  open; this static change does not enable any write path or cutover.

## Session 2026-11-16: bounded scheduler recreate — stopped before trigger

- Approved scope: recreate only `airflow-scheduler` with command-scoped
  `INGESTION_WRITE_ENABLED=true`; no dependencies, `.env` edit, other service
  restart, Gold launch, commit, push, merge, reset, retry, or cleanup.
- Pre-recreate effective compose evidence: `airflow-scheduler` image
  `ecommerce-airflow:3.1.7`, command `scheduler`; ingestion was `false`,
  warehouse/lakehouse writes were disabled, all six Iceberg cutover flags were
  `false`, Gold was disabled with `DRY_RUN`, publication was `0`, and
  historical backfill was disabled with `DRY_RUN`.
- Exact service action: `docker compose --env-file .env up -d --no-deps
  --force-recreate airflow-scheduler` with process-scoped
  `INGESTION_WRITE_ENABLED=true`. Result: container
  `ecommerce-data-platform-airflow-scheduler-1` was `Recreated` and `Started`;
  image remained `ecommerce-airflow:3.1.7`. The compose orphan warning for
  `ecommerce-spark` was informational; `--remove-orphans` was not used.
- Post-recreate service read-back: scheduler was `Up` (`13 seconds`) when
  checked. The sanitized-flag read-back command failed with
  `sh: 1: Syntax error: "(" unexpected` due to the shell filter expression.
- Gate result: **STOPPED BEFORE TRIGGER**. No new `run_id`, DagRun, task
  instance, API trigger, worker execution, MinIO manifest, schema, quality,
  reconciliation, or prefix read-back evidence exists for this session.
- Required next step: obtain explicit direction to run one corrected,
  read-only sanitized-flag preflight. If it passes, a fresh unique six-table
  run may be considered under the existing approval; if it fails, stop again.

## Session 2026-11-11: explicit Gold Parquet contract

- Implementation: Gold Iceberg table creation now declares
  `format-version=2` and `write.format.default=parquet`.
- QA verdict: **PASS WITH WARNINGS**; focused writer tests passed (`11 passed`),
  compileall and scoped diff checks passed.
- Recovery, manifest, fence, MERGE, cutover and write guards are unchanged.
- Runtime physical-file/Nessie snapshot verification remains pending a separate
  bounded approval; this session did not perform external writes or destructive
  Iceberg/object-store operations.

## Session 2026-11-11: read-only Gold quality-it metadata

- Dedicated `ecommerce-quality-it` Nessie catalog contains all seven Gold
  tables.
- Read-only `$files` inspection reported `PARQUET` for every table; each table
  had one file in the inspected snapshot, with records/bytes read back.
- Read-only table properties reported `format=iceberg/parquet` and
  `format-version=2` for the Gold tables; current snapshot IDs and Nessie
  commit metadata were present.
- No Gold `WRITE`, object mutation, cleanup, compaction, rollback or cutover
  was executed. Production/disposable certification remains pending.

## Session 2026-11-11: Airflow Gold UI layout

- Removed the `gold_iceberg` TaskGroup wrapper so `gold_dimensional_write` is
  displayed at DAG root.
- Preserved the atomic writer, six Silver artifact upstreams, seven Gold
  confirmation markers, task IDs and runtime behavior.
- QA verdict: **PASS WITH WARNINGS** from static/source review; pytest and
  Airflow DagBag import remain pending because the local environment lacks
  `pytest` and `airflow.sdk`.
- Follow-up Airflow container verification passed: `airflow dags list-import-errors`
  returned `No data found`, and `airflow dags show` rendered the writer at root
  with six artifact upstreams and seven confirmation downstreams. Container
  pytest remains unavailable because the image lacks `pytest`.

## Lifecycle semantic evidence

- Status: **PASS WITH WARNINGS (code-level)**.
- Policy: `REFUNDED` payments and `CANCELLED`/`DELETED` orders contribute one
  negative `signed_revenue` measure; raw/base monetary measures remain
  non-negative and are preserved.
- dbt fact model parity and the additive migration artifact are prepared, but
  the migration is **not applied**. PostgreSQL/dbt runtime parity remains
  pending.
- No production write, cutover, physical deletion or shared-service mutation
  was performed.

## Mục tiêu và phạm vi

Tiến hoá Gold thành dimensional Iceberg lakehouse:

`Bronze -> verified Silver Parquet -> Gold Dimensions/Facts (Iceberg) -> derived marts`

Local-only Gold validation remains bounded to the approved Iceberg/Nessie stack;
Nessie is used for Gold targets, not as the Silver source layer.
Production routing, consumer cutover, and production certification are out of
scope for this plan.

## Roadmap status

| Phase | Status | Ghi chú / next proof |
|---|---|---|
| A. Runtime certification | **Local runtime PASS**; production/disposable certification **pending** | Smoke, Gold `DRY_RUN`, local `PUBLISHED`/`ALREADY_PUBLISHED` retry và read-only metadata metrics đã được kiểm tra trong dedicated local scope. Chưa claim production hoặc approved disposable certification. |
| B. Semantic contract | **PASS (code-level; business/analytics sign-off pending)** | Contracts đã khai báo grain, keys, measures, FK, UTC/date semantics và parity fail-closed. Còn chờ business/analytics sign-off về metric ownership, additivity và downstream parity. |
| C. Late data, deletes, schema drift | **Local runtime PASS WITH WARNINGS**; operational proof **pending** | Đã chạy local-only real Silver Iceberg snapshots + Gold `DRY_RUN`; 4 dimensional quality checks PASS. Tombstone application, schema-drift operational replay và production/disposable proof còn pending. |
| D. Iceberg operations | **Local observability runtime PASS WITH WARNINGS**; operational proof **pending** | Bốn Gold local read-only metrics PASS cho snapshot/file/record/bytes. Query performance, baseline/threshold, maintenance safety và disposable operational proof còn pending. |
| E. Controlled cutover | **PASS (preflight code-readiness/DRY_RUN; controlled cutover pending)** | Immutable JSON preflight giữ route Parquet và `DRY_RUN`; không route consumer hoặc enable write. Controlled cutover vẫn bị block bởi các operational gates. |

## Phase A local runtime evidence và QA

- Local dedicated bucket/runtime validation: **PASS** cho Iceberg smoke,
  Gold `DRY_RUN`, first-run `PUBLISHED`, same-lineage retry
  `ALREADY_PUBLISHED`, và read-only Iceberg metadata metrics.
- QA verdict: **PASS WITH WARNINGS** cho local runtime scope.
- Warning: fixture teardown có `DROP TABLE` nhưng chỉ chạy trong dedicated local
  fixture bucket/catalog; đây không phải production cleanup và không phải quyền
  cho phép destructive maintenance ngoài scope.
- Chưa có production/disposable certification gate; operator provisioning,
  trusted evidence/digest và approval riêng vẫn bắt buộc.

## Phase B evidence và QA

- Contract/reconciliation suite: `11 passed`.
- Spark dimensional transformation suite: `7 passed, 1 warning`; chạy trên
  Windows với process-local environment.
- `py_compile`: PASS; `git diff --check`: PASS.
- QA verdict: **PASS WITH WARNINGS**.

## Phase C evidence và QA

- Local-only runtime proof: **PASS WITH WARNINGS**.
- Đã tạo/cập nhật Silver Iceberg tables trong dedicated `ecommerce-quality-it`
  local stack bằng `MERGE`, đọc snapshot IDs thật từ Iceberg metadata, publish
  attestation/evidence tương ứng và gọi Gold `DRY_RUN` với genuine pinned lineage.
- Bốn dimensional quality checks trả `PASS` trong Gold `DRY_RUN`.
- Boundary: không ghi Gold target, không bật Gold `WRITE`, không cleanup,
  maintenance, production/shared path hoặc cutover.
- Chưa có tombstone application, schema-drift operational replay,
  late-data replay trên production/disposable environment, hoặc certification
  gate được phê duyệt.

## Phase D evidence và QA

- Local observability runtime: **PASS WITH WARNINGS**.
- Bốn Gold local read-only metrics PASS: snapshot, file, record và bytes.
- Collector chỉ đọc metadata tables; không cleanup, snapshot expiration,
  compaction, rollback hoặc cutover.
- Query-performance metrics, baseline/threshold definition, maintenance safety
  proof và disposable operational certification vẫn pending.
- Warning: local metrics không đại diện cho production load hoặc SLO baseline.

## Phase E evidence và QA

- `tests/test_gold_cutover_preflight.py`: `3 passed`.
- `py_compile`: PASS; `git diff --check`: PASS.
- QA verdict: **PASS WITH WARNINGS** cho preflight code-readiness/`DRY_RUN`.
- Immutable preflight giữ `current_route=parquet`, `action=DRY_RUN`,
  `can_route=false`, `can_write=false`; tamper và non-Parquet route bị reject.
- Controlled cutover bị block bởi runtime certification, business/analytics
  sign-off, late/delete operational proof, Iceberg operational proof và
  consumer parity.

## Local validation evidence

Validation chỉ dùng stack local/disposable và bucket `ecommerce-quality-it`;
không phải production certification. Runbook tại
[QUALITY_IT_LOCAL.md](QUALITY_IT_LOCAL.md) mô tả smoke, fixture `DRY_RUN`,
local `WRITE` retry, recovery contract và Phase C local-stack runner.

## Approval boundaries

## Native Gold dimensional v1 expansion

## Session 2026-11-16: independent QA — disposable Airflow no-op diagnostic

- Verdict: **PASS WITH WARNINGS** for the scoped Airflow 3.1.7 no-op only.
- Confirmed target `confirm_gold_dimensional_dim_date` is an `EmptyOperator`;
  recorded runtime evidence shows exit `0`, task success for TI
  `01a1e447-9730-7342-a13f-04d0679965ba`, health `200`, protected endpoint
  `401` without secret exposure, no termination/error markers,
  `dag_run_found=false`, and no failed-run reuse or data writes.
- Warning: raw runtime logs are summarized rather than stored as immutable repo
  artifacts, and the working tree contains unrelated changes. No whole-tree
  approval is implied.
- Exact next step: obtain separate explicit approval before one fresh immutable
  bounded six-table run. Keep the failed run untouched and Gold WRITE/cutover
  disabled; production/runtime certification remains **OPEN/BLOCKED** until
  Silver lineage, exact-run identity, read-back and integrity evidence pass.

## Session 2026-11-16: bounded Gold snapshot-reuse writer integration

- Status: **PASS WITH WARNINGS (bounded code/unit scope)**.
- The writer consumes only an explicit, revalidated incremental merge plan;
  reused dimensions are passed as already-verified DataFrames. Existing Gold
  transformations, grain/FK quality checks, MERGE keys, fences and pinned
  readback remain authoritative.
- Default-off guard and fail-closed missing/tampered plan behavior are covered
  by pure/fake tests. Focused suite: **47 passed**; scoped `compileall` and
  `git diff --check` passed.
- No DAG wiring, feature enablement, Spark/MinIO/Nessie/Postgres external
  runtime, external write, checkpoint mutation, maintenance or cutover was
  performed. Runtime snapshot read/merge certification remains pending.
- Next step: independent QA reviewer handoff, followed by a separately
  approved controlled `DRY_RUN`; keep `GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED`
  disabled until those gates pass.

- Status: **PASS WITH WARNINGS (local-only runtime)**; production/disposable
  certification and cutover remain pending.
- Source contract now covers six pinned Silver sources: `customers`,
  `order_items`, `orders`, `products`, `payments`, and `shipments`.
- Native Gold output contract covers seven Iceberg targets: three dimensions
  (`dim_date`, `dim_customer`, `dim_product`) and four facts (`fact_orders`,
  `fact_order_items`, `fact_payments`, `fact_shipments`).
- One atomic dimensional writer owns the graph write order; feature flags remain
  default-off and the Airflow branch remains skip-safe. No PostgreSQL/Warehouse
  dependency was added to the native operator.
- Historical/session-specific evidence: the earlier reviewed dimensional suite
  recorded `31/33` checks passed; this is not treated as equivalent to the
  current focused suite. Current focused manifest/verifier/wiring suite:
  `24 passed`. Spark driver and worker used the project `.venv` Python in the
  Spark session; one PySpark/Pandas compatibility `FutureWarning` remains.
- Local runtime evidence: seven-output `DRY_RUN`, first local `WRITE`, and
  same-lineage `ALREADY_PUBLISHED` retry completed in the dedicated local scope.
- Implementation evidence: Airflow native local-E2E wiring now requires all six
  verified Silver results before dimensional lineage, passes canonical JSON via
  XCom `tojson`, verifies the graph-manifest barrier before seven non-writing
  confirmation nodes, and keeps aggregate Gold disabled in the local branch.
- QA follow-up: the graph barrier is now a read-only manifest verification task;
  it consumes the native writer result, checks the completed graph and exact
  seven unique table manifests, and fails closed on missing/mismatched or
  duplicate evidence. QA verdict: **PASS WITH WARNINGS**; duplicate-output
  regressions and focused wiring/manifest checks are complete.
- Local preflight implementation: the root Airflow local configuration example
  now bounds `BRONZE_BUCKET=ecommerce-quality-it`, Nessie/local flags, and
  explicit `GOLD_DIM_DATE_START`/`GOLD_DIM_DATE_END` without secrets. Native
  lineage is built only from six verified Silver artifact envelopes; manual
  `GOLD_DIMENSIONAL_SOURCE_LINEAGE` injection is no longer accepted.
- Airflow implementation now wires each of the six Silver branches through
  migration, attestation, reconciliation, and quality operators, then accepts
  all three XCom outputs only when they normalize to one `PASS` dimensional
  artifact envelope with matching table/snapshot/evidence identities.
- Runtime migration/attestation/reconciliation/quality execution and the
  resulting 7-output local E2E remain pending; no Docker recreate or data
  mutation was performed in this session.
- Runtime preflight failure evidence: an earlier local run showed
  `dimensional_artifact_*` and `verify_gold_dimensional_graph_manifest` starting
  before their upstream operators. The DAG has since added explicit artifact,
  writer, verifier, and confirmation dependencies; a new runtime proof is
  required before claiming the fix.
- Runtime r2 failure evidence: all `bronze_to_silver_*` branches were skipped
  because the local profile still used Parquet source enums. The local profile
  now explicitly selects `iceberg` for all six Silver tables; a new bounded
  runtime proof is required before claiming the configuration fix.
- Runtime r3 diagnosis: the Silver eligibility branch correctly requires
  validation mode `WRITE`; the local profile had not enabled
  `INGESTION_WRITE_ENABLED`, so it remained fail-closed and skipped all six
  transformations. The profile now enables ingestion writes only for the
  bounded quality-it run; a new runtime proof remains pending.
- Runtime r4 reconciliation diagnosis: a current 27-row manifest was compared
  with the retained 6,316-row Iceberg table head, producing a false whole-table
  mismatch. Manifest-scoped reconciliation now limits Iceberg parity/hash/FK
  checks to source keys and accepts the recorded manifest snapshot even when a
  later table snapshot exists. Local runtime proof of six deterministic source
  deltas remains pending; no existing artifacts were deleted or cleaned up.
- Local E2E source fixture: added a bounded PostgreSQL seed command using
  idempotent FK-ordered upserts for all six source tables. It requires the
  dedicated `ecommerce-quality-it` profile and `INGESTION_WRITE_ENABLED=true`;
  it performs no delete/drop/truncate. Runtime execution and six-branch proof
  remain pending because Docker was not run in this session.
- The seed is batch-scoped by `QUALITY_IT_RUN_ID`: same ID replays the same
  deterministic IDs/timestamp idempotently, while a new ID creates a new
  six-table delta within the bounded local Gold date window.
- Pending: production/disposable certification, consumer routing, and cutover.
  No production write or shared-service mutation is claimed.

## Session B — Local Airflow dimensional runtime fix

- Status: **PASS WITH WARNINGS (code + bounded local runtime evidence)**.
- Fixes: Gold writer now receives a read-only `lakehouse` mount; standalone
  Spark entrypoint import fallback is deterministic; pinned Silver reads project
  only canonical business columns so migration audit metadata does not trigger
  false schema drift.
- Evidence: focused regression suite `42 passed`; `git diff --check` passed.
- Local Airflow run `local-lakehouse-e2e-20261110-r33` completed all six
  dimensional artifacts and lineage successfully before exposing the schema
  projection defect; the stale local transient fence was removed and no
  production/shared bucket was touched. Follow-up runs `r34`/`r37` were
  DagRun-success but skipped the dimensional branch because no new Silver delta
  was eligible, so they are not treated as full writer certification.
- Independent QA verdict: **PASS WITH WARNINGS**. Remaining warning is the
  bounded JSON lineage command-argument size risk; no credential or production
  endpoint exposure found.
- Exact next proof: seed a newer eligible local six-table delta and capture a
  non-skipped run with `gold_dimensional_write`, graph manifest verification,
  and all seven confirmation tasks `success`. Production certification/cutover
  remain out of scope.
- Follow-up fix: `gold_dimensional_gate` now has explicit upstream edges from
  all six dimensional artifact tasks, preventing an early branch decision while
  attestation/reconciliation/quality tasks are still running.

## Session A — Lakehouse-only boundary and Warehouse/dbt removal

- Status: **PASS WITH WARNINGS** for the approved code/config scope.
- Removed the obsolete Raw Warehouse/dbt project, Warehouse-only Airflow tasks,
  dbt Compose service, Warehouse-only mounts/env settings, and related tests.
- Preserved the PostgreSQL source service, Airflow metadata database configuration,
  PostgreSQL/MinIO/Nessie services, and named volumes. No database, object-storage
  data, checkpoint, or shared service was modified.
- Refactored the supported orchestration/docs path to **Bronze → Silver → Gold**;
  Gold write guards and Parquet default remain unchanged.
- Evidence: Compose YAML parse **PASS**; targeted Gold/Compose tests **11 passed**;
  DAG/operator `py_compile` **PASS**; `git diff --check` **PASS**.
- Airflow 3 runtime parse evidence: `airflow dags list-import-errors` returned
  **No data found** after the lazy-import and deterministic XCom task-ID fixes.
- DAG wiring follow-up: all `silver_result_*` outputs are now explicit inputs to
  `pipeline_summary`, followed by the Gold gate and lineage tasks. Regression and
  targeted QA suite: `15 passed`; DAG parse/import check completed in about
  `0.47s`.
- Warnings: two legacy Lakehouse engine tests still mismatch the current merge
  contract and require separate test-contract alignment; they are not treated as
  a runtime certification result.
- Boundary: no Airflow task execution, backfill, migration, Docker runtime,
  production/disposable certification, or cutover was run or claimed.

- Mọi runtime write ngoài local-only scope cần approval riêng.
- `expire_snapshots`, orphan removal, rollback và cleanup/delete cần approval
  riêng.
- Iceberg cutover, consumer routing và production write flags chưa được bật.
## Session C — Bounded shared-bucket isolation review

Implementation now has a fail-closed `BoundedE2EScope` and scoped Gold
warehouse, Nessie ref, Silver namespace, lineage, attestation and evidence
helpers. Focused unit/import evidence is **PASS (54 tests)**.

Independent QA verdict after the follow-up fixes: **PASS WITH WARNINGS**.
Migration/reconcile, attestation CLI, quality CLI, and Airflow upstream
operators now derive and propagate the bounded warehouse, Nessie ref, Silver
namespace, and prefix before creating external clients. Focused evidence is
**PASS (74 tests)** and `git diff --check` is clean. No external write or
cutover was run; the remaining step for an absolute runtime PASS is a
controlled non-destructive DRY_RUN/integration proof on a fresh run ID.
### Controlled read-only scope proof (2026-11-10)

- Nessie `/api/v2/config`: HTTP 200.
- MinIO liveness: HTTP 200.
- Fresh run ID `dr20261110` derived to warehouse
  `s3a://ecommerce-data-lake/quality-it/dr20261110/lakehouse`, ref
  `quality_it_dr20261110`, namespaces `silver_dr20261110` and
  `gold_dr20261110`.
- Sample generated key remained under
  `quality-it/dr20261110/`.
- No table/object write, cleanup, or cutover was performed.

### Full local regression (2026-11-10)

With the local test environment explicitly set to the dedicated
`ecommerce-quality-it` bucket and workspace basetemp, the non-E2E/non-integration
regression completed with **539 passed, 180 deselected, 45 subtests passed**.
Independent review: **PASS WITH WARNINGS**. Runtime MinIO/Nessie/Airflow
certification remains a separate gate because this session intentionally did
not perform external table/object writes.

### Bounded MinIO/Nessie integration proof (2026-11-10)

- Created run-scoped Nessie branch `quality_it_dr20261110b` from `main` using
  the instance's v2 API.
- Spark 3.5.6 created and queried Iceberg table
  `lakehouse.silver_dr20261110b.bounded_smoke` using warehouse
  `s3a://ecommerce-data-lake/quality-it/dr20261110b/lakehouse`.
- Result: `BOUNDED_ICEBERG_SMOKE: PASS`.
- Read-only MinIO verification found 5 objects; every object started with
  `quality-it/dr20261110b/`.
- Read-only Nessie verification returned the expected branch and commit hash.
- No root/shared table was touched; no cleanup or cutover was performed.

### Airflow bounded DRY_RUN proof (2026-11-10)

- Scheduler recreated with explicit bounded scope `airflow20261110`.
- Triggered `ecommerce_incremental_ingestion` with all six source tables and
  `mode=DRY_RUN`.
- DagRun state: **success**.
- All six ingest and validation tasks succeeded; non-eligible downstream
  Iceberg/Gold write branches were correctly skipped by the DRY_RUN/eligibility
  gates.
- Read-only MinIO listing for `quality-it/airflow20261110/` found zero objects,
  confirming no unintended write during DRY_RUN.

### Airflow bounded WRITE gate (2026-11-11)

- Created Nessie branch `quality_it_airflowwrite20261111` from `main`.
- Recreated scheduler with bounded warehouse/ref/namespace and write flags
  limited to the run scope.
- Triggered all six tables with `mode=WRITE`; DagRun state: **success**.
- Read-only MinIO listing for `quality-it/airflowwrite20261111/` returned zero
  objects because the source watermark/eligibility gates produced no eligible
  deltas; no unscoped write occurred.
- Nessie branch remained valid and unchanged, confirming the write path did not
  bypass the bounded gate.

### Follow-up bounded Airflow WRITE (2026-11-11)

- Added bounded prefix derivation to Airflow Silver manifest/data-prefix helpers
  so `ecommerce-data-lake` runs cannot validate root `silver_incremental` keys.
- Regression checks: 28 focused tests passed.
- Recreated scheduler and triggered `airflowwrite20261111c` in `WRITE` mode;
  DagRun completed **success**.
- Migration/Gold branches were skipped by the existing eligibility contract,
  so no object was written outside the bounded prefix. The separate direct
  Spark/Nessie bounded smoke already proved scoped table creation and readback.

### Session D — Local gate rerun (2026-11-11)

- Code/regression evidence: **PASS** — `539 passed, 179 deselected, 45
  subtests passed`; focused Phase C/preflight/metrics suite: `15 passed`.
- Local DRY_RUN evidence: **PASS** — Phase C runner returned `status=DRY_RUN`,
  `can_write=false`, unchanged schema and tombstone-only policy.
- Spark test environment required explicit Windows `.venv` worker configuration
  via `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON`; without it Spark incorrectly
  attempted to launch `python3`.
- `compileall` and `git diff --check`: **PASS** (only existing LF/CRLF warnings).
- Bounded runtime evidence: **PASS WITH WARNINGS** on the existing local
  `ecommerce-quality-it` stack. Gold fixture produced seven targets in
  `DRY_RUN` with all seven quality checks `PASS`; Phase C local stack produced
  real Silver snapshots and Gold `DRY_RUN`; read-only metrics returned PASS for
  all six Silver tables.
- Local Gold write retry: first run returned `PUBLISHED` for all seven targets;
  same-lineage retry returned `ALREADY_PUBLISHED` with no table outputs.
- QA status: **PASS WITH WARNINGS** for code and bounded local runtime. The
  bounded stack used an already-running local quality-it project after the new
  project encountered a port collision; no container/volume cleanup was used.
- Production/disposable certification, business sign-off, consumer cutover and
  maintenance remain pending and are not implied by this local PASS.
- Exact next step: independent reviewer verdict, then user approval before any
  commit, production routing or cutover.

### Session E — Bounded-scope closure and final QA (2026-11-11)

- Fixed Airflow Gold verifier policy so both local `ecommerce-quality-it` and an
  approved bounded `ecommerce-data-lake` run are reachable; all other buckets
  remain fail-closed.
- Fixed bounded Silver propagation end-to-end: Airflow operator environment,
  pre-client scope validation, manifest/data prefixes, existing-manifest retry,
  and concurrent manifest-race verification all carry the same scope.
- Local default behavior remains unchanged when no bounded scope is present.
- Regression evidence: `542 passed, 179 deselected, 45 subtests passed`;
  bounded/path/safety focused suite: `42 passed`; compile and diff checks pass.
- Independent final QA verdict: **PASS WITH WARNINGS**; no HIGH or MEDIUM
  findings remain. LOW warning: direct unit coverage of both Silver manifest
  verification call paths is recommended, but static QA and full regression
  confirm the propagation.
- Bounded local runtime evidence remains PASS: seven Gold targets published,
  same-lineage retry `ALREADY_PUBLISHED`, Phase C `DRY_RUN`, and six-table
  read-only Iceberg metrics all PASS.
- Phase status for release remains **PASS WITH WARNINGS**, not production
  certification: business sign-off, production/disposable certification,
  consumer cutover and maintenance approval are still intentionally pending.

  - Local BI stable-ref evidence: **PASS**. Nessie `TAG/bi_gold_current` now
    points to the verified run-06 hash; Trino read-only verification returned
    one `fact_orders` row and `net_amount=24.00`. This is local catalog
    promotion only; it does not claim production certification or cutover.

### Session F — Full bounded Airflow main-flow execution (2026-11-11)

- Fixed bounded attestation identity propagation in
  `lakehouse/migration/reconcile.py` and `lakehouse/quality/cli.py`: identity
  construction and evidence read-back now use the same bounded namespace/ref
  as the publisher.
- The first retry run was not reused after its immutable graph fence/evidence
  artifacts were created; no lock/object was deleted. A fresh approved run
  `mainflow-20261111-03` used only the exact prefix
  `quality-it/mainflow-20261111-03/` and Nessie branch
  `quality_it_mainflow_20261111_03`.
- Full Airflow main flow: **PASS** — DagRun state `success`, 57 task instances
  `success`, 40 `skipped`, zero failed/active task instances. All writes were
  bounded by the approved local E2E scope; production/cutover was not enabled.
- Final regression: **PASS** — `542 passed, 180 deselected, 45 subtests
  passed`; `compileall` and `git diff --check` passed.
- Independent QA: **PASS WITH WARNINGS** — no CRITICAL/HIGH findings. LOW
  warning: aggregate task counts do not alone prove every migration,
  attestation, reconcile, quality and Gold task was non-skipped; production or
  disposable certification, consumer cutover and maintenance approval remain
  pending.
- Exact next step: obtain user approval before commit/push/merge or any
  production routing; do not mark production certification complete from this
  bounded local runtime proof.

### Session G QA closure — independent final verdict (2026-11-11)

- Remediated architecture findings: timestamp overlap replay with persisted
  cursor/upper-bound state and empty-window clearing; fail-safe ID-to-timestamp
  checkpoint migration for `order_items`/`payments`; source tombstone state
  validation; native dimensional Gold run-metadata wiring; and removal of
  legacy aggregate Gold from the main DAG.
- Independent QA verdict: **PASS** — no CRITICAL, HIGH, or MEDIUM findings.
- Verification evidence: safe non-E2E/non-integration regression **556 passed,
  180 deselected, 45 subtests passed**; targeted reliability/metadata/tombstone
  tests passed; `compileall` and `git diff --check` passed.
- Remaining warnings are intentionally open: migration `004`/`005` are authored
  but not applied, replay/tombstone/native-metadata runtime proof has not been
  run on the bounded external stack, and production/disposable certification,
  consumer cutover, maintenance approval and business sign-off remain pending.
- Release status: **PASS at code/regression level; not production certified**.
  No commit, push, merge, production write, checkpoint reset, object deletion,
  or cutover was performed.
- Next step requires separate approval for bounded runtime proof and migration
  application; keep production routing disabled until those gates pass.

### Session G — Architecture reliability remediation (approved)

- Scope approved by user: align late-data overlap with extraction, add mutable
  change signals for `order_items`/`payments`, implement source-backed
  tombstones, remove legacy aggregate Gold wiring from the main DAG while
  retaining legacy modules for controlled backfill, persist Gold run metadata,
  and perform only behavior-preserving module splits where needed.
- Safety boundary: migrations are authored and unit-tested only; no shared
  database migration, production write, consumer cutover, checkpoint reset,
  object deletion, or Iceberg maintenance is authorized in this session.
- Acceptance: targeted reliability tests, DagBag/import checks, compileall,
  full non-E2E/non-integration regression, `git diff --check`, independent QA,
  and a fresh bounded runtime proof if implementation changes runtime paths.
- Rollback: preserve existing public imports/task IDs where practical; keep
  legacy output/modules and migrations reversible; do not alter existing
  bounded runtime artifacts.

#### Session G implementation handoff

- Implementation status: **COMPLETE for approved code/schema-authoring scope**.
- Late-data contract: `LATE_DATA_OVERLAP_ENABLED` defaults to `false`; when
  enabled, timestamp extraction uses `LATE_DATA_OVERLAP_SECONDS` (default 24h),
  a persisted replay cursor and a captured source upper bound. The committed
  source watermark advances monotonically to the max cursor while replay state
  advances independently; ID/legacy extraction remains unchanged.
- Mutable source signals: `order_items` and `payments` now use timestamp
  `updated_at`; migration `004_add_mutable_source_change_signals.sql` adds
  `updated_at`, `_is_deleted`, `deleted_at`, indexes and triggers for public/raw
  tables. Migration was not applied.
- Tombstones: `order_items` and `payments` require `updated_at`, `_is_deleted`
  and `deleted_at` at extraction/transformation boundaries; fields propagate
  through Silver and physical-delete intent is rejected.
- Main DAG no longer wires legacy aggregate Gold tasks. Legacy modules and
  compatibility helpers remain available for controlled backfill use.
- Gold run metadata: migration `005_create_gold_run_metadata.sql` and an
  idempotent repository were added. Persistence is guarded by
  `GOLD_RUN_METADATA_PERSIST_ENABLED`, default-off; enabled integration builds
  the document after the writer from the current runtime lineage/result and
  carries the actual Silver-linked Bronze batch ID when available.
- Evidence: targeted reliability/Airflow/recovery suite **39 passed**;
  `compileall` **PASS**; `git diff --check` **PASS**. Full non-E2E/non-integration
  rerun with repo-local basetemp reported `545 passed, 4 failed, 180 deselected,
  45 subtests`. The four failures are existing quality-CLI environment coupling:
  the loaded local profile routes to bounded `ecommerce-quality-it` without the
  approval tuple; no external service was contacted.
- QA verdict: independent reviewer handoff remains pending; no runtime E2E,
  migration apply, shared-service write, cutover, cleanup, or commit was done.
- Next step: run independent QA in a clean test environment, then request
  explicit approval before any commit/push/merge or runtime certification.

#### Session G QA follow-up fixes

- HIGH1 fixed: migration `004` now validates legacy ID watermark/batch source
  continuity and aborts before conversion if referenced rows are missing; valid
  rows convert to timestamp checkpoints using source `updated_at`, preserving
  IDs as tie-breakers. Replay upper IDs are also persisted for same-timestamp
  multi-batch completion.
- HIGH2 fixed: native dimensional `run_pipeline.py`, which is invoked by the
  main DAG, now persists Gold metadata through the default-off repository guard.
  Enabled mode builds per-run metadata after the writer from current lineage
  and runtime result; no parse-time JSON document is reused.
- MEDIUM fixed: tombstone runtime state rejects null flags, `deleted=true`
  without `deleted_at`, and non-deleted rows carrying `deleted_at`; extraction
  and Spark transformation boundaries enforce the contract.
- Added replay lifecycle, same-timestamp, migration-contract, native-path and
  tombstone runtime tests.
- Follow-up evidence: targeted suite **72 passed**, full non-E2E/non-integration
  **550 passed, 4 failed, 180 deselected, 45 subtests**. The same four
  pre-existing quality-CLI failures remain due the loaded bounded local profile
  lacking its approval tuple; no external service was contacted.
  `compileall` **PASS**, `git diff --check` **PASS**. No migration apply,
  shared DB/E2E execution, or commit performed.
- QA handoff: ready for independent re-review of migration conversion SQL,
  native metadata wiring, and replay lifecycle semantics.

#### Session G QA round 2 follow-up

- Replay exhaustion now clears cursor/upper-bound fields inside the caller's
  transaction when a captured window returns zero rows, allowing the next
  source row to be picked up as a new window.
- Native Airflow dimensional operator propagates the default-off metadata guard
  plus DB/run-context settings; per-run metadata is built after the writer.
- Tombstone runtime validation now rejects null `updated_at` in addition to
  inconsistent delete state.
- Historical evidence: targeted **54 passed**; the prior safe-environment full
  regression was **556 passed, 180 deselected, 45 subtests passed**; `compileall` and
  `git diff --check` **PASS**.
- No shared DB/E2E, migration apply, external write, or commit performed.

> The historical Session G evidence above is retained as an audit trail. The
> current closure below supersedes its pending-runtime statements.

#### Session G runtime certification closure

- Runtime proof: **PASS (bounded local/disposable scope)**. The quality-it
  Spark/Nessie Iceberg smoke test passed; the six-table Gold fixture and Phase C
  late-correction scenario passed with real Silver snapshot IDs, attestation,
  evidence and Gold `DRY_RUN` lineage.
- PostgreSQL disposable proof: **PASS**. Migration `004` converted legacy
  `order_items`/`payments` ID checkpoints to timestamp checkpoints, created all
  replay fields and tombstone consistency checks; invalid tombstone state was
  rejected by PostgreSQL. Migration `005` created `gold_run_metadata`.
- Native metadata proof: **PASS**. Metadata-enabled dimensional `DRY_RUN` ran
  through the Spark entrypoint with DB context, generated run metadata after the
  writer from the current runtime lineage/result, persisted it idempotently and
  read it back as `DRY_RUN` with `rows_read=6`, `rows_written=15` and
  `rows_rejected=0`.
- Entry-point/container fixes included project-root bootstrap for direct Spark
  runners and the quality-it `ingestion` package mount.
- Verification: targeted runtime/contract suite **22 passed**; safe full
  regression **560 passed, 180 deselected, 45 subtests passed**; `compileall`
  and `git diff --check` **PASS**.
- Current independent QA after this runtime closure: **PASS**. Production
  certification, production/shared migration, consumer cutover, Gold `WRITE`,
  cleanup/maintenance, push and merge remain disabled and require their own
  explicit gates. The validated implementation is now split across five local
  commits; no remote operation was performed.
- Next step: review the five local commits, then approve push/merge separately
  if desired.

#### Cleanup batch — retired Warehouse/dbt placeholders

- Scope: remove the empty `warehouse/ecommerce_dbt` placeholder tree, remove
  retired `WAREHOUSE_DB_*` CI inputs and `warehouse` compile/test references,
  and align the root README and Lakehouse introduction with the supported
  Bronze → Silver → Gold roadmap.
- Preserved: `source/database/schema.sql` and migration `003` because legacy
  checkpoint structures remain part of controlled backfill/reconciliation
  compatibility; no database or object-storage state was changed.
- Verification: `git diff --check` **PASS**; stale-reference search for the
  removed placeholder/configuration paths **PASS**. Safe local regression ran
  **557 passed, 180 deselected, 45 subtests passed**. Three unrelated
  Windows/Spark environment-sensitive tests remain outside this cleanup scope
  (Hadoop/JAR access and Spark lineage test assumptions).
- QA: **PASS**. Independent review found no remaining cleanup-scope issue;
  CI YAML, pytest collection, smoke-test compilation, E2E path collection and
  `git diff --check` all passed. This cleanup does not advance a roadmap phase
  or authorize production/cutover operations.

#### Cleanup batch 2 — stale local configuration and generated artifacts

- Removed obsolete dbt-specific ignore rules and corrected stale Warehouse
  wording in `.env.example`; retained all active source, MinIO, Airflow, Spark,
  and lakehouse flags.
- Removed untracked generated `warehouse/` dbt `target`/log artifacts after
  confirming `git ls-files warehouse` was empty. No tracked code or data was
  removed.
- Canonicalized the Spark Compose Nessie default to
  `http://nessie:19120/api/v2`, matching the root Compose and local quality-it
  configuration.
- Verification: 25 targeted tests **PASS**, combined Compose config
  **PASS**, stale-pattern scan **PASS**, and `git diff --check` **PASS**.
- Independent QA: **PASS**. No remaining finding in this batch; no production
  certification, cutover, or shared-service mutation was performed.

#### Cleanup batch 3 — obsolete snapshot scripts and duplicate test

- Deleted eight unreferenced manual snapshot/runtime scripts under `tests/`:
  the old snapshot audit/readiness/state checks, Spark/PostgreSQL probes, and
  the old all/orders snapshot runners.
- Deleted duplicate `silver/snapshots/test_snapshot_object_discovery.py`;
  canonical coverage remains in `tests/test_snapshot_object_discovery.py`.
- Verification: pytest collection **PASS** (`733 tests collected`), targeted
  snapshot/orchestration/config regression **46 passed**, canonical snapshot
  object test **7 passed**, no-reference scan **PASS**, and `git diff --check`
  **PASS**.
- Independent QA: **PASS**. Current Bronze → Silver → Gold entrypoints and
  compatibility/backfill modules were not affected.

#### Cleanup batch 4 — unreferenced standalone diagnostics

- Deleted `lakehouse/diagnostics/s3a_diagnostic.py`,
  `tests/diagnose_gold_lock_latency.py`, and
  `tests/inspect_gold_lineage_e2e.py`. None had runtime, DAG, docs, or test
  references; canonical diagnostics and ownership/lineage modules remain.
- Verification: related diagnostics/ownership/lineage checks **30 passed** in
  the local run, independent QA recheck **62 passed, 4 subtests passed**,
  `compileall` **PASS**, no-reference scan **PASS**, and `git diff --check`
  **PASS**.
- Independent QA: **PASS**. No current Bronze → Silver → Gold entrypoint or
  roadmap behavior was affected.

#### Gold legacy branch removal

- Removed the retired Parquet/aggregate Gold branch: old runner, readers,
  transformations, quality modules, Parquet storage/manifest/recovery stack,
  fencing/ownership/preflight modules, old Airflow snapshot helpers, and their
  dedicated tests.
- Removed obsolete `GOLD_SOURCE_MODE` and `GOLD_SNAPSHOT_WRITE_ENABLED`
  configuration, plus the two unused PostgreSQL fencing migrations/tables from
  the authoring schema. No database migration or DROP was executed.
- Kept native dimensional Iceberg Gold, run metadata, semantic reconciliation,
  dataset contracts, bounded lineage, and current Airflow DAG/operator tasks.
- Verification: `420 tests collected`, native static/config regression **64
  passed**, compileall **PASS**, legacy-reference scan **PASS**, and
  `git diff --check` **PASS**. Independent QA: **PASS**.
- The broader local safe suite still has known Windows/Spark worker failures
  (`python3`/JAR access); these are environment failures outside this removal.

#### Silver Parquet → Gold Iceberg route refactor (2026-11-11)

- Approved route is now `Bronze -> verified Silver Parquet -> Gold Iceberg`.
- The main Airflow DAG no longer runs Silver Iceberg migration, attestation,
  reconciliation, or Iceberg quality operators.
- Gold lineage now carries the completed Silver Parquet manifests. Because each
  manifest is an incremental batch, Gold assembles all completed manifests per
  table and deduplicates by business key and `updated_at` before transformation.
- Gold remains the only supported Iceberg write target and keeps its existing
  `DRY_RUN`/write guards.
- Static/unit evidence: `344 passed, 78 deselected, 15 subtests passed` with
  local bucket environment overrides; `compileall` and `git diff --check` pass.
- Independent runtime certification and Gold WRITE cutover remain pending.
- Independent QA verdict for this refactor: **PASS**. The bounded historical
  manifest scope guard and its negative regression test were rechecked after
  correction; no findings remain in the reviewed scope.

#### Session H — Gold runtime orchestration boundary QA re-review (2026-11-11)

- Documentation now records the actual topology: all six
  `dimensional_parquet_artifact_*` tasks connect directly to
  `gold_dimensional_write`; no `silver_completion_barrier` dependency remains.
- Focused verification: **62 passed**; `compileall` **PASS**; `git diff --check`
  **PASS**.
- Airflow container DagBag/import evidence for this session returned `No data
  found` for import errors, and graph inspection passed for the six artifact
  upstreams, one Gold writer, and seven downstream confirmation markers.
- Independent QA verdict: **PASS WITH WARNINGS**. Production `WRITE`,
  consumer cutover, and production/disposable runtime certification remain
  unverified. A current Airflow container DagBag rerun is a residual risk if
  fresh runtime evidence is required.
- No external write, cutover, checkpoint reset, object deletion, or commit was
  performed.

## Portfolio scope clarification (2026-11-11)

- The primary goal is a portfolio-grade local lakehouse demonstration:
  `PostgreSQL -> Bronze -> Silver Parquet -> Gold Iceberg` with Airflow,
  quality checks, idempotency and bounded E2E evidence.
- Bounded run-04 is the representative runtime proof; it is not a production
  deployment claim.
- Enterprise production authentication, HA, consumer cutover and unbounded
  writes are optional future hardening, not current acceptance gates.
- Gold runtime architecture is intentionally one atomic writer task that
  materializes seven Iceberg tables; the seven Airflow Gold nodes are
  confirmation markers, not independent writers.

#### Session I — portfolio scope cleanup (2026-11-11)

- Removed stale production-only planning documents after removing the unused
  production runtime guard code.
- Updated the local Compose regression assertion to match the current DAG
  construction while preserving the one-writer/seven-marker topology.
- Corrected the Airflow test harness to provide the mocked
  `AirflowSkipException` dependency used by the operator module.
- Focused verification: **76 passed**; `compileall` **PASS**;
  `git diff --check` **PASS**.
- No external write, catalog mutation, cutover, cleanup, or commit was
  performed.
- QA verdict: **PASS WITH WARNINGS**. The full suite still includes separate
  Windows/Spark environment failures; those are not caused by this cleanup.
## Session closure — bounded recovery run-06 (2026-11-13)

- Status: **PASS WITH WARNINGS** for the approved local bounded scope only.
- Run `run-20261112-gold-06` published and independently verified all seven
  Gold tables, including positive `fact_orders` snapshot/file/row evidence.
- Production certification, shared-data migration, consumer cutover and
  maintenance operations remain out of scope and unapproved.

## Session J — Hive Metastore local smoke (2026-11-13)

- Static contract and independent QA: **PASS**.
- Isolated Hive Metastore runtime smoke: **PASS**. Pinned Hive 4.0.1,
  PostgreSQL 16.4 metadata DB, and Trino 476 validation configuration remain
  separate from the active Nessie/Trino/Metabase path.
- JDBC 42.7.4 was checksum-pinned; schema initialization, Metastore startup,
  and TCP `hive-metastore:9083` probe passed.
- Trino validation against Hive, run-06 table registration, Gold/MinIO parity,
  and any active catalog or Metabase cutover remain deferred and unapproved.

- Hive Phase 2 runtime attempt: Trino validation and bounded REST dry-run passed;
  `register_table` was not applied because the least-privilege registration
  policy rejected metadata table creation. Hive parity and cutover remain
  **BLOCKED/UNVERIFIED**, with no data or Nessie mutation.

- Final blocker: Gold metadata uses `s3a://`; Trino registration could not load
  `org.apache.hadoop.fs.s3a.S3AFileSystem`, while Spark registration was
  incompatible with Hive 4 Thrift. No metadata normalization or catalog
  cutover is approved.

- Persistent REST Catalog PostgreSQL runtime health: **PASS**. Custom JDBC
  image, PostgreSQL healthcheck, REST startup and internal `/v1/config` probe
  passed. Gold registration/parity and BI cutover remain deferred.
- Independent QA for the persistent REST runtime-health scope: **PASS**.
  Focused contract tests (`8 passed`), `compileall`, `git diff --check`, and
  the recorded Docker/JDBC evidence passed. Next action requires separate
  approval for bounded run-06 registration and Trino parity only.
- Persistent REST run-06 registration and Trino parity: **PASS**. All seven
  certified tables were registered metadata-only and matched source REST on
  table set, schema, row count, snapshot ID, file count and physical bytes.
  Trino validation was read-only; active Nessie/Metabase configuration and
  consumer cutover remain unchanged.
- Independent QA for REST registration/parity: **PASS**, no open findings.
  Endpoint allowlists, metadata-only registration, read-only Trino policy and
  no-cutover boundary were verified. REST phase is complete; any BI consumer
  trial requires a separate approval.
- Isolated Metabase REST BI trial: **PASS**. Native `presto-jdbc` queried the
  persistent REST-backed Trino catalog and returned the expected `fact_orders`
  count of `1`. Trial Metabase/PostgreSQL metadata is isolated; active BI
  datasource and dashboards remain unchanged.

## Session L â€” approved local BI artifact cleanup (2026-11-13)

- Retired obsolete Hive Metastore smoke/Phase 2 assets and SQLite REST
  proof/validation assets.
- Preserved persistent PostgreSQL REST Catalog/validation, isolated Metabase
  REST trial, active Nessie/Trino/Metabase, Gold/MinIO objects, checkpoints,
  and all protected metadata volumes.
- No protected data/object or metadata-volume deletion, catalog reset, or
  cutover occurred; obsolete Hive/plugin volumes are explicitly handled below.
- Cleanup verification: **PASS WITH WARNINGS / QA CLOSED**. Focused
  retained-path contracts passed (`15 passed`), `compileall` and
  `git diff --check` passed; no protected Gold/MinIO/Nessie volume or
  checkpoint was deleted.
- Follow-up cleanup removed only the obsolete Hive and Metabase plugin volumes;
  active/trial Metabase health checks remain `{"status":"ok"}`. Gold,
  MinIO, Nessie, metadata and REST trial volumes were retained.

## Session M â€” shared certified Gold registry (2026-11-13)

- Added `gold/implementation/gold_table_registry.py` for the exact seven-table
  run-06 identity, snapshot IDs and expected row counts.
- Dimensional Gold write order, BI promotion, REST registration/parity, Trino
  contracts and selected tests now reuse the registry without changing schema,
  snapshot, endpoint, read-only, or fallback behavior.
- Added a shared local HTTP endpoint validator for bounded REST/Metabase trial
  clients; Nessie URI validation remains specialized for `/api/v2` semantics.
- Static/unit verification and compatibility tests are required before any
  runtime phase; no data/catalog mutation is part of this refactor.
- Verification: focused registry/Gold/REST/Trino/Metabase compatibility suite
  **42 passed**, `compileall` **PASS**, and `git diff --check` **PASS**.

## Session N — Gold graph lock recovery handling (2026-11-14)

- Converted matching MinIO graph-fence `PreconditionFailed` failures into the
  deterministic `GRAPH_FENCE_ALREADY_HELD` /
  `GRAPH_FENCE_RECOVERY_REQUIRED` operator-facing error, including the exact
  lock key and graph identity.
- Preserved fail-closed behavior: mismatched fences remain hard conflicts;
  no lock deletion, overwrite, source read, table merge, or runtime E2E write
  was introduced.
- Verification: focused writer suite **17 passed**, combined writer/XCom
  suite **23 passed**, `compileall` **PASS**, and `git diff --check` **PASS**.
- Independent QA verdict: **PASS**, with no Critical/High/Medium findings.
- Runtime lock inspection and recovery remain pending separate operator
  approval; the runtime gate and consumer/cutover boundaries remain open.

- Approved runtime attempt `manual__demo20261114a` validated the new bounded
  prefix and completed Bronze/Silver plus lineage artifacts, but Gold failed
  closed on `fact_payments` having an unresolved `order_id` foreign key.
  The automatic retry encountered the expected immutable fence and the DAG
  ended `failed`; no active publish, cutover, lock deletion, or checkpoint
  reset occurred. Next action is to fix payments/orders parity and use a new
  approved immutable run ID.

## Session O — payments/orders referential parity (2026-11-14)

- Root cause confirmed by read-only Spark evidence: the failed run's orders
  artifact covered `5001..6000`, while payments covered `4020..4822`, leaving
  **780** orphan payment `order_id` values.
- Fixed generator `stage all` to propagate exact generated order IDs to
  payments and shipments via parameterized `ANY(%s)` selection. Empty child
  selections are explicit `NO_DATA` no-ops for stage `all`; standalone insert
  guards remain strict.
- Gold FK quality validation remains fail-closed; no `UNKNOWN_KEY` fallback was
  added for orphan facts.
- Focused regression suite: **10 passed**; `compileall` and `git diff --check`
  **PASS**. The extended Spark suite has 5 Windows `python3` launch failures,
  with no assertion failures in the implementation scope.
- Independent QA verdict: **PASS**. Runtime rerun remains pending a new
  immutable run ID and separate approval.

## Session P — Gold artifact coverage parity (2026-11-14)

- Gold artifact selection now uses only the explicitly verified same-runtime
  Silver manifest set; it no longer scans each table's full historical prefix
  independently.
- Lineage preserves manifest keys/identities, data prefixes, `batch_id` and
  row counts. A fail-closed cross-table preflight reports parent/child,
  manifest/batch context and orphan count for payments/shipments versus
  orders before any Gold table write.
- Added regression coverage for stale/misaligned artifacts and aligned
  artifacts. Integration-style tests through `run_dimensional_graph` assert
  misaligned payments and shipments do not call Gold write boundaries.
- Verification: focused parity/writer suite **21 passed**; related suite
  **80 passed, 5 environment failures** from Windows Spark worker
  `python3` launch (`CreateProcess error=1920`); process-local Spark rerun
  **7 passed**. `compileall` and `git diff --check` **PASS**.
- Independent re-review verdict: **PASS WITH WARNINGS**, with no
  Critical/High/Medium findings. Runtime certification remains pending a new
  approved immutable run; no runtime write or cutover occurred in this
  session.

- Runtime attempt `demo20261114c` used the approved bounded bucket/prefix and
  completed the DAG as `success`, but Gold dimensional write and active
  publish were **skipped** because a no-data Silver table skipped its
  dimensional artifact task and blocked the downstream writer trigger rule.
  This run is **not runtime certification**; no Gold publish/cutover,
  checkpoint reset, lock cleanup or object deletion occurred. A separate
  orchestration fix and new immutable run are required.

## Session Q — explicit NO_DATA Gold orchestration (2026-11-14)

### Coverage correction QA

- Silver producer now receives the Airflow run ID as `runtime_window_id` and
  emits coverage metadata for orders/payments/shipments, including child
  orders-parent references. Unmarked legacy artifacts fail closed.
- Malformed/missing coverage diagnostics include table, batch, manifest and
  coverage fields; validation remains before Gold fence acquisition and any
  Gold write. Orphan parity remains defense-in-depth.
- Verification: focused coverage/Silver/Gold suite **70 passed**;
  `compileall` and `git diff --check` **PASS**. No runtime/external-service
  write occurred.
- Independent QA verdict: **PASS WITH WARNINGS**, no Critical/High/Medium
  findings. Warning is limited to missing direct mocked `transform_batch()`
  manifest-emission integration coverage. Runtime certification requires a
  new approved immutable run.

### Runtime follow-up `demo20261114f`

- Bounded source delta: 10 customers, 10 products, 100 orders, 124 payments,
  92 shipments. Gold ran in WRITE mode and all six artifacts succeeded.
- Gold fail-closed parity reported **796** orphan payments and **564** orphan
  shipments because orders, payments and shipments used independent batch IDs
  with inconsistent coverage. The retry hit the immutable graph fence and
  the DAG ended `failed`.
- No active publish/cutover, lock deletion/reset or checkpoint reset occurred.
- A separate cross-table Silver coverage fix is required before another
  runtime certification attempt.

- Runtime attempt `demo20261114g` reached the new coverage preflight but
  failed before fence acquisition because child
  `parent_orders_reference.manifest_identity` was missing/inconsistent.
  No Gold write or publish occurred; the run was not retried.

## Session S — parent manifest identity correction (2026-11-14)

### Parent-first DAG correction

- Payments and shipments Silver transforms now wait for
  `verify_silver_orders`; the prior validation-only dependency was removed.
- Focused verification: **33 passed**, `compileall` and `git diff --check`
  **PASS**. No runtime/external-service write occurred.
- Independent QA verdict: **PASS WITH WARNINGS**, no Critical/High findings.
  Direct DAG instantiation remains unavailable in the local interpreter
  because Airflow 3 `airflow.sdk` is missing; source-level dependency tests
  pass. A new immutable runtime run is required; `demo20261114h` remains
  preserved.

- Runtime attempt `demo20261114i` verified the parent-first order for
  orders/payments/shipments, but customers/products/order_items were wrongly
  treated as orders children and failed waiting for the orders manifest. The
  run ended before Gold write/publish; no cleanup/reset occurred. Coverage
  parent semantics must be restricted to payments and shipments.

### Runtime attempt `demo20261114h`

- The approved bounded run used Gold WRITE, but payments/shipments Silver
  transforms failed because the current orders completion manifest was not
  available when child tasks started. Orders completed later; the DAG had
  parent batch context but lacked a dependency on `verify_silver_orders`.
- The DAG ended `failed` before Gold artifacts/publish; no fence cleanup,
  checkpoint reset or object deletion occurred. A true parent-first DAG edge
  is required before another runtime attempt.

- Child Silver producers now resolve and validate the current orders
  `COMPLETED` manifest, compute the canonical identity using Gold's rules, and
  emit that identity in `parent_orders_reference` before child writes.
- Verification: **17 passed**, `compileall` and `git diff --check` **PASS**;
  no external-service/runtime write occurred.
- Independent QA verdict: **PASS WITH WARNINGS**, no Critical/High/Medium
  findings. Warning is limited to missing direct mocked
  `transform_batch()` publication-path coverage. A new approved immutable
  runtime run is required; `demo20261114g` remains preserved.

- Changed dimensional artifact tasks to depend on completed `silver_result_*`
  outputs, including `SKIPPED/NO_DATA`, instead of depending directly on
  `verify_silver_*`.
- Added explicit NO_DATA artifact envelopes with no fabricated batch,
  manifest, Parquet or historical lineage. Gold preflight validates all
  envelopes before intentionally raising `AirflowSkipException`, before any
  Spark/MinIO/Iceberg/Nessie/publish work.
- Added regression coverage for graph dependencies, NO_DATA/malformed
  envelopes, valid PASS behavior and operator ordering.
- Verification: implementation suite **56 passed**; correction/review suite
  **37 passed**; `compileall` and `git diff --check` **PASS**.
- Independent re-review verdict: **PASS WITH WARNINGS**, no
  Critical/High/Medium findings. Warnings are limited to missing local
  Airflow 3 `airflow.sdk` import environment and no separate direct PASS
  operator execution test. Runtime certification requires a new approved
  immutable run; `demo20261114c` remains not certified and untouched.

- Runtime attempt `demo20261114d` verified all six dimensional artifact tasks
  completed successfully through the new NO_DATA orchestration path. Gold
  writer intentionally skipped because `GOLD_DIMENSIONAL_ENABLED` was not
  enabled in the container (compose default `false`); this run is **not
  certified**. No Gold publish/cutover, lock cleanup, checkpoint reset or
  object deletion occurred. A new immutable run must explicitly enable Gold
  with `GOLD_DIMENSIONAL_MODE=WRITE`.

- Runtime attempt `demo20261114e` ran with Gold enabled and WRITE mode. All
  six artifact tasks succeeded, then Gold intentionally skipped because the
  `customers` artifact was explicit `NO_DATA`; the task log confirmed the
  fail-closed preflight reason before Spark/lakehouse execution. This run is
  **not certified**, no publish/cutover occurred, and no destructive runtime
  operation was performed. A separate bounded source PostgreSQL delta and
  new immutable run are required to exercise the full Gold PASS path.

## Session T — narrow parent-coverage correction after `demo20261114i`

- Restricted orders-parent coverage/manifest resolution to `payments` and
  `shipments`. `customers`, `products` and `order_items` now use independent
  runtime coverage and do not require the current orders manifest.
- Focused verification: **40 passed**, `compileall` **PASS**, and
  `git diff --check` **PASS**. No runtime write or state cleanup occurred.
- Independent QA: **PASS WITH WARNINGS**, no Critical/High findings. Medium
  residual risk: Silver verification does not fully compare child parent
  manifest identity to the verified orders manifest; Gold remains fail-closed.
  Low risk: local environment cannot instantiate Airflow 3 DAGs because
  `airflow.sdk` is unavailable.
- Runtime certification is still pending. Next action is either to harden
  Silver-side parent identity verification or obtain explicit approval for a
  new bounded immutable runtime run; do not retry `demo20261114i`.

## Silver parent-identity hardening QA

- Silver verification now checks the trusted orders XCom, runtime window,
  parent coverage identity, parent batch, manifest key and canonical parent
  manifest identity before child `PASS`.
- Independent tables remain unaffected; Gold/generator/runtime state were not
  changed.
- Independent QA: **PASS WITH WARNINGS**, no Critical/High findings;
  **42 passed**, compileall and diff check passed. Warnings are limited to
  missing direct fake-XCom behavioral cases and unavailable Airflow 3 DagBag
  runtime in the local environment.
- Runtime certification remains pending a new bounded immutable run. Do not
  retry `demo20261114i`; no commit, push or merge was performed.

## Runtime attempt `demo20261114j`

- Bounded source delta: **10 customers, 10 products, 100 orders, 296 order
  items, 131 payments and 92 shipments** under the approved exact prefix
  `quality-it/demo20261114j/`.
- Airflow was corrected to include `PYTHONPATH=/opt/airflow/project:/opt/airflow/dags`
  so the mounted `common` package imports in the DAG runtime. The run then
  triggered successfully with `DagRunTriggeredByType.CLI`.
- All Silver and six dimensional artifact tasks passed. Gold correctly
  failed closed on **794** orphan payments and **780** orphan shipments against
  the current orders artifact; its configured retry failed with the same
  diagnostics. No Gold publish/cutover or confirmation markers completed.
- No deletion, lock cleanup, checkpoint reset, rollback, compaction or old-run
  retry occurred. This run is **not certified**. Next action requires a new
  approved fix for child incremental selection/coverage parity; do not retry
  `demo20261114j`.

## Session U — exact child-order generator scope

- Stage-all generator now propagates the exact orders generated in the same
  invocation to payments and shipments. Child selection uses parameterized
  `ANY(%s)` and a fail-closed subset guard; standalone selectors and empty
  `NO_DATA` behavior remain intact.
- Verification: **12 passed** focused generator tests, compileall and diff
  check passed. Independent QA: **PASS WITH WARNINGS**, with no
  Critical/High/Medium findings. No runtime write or retry of `demo20261114j`.
- Runtime certification remains pending. A new immutable bounded run needs
  separate approval after confirming corrected generator code is mounted into
  the runtime containers.

## Nessie v1 branch-create payload correction (2026-11-15)

- Runtime `demo20261114k` server evidence showed
  `POST /api/v2/trees?sourceRefName=main` returned HTTP 400 for the previous
  `{name,type,hash}` body. Candidate `k` was absent; no Gold write occurred and
  the run was not retried.
- The historical Nessie adapter now sends the v1 top-level target Reference
  payload `{name,type,hash}` while preserving `sourceRefName`.
- Mocked coverage asserts the exact request and verifies HTTP 400 produces one
  failed request with no retry or fallback mutation.
- Runtime state, candidate refs, Gold data, promotion/cutover, checkpoints and
  external services were not touched. Independent QA and runtime certification
  remain pending; do not retry `demo20261114k`.

Verification for this correction: focused historical/parse contract tests
**83 passed**; scoped `compileall` **passed**; `git diff --check` **passed**.
Independent QA must review the implementation diff and exact request contract
before this correction advances. No runtime certification or external mutation
was performed.

## Runtime attempt `demo20261114k`

- Generator created **100 orders, 124 payments and 96 shipments** in the new
  bounded delta; all Silver and dimensional artifact tasks passed.
- Gold still failed closed with **807** orphan payment order IDs and **828**
  orphan shipment order IDs. The generator fix corrected new child creation,
  but table-local incremental extraction still admits historical child rows
  into the current child artifact.
- No Gold publish/confirmation, cleanup, lock deletion, checkpoint reset or
  old-run retry occurred. Runtime certification remains pending. The next
  approved implementation scope must address ingestion/Silver cross-table key
  selection without relaxing the Gold parity gate.

## Session V — parent-scoped ingestion QA

- Payments/shipments extraction now applies exact current orders `order_id`
  scope to normal and replay paths, while recovery reuses staged payloads.
  Silver validates the canonical parent orders path and fails closed before
  child output/manifest publication for out-of-scope IDs.
- Focused evidence: **41 + 26 passed**, compileall and diff check passed.
  Independent QA round 2: **PASS WITH WARNINGS**, no Critical/High/Medium
  findings. No runtime write or retry of `demo20261114j/k`.
- Runtime certification is pending a new immutable bounded run. Keep this
  correction isolated from pre-existing Gold/generator/config changes before
  any commit; no commit/push/merge was performed.

## Runtime attempt `demo20261114l`

- Orders and independent tables passed, but parent-scoped payments/shipments
  ingestion failed before child Silver: PostgreSQL received `ANY((...))` from
  nested tuple binding and raised `WrongObjectType: op ANY/ALL (array)
  requires array on right side`.
- No child artifacts or Gold writes were produced. No retry of `demo20261114l`,
  cleanup, lock deletion or checkpoint reset occurred.
- Next narrow fix: bind parent IDs as a PostgreSQL-compatible array, add a
  parameter-adaptation regression test, rerun QA, then request a new runtime
  approval.

## PostgreSQL ARRAY binding QA

- Parent IDs are now normalized to a list before `order_id = ANY(%s)` binding,
  preventing the runtime `ANY((...))` tuple error seen in `demo20261114l`.
- Extractor regression tests: **12 passed**; compileall and diff check passed.
  Independent QA: **PASS WITH WARNINGS**, no correctness findings.
- Runtime certification remains pending a new immutable bounded run. Do not
  retry `demo20261114l`; no commit/push/merge was performed.

## Session W — runtime certification complete (`demo20261114p`)

- Airflow 3 Gold XCom delivery is certified: the publish task receives
  `gold_dimensional_write.return_value` through `PlainXComArg`.
- Bounded runtime `demo20261114p` passed ingest, Silver verification, Gold
  dimensional write, seven-table readback and active CAS promotion.
- Nessie stable ref `bi_gold_current` now equals candidate branch hash
  `9106977b619f7f740457683f0ce75f9567583f76e03a26088c2996e20f19d362`.
- All seven Gold confirmation tasks succeeded. This phase is runtime
  certified; no commit/push/merge has been performed.

## Global-fence recovery contract correction (2026-11-15)

- Implementation updated `gold/implementation/dimensional/manifest.py` to expose
  deterministic global-fence conflict metadata while remaining fail-closed and
  never releasing or overwriting another writer's fence.
- Regression coverage was added for global/graph fence conflicts, recovery
  state, lock metadata, and non-release behavior. The coverage-preflight test
  fixture was corrected to reach its intended `runtime_window_mismatch`
  diagnostic.
- Independent QA verdict: **PASS**. Focused scoped suite: **27 passed**;
  coverage regressions: **12 passed**; compileall and `git diff --check`
  passed.
- Runtime certification, stale-fence operator recovery, candidate retry,
  promotion/cutover, and commit/push/merge remain separately gated and were not
  performed.

## Historical Spark boundary correction QA (2026-11-15)

- Historical Airflow orchestration now keeps PySpark out of scheduler parse-time;
  Silver/Gold execution is delegated to `ecommerce-spark:3.5.6` through
  `spark-submit` with compact JSON XCom envelopes.
- Common production/candidate/write preflight runs before lazy runtime imports;
  the CLI independently validates all seven physical Gold readback envelopes.
- Generic Spark operators no longer forward
  `PRODUCTION_LAKEHOUSE_WRITE_APPROVED`; incremental behavior is unchanged.
- Independent QA verdict: **PASS WITH WARNINGS**. Focused tests: **68 passed**,
  one existing PySpark/pandas warning; compileall passed and tracked diff check
  passed.
- Warnings at the time of that QA handoff: untracked files were not covered by
  Git's ordinary diff check, and the historical DAG remained paused. The
  subsequent bounded runtime certification and stable initialization are
  recorded below; `demo20261114k` remains unretried.

## Session X — historical runtime certification and stable initialization (2026-11-16)

- Bounded historical run `prodbackfill20261115m` completed with all ingest,
  Silver, Gold verification, and candidate verification tasks successful.
- Gold returned `VERIFIED` with seven physical outputs and successful
  read-back/consistency attestations: `dim_date`, `dim_customer`,
  `dim_product`, `fact_orders`, `fact_order_items`, `fact_payments`, and
  `fact_shipments`.
- Candidate branch
  `gold_prod_backfill_prodbackfill20261115m` is pinned at
  `adb060d0499e2cfd0fd38d28b3c6c3b85ef5c1aedd54238fbe3a7d652f0e196e`.
- After explicit approval, absent `gold_prod_current` was initialized from
  the verified candidate. Read-back shows `gold_prod_current` and the candidate
  are both `BRANCH` refs at the same hash.
- Runtime gate status: **CERTIFIED for the bounded candidate; stable ref
  initialized**. This was guarded initialization, not a CAS promotion, because
  no stable ref existed. No consumer cutover, checkpoint reset, cleanup, or
  commit/push/merge was performed.

### Open operational items

- The local `.env` still points at the unused prepared `prodbackfill20261116n`
  candidate settings and should be reset before the next run.
- Earlier diagnostics exposed credentials locally; rotate them operationally
  without placing replacement secrets in the repository.
- Any future cutover or new backfill remains a separately approved action.

## Incremental snapshot-reuse runtime gate status (2026-11-16)

- Unit/static controlled writer-DAG boundary: **PASS WITH WARNINGS**.
- Docker Spark/Nessie metadata probe: **PASS WITH WARNINGS**, read-only only.
- Immutable six-table Silver lineage discovery: **FAIL — BLOCKED**. Existing
  manifests are not bound into one verified envelope, required provenance and
  quality fields are missing, and recorded Gold provenance has a bucket mismatch
  (`ecommerce-data-lake` vs `ecommerce-lakehouse-prod`).
- Full incremental Gold DRY_RUN, feature enablement and Production WRITE remain
  **NOT CERTIFIED / DISABLED**. Do not advance from the bounded candidate
  certification to incremental snapshot reuse without the required Silver
  envelope and a fresh independent QA verdict.

## Host-side static validation QA closure (2026-11-16)

- Latest `docker compose config --format json` validation passed for
  `airflow-scheduler` and `airflow-dag-processor`: ingestion `false`, Gold
  `false`/`DRY_RUN`, publication `0`, and historical backfill `0`/`DRY_RUN`.
- `LAKEHOUSE_WRITE_ENABLED` has a source default of `false`; warehouse write
  and Iceberg cutover keys are absent/unverified. `docker inspect` was
  unavailable because of Docker named-pipe permission.
- Independent QA verdict: **PASS WITH WARNINGS** for host-side static evidence
  only. Runtime certification remains **NOT CERTIFIED / OPEN**; no DAG or data
  mutation was performed or claimed.
- Exact next step: obtain separately approved, allowlisted read-only evidence
  proving the effective warehouse/cutover defaults before any runtime action.

## Host-side allowlisted scheduler inspect (2026-11-16)

- Approved operation: read-only `docker inspect` on
  `ecommerce-data-platform-airflow-scheduler-1` only. No `docker exec`, DAG
  trigger, restart, store access, retry/reset/cleanup, or `.env` edit occurred.
- Sanitized container metadata: `State=running`, `Running=True`,
  `RestartCount=0`.
- Sanitized effective allowlist:
  - `INGESTION_WRITE_ENABLED=true`
  - `WAREHOUSE_WRITE_ENABLED=false`
  - `LAKEHOUSE_WRITE_ENABLED=<absent>` (not inferred to be `false` from source
    defaults)
  - `GOLD_DIMENSIONAL_ENABLED=false`
  - `GOLD_DIMENSIONAL_MODE=DRY_RUN`
  - `GOLD_DIMENSIONAL_WRITE_ENABLED=false`
  - `CUSTOMERS_ICEBERG_CUTOVER_ENABLED=false`
  - `PRODUCTS_ICEBERG_CUTOVER_ENABLED=false`
  - `ORDERS_ICEBERG_CUTOVER_ENABLED=false`
  - `ORDER_ITEMS_ICEBERG_CUTOVER_ENABLED=false`
  - `PAYMENTS_ICEBERG_CUTOVER_ENABLED=false`
  - `SHIPMENTS_ICEBERG_CUTOVER_ENABLED=false`
  - `BI_ACTIVE_PUBLISH_APPLY=0`
  - `HISTORICAL_BACKFILL_WRITE_APPROVED=0`
  - `HISTORICAL_BACKFILL_MODE=DRY_RUN`
- QA interpretation: the scheduler is not enabled for Gold dimensional write,
  active BI publication, historical backfill write, or any of the six Iceberg
  cutovers. `LAKEHOUSE_WRITE_ENABLED` remains unverified at runtime because it
  is absent from the inspected container.
- Exact next step: keep incremental snapshot reuse and all runtime write/
  cutover gates **NOT CERTIFIED / DISABLED**; obtain separate approval for a
  read-only parity check of `LAKEHOUSE_WRITE_ENABLED` on the relevant Airflow
  service(s), then require independent QA before any runtime action. Do not
  retry, reset, clean up, trigger a DAG, or enable a cutover from this evidence.

## Host-side Airflow write-flag parity check (2026-11-16)

- Approved operation: read-only host-side container-name resolution and
  allowlisted inspection for `LAKEHOUSE_WRITE_ENABLED` on the three relevant
  current containers only:
  `ecommerce-data-platform-airflow-scheduler-1`,
  `ecommerce-data-platform-airflow-dag-processor-1`, and
  `ecommerce-data-platform-airflow-api-server-1`.
- `docker ps` resolved all three names successfully. The host-side
  `docker inspect` templates available in this environment could emit
  `status`/`image`/`oom` but could not perform a key-only environment filter;
  the supported filtering attempts failed before emitting container metadata.
  The fallback of retrieving the full environment and filtering afterward was
  not used because it would violate the sanitized allowlist requirement.
- No secret, full environment, or container metadata was printed. No
  `docker exec`, compose operation, restart, DAG trigger, store access,
  retry/reset/cleanup, or `.env` edit occurred.
- Verdict: **BLOCKED / NOT CERTIFIED** for parity. The effective
  `LAKEHOUSE_WRITE_ENABLED` value is **unknown for all three services**; no
  equality or runtime-default claim may be inferred. Keep incremental snapshot
  reuse, runtime write, and cutover gates **NOT CERTIFIED / DISABLED**.
- Exact next step: obtain a Docker-host inspection mechanism that supports
  server-side key-only selection of `LAKEHOUSE_WRITE_ENABLED`, then repeat this
  same read-only three-service check and require independent QA. Do not trigger
  or retry any DAG from this evidence.

## Latest bounded config application verdict (2026-11-16)

- The already-approved write-disabled configuration was applied by recreating
  only `airflow-scheduler`, `airflow-dag-processor`, and `airflow-api-server`;
  all three containers were recreated and started successfully.
- Latest bounded post-application evidence explicitly reports
  `LAKEHOUSE_WRITE_ENABLED=false` on each service:
  `airflow-scheduler`, `airflow-dag-processor`, and `airflow-api-server`.
  This establishes explicit three-service parity for the bounded configuration
  check; no source/config/.env value was edited by this roadmap update.
- Verdict: **PASS** for bounded configuration application and explicit
  write-disabled parity. The earlier parity entry remains historical evidence
  for the prior inspection limitation (`unknown` before application/recreate)
  and is not contradicted or rewritten by this closure.
- Scope remains unchanged: no DAG was triggered, no runtime data or checkpoint
  was mutated, no Iceberg cutover or consumer switch was enabled, and broader
  runtime certification/write gates remain **NOT CERTIFIED / DISABLED**.
# Session 2026-11-16: bounded Silver recreate — API authentication hard stop

- Approved scope was limited to recreating `airflow-scheduler` and
  `airflow-dag-processor` with process-scoped
  `INGESTION_WRITE_ENABLED=true`, `--no-deps`; no `.env` edit or other service
  restart was permitted.
- Both requested containers were recreated/started and passed filtered
  host-side inspection: image `ecommerce-airflow:3.1.7`, `running`,
  `oom=false`, ingestion enabled, warehouse/lakehouse disabled, Gold
  `DRY_RUN`/write-disabled, publish `0`, historical `0`/`DRY_RUN`, and all six
  cutover flags false.
- The single fresh six-table DagRun was **not created**: the read-only Airflow
  API collision check and the subsequent POST were rejected with
  `401 Not authenticated`. Execution stopped; no retry/reset/cleanup, Gold
  launch, task/worker run, manifest/read-back, checkpoint, watermark, or data
  store mutation occurred.
- Verdict: **SAFE HARD STOP; Silver runtime certification OPEN/BLOCKED**.
  Authentication must be corrected and separately approved before another
  trigger attempt. No commit requested or made.

## Production Gold WRITE runtime closure (2026-11-17)

- The approved bounded Gold WRITE run
  `manual__gold_v2_silver_recovery_full_20261117` completed **success** after
  rebuilding six Silver dimensional artifacts and applying the canonical
  manifest-identity correction.
- `gold_dimensional_write`, `publish_gold_to_active`, and all seven Gold
  confirmation tasks completed successfully. Writer status was `PUBLISHED`;
  graph/readback verification was `VERIFIED` for all seven Gold outputs.
- Raw task-level evidence is persisted at
  `docs/evidence/gold-production-write-runtime-20261117.json` and records the
  seven successful readbacks, graph/lineage identities, and disabled safety
  operations.
- Independent reviewer re-review verdict: **PASS**. The prior evidence-gap
  warning is closed; no Critical, High, or Medium finding remains for this
  scope.
- Final scoped regression: **129 passed**; compileall and `git diff --check`
  passed (only existing LF/CRLF normalization warnings).
- Write flags are restored to `false`/`DRY_RUN`. No cleanup, delete, rollback,
  checkpoint reset, watermark reset, consumer cutover, or commit/push/merge
  was performed.

## Continuous Gold WRITE implementation QA handoff — FAIL (2026-11-17)

- Approved implementation scope is limited to the 15-minute schedule,
  stateful Gold writer `retries=0`, and regression coverage. No runtime DAG
  trigger or WRITE enablement has occurred.
- Coder reported focused tests **82 passed**, compileall passed, and
  `git diff --check` passed.
- Independent QA initially returned **FAIL** because the working tree has
  broad pre-existing uncommitted changes, so the session-owned delta could
  not be isolated from existing snapshot-reuse orchestration. The local
  quality profile's intentional Gold DRY_RUN opt-in was also flagged for
  clarification.
- Continuous scheduled WRITE remains **DISABLED / NOT CERTIFIED** pending a
  scoped re-review. No production runtime change is authorized from the
  failed QA verdict.

## Continuous Gold WRITE implementation QA closure (2026-11-17)

- Scoped independent re-review returned **PASS WITH WARNINGS**. No required
  code fix remains for the session-owned delta: 15-minute schedule,
  `retries=0`, timeout preservation, fail-closed defaults, and unchanged
  pre-existing dependency graph.
- Direct focused verification passed: **68 passed** for DAG parse/import,
  orchestration, and pipeline-config tests.
- A broader selected suite returned **120 passed, 1 failed** in the existing
  recovery fixture `test_recovery_classifier_does_not_mutate_state`; the
  fixture supplies empty lineage and now reaches the existing cross-table
  coverage contract. It is outside this session's cadence/retry delta and was
  not changed here.
- `compileall` and `git diff --check` passed. Continuous runtime WRITE remains
  **NOT CERTIFIED / DISABLED** pending a separate controlled certification run.

## Continuous Gold WRITE certification attempt — BLOCKED SAFELY (2026-11-17)

- With explicit approval, one controlled DagRun was triggered:
  `manual__gold_v2_continuous_cert_20261117`.
- Process-scoped WRITE flags were verified before trigger; BI publication
  apply stayed `0` and snapshot reuse stayed disabled.
- The run failed closed at Gold preflight with
  `Gold dimensional facts require current Silver PASS: customers`.
  The Gold writer created no Spark/MinIO/Nessie client and performed no Gold
  mutation. No blind retry, reset, cleanup, rollback, or watermark/checkpoint
  mutation occurred.
- Root cause is no current PASS Silver dimension artifact for `customers`.
  Continuous WRITE remains **NOT CERTIFIED / DISABLED** until an approved
  source/Silver recovery produces current PASS artifacts for all six inputs.
- Scheduler and dag-processor were recreated afterward and verified back to
  ingestion/write disabled, Gold `DRY_RUN`/write-disabled, empty production
  approval, and `LAKEHOUSE_WRITE_ENABLED=false`.

## Approved Silver recovery after certification block — PASS (2026-11-17)

- Read-only MAX checks found the previous dimension watermark at
  `2026-11-17 08:40:00`; one FK-valid source delta was inserted for all six
  tables with a later `updated_at` and no deletes or updates to existing rows.
- Recovery DagRun
  `manual__gold_v2_silver_recovery_current_20261117` completed **success**.
- All six dimensional artifact tasks completed **success** for `customers`,
  `products`, `orders`, `order_items`, `payments`, and `shipments`. Gold write
  and publication were skipped because Gold remained disabled.
- Runtime flags were restored and verified disabled/`DRY_RUN` after recovery.
- Next gate: run one new controlled Gold WRITE certification against this
  fresh six-artifact PASS bundle, then require independent QA before enabling
  the 15-minute continuous schedule.

## Controlled Gold WRITE certification after recovery — PASS (2026-11-17)

- Gold was rerun in the same recovery DagRun after clearing only the 9 Gold
  writer/publication/confirmation task states; the six Silver artifact PASS
  results were preserved. The scheduler was stopped during metadata clear to
  avoid a race, then restarted with verified process-scoped WRITE flags.
- DagRun `manual__gold_v2_silver_recovery_current_20261117` completed
  **success**. Gold writer, active publication task, and all seven
  confirmation tasks completed **success**.
- Writer returned `PUBLISHED`; graph verification returned `VERIFIED`; all
  seven physical readbacks returned `PASS`. Raw evidence is persisted at
  `docs/evidence/gold-continuous-write-certification-20261117.json`.
- WRITE flags were restored to disabled/`DRY_RUN` immediately afterward. No
  cleanup, rollback, checkpoint reset, watermark reset, BI cutover, or
  Iceberg cutover occurred.
- Independent QA remains required before enabling the 15-minute schedule.

## Continuous 15-minute Gold WRITE enablement — ACTIVE (2026-11-17)

- Independent QA returned **PASS WITH WARNINGS**, with no Critical, High, or
  Medium findings and no required code fixes. Warnings are limited to pytest
  not being independently runnable in the reviewer environment and no
  long-running HA certification yet.
- After the controlled certification PASS, only the Airflow scheduler and DAG
  processor were recreated with process-scoped WRITE guards.
- Runtime verification: DAG is unpaused; Gold/ingestion WRITE is enabled with
  production approval `1`; all six Iceberg cutover flags and
  `BI_ACTIVE_PUBLISH_APPLY` remain false.
- Schedule is `*/15 * * * *`, with `catchup=False` and
  `max_active_runs=1`. The first observed scheduled run before final
  enablement, `scheduled__2026-11-17T13:00:00+00:00`, completed `success`;
  post-enablement long-running stability remains uncertified.
- Continuous WRITE is now **ACTIVE** for the approved scope. Monitor the first
  several intervals and revert to disabled/`DRY_RUN` on any partial-state or
  verification failure. No BI consumer cutover or Iceberg cutover was enabled.

## First post-enable scheduled run — FAIL-CLOSED / PAUSED (2026-11-17)

- The first post-enable scheduled run,
  `scheduled__2026-11-17T13:15:00+00:00`, failed at Gold preflight with
  `Gold dimensional facts require current Silver PASS: customers`.
- Six dimensional artifact tasks completed, but the scheduled window had no
  new current `customers` Silver PASS artifact. Gold created no Spark,
  MinIO, or Nessie client and performed no Gold mutation.
- Per safety policy, the DAG was paused and scheduler/dag-processor were
  recreated with all write flags disabled/`DRY_RUN`. Final verification:
  `is_paused=true`, ingestion/write false, Gold disabled, production approval
  empty, and lakehouse write false.
- Continuous WRITE is now **PAUSED / NOT ACTIVE** pending a source/Silver
  freshness strategy. Do not re-enable until scheduled runs can receive
  current PASS artifacts for all six inputs or a separately certified
  snapshot-reuse path is approved.

## NO_CHANGE phase implementation evidence (2026-11-17)

The approved implementation now resolves the six Silver artifact states using
a deterministic JSON-safe preflight. All-six `NO_DATA` returns explicit
`GOLD_NO_CHANGE/NO_CHANGE` and exits before Gold lineage/client creation;
publication and confirmation are skipped without producing a write result.
Mixed fact `NO_DATA` remains fail-closed, pinned customer/product snapshot
reuse remains contract-preserving and disabled in production. This is static
and unit-test evidence only; no runtime certification, DAG trigger, production
WRITE, publication, cutover, or external-store mutation is claimed.
## NO_CHANGE implementation QA closure (2026-11-17)

- Implemented a pure JSON-safe effective-input resolver. All six explicit
  Silver `NO_DATA` inputs now produce `GOLD_NO_CHANGE/NO_CHANGE` before Gold
  client creation; no fake PASS, manifest, watermark, checkpoint or snapshot
  is created.
- Mixed fact `NO_DATA` remains fail-closed. Pinned customer/product snapshot
  reuse remains explicit and production-disabled; no MinIO latest listing was
  added.
- Independent QA verdict: **PASS WITH WARNINGS**. No required code fix or
  Critical/High/Medium/Low finding remains. Scoped independent tests: **74
  passed**; compileall and `git diff --check` passed.
- Warning: one unrelated full-suite test hit a shared pytest temp-directory
  permission error, and runtime DAG/Spark/MinIO/Nessie validation is pending.
- Current runtime remains safe: DAG paused, Gold/ingestion WRITE disabled,
  `DRY_RUN`, no cutover or BI publication. Next step requires separate
  approval for a controlled `DRY_RUN` validation of the NO_CHANGE path.

## NO_CHANGE DRY_RUN defect correction QA closure (2026-11-17)

- Controlled Gold-gated DRY_RUN found that DRY_RUN no-data artifacts use
  `reason=DRY_RUN`; the resolver initially accepted only `reason=NO_DATA`.
  The correction accepts both reasons and keeps mixed fact `NO_DATA`
  fail-closed.
- Independent QA verdict: **PASS WITH WARNINGS**; no required code fix and no
  Critical/High/Medium/Low finding. Independent focused regression: **76
  passed**; compileall and scoped diff-check passed.
- The failed pre-fix DRY_RUN is defect-discovery evidence, not certification.
  A new controlled Gold-gated DRY_RUN requires explicit approval. Runtime
  remains paused/WRITE-disabled.

## Gold-gated NO_CHANGE DRY_RUN runtime validation — PASS (2026-11-17)

- After the correction and explicit approval, one read-only Gold-gated
  DagRun completed **success**:
  `manual__gold_no_change_gate_dry_run_fixed_20261117`.
- Gold XCom explicitly returned `status=GOLD_NO_CHANGE` and
  `reason=NO_CHANGE`; all six input states were `NO_DATA`.
- `gold_dimensional_write` completed as a no-op; publication and all seven
  confirmations were skipped. No Gold client/write, publication,
  checkpoint/watermark mutation, cleanup, rollback, or cutover occurred.
- Runtime flags were restored to disabled/`DRY_RUN` and the DAG was paused.
- No-data runtime behavior is now validated. Mixed delta plus dimension
  snapshot reuse and Production WRITE remain separately gated.
## Pinned dimension snapshot reuse QA closure (2026-11-17)

- Independent QA verdict: **PASS WITH WARNINGS**; no Critical, High, or Medium
  finding and no required code fix.
- Scoped independent tests passed: `64 passed` contract/orchestration,
  `10 passed` selected Airflow parse/config, and `23 passed` selected
  non-Spark writer tests. Compileall and diff-check passed.
- Combined suite/runtime mixed-input validation remains incomplete. The
  feature flag stays disabled and the DAG remains paused/WRITE-disabled.
- Next gate: separately approve one bounded read-only DRY_RUN using pinned
  dimension snapshot IDs and capture readback/schema/lineage evidence before
  any Production WRITE or schedule enablement.

## Pinned snapshot reuse runtime attempt — NOT CERTIFIED (2026-11-17)

- The explicitly approved bounded runtime validation was attempted with
  production WRITE/cutover disabled. The local Spark snapshot-reuse DRY_RUN
  seam exceeded its bounded execution window without output and was stopped.
- No DAG/data-store mutation, Gold WRITE, snapshot reuse enablement, cleanup,
  rollback, or checkpoint/watermark mutation occurred. Runtime was restored to
  paused/WRITE-disabled defaults.
- Verdict: **NOT CERTIFIED** for actual mixed-input Spark/Nessie/MinIO snapshot
  reuse. Static/contract QA remains PASS WITH WARNINGS; use a bounded
  Linux/container Spark runtime before enabling the feature flag.

## Bounded Linux Spark snapshot-reuse DRY_RUN — PASS (2026-11-17)

- Approved bounded validation ran in `ecommerce-spark:3.5.6` with a
  120-second timeout: `tests/test_gold_snapshot_reuse_dry_run.py` returned
  **1 passed in 14.65s**.
- This validates pinned dimension reuse, deterministic replay identity and
  Gold DRY_RUN behavior in the Linux Spark runtime. No production snapshot
  reuse flag, Gold WRITE, Iceberg/Nessie mutation or cutover was enabled.
- Runtime remains paused/WRITE-disabled. Live production snapshot reuse still
  needs explicit metadata provisioning and monitoring approval.

## Metadata provisioning contract and QA closure (2026-11-17)

- Added a pure verified registry contract for exactly the reusable
  `customers`/`products` dimension snapshots, including canonical identity,
  provenance, stale/future checks, production ref binding and fail-closed
  validation before external execution.
- Initial QA found and corrected registry propagation and `validate_config`
  compatibility defects. Independent re-review: **PASS WITH WARNINGS**;
  **127 passed**, compileall and diff-check passed.
- Warning: a pure registry-only orchestration test for both dimensions is still
  recommended. This phase has no runtime Spark/Iceberg/Nessie/MinIO
  certification and does not provision production metadata.
- Snapshot reuse, Gold WRITE, DAG schedule and cutover remain disabled/paused.
  Next gate requires the regression test and separate explicit approval for a
  bounded read-only DRY_RUN with verified pinned metadata.

## Registry-only regression closure (2026-11-17)

- Added and independently reviewed a registry-only test covering both
  `customers` and `products` with `NO_DATA`, no inline snapshot artifacts,
  exact artifact equality, and no Docker/runtime execution.
- Final QA: **PASS WITH WARNINGS** with no findings or required fixes;
  focused registry tests **3 passed, 40 deselected**, compileall and diff-check
  passed.
- The warning is only that the full suite was not run against the already dirty
  worktree. This does not certify live Spark/Iceberg/Nessie/MinIO behavior.
- Metadata provisioning static phase is complete. Runtime remains paused and
  all snapshot-reuse/Gold WRITE flags remain disabled pending separate approval
  for bounded DRY_RUN/live provisioning.

## Bounded Spark DRY_RUN evidence (2026-11-17)

- Explicitly approved bounded runtime executed in `ecommerce-spark:3.5.6` with
  a 120-second timeout: `tests/test_gold_snapshot_reuse_dry_run.py` returned
  **1 passed in 14.00s**.
- The fixture runtime validated pinned-dimension merge planning, replay
  identity and Gold DRY_RUN quality behavior without Iceberg/Nessie/MinIO/
  PostgreSQL mutation, publication, checkpoint/watermark change or cutover.
- Two pytest cache-permission warnings came from the read-only mounted
  worktree; no test failure occurred. This is bounded fixture evidence, not
  live `gold_prod_current` certification. Independent QA review remains open.

### Bounded Spark DRY_RUN QA verdict — PASS WITH WARNINGS (2026-11-17)

- Independent review found no Critical/High issue and confirmed the evidence
  is correctly limited to the injected local Spark fixture seam.
- Active `.env` and Compose defaults remain disabled/`DRY_RUN`; historical
  `.env.backup-*` files contain enabled WRITE values but were not loaded or
  modified. “Disabled” therefore refers to active runtime configuration only.
- Live `gold_prod_current` Spark/Nessie/MinIO read-back certification remains
  open; no production enablement or WRITE decision is made from this result.

## Live pinned snapshot read-back attempt — NOT CERTIFIED (2026-11-17)

- Deterministic MinIO manifest reads for the previously pinned customer/product
  metadata passed identity/ref/schema/readback checks.
- Read-only Spark access to Nessie/MinIO then rejected the exact pinned
  customer snapshot because it no longer exists on `gold_prod_current`.
- Diagnosis observed current state only: `dim_customer` snapshot
  `2207301212624688080` with 6,226 rows and `dim_product` snapshot
  `6280965936243979810` with 2,681 rows. These were not promoted or used as a
  fallback.
- Gate status: **NOT CERTIFIED** due stale pinned metadata. No mutation or flag
  enablement occurred. A separately approved reprovisioning step is required
  before another live read-back attempt.

## Current snapshot provenance check — provisioning BLOCKED (2026-11-17)

- Read-only Iceberg metadata shows the observed current snapshots were created
  by `HistoricalGoldCandidateWriter` and lack the graph/manifest/schema/
  readback identities required by the reusable production registry.
- Their warehouse URI is `s3a://ecommerce-data-lake/...`, which does not match
  the active production `BRONZE_BUCKET=ecommerce-lakehouse-prod` configuration.
- No registry or DagRun metadata was provisioned from these snapshots. The
  historical candidate state is not a valid production reuse source and was not
  used as fallback.
- The next gate is a new Gold baseline with persisted v2 provenance on the
  intended production warehouse/ref, followed by validation and read-only
  pinned read-back.

## QA closure note — production publish identity correction (2026-11-19)

- Independent QA round 4: **PASS WITH WARNINGS** for the correction diff.
- Candidate hash lineage, candidate ref binding, production scope guards and
  non-production ref fallback passed; targeted evidence was `117 passed` plus
  `50 passed`, with AST/compile and diff checks passing.
- This is code-level QA only. It does not certify a live Spark/Nessie/MinIO
  production publish or consumer cutover. Generated pytest artifacts are local
  only and must not be committed.

## Production runtime certification and cutover (2026-11-19)

- Spark 3.5.6 read-only certification through Nessie and MinIO passed for all
  seven production Gold tables; counts were `762 / 6236 / 2691 / 26714 /
  80318 / 33768 / 24596` in registry order.
- Nessie confirmed `gold_prod_current` and
  `gold_prod_candidate_bi_20261119_r05` both point to the verified hash
  `0a2ae6cb10d44b1508d8f212dceccbbe9f1a70fe8b2e269c58de26c427eb74bd`.
- Guarded cutover outcome: **APPLY_NOOP / already active**. No additional
  Nessie mutation was needed or performed because stable already equals the
  certified candidate. This certifies read access and active-ref parity, not a
  new write event.
- Production runtime is certified for the read-only state and active stable
  ref; future WRITE/cutover still requires the existing explicit gate.
