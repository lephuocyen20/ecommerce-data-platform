# Gold Lakehouse Development Plan

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

`Bronze -> verified Silver Iceberg -> Gold Dimensions/Facts -> derived marts`

Local-only Gold validation remains bounded to the approved Iceberg/Nessie stack.
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
