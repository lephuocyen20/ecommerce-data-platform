# Production BI Active Publish Plan

## Scope

Implement a production-only, fail-closed path for advancing `bi_gold_current`
from a certified Gold WRITE run in `ecommerce-lakehouse-prod`. Preserve the
bounded `ecommerce-data-lake` E2E exception and do not enable runtime publish in
this planning step.

## Acceptance criteria

- Production publish is allowed only when `PRODUCTION_LAKEHOUSE_WRITE_APPROVED=1`,
  `BI_ACTIVE_PUBLISH_APPLY=1`, and the writer certification is verified.
- The candidate is restricted to the production bucket, expected seven-table
  output set, positive snapshot IDs, readback `PASS`, and the configured
  production namespace/ref.
- Nessie promotion uses expected-hash CAS and fails closed on a stale hash.
- A no-op/replay of the already-active candidate is idempotent.
- Bounded E2E publication remains restricted to its immutable test prefix.
- Tests cover allow/deny paths, stale CAS, scope mismatch, and idempotent replay.
- Runtime verification remains read-only until a separate cutover execution gate.

## Risks and controls

- Wrong branch or namespace: validate exact production ref and namespace.
- Stale expected hash: require explicit `BI_GOLD_CURRENT_EXPECTED_HASH` and CAS.
- Partial consumer visibility: require post-publish smoke query and rollback target
  before runtime activation.
- Accidental test-to-production promotion: keep E2E scope validation separate.

## Files likely in scope

- `airflow/dags/pipeline_tasks/gold.py`
- `gold/implementation/bi_ref_promotion.py`
- `common/bounded_e2e_scope.py` (only if shared validation needs extraction)
- focused local tests under ignored `tests/`
- this plan and `docs/GOLD_LAKEHOUSE_PLAN.md`

## Verification commands

```powershell
pytest -q tests/test_bi_ref_promotion.py tests/test_gold_dimensional_lineage.py
python -m compileall airflow/dags gold common
git diff --check
```

Runtime publish is not part of the implementation verification command set.

## Planning review and unresolved inputs

Independent planning review confirms that the current publish task is wired to
the bounded E2E contract and is not yet a production cutover path. Before code
implementation, the following production-owned values must be confirmed:

- stable BI ref: `bi_gold_current` or `gold_prod_current`;
- production Trino catalog and schema used by the BI consumer;
- named rollback owner;
- validated known-good rollback target hash.

These values materially affect the contract and cannot be inferred safely from
the local configuration. No runtime publish was enabled and no external state
was changed during planning.

### Resolved from repository evidence

- Stable BI ref: `gold_prod_current` (user-confirmed).
- Trino catalog: `lakehouse`.
- Current documented consumer schema: `gold_run_20261112_gold_06`.

### Rollback evidence discovered

- Candidate promotion evidence records the previous stable hash before the
  r04 CAS as the known-good rollback target:
  `a4274ec5b703fc37a34ecb8efab17931773aa82987e37ca324a7c798cbfb59e7`.
- Dataset contracts declare semantic ownership as `analytics`. This is recorded
  as the provisional rollback owner, but it is not treated as an operational
  on-call escalation until the runbook confirms it.

Active publication remains disabled until the owner is confirmed as an
operational owner and the production publish implementation is independently
QA-approved.

## Independent QA verdict

Final independent QA verdict: **PASS WITH WARNINGS**.

Evidence:

- Production Spark warehouse and candidate ref are explicit and fail closed;
- no production `main` fallback remains;
- exact production bucket/catalog/schema and seven-table readback are enforced;
- candidate Nessie ref must be a `BRANCH` and its hash must match evidence;
- expected-hash CAS and idempotent replay are covered by pure smoke checks;
- bounded E2E isolation and Compose safe defaults pass static review;
- compile, diff check, Compose config and pure promotion smoke pass.

Warning: focused pytest could not run because the current environment has no
`pytest` module. Runtime Nessie/MinIO/Spark certification and active BI cutover
remain intentionally deferred. `BI_ACTIVE_PUBLISH_APPLY` remains disabled.

### Final QA closure

After the production Spark pinning and regression-test compatibility fixes,
independent QA returned **PASS WITH WARNINGS**:

- focused regression suite: `59 passed`;
- compileall: PASS;
- `git diff --check`: PASS;
- production Spark warehouse/ref pinning: PASS;
- strict production identity and rollback gates: PASS;
- candidate Nessie branch/hash parity, CAS and replay: PASS;
- no runtime write, cutover, checkpoint reset or cleanup performed.

Residual warnings are limited to runtime production certification not yet being
executed and the focused test files remaining local-only because `tests/` is
ignored by repository policy. The exact next step is a separately approved
execution gate for real Nessie/Spark/BI consumer read-only verification, followed
by active publish only if that gate passes.

## Implementation evidence (2026-11-19)

- Added a production-only fail-closed contract in
  `gold/implementation/bi_ref_promotion.py` for bucket
  `ecommerce-lakehouse-prod`, stable ref `gold_prod_current`, catalog
  `lakehouse`, schema `gold_run_20261112_gold_06`, the exact seven-table output
  set, positive snapshot IDs, and `PASS` readback evidence.
- Added rollback metadata validation requiring hash
  `a4274ec5b703fc37a34ecb8efab17931773aa82987e37ca324a7c798cbfb59e7` and
  owner `analytics`. Production publication uses expected-hash CAS and returns
  `APPLY_NOOP` for an already-active candidate.
- Updated the Airflow active publish boundary to select the production
  contract only for the production bucket. The bounded
  `ecommerce-data-lake` path still validates its immutable scope before
  constructing the Nessie adapter.
- Added Compose pass-through/defaults with `BI_ACTIVE_PUBLISH_APPLY=0` and
  `PRODUCTION_LAKEHOUSE_WRITE_APPROVED=0`; no `.env` or runtime state was
  changed.
- Focused tests were added under ignored `tests/` convention, but could not be
  executed because neither `pytest` nor a Python interpreter is installed in
  the host environment. `docker compose config --quiet` and `git diff --check`
  completed successfully. The requested compile/import check could not run
  because Docker API access is unavailable.

## QA FAIL remediation implementation (2026-11-19)

The QA FAIL findings were corrected within the approved scope:

- Production publication now reads the candidate ref from Nessie before
  validation, requires the reference to be an existing `BRANCH`, and requires
  exact `candidate_hash` parity between Nessie and the certification report.
  Candidate names are validated by the strict `gold_prod_candidate_*` regex;
  `gold_prod_current` and `main` are not accepted as candidate refs.
- Production evidence now requires exact report identity: bucket
  `ecommerce-lakehouse-prod`, catalog `lakehouse`, and schema
  `gold_run_20261112_gold_06`; missing values are rejected rather than
  defaulted.
- The production Spark writer/operator boundary now requires explicit
  `GOLD_CANDIDATE_REF`, `GOLD_TARGET_CATALOG`, and `GOLD_TARGET_NAMESPACE`,
  emits them as `candidate_ref`, `catalog`, and `schema` evidence, and fails
  closed before Spark when they are absent or mismatched. The prior
  production `ref=main` compatibility fallback was removed.
- The canonical root Compose quality bucket default is now
  `ecommerce-lakehouse-quality-it`.
- Focused tests cover exact report identity, Nessie branch/hash parity, strict
  candidate refs, Airflow producer/consumer evidence wiring, and the Compose
  default.

No `BI_ACTIVE_PUBLISH_APPLY` value was enabled, no runtime write/cutover was
performed, and no secrets or external data services were modified.

Verification evidence for this remediation:

- `python -m compileall -q gold airflow/dags common tests`: **PASS**.
- `git diff --check`: **PASS**.
- `python -m pytest -q tests/test_production_bi_active_publish.py
  tests/test_quality_it_local_compose.py tests/test_bi_ref_promotion.py
  tests/test_gold_publish_xcom.py`: **NOT RUN — host Python has no pytest
  module** (`No module named pytest`).
- Docker Compose config/runtime checks were not run; no external-service test
  was requested or approved.

Independent QA verdict: **PENDING**. Runtime publish/cutover remains disabled;
the next step is independent reviewer QA of this remediation diff and, if
approved separately, execution in a Python environment with pytest installed.

## QA round-2 FAIL remediation (2026-11-19)

Approved scope remains production BI active publish implementation only; no
runtime writes, consumer cutover, Iceberg cutover, or secret changes were
performed, and `BI_ACTIVE_PUBLISH_APPLY` remains `0`.

Implemented corrections:

- Production Spark now explicitly configures
  `spark.sql.catalog.lakehouse.warehouse=s3a://ecommerce-lakehouse-prod/lakehouse`
  and `spark.sql.catalog.lakehouse.ref=GOLD_CANDIDATE_REF` before
  `getOrCreate()`; bounded E2E continues to use only its immutable scope.
- The dimensional writer requires an explicit runtime bucket and Nessie ref
  for WRITE, and table/reuse manifests no longer default their ref to `main`.
- Production apply configuration now rejects missing explicit
  `NESSIE_REF`, `GOLD_CANDIDATE_REF`, target ref/catalog/schema, rollback hash,
  or rollback owner before adapter access.
- Regression coverage asserts the production Spark coordinates, rejects
  missing apply configuration, and rejects `main` compatibility fallback.

Verification attempted:

- `python -m compileall -q gold airflow/dags common tests`: PASS.
- `git diff --check`: PASS.
- `python -m pytest ...`: NOT RUN; host has Python but no `pytest` module.
- `docker compose config --quiet`: BLOCKED by local Docker config permission
  (`C:\Users\HITLAB\.docker\config.json` access denied); no Docker service
  was started and no external write test was run.

QA round-2 verdict: **PENDING independent reviewer QA**. Next step is review
of this exact diff and the focused tests in an environment with pytest and
Compose config access; runtime publish remains disabled.

## Final QA and read-only runtime status (2026-11-19)

Independent QA after the remediation returned **PASS WITH WARNINGS**:

- focused regression suite: `59 passed`;
- compileall and `git diff --check`: PASS;
- production Spark warehouse/ref pinning, strict identity gates, Nessie
  branch/hash parity, CAS/replay and bounded E2E isolation: PASS;
- no runtime write, cutover, checkpoint reset or cleanup performed.

The real Nessie read-only probe also passed: `gold_prod_current` is a `BRANCH`
at hash `a94321570216b4cfae1e81306400c063bccdb82177242722f2c165289569c6a5`.

The remaining runtime certification is blocked by infrastructure availability:
the current Compose stack has no Spark container and no Trino service, so the
Spark/Iceberg and BI consumer read-only probes cannot run. No service was
started and no external state was changed. `BI_ACTIVE_PUBLISH_APPLY` remains
disabled until those read-only services are available and the execution gate is
approved.

### Runtime evidence update

The approved read-only Spark service was started from
`spark/docker-compose.spark.yml` without enabling any write flag. The probe
passed against `gold_prod_current` with warehouse
`s3a://ecommerce-lakehouse-prod/lakehouse`:

- Nessie ref remained a `BRANCH` at the expected hash;
- namespace discovery returned `gold` and the seven allowlisted Gold tables;
- `fact_orders` schema read successfully;
- read-only `COUNT(*)` returned `26705`.

The Trino BI consumer probe remains blocked because the local configuration does
not provide the required `TRINO_S3_READONLY_ACCESS_KEY` and
`TRINO_S3_READONLY_SECRET_KEY`; root MinIO credentials are not permitted for
this probe. No Trino service was started with unsafe credentials.

### Trino read-only probe result

A local-only MinIO service account with read access limited to
`ecommerce-lakehouse-prod` was provisioned, and Trino was started with the
production Nessie ref/warehouse settings. The consumer smoke query correctly
failed closed: the Nessie metadata for `lakehouse.gold.fact_orders` points to
`s3a://ecommerce-data-lake/lakehouse/...`, so the read-only account received
`403 Forbidden` rather than reading outside the approved production bucket.

Spark `DESCRIBE EXTENDED` independently confirmed the same location while the
table commit ID matched the current `gold_prod_current` hash. This is a real
production provenance mismatch, not a credential problem. Resolving it requires
a separately approved Gold candidate WRITE/promotion that materializes metadata
in `ecommerce-lakehouse-prod`; no such write or cutover was performed.

### Silver bundle discovery result

A read-only scan of `ecommerce-lakehouse-prod/silver/` found 11 run IDs with
all six source tables present. None contained a complete immutable candidate
lineage envelope: at least one required field was missing from every run,
including `manifest_identity`, `manifest_keys`, `manifest_identities`, or
`data_prefixes`. The candidate CLI therefore correctly has no safe input bundle
to consume. No new candidate branch was created and no partial writer retry was
issued.

### Production candidate r03 execution — blocked by global fence (2026-11-19)

- Source fixture verification passed: all six PostgreSQL source tables had a
  new watermark at `2026-11-19 01:56:16`; `orders` count was `26714`.
- DagRun `manual__production_candidate_bi_20261119_r03` was triggered once with
  explicit `WRITE` mode and all six tables. Bronze, Silver, artifact creation,
  and Silver verification passed.
- The first Gold attempt failed before any Gold table write because the
  candidate branch did not yet exist. The branch was then bootstrapped with
  `ensure_candidate_ref` from the read-back stable hash
  `a94321570216b4cfae1e81306400c063bccdb82177242722f2c165289569c6a5`.
- The controlled Gold retry then failed closed before table writes on the
  existing global fence object
  `gold/dimensional/locks/v1/global.json`, whose holder identity is
  `0e57976be88f05bc1de9592efcdd61b9d22992e18578d3c6b0f0ff65b50a9d6c`.
  The requested graph identity matched the holder, but the fence is still
  treated as operator recovery state by the writer.
- No lock/object/checkpoint was deleted or overwritten. No stable ref was
  advanced, no publish occurred, and no consumer cutover occurred.

Current gate: **BLOCKED — explicit operator recovery approval required**.
Next step: inspect the holder's partial Gold manifests and active task state;
only after explicit recovery authorization may the verified global fence be
released through the guarded recovery procedure, followed by a fresh candidate
WRITE and independent QA.

### r03 recovery and publish execution (2026-11-19)

- Explicit operator recovery approval was received. The verified stale global
  fence and matching graph fence for graph identity
  `0e57976be88f05bc1de9592efcdd61b9d22992e18578d3c6b0f0ff65b50a9d6c` were
  removed after read-only verification showed no completed holder manifests and
  no Gold task running. No checkpoints, candidate branches, or source data were
  removed.
- Gold retry passed all seven tables with readback verification. The active
  publish retry initially exposed a validation defect: the writer report did
  not carry the final Nessie commit hash, while production validation required
  it. `airflow/dags/pipeline_tasks/gold.py` now binds the read-back candidate
  hash immediately before production validation.
- Focused verification after the fix: compileall PASS, `git diff --check`
  PASS, and `21 passed, 41 deselected` for the focused promotion/orchestration
  tests.
- Publish and all seven confirmation tasks then passed. DagRun
  `manual__production_candidate_bi_20261119_r03` is `success`; stable and
  candidate refs both point to
  `761c847646be22d5096fc62a71f9bfe6d9c44c6de7389cfc87ba88538803a4ad`.
- Final Spark read-only probe found seven active tables and `fact_orders` row
  count `26706`, but `DESCRIBE EXTENDED` still reports a table location under
  `s3a://ecommerce-data-lake/...`. This is a production provenance mismatch:
  the ref CAS succeeded, but BI consumers restricted to
  `ecommerce-lakehouse-prod` cannot be certified against this metadata.

QA/runtime gate: **BLOCKED — DAG/publish PASS, BI consumer certification FAIL**.
The next implementation must materialize Gold table metadata and data under
the approved production warehouse/bucket (without granting BI access to the
legacy bucket), then run independent QA and a fresh candidate promotion.

### r04 production-scope remediation runtime (2026-11-19)

- Added `ProductionGoldScope` and wired production WRITE through it. The scope
  keeps production outside the bounded E2E exception while forcing the
  candidate ref, `gold_run_20261112_gold_06` namespace, and
  `s3a://ecommerce-lakehouse-prod/lakehouse` warehouse.
- Fixed certification validation for an explicitly empty production prefix.
  Focused static verification now passes: `25 passed, 24 deselected`, compileall
  PASS, and `git diff --check` PASS.
- Candidate r04 initially failed before write because its Nessie branch was not
  bootstrapped, then failed on its first graph fence. Both recoveries were
  bounded to verified stale fence objects with no partial table manifests.
- After controlled recovery/replay, DagRun
  `manual__production_candidate_bi_20261119_r04` reached `success`, and active
  publish/confirm tasks passed. Stable and candidate refs point to
  `7eabb1e7a66c80ec9c857e08e3983d6f3ef2420345465b8a71a392f76b359b83`.
- Spark read-only certification passed the seven-table namespace check and
  confirmed `fact_orders` location under
  `s3a://ecommerce-lakehouse-prod/lakehouse/gold_run_20261112_gold_06/...`.

Remaining gate: **BLOCKED — completeness/parity review required**. The new
production namespace currently contains only the incremental fixture
(`fact_orders` read count `1`), not the prior full active dataset. Provenance is
fixed, but BI completeness is not. Do not claim final PASS or cut over consumers
until a safe baseline migration/full rebuild or verified snapshot-reuse path is
implemented and independently QAed.

### Rollback to complete baseline (2026-11-19)

- Per explicit approval, `gold_prod_current` was CAS-rolled back from r04 hash
  `7eabb1e7a66c80ec9c857e08e3983d6f3ef2420345465b8a71a392f76b359b83` to the
  previous complete baseline hash
  `761c847646be22d5096fc62a71f9bfe6d9c44c6de7389cfc87ba88538803a4ad`.
- Nessie read-back confirms stable is a `BRANCH` at `761c…`; r04 remains an
  untouched candidate branch at `7eabb1…` for future baseline migration.
- Spark read-only confirms the rolled-back baseline has `fact_orders=26706`.
  Its legacy location remains `s3a://ecommerce-data-lake/...`; no legacy bucket
  permission was granted to BI consumers.

Current gate: **BLOCKED — baseline is restored, but approved production-bucket
full rebuild/migration is still required before BI consumer cutover**.

### Full rebuild and incremental write certification (2026-11-19)

- Historical candidate `prodbackfill20261119full01` passed independent QA and
  was promoted by guarded CAS to `gold_prod_current` at
  `0a2ae6cb10d44b1508d8f212dceccbbe9f1a70fe8b2e269c58de26c427eb74bd`.
- The first post-promotion incremental run
  `manual__production_write_20261119_r05` completed **successfully**.
- Its Gold result was `GOLD_NO_CHANGE` with all six source states `NO_DATA`.
  Gold write, active publish and table confirmations were skipped by contract;
  stable and candidate r05 remained at the same hash. This is a successful
  no-op, not a failed write.

### BI read-only certification (2026-11-19)

- Trino local endpoint `http://localhost:8081` was queried with the policy
  user `metabase` against `lakehouse.gold_run_20261112_gold_06`.
- All seven Gold tables completed read-only count queries successfully; no
  current query reproduced `ICEBERG_INVALID_METADATA`.
- Counts: `dim_date=762`, `dim_customer=6236`, `dim_product=2691`,
  `fact_orders=26714`, `fact_order_items=80318`, `fact_payments=33768`,
  `fact_shipments=24596`.
- The earlier error was from a stale startup-era query; the current Nessie
  branch and production-bucket metadata are readable by BI.
## QA correction plan — publish identity and low-level scope guards

### Scope

Correct the independent QA findings from the review of commits `d1980b9`,
`8a45dcf`, and `4662851`:

1. Preserve the certification `candidate_hash` and compare it with the current
   Nessie candidate branch hash; never rebind the report to the branch HEAD.
2. Require `report.candidate_ref` to equal the configured
   `GOLD_CANDIDATE_REF` before any promotion client operation.
3. Make `ProductionGoldScope` and the low-level dimensional writer reject an
   invalid production bucket, candidate ref, or namespace even when called
   outside the CLI/Airflow boundary.
4. Add regression tests for hash mismatch, candidate-ref mismatch, and unsafe
   direct scope construction. Tests remain local-only and are not committed,
   consistent with the repository test-folder policy.

### Acceptance criteria

- A stale or mismatched certification hash fails closed before production ref
  promotion.
- A candidate ref mismatch between environment and certification fails closed.
- `ProductionGoldScope` rejects `main`, `gold_prod_current`, arbitrary refs,
  and non-certified namespaces for production WRITE.
- Targeted tests pass and changed Python files compile.
- No DAG trigger, Nessie/MinIO/PostgreSQL mutation, BI cutover, commit, or push
  occurs until independent QA passes.

### Verification

```powershell
.\.venv\Scripts\python.exe -m pytest -q --basetemp .pytest-tmp-focused `
  tests/test_bi_ref_promotion.py tests/test_gold_dimensional_writer.py `
  tests/test_gold_dimensional_verification.py tests/test_storage_layout.py
.\.venv\Scripts\python.exe -m compileall -q airflow common gold
git diff --check
```

### Status

- Approved by user in chat.
- Implementation correction round 1 complete; targeted local verification
  passed, but QA round 2 found a producer/consumer contract gap.
- QA round 2: **FAIL**. Consumer-side hash preservation and scope guards
  passed, but the production certification producer did not emit
  `candidate_hash`, so every real production promotion would fail closed.
  Existing tests used a hand-built report and did not cover producer-to-
  consumer wiring.
- Correction round 2 scope: read back the candidate branch hash from Nessie
  after the WRITE graph completes and include it in the production
  certification report; add producer-to-consumer regression coverage without
  overwriting the report in the publish task.
- Independent QA required again after correction.
- QA round 3: **PASS WITH WARNINGS**. Producer-to-consumer hash lineage,
  candidate binding, and low-level scope guards passed independent review.
  Targeted suites passed (`67 passed`), compileall and diff-check passed.
  Warning: two existing quality-it orchestration tests fail because their
  fixture omits `NESSIE_REF`, and the full suite was not completed.
- Follow-up correction: preserve the non-production fallback ref for test and
  quality-it orchestration paths without weakening production guards; rerun the
  two affected tests and update this plan before commit.
- QA round 4: **PASS WITH WARNINGS**. Independent review found no Critical,
  High, or Medium issue. The candidate hash producer/consumer chain,
  candidate-ref binding, production scope guards, bounded immutability and
  non-production `main` fallback passed review. Evidence: `117 passed` plus
  `50 passed`, AST/compile checks and `git diff --check` passed.
- Remaining warnings: full repository pytest was not completed, production
  Spark/Nessie/MinIO certification was not run, and local pytest artifacts must
  not be committed.
- Next step: commit the correction after excluding generated pytest artifacts;
  keep runtime production publish/cutover behind its existing approval gate.

## Runtime certification and cutover execution request

- User explicitly requested production runtime certification and production
  cutover/publish.
- Certification scope: read-only Spark/Nessie/MinIO validation of the current
  certified seven-table Gold graph, candidate branch identity, manifest/hash
  parity, schema and row-count readback, rollback metadata, and BI consumer
  smoke query.
- Cutover scope: only the guarded `gold_prod_current` CAS promotion from the
  verified production candidate, with the exact expected current hash and
  `PRODUCTION_LAKEHOUSE_WRITE_APPROVED=1`; no data cleanup, checkpoint reset,
  rollback, compaction, snapshot expiry or orphan removal.
- Preflight must stop if the candidate hash, stable hash, candidate ref,
  catalog/schema, rollback hash/owner or approval flags do not match the
  certified plan. Runtime evidence will be recorded after execution.

## Runtime certification and cutover result (2026-11-19)

- Preflight matched the approved production contract: bucket
  `ecommerce-lakehouse-prod`, stable ref `gold_prod_current`, candidate ref
  `gold_prod_candidate_bi_20261119_r05`, catalog `lakehouse`, schema
  `gold_run_20261112_gold_06`, production approval flags enabled, and rollback
  hash/owner present.
- Nessie read-only GET confirmed stable and candidate are both BRANCH refs at
  hash `0a2ae6cb10d44b1508d8f212dceccbbe9f1a70fe8b2e269c58de26c427eb74bd`.
- Spark 3.5.6 read-only certification through Nessie and MinIO completed with
  all seven tables readable and counts matching the certified baseline:
  `dim_date=762`, `dim_customer=6236`, `dim_product=2691`,
  `fact_orders=26714`, `fact_order_items=80318`, `fact_payments=33768`,
  `fact_shipments=24596`.
- Cutover result: **APPLY_NOOP / already active**. The guarded production
  promotion had no state change to perform because `gold_prod_current` already
  pointed to the exact verified candidate hash. No Nessie update, Iceberg write,
  cleanup, checkpoint reset or rollback was executed.
- Trino and Metabase read-only smoke remained PASS after certification; no
  active consumer cutover was required beyond the already-active stable ref.

## Approved local production-mode delta seed (2026-11-19)

- User approved a real incremental WRITE event using the local Docker
  PostgreSQL target, with a larger deterministic delta.
- Immutable run ID: `prodwrite20261119r06`.
- Target: local `postgres` host, database `ecommerce_platform`, in the
  production-mode pipeline configuration; no external database.
- Planned rows: 5 customers, 8 products, 20 orders, 40 order items, 20
  payments, and 20 shipments. Parent rows are inserted before dependents.
- Allowed operation: idempotent `INSERT ... ON CONFLICT DO NOTHING` only. No
  delete, truncate, drop, checkpoint reset, watermark reset, cleanup,
  compaction or snapshot expiration.
- The seed command must fail closed unless its explicit production-mode approval
  flag, local source host/database, production bucket and exact run ID are all
  present before opening a PostgreSQL client.
- Seed QA round 1: **FAIL**. Independent review found that PostgreSQL's
  `set_updated_at()` trigger overwrites `updated_at` on `ON CONFLICT DO UPDATE`,
  so replaying the same run ID was not deterministic; `DB_PORT` was also not
  restricted to the local Compose port.
- Required correction before seed: use conflict handling that leaves existing
  run-scoped rows untouched on replay, require `DB_PORT=5432`, then rerun
  independent static QA and only execute the mutation after it passes.

### Seed QA round 1 correction scope (2026-11-19)

- Corrected `scripts/seed_production_incremental_delta.py` within the approved
  scope: all six table statements now use `INSERT ... ON CONFLICT DO NOTHING`.
  Existing run-scoped IDs are therefore not updated on replay, so PostgreSQL
  `set_updated_at()` triggers cannot change replayed rows. Parent tables remain
  ordered before dependents, preserving the FK-valid cardinalities of 5
  customers, 8 products, 20 orders, 40 order items, 20 payments, and 20
  shipments.
- Added a strict `DB_PORT=5432` requirement to the pre-connect scope guard; the
  connection still receives the guarded environment value.
- No seed execution, PostgreSQL client creation, external-service test, or
  destructive SQL was performed.
- Static correction checks completed: exactly six `ON CONFLICT ... DO NOTHING`
  clauses, no `DO UPDATE`, strict port guard present before `connect()`, six
  table order/cardinality contracts present, and `git diff --check` passed.
- Independent QA round 3: **PASS** for the corrected static contract. Evidence:
  six `DO NOTHING` handlers, zero `DO UPDATE` handlers, strict local port and
  credential guards before client creation, and `git diff --check` passed. No
  seed or database connection was executed during QA.
- Runtime mutation remains the next approved action, using the exact local
  target and immutable run ID above.

### QA round 3 follow-up implementation (2026-11-19)

- Approved scope was limited to the two quality-it orchestration failures where
  an omitted `NESSIE_REF` reached certification as `None`.
- The runner now resolves one runtime ref for graph execution and certification:
  bounded scope ref first, production candidate ref second, and `main` only for
  non-production unscoped execution when `NESSIE_REF` is absent.
- Production `NESSIE_REF=gold_prod_current`, candidate hash read-back,
  production approval, namespace and candidate guards remain unchanged.
- Regression coverage asserts the quality-it DRY_RUN certification ref is
  `main`; the companion WRITE test exercises the same fallback path.
- No DAG, Spark runtime, MinIO/Nessie/PostgreSQL service, external mutation,
  Iceberg cutover, checkpoint/watermark change, or commit was performed.
- Verification evidence: the two affected tests passed (`2 passed`); full
  orchestration plus publish-XCom tests passed (`50 passed`); targeted
  producer/publish, writer, verification and storage-layout suites passed
  (`93 passed`) when using a workspace-local pytest `--basetemp`.
- `compileall -q airflow common gold tests` passed and `git diff --check`
  passed. The first targeted invocation hit three pytest temp-directory
  `PermissionError`s under the default user temp root; no test assertion or
  implementation failure was involved, and the rerun with local `--basetemp`
  was fully green.
- Implementation handoff is ready for independent QA; no runtime certification
  or production enablement is claimed.

## Local production-mode delta seed implementation (2026-11-19)

- Added `scripts/seed_production_incremental_delta.py` for the approved local
  production-mode event `prodwrite20261119r06`.
- The script validates, before importing or constructing a PostgreSQL client:
  `PRODUCTION_INCREMENTAL_WRITE_APPROVED=1`,
  `BRONZE_BUCKET=ecommerce-lakehouse-prod`,
  `LAKEHOUSE_STORAGE_ENV=production`, local `DB_HOST` (`postgres`,
  `localhost`, or `127.0.0.1`), and `DB_NAME=ecommerce_platform`.
- The deterministic FK-valid delta contains 5 customers, 8 products, 20
  orders, 40 order items, 20 payments, and 20 shipments with a fixed newer
  `updated_at` timestamp. Parent tables are inserted before dependents.
- SQL is limited to `INSERT ... ON CONFLICT DO NOTHING`; replaying the same
  run ID leaves existing rows untouched, avoiding the source `updated_at`
  trigger. No seed execution,
  PostgreSQL client creation, database mutation, checkpoint/watermark change,
  or external-service test was performed.
- `DB_PORT=5432`, `DB_USER`, and `DB_PASSWORD` are required explicitly; the
  seed script has no credential fallback.
- Static verification command:
  `powershell -NoProfile -Command "$p=Get-Content -Raw scripts/seed_production_incremental_delta.py; if ($p -notmatch 'PRODUCTION_INCREMENTAL_WRITE_APPROVED' -or $p -notmatch 'ecommerce-lakehouse-prod' -or $p -notmatch 'LAKEHOUSE_STORAGE_ENV' -or $p -notmatch 'ecommerce_platform' -or $p -notmatch 'prodwrite20261119r06') { throw 'required guard missing' }; if (($p | Select-String -Pattern 'ON CONFLICT' -AllMatches).Matches.Count -ne 6) { throw 'expected six upserts' }; if ($p -match '(?i)\\b(DELETE|TRUNCATE|DROP|RESET|CLEANUP)\\b') { throw 'destructive token found' }"`
- `git diff --check` completed without output. Python compile/pytest were not
  run because no Python interpreter is installed in the host environment.

### Seed QA round 3 final QA record

- QA round 1: **FAIL** because `ON CONFLICT DO UPDATE` activated the source
  `set_updated_at()` trigger on replay and `DB_PORT` was not restricted.
- Correction changed all six conflict handlers to `DO NOTHING`, added strict
  `DB_PORT=5432`, and removed credential fallbacks.
- QA round 3 independently confirmed the corrected plan/source contract before
  the approved mutation. The only remaining evidence gap is runtime seed and
  downstream production WRITE verification.

### Seed QA round 4 correction (2026-11-19)

- QA round 4 functional review: **PASS**. The only failure was Git diff safety:
  the new seed script was untracked, so the standard diff did not cover the
  implementation.
- Correction: stage the seed script and this plan together before the next QA;
  this is a review-boundary change only, not a commit or runtime mutation.
- The next QA must verify the staged full intended diff, then the approved
  runtime seed may proceed. Runtime seed and downstream production WRITE remain
  unverified.

### Runtime production incremental WRITE evidence (2026-11-19)

- Execution gate passed against the local Docker PostgreSQL target. The exact
  ID ranges were absent before mutation; the approved seed then returned
  `PRODUCTION_INCREMENTAL_DELTA_SEED=PASS` for run
  `prodwrite20261119r06`, with 5 customers, 8 products, 20 orders, 40 order
  items, 20 payments, and 20 shipments.
- PostgreSQL read-back returned exactly `5/8/20/40/20/20` matching rows and
  FK read-back returned `20/40/20/20` valid child relationships. Replaying the
  same immutable run ID returned PASS and did not create duplicates; the
  script remains `ON CONFLICT DO NOTHING` only.
- Airflow production WRITE event `manual__prodwrite20261119r06` completed
  successfully. All 59 task instances reached terminal state: 53 success and
  6 intentional `skip_silver_*` states. `gold_dimensional_write`,
  `publish_gold_to_active`, and all seven Gold confirmation tasks were
  successful.
- Nessie read-back confirmed `gold_prod_current` and the configured candidate
  branch now point to hash
  `7fe1816313bf5cb6964a786c6a09a6f94dac16c9d07e9f8da67293315c91ef54`.
- This closes the previously missing production WRITE-event evidence. No
  cleanup, rollback, checkpoint reset, or snapshot expiration was performed.
