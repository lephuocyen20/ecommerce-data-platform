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
