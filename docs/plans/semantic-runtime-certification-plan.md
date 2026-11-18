# Plan: Gold semantic contract and runtime certification

## Workstream B — `customers`/`products` `NO_DATA` diagnosis closure (2026-11-17)

### Approved Task and Scope

Read-only diagnosis only for candidate
`production_silver____dee17da68cca499299c18eb45cbbabcf`: sanitized source
row counts and `MAX(updated_at)`, current watermark/checkpoint and batch
metadata, table selection/eligibility, and exact recorded production run
evidence. No writes, retry, DAG trigger, reset, cleanup, MinIO/Nessie/Spark
mutation, secrets, or full rows.

### Sanitized Evidence

- At `2026-11-17T03:10:58Z`, a PostgreSQL transaction reported
  `transaction_read_only=on` and was rolled back after the read-only probes.
- `customers`: source count `6225`, `MAX(updated_at)=2026-11-14
  07:20:17.634963 UTC`; watermark `(TIMESTAMP, last_timestamp same,
  last_id=6225)`; replay cursor/upper bounds `NULL/0`; qualifying count under
  `(updated_at, customer_id) > (last_timestamp, last_id)` was `0`.
- `products`: source count `2680`, `MAX(updated_at)=2026-11-14
  07:20:17.634963 UTC`; watermark `(TIMESTAMP, last_timestamp same,
  last_id=2680)`; replay cursor/upper bounds `NULL/0`; qualifying count under
  `(updated_at, product_id) > (last_timestamp, last_id)` was `0`.
- `TABLE_CONFIG` confirms `customers -> (customer_id, updated_at, TIMESTAMP)`
  and `products -> (product_id, updated_at, TIMESTAMP)`. The candidate's
  recorded selection was all six tables, and its sanitized task read-back was
  `59 total / 42 success / 17 skipped / 0 failed / 0 upstream_failed`.
- The candidate record reports `NO_DATA` for exactly `customers` and
  `products`, with no batch or Silver artifacts for those tables. The exact
  candidate/run record remains the prior sanitized evidence in this plan.

### Classification

**CONFIRMED — expected no delta.** Both source tables contain rows, but no row
is newer than the effective checkpoint and replay is inactive. This is
consistent with a true zero-row incremental extraction; it is not evidence of
an extractor exception or selection mismatch.

### Separate disposition

The `gold/implementation/dimensional/reader.py` reader-path hardening warning
(verified-prefix loading versus explicit manifest object paths) is tracked
separately. This diagnosis neither resolves nor implements that hardening and
does not use it to explain `NO_DATA`.

### Tests, limits, and handoff

Executed only sanitized read-only SQL/metadata probes and static configuration
inspection. No DAG trigger, retry, reset, cleanup, data-store mutation, or
Iceberg cutover occurred. Any remediation or new production run requires a new
explicit approval and a separate plan.

## Approved fresh production Silver run - completed with incomplete six-table evidence (2026-11-17)

### 1. Approved Task and Scope

Exactly one new production `ecommerce_incremental_ingestion` DagRun was
triggered with Bronze/Silver `WRITE` for the six requested tables. Production
bucket/ref were `ecommerce-lakehouse-prod` / `gold_prod_current`, with
`LAKEHOUSE_STORAGE_ENV=production` and `INGESTION_WRITE_ENABLED=true`.
E2E/quality-it was unset; warehouse/lakehouse/Gold write, publish, cutover,
backfill, checkpoint/watermark, and cleanup paths remained disabled. Gold was
not launched. No retry, reset, cleanup, commit, or push was performed.

### 2. Files Changed

- `docs/plans/semantic-runtime-certification-plan.md`
- `docs/GOLD_LAKEHOUSE_PLAN.md`

### 3. Implementation Summary

- JWT was acquired memory-only and was not printed or persisted.
- Exact candidate: `production_silver____dee17da68cca499299c18eb45cbbabcf`.
- Logical date: `2026-11-17T02:45:56Z`.
- Single POST to `/api/v2/dags/ecommerce_incremental_ingestion/dagRuns`:
  HTTP `200`; no second POST was sent.
- DagRun: `success`; start `2026-11-17T02:45:57.151582Z`; end
  `2026-11-17T02:46:32.834911Z`.
- Task read-back: 59 total, 42 `success`, 17 `skipped`, 0 `failed`, 0
  `upstream_failed`; no task error was present.

### 4. Tests Executed and Results

- Production flag/container inspection and authenticated Airflow preflight:
  **PASS**.
- DagRun/task state polling and read-back: **PASS**.
- MinIO exact-prefix read-back: **PASS** for committed `orders`,
  `order_items`, `payments`, and `shipments`. Each has one Bronze `data.csv`,
  one Silver manifest, one Parquet file, `_SUCCESS`, quality evidence, and
  reconciliation evidence; listed objects are prefix-contained.
- Six-table coverage: **INCOMPLETE**. `customers` and `products` returned
  `NO_DATA`, so no manifest/schema/quality/reconciliation artifact exists for
  either table in this run.
- Manifest provenance: **BLOCKED WITH WARNING**. Stored MinIO manifest
  identities differ from the corresponding Silver XCom identities for all
  four committed tables. Persisted quality/reconciliation records report
  `PASS` and include schema fingerprints, but the cross-boundary identity
  mismatch prevents immutable six-table certification.

### 5. Risks and Limitations

The Airflow run and production Bronze/Silver writes succeeded and remain
untouched. Runtime certification remains **OPEN / NOT CERTIFIED** for the
requested six-table envelope. Do not retry or reuse the candidate, reset
checkpoints/watermarks, delete/clean objects, or launch Gold. The identity
mismatch and two `NO_DATA` tables require independently reviewed diagnosis.

### 6. QA Reviewer Handoff

Review the exact candidate, HTTP 200 POST, terminal task states, four committed
inventories, two `NO_DATA` results, and stored-vs-XCom manifest identity
mismatches. Verdict: **PASS** for safe single-run execution and bounded
read-back containment; **FAIL / NOT CERTIFIED** for the six-table runtime gate.

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

## Runtime attempt — Spark 3.5.6 production read-only path (2026-11-16)

- Image availability: **AVAILABLE**. `ecommerce-spark:3.5.6` was present with
  digest `sha256:f404059053e7ed4a8771a24a7bd9d60c2b118524329e24373a0e33529d1637fd`.
  Existing container `ecommerce-spark` was running with project code mounted
  read-only at `/opt/spark/work-dir` and Spark **3.5.6** on Java 17.
- Read-only endpoint evidence: `GET http://nessie:19120/api/v2/trees/gold_prod_current`
  returned a `BRANCH` at hash
  `adb060d0499e2cfd0fd38d28b3c6c3b85ef5c1aedd54238fbe3a7d652f0e196e`.
  Spark SQL with `spark.sql.catalog.lakehouse.ref=gold_prod_current` and
  warehouse `s3a://ecommerce-lakehouse-prod/lakehouse` returned namespace
  `gold`.
- The full Gold `DRY_RUN` was **BLOCKED / NOT CERTIFIED**: the mounted project
  contained no verified six-table Silver lineage/artifact envelope for this
  production path. `run_pipeline.py` requires that contract before creating
  Spark/MinIO clients; fabricating lineage or using the fixture would violate
  the manifest and no-mutation guards.
- No writer, Iceberg `MERGE`, Nessie ref update, checkpoint/watermark change,
  MinIO/PostgreSQL mutation, publication, or cutover was performed. No source
  or harness code changes were required.
- Exact read-only probe: `docker exec -e NESSIE_REF=gold_prod_current
  -e BRONZE_BUCKET=ecommerce-lakehouse-prod -e MINIO_ENDPOINT=minio:9000
  ecommerce-spark /opt/spark/bin/spark-sql --conf
  spark.sql.catalog.lakehouse.ref=gold_prod_current --conf
  spark.sql.catalog.lakehouse.warehouse=s3a://ecommerce-lakehouse-prod/lakehouse
  -e 'SHOW NAMESPACES IN lakehouse'`.

Next step: obtain a separately approved, immutable run input containing the
verified Silver lineage/artifacts, then repeat the single read-only Spark
`DRY_RUN`; do not claim runtime certification from this metadata-only probe.

### Independent QA verdict for Docker Spark probe (2026-11-16)

Verdict: **PASS WITH WARNINGS — metadata/read-only probe only**.

QA confirmed the recorded image digest, Spark/Java versions, `gold_prod_current`
ref hash, `gold` namespace probe, read-only scoped mounts and absence of
mutation. The full Gold DRY_RUN is correctly **BLOCKED / NOT CERTIFIED** because
there is no verified immutable six-table Silver lineage/artifact envelope. No
fixture was substituted and no false runtime pass was recorded.

Low-risk note: the Spark Compose file leaves `transformation` and `validation`
mounts without `:ro`; the modules used by this metadata probe were read-only,
but a future hardening change should make the read-only claim cover every code
mount. This does not block the current metadata-only verdict.

Exact next gate: obtain separate approval for an immutable verified Silver
lineage/artifact input, then run one read-only Spark Gold DRY_RUN and capture
snapshot/schema/row/file plus quality and replay evidence. Gold WRITE, cutover
and production routing remain disabled.

## Approved immutable Silver lineage discovery gate (2026-11-16)

Approval received to discover and validate an existing immutable six-table
Silver lineage/artifact envelope for `gold_prod_current`. Scope is read-only:
inspect existing manifests/artifacts and their identities, validate source
tables, ref/run binding, schema/row/file evidence and completeness, then use
the verified envelope for one subsequent Spark Gold DRY_RUN. No artifact,
manifest, checkpoint, watermark, MinIO object, PostgreSQL row, Nessie ref or
Iceberg table may be created or modified. Fixtures and fabricated lineage are
prohibited.

Acceptance criteria:

- All six Silver inputs are present and bound to one immutable verified run.
- Envelope identity, ref/run binding, table names, schema and artifact
  provenance pass existing contracts.
- No write or mutation occurs during discovery.
- If any input is missing, stale, ambiguous or unverifiable, report BLOCKED and
  do not launch the Gold runtime DRY_RUN.

### Discovery result — BLOCKED (2026-11-16)

The immutable six-table Silver lineage/artifact envelope was **not found or
verifiable** for production path `ecommerce-lakehouse-prod` / Nessie ref
`gold_prod_current`. Gold DRY_RUN was not launched.

Read-only evidence:

- Nessie `GET /api/v2/trees/gold_prod_current` returned branch hash
  `adb060d0499e2cfd0fd38d28b3c6c3b85ef5c1aedd54238fbe3a7d652f0e196e`.
- Nessie entries at that ref contain namespace `gold` and exactly seven Gold
  Iceberg tables (`dim_customer`, `dim_date`, `dim_product`,
  `fact_order_items`, `fact_orders`, `fact_payments`, `fact_shipments`). No
  Silver namespace/table entries were present.
- MinIO bucket `ecommerce-lakehouse-prod` existed and had 467 objects. It had
  77 Silver manifest objects and **zero** objects under `lakehouse/`, so no
  read-back `snapshot_attestation`, `quality`, or `reconciliation` evidence
  was available at the contract paths.
- Silver has eight common `COMPLETED` run IDs across all six tables:
  `prodbackfill20261114h`, `prodbackfill20261115g`,
  `prodbackfill20261115h`, `prodbackfill20261115i`,
  `prodbackfill20261115j`, `prodbackfill20261115k`,
  `prodbackfill20261115l`, and `prodbackfill20261115m`. These are separate
  table manifests, not a six-table envelope bound to `gold_prod_current`.
- For every one of those runs and all six tables, the stored completed
  manifest has `status=COMPLETED`, one `parquet_objects` entry and a row count,
  but is missing the required `parquet_manifest` lineage fields:
  `source_type`, `bucket`, `manifest_key`, `manifest_identity`,
  `manifest_keys`, `manifest_identities`, and `data_prefixes`. The manifests
  also contain no Nessie `ref`, run-envelope identity, schema fingerprint,
  snapshot attestation identity, quality evidence identity, or reconciliation
  evidence identity.
- Spark 3.5.6 read-only inspection with `ref=gold_prod_current` listed the
  seven Gold tables and returned `fact_orders` schema/snapshot/file metadata.
  The latest snapshot was `9080989530478927852` and its data file location was
  `s3a://ecommerce-data-lake/lakehouse/gold/...`, which does not match the
  requested production bucket `ecommerce-lakehouse-prod`; this is an additional
  unresolved provenance/bucket mismatch.

Exact missing evidence required before the next gate:

1. One immutable envelope identity that covers exactly the six Silver tables:
   `customers`, `products`, `orders`, `order_items`, `payments`, `shipments`.
2. For each table, a validated `parquet_lineage` artifact with the complete
   `parquet_manifest` fields, content-addressed manifest identity, exact
   bucket/key/data prefix, batch ID, row count, manifest set and object/file
   inventory, plus schema evidence/fingerprint.
3. A single immutable run identity and explicit binding of all six artifacts to
   `gold_prod_current` (or a documented production snapshot/ref contract), with
   no mixed or ambiguous runs.
4. Read-back PASS evidence for provenance/integrity, quality and reconciliation
   for each table, and proof that all referenced objects are in the approved
   production bucket/prefix. The current `lakehouse/` evidence objects are
   absent, so this cannot be inferred from the Silver manifests.
5. Resolution of the observed Gold Iceberg data-location mismatch before any
   runtime certification claim.

No MinIO object, PostgreSQL row, Nessie ref, Iceberg table, checkpoint or
watermark was created or modified. No fixture or fabricated lineage was used.
The next action remains blocked until the exact evidence above exists and is
separately approved for read-only validation.

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

## Independent QA verdict: immutable Silver lineage discovery (2026-11-16)

Verdict: **FAIL — BLOCKED**.

QA confirmed the discovery correctly failed closed:

- `gold_prod_current` exists at the recorded Nessie hash, but the ref exposes
  only the Gold namespace/tables; no Silver tables are present.
- The production bucket contains Silver manifests, but no immutable six-table
  envelope. Available completed runs are separate table manifests without one
  run/ref binding.
- Required provenance is missing: source type, bucket/key, manifest
  identity/inventory, data prefixes, schema fingerprint, ref/run identity,
  snapshot attestation, quality and reconciliation evidence.
- Recorded Gold file provenance points to `ecommerce-data-lake`, not the
  approved `ecommerce-lakehouse-prod` bucket.
- No Gold DRY_RUN was launched and no MinIO, Nessie, PostgreSQL, Iceberg,
  checkpoint or watermark mutation occurred.

This is a provenance/data-contract block, not a runtime timeout. Do not
fabricate an envelope, combine unrelated runs, substitute fixtures or launch
Gold DRY_RUN. The next gate requires one immutable identity covering all six
Silver tables, complete `PASS` `parquet_lineage` evidence, a resolved bucket
binding and separately approved read-only validation before retrying Gold.

## Approved Silver envelope remediation (2026-11-16)

User explicitly authorized creation of an immutable Silver lineage envelope in
MinIO bucket `ecommerce-lakehouse-prod`. Scope is metadata-only and bounded to
one existing, internally consistent six-table Silver run selected after
read-only validation. The remediation may write the envelope/manifest metadata
required by the lineage contract, but must not rewrite Silver data, create a
new data run, mutate PostgreSQL/checkpoints/watermarks, update Nessie refs,
merge or publish Gold, or use fixtures.

Acceptance criteria:

- Select one real run with all six tables and matching run identity; fail closed
  if no such run exists.
- Validate and record bucket/key/data-prefix, manifest identity/inventory,
  schema fingerprint, row/file evidence, ref/run binding, quality and
  reconciliation provenance before writing metadata.
- Write exactly one immutable envelope under the approved production bucket;
  verify read-back identity without overwriting existing objects.
- Keep all Gold feature/WRITE flags disabled and require independent QA before
  using the envelope for a Gold DRY_RUN.

## Silver envelope remediation execution result — BLOCKED (2026-11-16)

The approved metadata-only remediation was inspected and failed closed before
any MinIO client write or object creation.

Read-only evidence from the six Silver manifest sets:

- The production bucket contains separate `COMPLETED` table manifests, with
  common run IDs recorded as `prodbackfill20261114h`, `prodbackfill20261115g`,
  `prodbackfill20261115h`, `prodbackfill20261115i`, `prodbackfill20261115j`,
  `prodbackfill20261115k`, `prodbackfill20261115l`, and
  `prodbackfill20261115m`.
- None of these candidates provides a complete six-table immutable envelope
  bound to one run/ref identity. The stored manifests lack the required
  `parquet_manifest` provenance fields: `source_type`, `bucket`,
  `manifest_key`, `manifest_identity`, `manifest_keys`,
  `manifest_identities`, and `data_prefixes`.
- The same manifests do not provide the required schema fingerprint,
  snapshot-attestation identity, quality evidence identity, reconciliation
  evidence identity, or explicit `gold_prod_current` binding. Therefore
  bucket/key/data-prefix, manifest inventory, schema, row/file, ref/run,
  quality, and reconciliation validation cannot pass.
- The recorded Gold read-only evidence also points at
  `ecommerce-data-lake/lakehouse/gold/...`, which conflicts with the approved
  production bucket `ecommerce-lakehouse-prod`.
- Current `.env` values include enabled ingestion, lakehouse, Gold WRITE, and
  all six Silver cutover flags. They were not changed because preserving the
  dirty worktree and not modifying credentials/configuration is mandatory.

Result: **BLOCKED**. No real run was selected; exactly one immutable envelope
was not written; no read-back object key/hash exists. No Silver data rewrite,
PostgreSQL/checkpoint/watermark mutation, Nessie ref update, Gold merge/publish,
fixture use, or cleanup was performed.

Required next step: provide or produce (under a separately reviewed and
approved change) one complete immutable six-table lineage envelope with the
missing provenance/evidence fields, and run with all feature/WRITE/cutover
flags explicitly disabled. Independent QA is required before any Gold
DRY_RUN.

## Independent QA verdict: Silver envelope remediation attempt (2026-11-16)

Verdict: **FAIL — BLOCKED**.

QA confirmed the remediation stopped safely before any write because no valid
real run could satisfy the immutable six-table envelope contract. No objects
were created and no manifests were fabricated or combined across runs. The
bucket/path mismatch and missing provenance/schema/quality/reconciliation
evidence remain unresolved.

Additional safety finding: the ignored local `.env` currently has ingestion,
lakehouse/Gold WRITE and all six Silver cutover flags enabled. It was not
modified during this attempt, but no further Silver/Gold runtime or envelope
write is safe while those settings remain active.

Next safe action: explicitly disable those flags (or supply an equivalent clean
environment override), then prepare a separately reviewed envelope-generation
change with complete provenance and a unique run/ref binding. Re-run
independent QA before any Gold DRY_RUN; Gold WRITE and cutover remain disabled.

## Safety configuration remediation (2026-11-16)

With explicit user approval, the ignored local `.env` was changed only for
safety flags: ingestion, warehouse/lakehouse writes, all six Iceberg cutovers
and Gold dimensional enable/write are disabled; Gold mode is `DRY_RUN`.
Credentials and data paths were not changed. Verification confirms every
relevant flag is `false` and no runtime was launched. The immutable Silver
envelope remains **BLOCKED** until a real six-table run with complete
provenance is available.

## Approved bounded Silver batch for envelope creation (2026-11-16)

User approved one new Silver batch with a unique run ID to produce the missing
real six-table lineage inputs. Scope is limited to Bronze/Silver artifacts and
their manifests in `ecommerce-lakehouse-prod`; Gold dimensional execution,
Nessie ref/cutover, publication, cleanup, checkpoint reset and watermark reset
remain disabled. The batch must be idempotent, fail closed on incomplete table
coverage, and preserve the disabled local write flags outside the explicit
bounded command environment.

Required evidence:

- one run identity covering customers, products, orders, order_items, payments
  and shipments;
- complete manifest/provenance fields, bucket/key/data-prefix and schema/
  row/file evidence;
- quality/reconciliation evidence and immutable envelope read-back;
- no Gold write or ref mutation.

## Approved bounded Silver batch preflight — BLOCKED (2026-11-16)

Derived immutable run ID: `silver-lineage-20261116-083266e67ab0`.

Pure safety preflight passed before any external client was created:

- bucket: `ecommerce-lakehouse-prod`;
- layout: `production`;
- ref value: `gold_prod_current` was validated as configuration only; no Nessie
  client or ref mutation was attempted;
- planned Bronze prefixes:
  `bronze/{customers,products,orders,order_items,payments,shipments}/run_id=silver-lineage-20261116-083266e67ab0/`;
- planned Silver prefixes:
  `silver/{customers,products,orders,order_items,payments,shipments}/run_id=silver-lineage-20261116-083266e67ab0/`;
- Gold, warehouse, ingestion and Iceberg cutover flags were explicitly disabled
  in the command-scoped preflight environment; `.env` was not sourced or
  modified.

Exact block reason before write: the current real Bronze→Silver producer
contract cannot produce the complete provenance required by this gate. The
historical Silver producer emits `status`, table/run/batch identities,
manifest/data prefixes, Parquet inventory/checksums and row counts, but does
not emit a validated `schema_fingerprint` nor read-back `quality` and
`reconciliation` evidence for all six tables. Writing its output would create
incomplete lineage and would violate the fail-closed requirement.

No external client was created. No Bronze/Silver object, manifest, envelope,
PostgreSQL row, checkpoint, watermark, Nessie ref, Gold output, cleanup or
publication was performed. Consequently there are no object keys or hashes to
report for this run; the planned prefixes above remain unused.

Verification evidence: command-scoped pure `validate_storage_environment` and
`production_environment` preflight returned `PREFLIGHT=PASS`; producer/contract
inspection then classified the batch as `BLOCKED` before write. No runtime or
external-service test was executed.

Exact next step: add and independently QA a producer change that derives and
persists real schema, quality and reconciliation evidence under the approved
Bronze/Silver-only scope, then request approval for a new bounded run. Do not
reuse this blocked run ID and do not enable Gold, Nessie cutover, publication,
cleanup, checkpoint reset or watermark reset.

## Independent QA verdict: bounded Silver batch preflight (2026-11-16)

Verdict: **PASS WITH WARNINGS — preflight decision correct; batch BLOCKED**.

QA confirmed storage/layout preflight passed, but the producer cannot emit the
required `schema_fingerprint`, persisted/read-back quality evidence and
reconciliation evidence. The batch stopped before Spark/MinIO client creation,
no object was written, and the failed run ID
`silver-lineage-20261116-083266e67ab0` must not be reused.

The executable producer still needs a contract gate before client creation and
manifest publication. Historical backfill safety was also disabled locally:
`HISTORICAL_BACKFILL_WRITE_APPROVED=0` and
`HISTORICAL_BACKFILL_MODE=DRY_RUN`. Gold, Silver cutover, ingestion and all
other write flags remain disabled.

Next gate: implement and independently QA producer evidence generation and
preflight, then obtain approval for a new run ID. Do not run Silver/Gold
runtime or create an envelope until that gate passes.

## Independent QA verdict: producer contract remediation round 1 (2026-11-16)

Verdict: **FAIL**.

Focused tests passed (`28 passed` for producer/summary and `42 passed` for
related Gold lineage/coverage suites), compileall and diff-check passed, and no
external client/write was used. The implementation is not yet ready for a new
Silver batch because:

- Existing/retry verification checks stored identity fields but does not
  recompute canonical identities from the full persisted quality/reconciliation
  JSON or compare the persisted schema fingerprint with read-back schema.
- Silver manifests lack Gold-compatible `source_type`, `manifest_keys`,
  `manifest_identities` and `data_prefixes` fields.
- Evidence keys are not fail-closed against escaping the immutable data prefix.

Required remediation: recompute and validate persisted identities/schema,
enforce evidence-key prefix binding, emit the complete plural manifest binding
contract, add tamper/retry/schema/key-escape/no-overwrite tests, then repeat
independent QA. No new run ID or external write is authorized from this failed
round.

## Approved producer contract remediation implementation (2026-11-16)

### Approved Task and Scope

Implement only the Silver producer contract needed to fail closed before
Spark/MinIO client creation and before completion-manifest publication. The
producer now derives schema, quality and reconciliation evidence only from the
persisted/read-back Silver DataFrame and real Parquet inventory; no fixtures,
external batch, object-storage write, Gold/Nessie/cutover/checkpoint behavior
was used or changed. Local write flags remain untouched and runtime execution
was not launched.

### Changed files

- `silver/incremental.py`: canonical evidence identities, pure preflight,
  create-if-absent evidence publication/read-back, manifest lineage fields and
  fail-closed verification.
- `tests/test_silver_coverage_producer.py`: focused contract/preflight
  regression coverage (the worktree already contains related producer tests).
- `docs/plans/semantic-runtime-certification-plan.md`: this implementation
  evidence and handoff.

### Implementation summary and acceptance evidence

- Each producer result includes `schema_fingerprint` for the table plus
  `quality_evidence_identity` and `reconciliation_evidence_identity`.
- Quality and reconciliation JSON evidence are published below the immutable
  run `data_prefix` with `If-None-Match: *`, then read back and compared before
  the completion manifest is constructed/published.
- Manifest identity, bucket, manifest key, run ID, data prefix, Parquet
  inventory and evidence references are required by the contract. Existing
  manifests lacking these fields fail closed, preserving no-overwrite and
  idempotent retry behavior.
- `preflight_producer_contract` validates arguments, storage layout and local
  Gold/historical write flags before `SparkSession` or `Minio` construction.

### Tests executed and results

- `python -m compileall -q silver tests`: PASS.
- `git diff --check`: PASS.
- `python -m pytest -q tests/test_silver_coverage_producer.py tests/test_silver_pipeline_summary.py`:
  NOT EXECUTED; environment reports `No module named pytest`.
- No external Spark/MinIO/PostgreSQL/Nessie client, batch, or write was run.

### Risks and limitations

The focused pytest suite could not execute because pytest is unavailable in the
current interpreter. Runtime evidence is therefore not certified. The
reconciliation evidence proves the exact Bronze checksum binding and persisted
Silver read-back/inventory consistency; it does not replace a future bounded
end-to-end source reconciliation.

### QA Reviewer Handoff

Review the diff against this approved scope, especially evidence key
immutability, identity recomputation, manifest/run/bucket/key binding, and the
preflight ordering before client creation. Re-run the focused pytest command in
an environment with the project test dependencies; do not run external writes.

## QA remediation round 1 completion (2026-11-16)

### Approved Task and Scope

The producer remediation was completed only in `silver/incremental.py`,
`tests/test_silver_coverage_producer.py`, and this plan. No `.env` or
credentials were changed. No Spark/MinIO/PostgreSQL/Nessie client, external
batch, object-storage write, Gold write, cutover, checkpoint, or fixture for
runtime evidence was used.

### Implementation Summary

- Fresh publication and retry/read-existing verification now recompute the
  canonical SHA-256 identity from the complete persisted quality and
  reconciliation JSON, not only the stored identity field.
- Fresh and retry paths validate the schema fingerprint against the persisted
  or read-back Spark DataFrame schema and both evidence records.
- Evidence references are fail-closed to the exact immutable run prefix and
  expected `_lineage/{quality,reconciliation}.json` keys.
- Completion manifests now emit `source_type=parquet_manifest` and the
  Gold-compatible `manifest_keys`, `manifest_identities`, and `data_prefixes`
  fields alongside singular bindings. Conditional publication remains
  create-if-absent.

### Tests Executed and Results

- `.venv\Scripts\python.exe -m pytest -q tests/test_silver_coverage_producer.py tests/test_silver_pipeline_summary.py`: **33 passed**.
- `.venv\Scripts\python.exe -m compileall -q silver tests`: **PASS**.
- `git diff --check`: **PASS**.
- Added regression coverage for tampered evidence, retry/read-back schema
  mismatch, evidence-key escape, Gold-compatible plural fields, and
  no-overwrite publication behavior.

### QA Verdict and Risks

QA remediation evidence is **PASS for static/focused unit scope**. Runtime
Silver execution and bounded E2E remain uncertified and require a separate
explicit approval; no new run ID or external write is authorized by this
session.

### QA Reviewer Handoff

Independently review the implementation diff against this remediation,
focusing on full-JSON identity recomputation on both fresh/retry paths,
read-back schema binding, exact evidence-key containment, plural manifest
bindings, and preservation of conditional no-overwrite semantics. Do not run
external services or writes.

## Independent QA verdict: producer contract remediation round 2 (2026-11-16)

Verdict: **PASS WITH WARNINGS**.

QA confirmed the producer now recomputes persisted evidence identities, binds
schema fingerprints on retry/read-back, contains evidence keys under the
immutable data prefix, emits Gold-compatible plural manifest fields, and keeps
preflight before Spark/MinIO client creation with conditional no-overwrite.

Evidence: Silver focused tests **33 passed**, Gold lineage compatibility tests
**56 passed**, compileall passed and diff-check passed. No external client,
batch or write was run.

Low-risk follow-up: add a direct assertion of the `If-None-Match: *` header in
the publication test. Runtime Silver execution remains un-certified, and the
blocked run ID `silver-lineage-20261116-083266e67ab0` must not be reused. The
next action requires separate approval for a new bounded run ID.

## Approved new bounded Silver run (2026-11-16)

User approved one new run after producer QA passed. The run must use a fresh
run ID and command-scoped Bronze/Silver write settings only. The ignored `.env`
must remain safe; Gold dimensional mode/write, Nessie cutover/ref mutation,
publication, cleanup, checkpoint and watermark reset remain disabled.

The run may write only the six Bronze/Silver artifact/manifests required for
the envelope. It must stop on incomplete coverage or evidence validation, then
read back the manifest/evidence identities before any later Gold DRY_RUN.

## Approved new bounded Silver run execution — BLOCKED (2026-11-16)

### Approved Task and Scope

One fresh command-scoped Bronze/Silver producer run for exactly
`customers`, `products`, `orders`, `order_items`, `payments`, and `shipments`
in `ecommerce-lakehouse-prod`. The ignored `.env` was not modified. The
command overrides set `INGESTION_WRITE_ENABLED=true` and kept
`GOLD_DIMENSIONAL_ENABLED=false`, `GOLD_DIMENSIONAL_MODE=DRY_RUN`,
`GOLD_DIMENSIONAL_WRITE_ENABLED=false`, Gold metadata persistence/publication,
all six Iceberg cutover flags, cleanup, checkpoint reset and watermark reset
disabled. No Gold DRY_RUN was launched.

### Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` (this execution record)

### Implementation Summary

The single attempted DagRun was created with exact Airflow run ID
`manual__2026-11-16T08:59:53.431664+00:00` and logical date
`2026-11-16T09:00:00+00:00`. `validate_config` passed with the exact six-table
selection. The run then failed closed at the Airflow `dags test` orchestration
layer: sequential task execution received `Server returned error` while
starting task instances already in `success`/`up_for_retry` states, followed by
PostgreSQL `DeadlockDetected` updating `dag_run` (`dag_run.id=41`). The DAG
reported unrunnable tasks and exited non-zero. No retry or second run was
started.

The only observed Bronze result was the existing/recovered orders batch:
`batch_id=d9371e95c8304487a62d0aad651f29d5`,
`action=RECOVER_PENDING`, `status=COMMITTED`, `persisted=true`. It was not a
new Silver artifact. `products` and `customers` returned `NO_DATA`; the
remaining table tasks did not reach a valid complete producer barrier.

### Tests Added or Updated

None. This was a runtime execution/evidence session only.

### Tests Executed and Results

- Airflow bounded execution: **FAIL/BLOCKED**; exact cause was the Airflow
  task-state API invalid-state errors and final PostgreSQL deadlock described
  above.
- DagRun read-back: **PASS** for identity/state read; exact state is
  `failed` for `manual__2026-11-16T08:59:53.431664+00:00`.
- MinIO read-back in `ecommerce-lakehouse-prod`: **PASS for absence check**;
  `silver/` contained no key containing this run ID, and there was no new
  Silver completion manifest, Parquet data prefix, `_lineage/quality.json`, or
  `_lineage/reconciliation.json` for this run. The queried legacy
  `silver_incremental/` prefix had `COUNT 0`.
- Observed Bronze object inventory/hash: key
  `bronze/orders/batch_id=d9371e95c8304487a62d0aad651f29d5/data.csv`, size
  `68867`, ETag `6104d89fa4cadc8630e0f1fc511304b2`. The exact batch prefix had
  `COUNT 1`; no `_BATCH_METADATA.json` was present in that prefix.
- Coverage, schema fingerprints, quality/reconciliation identities, Silver
  bucket/key/data-prefix binding, Silver inventory/row counts and
  create-if-absent semantics: **NOT CERTIFIED**, because no Silver producer
  completed and no evidence envelope was published.

### Risks and Limitations

The requested six-table Silver run was not achieved. The DagRun left Airflow
metadata in `failed` state and observed an existing orders recovery result;
this session did not reset checkpoints/watermarks or remove/overwrite any
object. Because the orchestration path deadlocked before the Silver barrier,
there is no valid partial Silver envelope to validate. The stale scheduler
environment had Gold flags enabled, but the attempted process used explicit
command-scoped disabled overrides; no Gold task/write/cutover/publication was
executed. Do not reuse the failed run ID or infer producer PASS from this
attempt.

### QA Reviewer Handoff

QA must review the exact failed DagRun and orchestration deadlock before any
new approval. Evidence to review: run ID
`manual__2026-11-16T08:59:53.431664+00:00`, DagRun state `failed`, task-state
invalid-state/API errors, PostgreSQL `DeadlockDetected`, and the sole observed
Bronze key/hash above. The next action requires a new explicit approval and a
corrected single-run execution method; no Gold DRY_RUN is authorized from this
evidence.

## Independent QA verdict: new bounded Silver run (2026-11-16)

Verdict: **FAIL — BLOCKED**.

QA confirmed run `manual__2026-11-16T08:59:53.431664+00:00` failed before the
Silver producer barrier due to Airflow task-state API errors and PostgreSQL
`DeadlockDetected`. Read-back found no Silver completion manifest, Parquet
prefix, quality/reconciliation evidence or `silver_incremental` artifact for
the run. The observed Bronze orders object was an existing recovery result and
is not evidence for this run.

No retry, checkpoint/watermark reset, cleanup, overwrite, Gold DRY_RUN,
cutover or publication occurred. The run ID must not be reused.

Next gate: diagnose and fix the Airflow task-state/API orchestration path and
PostgreSQL deadlock handling, validate that path without external writes, then
obtain fresh approval for one corrected bounded Silver run ID.

## Approved orchestration remediation until QA PASS (2026-11-16)

User approved continued remediation until the orchestration gate passes. Scope
is limited to the Airflow task-state/API and PostgreSQL deadlock path exposed by
the failed bounded Silver run. Implement deterministic single-run handling,
fail-closed task-state transitions and safe retry/idempotency behavior with
unit/contract evidence first. Do not rerun external Silver, reset checkpoints,
cleanup or mutate MinIO/PostgreSQL/Nessie until independent QA passes the code
and test changes.

Acceptance criteria:

- No invalid Airflow task-state transitions or stale task-state API calls.
- DagRun/task updates avoid the observed PostgreSQL deadlock path and remain
  idempotent under retry.
- Failed/stale runs are not automatically retried with the same run ID or
  partial artifacts.
- Focused tests cover state transitions, deadlock/retry handling and safe
  failure; external write flags remain disabled.

## Orchestration remediation implementation evidence (2026-11-16)

### Findings

- Repository inspection found no project-owned trigger/clear/state-update
  helper; the failed run was using Airflow runtime state APIs directly. The
  existing DAG remains graph-only and no external runtime was rerun.
- The remediation boundary therefore lives in a dependency-free helper and
  requires the caller to provide a metadata read plus atomic compare-and-set
  write. This prevents stale task-state writes from being converted into API
  errors or duplicate transitions.
- PostgreSQL deadlock detection recognizes SQLSTATE `40P01`, `pgcode`, or a
  nested exception message. Only the complete read/CAS attempt is retried, with
  a bounded budget; if the write committed before the deadlock response, the
  next read returns the target and finishes as an idempotent no-op.
- Terminal state changes, clearing non-failed tasks, stale expected states and
  reuse of any existing run ID all fail closed. No partial artifact or failed
  run ID is automatically reused.

### Files changed in this remediation

- `common/airflow_orchestration.py` — pure state-transition, deadlock retry,
  clear guard and fresh-run-ID helpers; no Airflow/database/storage client.
- `tests/test_airflow_orchestration.py` — legal/illegal transitions, duplicate
  no-op, stale rejection, deadlock commit-window idempotency, retry exhaustion,
  nested SQLSTATE detection, clear guard and run-ID reuse contracts.
- `docs/plans/semantic-runtime-certification-plan.md` — this evidence and
  handoff record.

### Tests executed and results

- `.venv\Scripts\python.exe -m pytest -q tests\test_airflow_orchestration.py
  tests\test_airflow_parse_imports.py tests\test_silver_pipeline_summary.py
  tests\test_silver_coverage_producer.py` — **83 passed**.
- `.venv\Scripts\python.exe -m compileall -q airflow common tests` — **PASS**.
- `git diff --check` — **PASS**.
- A full `pytest -q` attempt was **stopped** after it entered existing
  runtime/integration coverage and reported unrelated errors; it is not
  claimed as passed and no external Silver runtime was invoked.
- `.env` write flags remain disabled (`INGESTION_WRITE_ENABLED=false`,
  `GOLD_DIMENSIONAL_WRITE_ENABLED=false`, `LAKEHOUSE_E2E_APPROVED=0`). No
  MinIO, PostgreSQL, Nessie, Gold, checkpoint or watermark mutation occurred.

### Risks and limitations

The helper is an adapter contract, not a live Airflow metadata transaction;
the production adapter must implement the documented atomic CAS and keep the
transaction short. Runtime Airflow certification and a new immutable Silver
run remain blocked until independent QA reviews this diff and test evidence.

### QA Reviewer Handoff

Review only the remediation diff against the acceptance criteria above.
Confirm that state writes are always expected-state CAS operations, deadlock
retry is bounded and idempotent, stale/illegal/clear/reuse paths fail closed,
and no external write path was exercised. QA verdict is **PENDING**; the next
step after QA PASS requires a separate approval for any new bounded runtime
run ID.

## Independent QA verdict: orchestration remediation round 1 (2026-11-16)

Verdict: **FAIL**.

The focused unit/contract suites passed (`24` orchestration tests and `83`
regression tests), but QA found two blocking gaps:

- CAS updates do not inspect affected-row count, so a concurrent state change
  can be reported as a successful transition.
- The helper is not used by any production DagRun/TaskInstance adapter or DAG
  caller; it therefore does not protect the runtime path that deadlocked.

Additional risks: fresh run-ID checking is not atomic, Airflow 3.1.7 state
coverage was not runtime-verified in the local venv, and deadlock retry catches
`BaseException`. No external runtime or mutation occurred.

Required remediation: enforce CAS affected-row semantics, integrate the helper
at the actual Airflow state/API boundary, make run-ID reservation atomic, align
state coverage with Airflow 3.1.7, narrow exception handling, add race/retry
tests, then repeat independent QA.

## QA round-1 orchestration correction attempt (2026-11-16)

### Approved task and scope

Correct only the orchestration findings above. No Silver rerun, PostgreSQL,
MinIO or Nessie write, checkpoint/watermark reset, cutover, credential or
`.env` change was permitted.

### Implementation

- `common/airflow_orchestration.py` now requires a CAS writer to return exactly
  one affected row; zero or multiple rows raise `StateCasConflict`. Deadlock
  retry catches `Exception` only.
- `reserve_fresh_run_id()` reserves through one unique-key-protected insert
  callback and rejects a zero-row race; the old read-only checker remains for
  compatibility but is not an atomic reservation primitive.
- State coverage includes Airflow 3.1.7 task states used by this path:
  `restarting`, `up_for_reschedule`, and `deferred`, plus DagRun `canceled`.
- `common/airflow_state_adapter.py` provides the read/CAS/retry/reservation
  adapter. `create_spark_submit_operator()` accepts an explicit injected
  `AirflowStateClient` and wires its failure callback without constructing a
  client during DAG parsing.
- Added race, BaseException, reservation, adapter and task identity contract
  tests in `tests/test_airflow_orchestration.py` and
  `tests/test_airflow_state_adapter.py`.

### Verification

- Focused `.venv` suite: **90 passed**.
- `.venv\Scripts\python.exe -m compileall -q airflow common tests`: **PASS**.
- `git diff --check`: **PASS**.
- No external client was created and no external service was mutated.

### Limitation / blocker

The repository still has no project-owned Airflow REST/API transport or
runtime configuration that can supply an `AirflowStateClient` to the failed
DagRun path. The production operator boundary is now explicit and safe when a
runtime caller injects that client, but the default DAG path does not silently
open metadata DB/API connections. Therefore this is **NOT a QA PASS** for live
Airflow runtime protection; independent QA must either verify the existing
runtime caller injects the adapter or approve a separate transport-wiring task.

### QA reviewer handoff

Review the scoped helper, adapter, operator injection point and 90-test
evidence. Confirm affected-row CAS semantics, Airflow 3.1.7 state coverage,
`Exception`-only retry handling, atomic reservation and the explicit absence of
external writes. Keep the runtime orchestration gate **OPEN/BLOCKED** unless a
real Airflow API client is shown to inject `AirflowStateAdapter` on the failed
DagRun/TaskInstance path.

## Independent QA verdict: orchestration remediation round 2 (2026-11-16)

Verdict: **PASS WITH WARNINGS — code-level remediation; live gate OPEN**.

QA confirmed CAS affected-row semantics, stale/zero/multi-row fail-closed
behavior, bounded `Exception`-only deadlock retry, atomic reservation contract,
state mapping and explicit operator callback injection. Focused tests: **57
passed**; compileall and diff-check passed; no external runtime/write ran.

Blocking live limitation: no project-owned Airflow API/metadata transport or
production caller currently injects `AirflowStateAdapter` into the failed
DagRun/TaskInstance path. Add that integration, a direct operator-boundary
test, a dedicated multi-row CAS test, and runtime-verify state coverage in the
Airflow 3.1.7 container before closing this gate or requesting another Silver
run.

## Orchestration remediation round 3 — transport integration (2026-11-16)

### Approved task and scope

Continue only the orchestration remediation. Add a project-owned Airflow 3.1.7
state/API transport, wire it into the production operator failure boundary
without DAG-parse client construction, fail closed when runtime API config is
missing, and add direct boundary/regression coverage. Keep Silver/Postgres/
MinIO/Nessie external runtime disabled; do not rerun or mutate checkpoints,
watermarks, objects, tables or Iceberg state.

### Implementation

- `common/airflow_state_adapter.py` now contains `AirflowStateApiTransport`, a
  dependency-free Airflow 3.1.x Stable REST API client using the v2 task
  instance GET/PATCH contract and Bearer token runtime configuration.
- `build_airflow_state_client_from_runtime()` requires explicit
  `AIRFLOW_STATE_API_URL` and `AIRFLOW_STATE_API_TOKEN`; it has no DB fallback
  and is called only inside the failure callback.
- `make_runtime_task_failure_callback()` preserves explicit injected clients
  for tests/runtime callers while lazily constructing the configured API
  transport only after a task failure.
- `create_spark_submit_operator()` now attaches this lazy callback to every
  production Spark operator, including the existing Silver and Gold factories.
- Added tests for the actual operator boundary, API URL/auth/PATCH behavior,
  missing runtime config, `affected_rows=2`, and the Airflow 3.1.7 task-state
  enum contract.

### Verification

- `.venv\Scripts\python.exe -m pytest -q tests\test_airflow_orchestration.py tests\test_airflow_state_adapter.py tests\test_airflow_parse_imports.py tests\test_lakehouse_safety_guards.py` — **70 passed**.
- `.venv\Scripts\python.exe -m compileall -q airflow common tests` — **PASS**.
- `git diff --check` — **PASS**.
- No Airflow container was available locally (`apache/airflow:3.1.7` image not
  present), so no container runtime verification was claimed.
- No external Silver/Postgres/MinIO/Nessie client was created; no rerun or
  external mutation occurred.

### Live gate status and exact blocker

**OPEN/BLOCKED — not a live PASS.** The code path is wired and fail-closed, but
the current runtime has not supplied or authenticated
`AIRFLOW_STATE_API_URL`/`AIRFLOW_STATE_API_TOKEN`, and the pinned Airflow 3.1.7
container is not locally available for response-shape verification. Closing
the live gate requires a separately approved, non-mutating API smoke test with
those runtime values (or an available pinned container/source validation).

### QA reviewer handoff

Review only this round-3 diff against the approved scope. Confirm lazy client
construction, no DB fallback, the actual operator callback route, REST path and
payload, fail-closed missing config, multi-row CAS rejection, and state enum
compatibility. Verdict remains **PENDING; live gate OPEN/BLOCKED** until
independent QA and approved runtime verification are complete.

## Independent QA verdict: orchestration remediation round 3 (2026-11-16)

Verdict: **PASS WITH WARNINGS — code-level remediation; live gate OPEN**.

QA confirmed lazy Airflow REST transport construction, no DB fallback,
fail-closed runtime configuration, operator callback wiring, REST endpoint/auth
payload, affected-row helper checks and Airflow 3.1.7 state mappings. Focused
tests: **70 passed**; compileall and diff-check passed; no external runtime or
mutation ran.

Live blockers remain: `AIRFLOW_STATE_API_URL` and
`AIRFLOW_STATE_API_TOKEN` are not configured, the Airflow 3.1.7 container is
not available for response/auth verification, and REST PATCH cannot claim true
database CAS until `409` stale-state conflict behavior is observed or the
contract is explicitly narrowed to idempotent state update.

Next gate: configure the API endpoint/token through the runtime environment or
secret store (never commit or paste the token), then run one approved,
non-mutating API smoke test covering GET/PATCH/auth/409 and lazy construction.
Do not run Silver/Gold runtime until that gate passes.

## Approved Airflow API smoke evidence (2026-11-16)

Using credentials already present inside the running Airflow API container, a
short-lived JWT was acquired in memory and never printed or persisted. The
non-mutating smoke checks returned:

- `AUTH=PASS API=PASS` against `http://127.0.0.1:8080` inside the
  `ecommerce-airflow:3.1.7` API container;
- DAG listing returned `ecommerce_incremental_ingestion` and the historical DAG;
- `GET /api/v2/dags/ecommerce_incremental_ingestion/dagRuns/<failed-run>`:
  `PASS`, state `failed`;
- task-instance read for that run: `PASS`, 50 task instances.

No PATCH was issued, so no task/DagRun state, PostgreSQL row, checkpoint,
watermark or object was mutated. This proves authentication and GET routing
only. REST PATCH/409 stale-state conflict semantics and live callback execution
remain unverified; the live orchestration gate is not yet closed.

## Approved idempotent Airflow PATCH smoke (2026-11-16)

With explicit approval, one TaskInstance already in `failed` state received an
idempotent `PATCH new_state=failed` through the live Airflow 3.1.7 API. The API
returned **HTTP 409**, demonstrating conflict protection rather than applying a
state transition. A subsequent read-only task-instance listing remained
`FAILED_AFTER=3 TOTAL=50`; no retry or state change occurred. No Silver/Gold,
PostgreSQL, MinIO, Nessie, checkpoint or watermark data was touched.

This closes the REST conflict-semantics evidence for the adapter. Actual
failure-callback execution remains covered by operator boundary tests and was
not induced against a production task.

## Independent QA verdict: Airflow API conflict smoke (2026-11-16)

Verdict: **PASS — live orchestration gate CLOSED**.

The approved smoke acquired a short-lived JWT in memory, read the failed
DagRun/task instances, sent an idempotent `PATCH new_state=failed` to a task
already in `failed`, received HTTP `409`, and verified the post-condition
remained `3 failed / 50 total`. No retry, state transition, Silver/Gold data,
PostgreSQL, MinIO, Nessie, checkpoint or watermark mutation occurred.

Independent QA ran **45 tests passed**, with diff-check passing. Production
failure callback execution was not induced; its operator boundary remains
unit-tested. The orchestration remediation gate is closed, but no new Silver
run is authorized by this evidence; obtain separate approval before launching
a fresh run ID.

## Approved fresh Silver run after orchestration PASS (2026-11-16)

User approved one new bounded Silver run after the live orchestration gate
closed. Use a fresh unique run ID only; the failed
`manual__2026-11-16T08:59:53.431664+00:00` and blocked
`silver-lineage-20261116-083266e67ab0` IDs remain forbidden. Enable only the
command-scoped Bronze/Silver writes required for the six-table artifact
production. Keep `.env`, Gold dimensional write, Nessie ref/cutover,
publication, cleanup, checkpoint and watermark settings safe. Stop on any
incomplete evidence and read back all six manifests before proceeding.

## Fresh bounded Silver run execution — BLOCKED (2026-11-16)

### Approved Task and Scope

One command-scoped production Bronze/Silver run for exactly
`customers`, `products`, `orders`, `order_items`, `payments`, and `shipments`
in `ecommerce-lakehouse-prod`. The ignored `.env` was not modified. Explicit
settings enabled only `INGESTION_WRITE_ENABLED=true`; Gold dimensional mode/
write, publication, Nessie cutover/ref mutation, cleanup, checkpoint reset and
watermark reset remained disabled. No retry, reset or cleanup was performed.

### Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` (this execution record)

### Implementation Summary

The single permitted DagRun was created with fresh run ID
`manual__2026-11-16T11:26:02.350754+00:00` and logical date
`2026-11-16T09:30:00+00:00`. `validate_config` passed with the exact six-table
selection. The run failed at the Airflow 3.1.7 task-instance start/API
boundary with `ServerResponseError: Server returned error`; worker processes
were terminated and the DagRun ended `failed`. No `DeadlockDetected` appeared
in this run output.

Read-only task-state evidence showed `bronze_to_silver_orders` and
`bronze_to_silver_order_items` failed; downstream Silver verification/results
were `upstream_failed`. Gold writer and publication were also
`upstream_failed`, so no Gold runtime/write/cutover/publication executed.

### Tests Added or Updated

None. This was a runtime execution/evidence session only.

### Tests Executed and Results

- Airflow bounded execution: **FAIL/BLOCKED**; task-instance start returned
  `Server returned error`, with worker `SIGKILL`/`SERVER_TERMINATED` events.
- DagRun/task-state read-back: **PASS for read-only evidence**; exact run ID
  is `manual__2026-11-16T11:26:02.350754+00:00`, final state `failed`.
- MinIO read-only scan of `ecommerce-lakehouse-prod` for the exact run ID:
  **0 matching objects** across `bronze/`, `silver/` and
  `silver_incremental/`. No Silver manifest, Parquet data prefix, quality or
  reconciliation evidence identity was found for this run.
- Six-table coverage, schema fingerprints, quality/reconciliation identities,
  bucket/key/data-prefix binding, inventory/row counts and run-envelope
  certification: **NOT CERTIFIED** because the Silver barrier did not complete.

### Risks and Limitations

The requested six-table Silver PASS was not achieved. Bronze task success may
have produced batch-keyed Bronze artifacts, but this run has no valid complete
Silver envelope; partial results must not be combined or inferred as PASS.
The failed run ID must not be retried or reused. No checkpoint/watermark reset,
object deletion/overwrite, cleanup, Nessie mutation, Gold write or publication
was performed. Gold DRY_RUN remains unauthorized until a fresh run produces
six-table Silver PASS and all read-back evidence identities.

### QA Reviewer Handoff

Review the exact failed DagRun and partial barrier evidence: run ID
`manual__2026-11-16T11:26:02.350754+00:00`, `validate_config=success`,
`bronze_to_silver_orders=failed`, `bronze_to_silver_order_items=failed`,
downstream Silver/Gold states `upstream_failed`, and MinIO exact-run scan
`count=0`. Confirm the Airflow API start failure and that no retry/reset/
cleanup or Gold DRY_RUN occurred. Runtime certification remains **FAIL —
BLOCKED**; any next runtime attempt requires new explicit approval and a new
fresh run ID after remediation.

## Independent QA verdict: exact runtime execution (2026-11-16)

Verdict: **FAIL / BLOCKED**.

### Review scope and evidence

- Scope was limited to run `manual__2026-11-16T11:26:02.350754+00:00` and this
  plan. No retry, reset, cleanup, runtime change, or Gold launch was performed.
- Direct read-only PostgreSQL query against Airflow metadata confirmed the exact
  DagRun state is `failed` with logical date `2026-11-16 09:30:00+00:00`.
- Direct read-only task-state query returned 59 task instances. Verified:
  `validate_config=success`; `bronze_to_silver_orders=failed`;
  `bronze_to_silver_order_items=failed`; downstream Silver branches were
  `upstream_failed`; `gold_dimensional_write=upstream_failed`; and
  `publish_gold_to_active=upstream_failed`.
- `gold_incremental_snapshot_reuse_dry_run` was `success`, but its read-only
  XCom value was `{"status":"SKIPPED","reason":"GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED=false"}`;
  this is not Gold execution or publication evidence.
- Direct read-only MinIO listing of `ecommerce-lakehouse-prod`, filtered by the
  exact run ID under `bronze/`, `silver/`, and `silver_incremental/`, returned
  `0` matching objects. No exact-run Silver envelope, manifest, Parquet data
  prefix, quality evidence, or reconciliation evidence exists.
- `SIGKILL`/`SERVER_TERMINATED` and `Server returned error` are recorded at
  plan lines 1094-1111; QA did not re-induce them.

### Acceptance criteria result

The six-table Silver barrier did not complete, exact-run artifact count is zero,
and the run cannot provide lineage/schema/quality/reconciliation certification.
Runtime certification is therefore **not achieved** and Gold remains disabled.

### Findings

- **HIGH** — Airflow task-instance start/API failure terminated the bounded run
  before a complete Silver barrier; this run must not be retried or reused.
- **HIGH** — No exact-run Silver artifacts or manifests exist in the approved
  production bucket; downstream Gold certification would be unsupported.

### Required next step

Keep this phase open. Remediate and independently verify the Airflow
task-instance start/API boundary, then obtain separate approval for one new
fresh run ID. Require six-table Silver manifests and read-back identity checks
before any Gold action. Do not retry this run, reset checkpoints/watermarks,
clean objects, or launch Gold.

## HIGH remediation plan — Airflow TaskInstance start/API boundary (2026-11-16)

### Scope and current architecture

The production-shaped Airflow stack pins `apache/airflow:3.1.7` and uses
`LocalExecutor` with a PostgreSQL metadata database. The scheduler, API server,
and DAG processor share the execution API configuration
`AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/`.
Spark stages are created by `airflow/dags/pipeline_operators.py` as
`DockerOperator` tasks. The project-owned failure callback lazily constructs
`common.airflow_state_adapter.AirflowStateApiTransport` and PATCHes the
TaskInstance Stable REST endpoint; it is not constructed during DAG parsing.
Pure transition/CAS rules live in `common/airflow_orchestration.py`.

### Finding and evidence

**HIGH:** fresh run
`manual__2026-11-16T11:26:02.350754+00:00` failed while Airflow 3.1.7 was
starting TaskInstances at the API boundary with `ServerResponseError: Server
returned error`; worker events recorded `SIGKILL`/`SERVER_TERMINATED`. The
DagRun ended `failed`, `validate_config` succeeded, two Bronze-to-Silver tasks
failed, and downstream Silver/Gold tasks were `upstream_failed`. Read-only
MinIO inspection found zero exact-run objects. No retry, reset, cleanup,
checkpoint/watermark mutation, Gold write, or cutover occurred.

The repository supports these contributing hypotheses, but none is proven yet:

1. **Execution API reachability/auth/config mismatch:** the stack depends on an
   Airflow 3 execution API URL and JWT/API secrets, while no deterministic
   response/status/body evidence is retained at the failing `TaskInstance.start`
   call. A 4xx/5xx or wrong execution endpoint can surface as the generic
   `Server returned error`.
2. **Worker/process lifecycle termination:** `SIGKILL`/`SERVER_TERMINATED` may
   be the primary failure (resource/process supervisor/container termination),
   with the API error being the caller's observation. Current tests do not
   exercise a real Airflow 3.1.7 `TaskInstance` start through the pinned image.
3. **State/concurrency boundary:** the project adapter covers read/PATCH and
   pure CAS semantics, but it does not own or instrument Airflow's internal
   `TaskInstance.start` execution path. The REST PATCH is not evidence that the
   execution API's start transaction or scheduler heartbeat is healthy.

### Proposed remediation

Add diagnostics and a non-mutating verification seam before changing retry or
data behavior. First capture the exact execution API request path, HTTP status,
response body/correlation ID, Airflow component logs, process exit/signal, and
metadata state transitions for a disposable `TaskInstance`/DAG test. Validate
the pinned container's effective executor, execution API URL, JWT/API secret
consistency, service DNS/port, and health/auth responses. Then isolate a
minimal no-op task and a single `DockerOperator` boundary test so the failure
can be classified as API transport, worker lifecycle, or state concurrency.

Only after classification, implement the smallest fix in the affected boundary:
configuration/service wiring for an API problem; bounded process/resource
handling for a worker termination; or an Airflow-version-compatible state
transition/instrumentation change for a concurrency problem. Preserve the
existing fail-closed behavior: no automatic clear/retry of the failed run, no
fallback metadata DB writes, and no change to external-write gates.

### Files/components in scope

- `docker-compose.yml`, `airflow/Dockerfile`: effective Airflow 3.1.7
  executor, API-server/execution-API wiring, shared secrets, health/readiness
  and service dependencies.
- `airflow/dags/ecommerce_incremental_ingestion.py`: task graph and manual-run
  boundary; do not alter data dependencies as part of this diagnosis.
- `airflow/dags/pipeline_operators.py`: `DockerOperator` construction,
  timeout/retry settings, and failure callback boundary.
- `common/airflow_state_adapter.py`: API URL/auth/request/response handling;
  add only evidence-preserving behavior if required by the diagnosed failure.
- `common/airflow_orchestration.py`: pure state transition contract; preserve
  fail-closed illegal/stale/CAS behavior.
- `tests/test_airflow_state_adapter.py`, `tests/test_airflow_orchestration.py`,
  `tests/test_airflow_parse_imports.py`, and a proposed focused Airflow runtime
  test module: transport error classification, callback boundary, state
  transitions, and no external-write guarantees.
- `docs/plans/semantic-runtime-certification-plan.md`: this remediation and
  evidence record; `docs/GOLD_LAKEHOUSE_PLAN.md` remains uncertified/open.

### Implementation sequence (approval required before coding)

1. Reproduce only in a non-writing Airflow scope with a no-op task and collect
   API status/body, component logs, process signal, and metadata state reads;
   do not use the failed run ID and do not run the six-table DAG.
2. Add deterministic unit/contract tests for the observed failure class and
   assert no retry/reset/cleanup/external client construction occurs on API or
   `BaseException`/termination paths.
3. Apply the minimal approved configuration/code correction and run static
   checks plus the focused tests. Do not change Silver/Gold retry semantics or
   enable Iceberg cutover.
4. Run a read-only Airflow API smoke and a no-op TaskInstance runtime check in
   the pinned `3.1.7` environment. Record exact endpoint, response status,
   task/DagRun states, and process health.
5. Request separate approval for one new immutable fresh run ID only after
   independent QA passes. Treat the six-table Silver barrier and exact-run
   manifests as prerequisites for any Gold action.

### Acceptance criteria

- Root cause is classified with direct evidence; `Server returned error` is not
  accepted as a sufficient diagnosis.
- A no-op TaskInstance starts and completes through the pinned Airflow 3.1.7
  execution path without `SIGKILL`/`SERVER_TERMINATED`; API status/body and
  component logs are captured.
- Focused tests pass for API success, non-2xx/error-body handling, stale/illegal
  state transitions, bounded retry behavior, and callback wiring. No test
  creates MinIO/Nessie/Spark/PostgreSQL data clients or mutates checkpoints.
- The failed run remains untouched: no clear, retry, state reset, cleanup,
  overwrite, checkpoint/watermark reset, or external write.
- Independent QA verifies the scoped diff and runtime evidence. Only then may
  a separately approved fresh run be considered; runtime certification still
  requires all six Silver manifests and identity/read-back checks.

### Testing and verification strategy

Static/unit: `pytest` focused Airflow orchestration/state-adapter/parse tests,
`compileall`, and `git diff --check`. Container verification: pinned Airflow
3.1.7 version/config inspection, API health/auth/read-only endpoint check, and
no-op `TaskInstance` start. Diagnostics must distinguish HTTP 4xx/5xx, DNS or
connection failure, JWT/auth failure, scheduler/API timeout, and process signal.
No `airflow dags test` of the six-table pipeline is part of remediation.

### Risks and rollback

The main risk is masking an infrastructure/process OOM or supervisor issue by
changing retries, which could duplicate external effects. A second risk is
confusing the project failure callback PATCH with Airflow's execution API start
transaction. Roll back only the approved configuration/code diff; do not clear
the failed DagRun, remove objects, reset checkpoints, or revert shared service
state. If diagnostics remain inconclusive, keep the live gate BLOCKED and stop.

### Coding Agent handoff

Do not implement until the user approves this remediation scope. The Coding
Agent must first produce the non-mutating diagnostic/tests, report exact files
and evidence, then make only the smallest classified fix. It must not retry
`manual__2026-11-16T11:26:02.350754+00:00`, reset/cleanup any state, run the
six-table DAG, enable external writes, or enable Iceberg cutover. Reviewer QA
must inspect the actual diff against this section and keep the runtime gate
open unless the pinned Airflow 3.1.7 no-op TaskInstance evidence is successful.

### Plan closure — current evidence and decision (2026-11-16)

**Root-cause conclusion for planning:** the confirmed failure boundary is
Airflow 3.1.7 `TaskInstance` start/execution-API orchestration, not a Silver,
Gold, MinIO, Nessie, checkpoint, or data-contract failure. Evidence is limited
to the observed `ServerResponseError: Server returned error`,
`SIGKILL`/`SERVER_TERMINATED`, failed DagRun state, failed
`bronze_to_silver_orders`/`bronze_to_silver_order_items`, downstream
`upstream_failed` states, and zero exact-run artifacts. The evidence does not
distinguish execution-API auth/reachability, worker/process termination, or
Airflow state concurrency; these remain implementation hypotheses, not facts.

**Plan status:** **CHỈNH SỬA ĐƯỢC LẬP KẾ HOẠCH / LIVE GATE BLOCKED**. The failed
run remains immutable and must not be retried, cleared, reset, cleaned, or
reused. No external write, checkpoint/watermark reset, or Iceberg cutover is
authorized by this plan.

**Final acceptance criteria for remediation:**

1. A non-mutating diagnostic identifies the failing class with direct evidence:
   execution-API HTTP status/body and auth/reachability, or worker/process
   signal/lifecycle, or state/concurrency transition evidence.
2. A pinned Airflow `3.1.7` no-op `TaskInstance` starts and completes without
   `Server returned error`, `SIGKILL`, or `SERVER_TERMINATED`; component logs
   and final task/DagRun states are recorded.
3. Focused unit/contract tests cover the diagnosed boundary, callback wiring,
   fail-closed state transitions, and no retry/reset/cleanup/external-client
   behavior on failure paths.
4. Independent QA passes the implementation diff and evidence. Only after
   that approval may a separately approved fresh run be created; runtime
   certification additionally requires all six Silver manifests and exact-run
   identity/read-back checks.

## Airflow TaskInstance boundary remediation evidence (2026-11-16)

### Approved scope executed

Implemented only focused, non-mutating contract/regression tests for the
Airflow 3.1.7 TaskInstance start/API boundary. No production configuration,
retry policy, DAG dependency, state adapter behavior, external-write gate,
`.env`, database, object storage, checkpoint, watermark, Nessie ref, or
Iceberg setting was changed.

### Changed files

- `tests/test_airflow_state_adapter.py`
  - preserves HTTP non-2xx status and bounded response body evidence;
  - verifies runtime callback client construction is lazy and fails closed;
  - verifies a `BaseException` termination path is propagated and does not
    become a retry or a second state write.
- `tests/test_airflow_orchestration.py`
  - verifies a termination signal path is not caught as a deadlock and the
    CAS write is invoked only once.
- `docs/plans/semantic-runtime-certification-plan.md`
  - records this implementation and verification evidence.

### Verification evidence

Exact commands executed from the repository root:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_airflow_state_adapter.py tests/test_airflow_orchestration.py tests/test_airflow_parse_imports.py -p no:cacheprovider
.\.venv\Scripts\python.exe -m compileall -q common airflow tests
git diff --check
```

Results:

- Focused pytest: **PASS — 68 passed in 0.13s**.
- Compile check: **PASS**.
- Diff check: **PASS**. Git emitted only existing LF/CRLF normalization
  warnings; no whitespace errors were reported.

### Runtime boundary status

Pinned Airflow `3.1.7` API health/auth and no-op `TaskInstance` verification
were **not executed** in this implementation session. Those checks require
an external Airflow/PostgreSQL service interaction and were intentionally not
started; no `DagRun` or `TaskInstance` was created, no DAG was launched, and
no external service state was changed. Therefore the tests do not prove the
runtime acceptance criterion and do not classify the original
`Server returned error` as API, worker-lifecycle, or concurrency root cause.

The failed run
`manual__2026-11-16T11:26:02.350754+00:00` remains untouched and was not
retried, cleared, reset, cleaned, or reused. Gold WRITE/cutover remains
disabled. The live gate remains **BLOCKED / OPEN** pending separately approved
read-only API evidence and a disposable pinned `3.1.7` no-op verification,
followed by independent QA.

### Next exact step

Obtain explicit approval for external-service checks, then run only a fresh,
disposable no-op verification in the pinned `ecommerce-airflow:3.1.7`
environment. Capture the exact execution API endpoint, HTTP status/body and
correlation ID, component logs, process exit/signal, and final task/DagRun
 states. Do not run the six-table DAG or reuse the failed run ID.

## Independent QA verdict: Airflow TaskInstance boundary remediation (2026-11-16)

### Review scope and actual diff

Reviewed the actual contents of `tests/test_airflow_state_adapter.py`,
`tests/test_airflow_orchestration.py`, and this plan. The two test files use
local fakes/module stubs and do not launch a DAG, construct an external client
during test setup, or write to Airflow, PostgreSQL, MinIO, Nessie, Iceberg,
checkpoints or watermarks. No production source or configuration change is
present in these three remediation files.

### Acceptance criteria results

- Focused contract/regression coverage: **PASS** — callback wiring, API
  contract/error preservation, lazy fail-closed client construction, state
  transitions, CAS affected-row handling, deadlock retry behavior, fresh
  run-ID reservation and `BaseException` termination paths are covered.
- Test evidence: **PASS** — `68 passed in 0.13s`.
- Compile check: **PASS** — `.\\venv\\Scripts\\python.exe -m compileall -q common airflow tests`.
- Whitespace check: **PASS** — `git diff --check` returned no whitespace
  errors; only LF/CRLF normalization warnings were emitted.
- No DAG launch or external write: **PASS** — no live Airflow API, DagRun,
  TaskInstance, database, object-storage or Nessie operation was executed by
  this QA review.
- Live Airflow 3.1.7 gate: **CORRECTLY BLOCKED / OPEN**. The plan explicitly
  states that pinned API health/auth and disposable no-op TaskInstance
  verification were not executed; unit tests therefore do not satisfy runtime
  certification or prove the original failure root cause.

### Warning about repository baseline

The working tree also contains many unrelated tracked production/config changes
and additional untracked files. QA can therefore verify the “no production
behavior/config change” claim only for the three remediation files listed in
this section; it cannot certify the entire working tree as production-neutral
without a clean baseline, commit range, or isolated diff. This is a scope and
provenance warning, not evidence that the reviewed test files mutate production.

### QA verdict and next step

**Verdict: PASS WITH WARNINGS.** The reviewed remediation is non-mutating and
its local evidence is reproducible. Keep the live gate **BLOCKED / OPEN**.

Next step: obtain separate approval for external-service checks, then run only
one fresh disposable no-op TaskInstance verification in the pinned
`ecommerce-airflow:3.1.7` environment. Capture API endpoint/status/body and
correlation ID, component logs, process exit/signal, and final task/DagRun
states. Do not run the six-table DAG, retry/reuse the failed run, enable Gold
WRITE/cutover, or treat this unit-test pass as runtime certification.

## Approved disposable Airflow 3.1.7 no-op TaskInstance diagnostic (2026-11-16)

### Approved Task and Scope

One disposable, read-only/no-op `TaskInstance` diagnostic only. The existing
`ecommerce_incremental_ingestion.confirm_gold_dimensional_dim_date` task was
selected because it is an `EmptyOperator`; Airflow 3.1.7 `airflow tasks test`
documents that it runs one task without dependency checks or recording state.
The failed run `manual__2026-11-16T11:26:02.350754+00:00` was not reused.
No six-table DAG run, retry, clear, checkpoint/watermark reset, MinIO,
PostgreSQL application-data, Nessie, Gold WRITE, cutover, or cleanup operation
was requested or performed.

### Exact invocation and runtime identity

Pinned runtime checks confirmed `airflow version` = `3.1.7` and image
`ecommerce-airflow:3.1.7` with digest
`sha256:df8f2671c6baa100b47a150d50cfe896df6bafd7d4da5eca8e9f7acce12cf75d`.
The single no-op invocation was:

```powershell
docker compose exec -T airflow-scheduler airflow tasks test \
  ecommerce_incremental_ingestion confirm_gold_dimensional_dim_date \
  2026-11-16T12:20:00+00:00
```

Diagnostic start timestamp: `2026-11-16T12:14:16.695Z`.
Airflow created only its disposable temporary execution context with run ID
`__airflow_temporary_run_2026-11-16T12:14:19.319337+00:00__`; the final
read-only metadata query found `dag_run_found=false`, confirming that the
temporary DagRun was removed by the `tasks test` lifecycle. No persistent
DagRun or TaskInstance remained.

### Exact evidence

- API health before and after: `GET http://127.0.0.1:8080/api/v2/monitor/health`
  returned HTTP `200`; the body reported `metadatabase=healthy`,
  `scheduler=healthy`, and `dag_processor=healthy`.
- Protected API auth check: `GET http://127.0.0.1:8080/api/v2/dags`
  returned HTTP `401` with body `{"detail":"Not authenticated"}`. No token,
  password, or secret was printed. The pre-probe wrapper terminated on this
  expected 401 before printing that one response, while the post-probe
  captured the exact status/body.
- Task execution: the scheduler command exited with code `0`. Airflow logs
  recorded `Task started`, `previous_state=queued`, then
  `Task instance state updated ... rows_affected=1` and
  `new_state=success` for TI UUID
  `01a1e447-9730-7342-a13f-04d0679965ba`, followed by `[DAG TEST] end task`.
- Final TaskInstance/DagRun state: read-only metadata query returned
  `dag_run_found=false` and therefore no persistent final state; the task
  execution itself completed successfully before disposable-context cleanup.
- Process/container state: API server, scheduler, and DAG processor were all
  `running`; `exit=0`, `oom=false`, and no container error was reported.
- Component log scan: no `ServerResponseError`, `SERVER_TERMINATED`,
  `SIGKILL`, `SIGTERM`, `Traceback`, or `ERROR` signal was found for the
  diagnostic window. No explicit HTTP correlation/request ID was emitted by
  this Airflow path; the TI UUID above is the available execution identifier.

### Tests Executed and Results

- `docker compose exec -T airflow-scheduler airflow version`: **PASS — 3.1.7**.
- Existing no-op task listing: **PASS — target exists as an EmptyOperator**.
- Single disposable `tasks test`: **PASS — exit code 0; task success**.
- API health/auth probes: **PASS — health 200 and protected endpoint 401**.
- Final metadata/process/log read-back: **PASS — temporary DagRun absent,
  services healthy, no termination/error signal**.

### Gate Verdict and Limitations

The disposable no-op TaskInstance diagnostic is **PASS**. It directly proves
that the pinned Airflow 3.1.7 runtime can execute this no-op through the
execution API boundary without the previously observed generic error or
worker termination. It does not prove the six-table pipeline, external-write
behavior, or Gold runtime certification. The original failure remains
unclassified for non-no-op `DockerOperator`/resource behavior, and no explicit
HTTP correlation ID was available. Keep Gold runtime certification and any
new Silver run **OPEN/BLOCKED** pending independent QA and separate approval.

### Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` only.

### QA Reviewer Handoff

Review the exact invocation, pinned image digest, API status/body, execution
log transitions, temporary DagRun cleanup, container state, and absence of
error/signal markers. Confirm that no failed run was reused and that no
six-table DAG, external data write, retry, clear, reset, cleanup, or cutover
occurred. The no-op diagnostic verdict is **PASS**; overall Gold/Silver
runtime certification remains **OPEN/BLOCKED**.

## Independent QA verdict — disposable Airflow 3.1.7 no-op diagnostic (2026-11-16)

### Review Scope

Reviewed the approved disposable diagnostic record, the actual DAG/operator
definition, and the actual working-tree diff relevant to this diagnostic. The
target is confirmed as `ecommerce_incremental_ingestion.confirm_gold_dimensional_dim_date`,
created by `create_gold_dimensional_confirmation_operator()` as an
`EmptyOperator` in `airflow/dags/pipeline_operators.py:286-292`; the DAG wires
the `dim_date` confirmation at `airflow/dags/ecommerce_incremental_ingestion.py:88-100`.
No DAG was launched by this QA review and no runtime state was mutated.

### Commit or Diff Reviewed

The diagnostic evidence is recorded in the working-tree addition to this plan;
the plan records `docs/plans/semantic-runtime-certification-plan.md` as the
only intended diagnostic file change. The working tree also contains many
unrelated tracked and untracked changes, including Airflow source/config and
data-pipeline files. Therefore this QA approves only the scoped no-op evidence,
not the entire working tree or unrelated changes.

### Acceptance Criteria Results

- **PASS** — pinned image identity: `ecommerce-airflow:3.1.7`, digest
  `sha256:df8f2671c6baa100b47a150d50cfe896df6bafd7d4da5eca8e9f7acce12cf75d`.
- **PASS** — target is an `EmptyOperator`; the recorded `airflow tasks test`
  exited `0` and the task reached `success` for TI UUID
  `01a1e447-9730-7342-a13f-04d0679965ba`.
- **PASS** — health endpoint returned `200`; the protected `/api/v2/dags`
  endpoint returned `401` with `Not authenticated`, and the record exposes no
  token, password, or secret.
- **PASS** — recorded component scan found no `ServerResponseError`,
  `SIGKILL`, `SERVER_TERMINATED`, `SIGTERM`, `Traceback`, or `ERROR` marker;
  process/container evidence records `exit=0` and `oom=false`.
- **PASS** — read-only metadata verification recorded `dag_run_found=false`;
  the disposable temporary context was not retained.
- **PASS** — the prior failed run was explicitly not reused; no six-table DAG,
  retry, clear, reset, cleanup, cutover, or external data write was recorded.
- **WARNING** — no explicit HTTP correlation/request ID was emitted. The TI
  UUID is the available execution identifier.

### Findings by Severity

- **CRITICAL/HIGH/MEDIUM:** None identified for the scoped no-op diagnostic.
- **LOW:** Runtime raw logs, command output, and container inspection artifacts
  are summarized in the plan rather than checked in as immutable evidence. This
  limits independent re-verification after the runtime window.
- **LOW:** The broad dirty working tree prevents a whole-repository
  production-neutrality conclusion; unrelated changes remain outside scope.

### Tests Executed

- Read-only source inspection of the target operator and DAG wiring.
- `git diff --check` — completed with no whitespace errors; only existing
  LF/CRLF normalization warnings were emitted.
- No Airflow, Docker, API, database, object-storage, Nessie, Spark, or DAG
  command was executed during this QA review, by design.

### Test Results

The recorded runtime evidence satisfies the requested no-op gate: Airflow
`3.1.7` version check, single `tasks test`, task success, health `200`,
protected endpoint `401`, no termination/error markers, `dag_run_found=false`,
and no reuse of the failed run. These are verified as plan-recorded runtime
facts; raw external logs were not available as repository artifacts for a
second byte-level replay.

### Regression Risks

This diagnostic does not exercise `DockerOperator`, worker resource behavior,
the six-table Silver pipeline, Gold writes, Iceberg/Nessie publication, or
checkpoint/watermark handling. A passing `EmptyOperator` no-op must not be
interpreted as production pipeline certification.

### Security and Data Safety

The reviewed scope is read-only/disposable. The recorded protected endpoint
returned `401` without secret exposure. No data write, checkpoint mutation,
watermark reset, failed-run retry/reuse, cleanup, cutover, or Iceberg
maintenance was authorized or performed.

### Final QA Verdict

**PASS WITH WARNINGS** for the approved disposable Airflow 3.1.7 no-op
diagnostic. The no-op acceptance gate is closed successfully. Gold/Silver
runtime certification remains **OPEN/BLOCKED**.

### Required Fixes

No fix is required for this scoped no-op diagnostic. Preserve the failed run
as-is and keep Gold WRITE/cutover disabled.

### Exact Next Step

Request separate explicit approval before creating one fresh immutable bounded
run for the six-table pipeline. Before that run, retain the current failed run
untouched and verify the approved run ID/scope; do not retry or reuse the failed
run, launch the DAG speculatively, enable Gold WRITE/cutover, or perform any
cleanup. Runtime certification still requires complete six-table Silver
lineage, exact-run identity, read-back, and data-integrity evidence.

## Approved scheduler/processor restart and verification — STOPPED (2026-11-16)

### Approved Task and Scope

Only `airflow-scheduler` and `airflow-dag-processor` were recreated with
command-scoped safety overrides. `.env` was not edited. No other service was
targeted; the Compose orphan warning for `ecommerce-spark` was observed and
`--remove-orphans` was not used.

### Restart Evidence

- `ecommerce-data-platform-airflow-scheduler-1`: recreated and started;
  `status=running`, `exit=0`, `oom=false`, image `ecommerce-airflow:3.1.7`.
- `ecommerce-data-platform-airflow-dag-processor-1`: recreated and started;
  `status=running`, `exit=0`, `oom=false`, image `ecommerce-airflow:3.1.7`.
- No DagRun or task was triggered. There is no new run ID, task state, MinIO
  inventory, manifest, schema, quality, reconciliation, or prefix evidence.

### Verification Result

Verification was stopped on the first failure as required. The read-only
`docker exec` environment inspection command failed in both target containers
with `/bin/sh: 1: Syntax error: "(" unexpected`. Therefore the requested
inside-container safety flag verification is **NOT CERTIFIED**. This was a
verification-command failure, not evidence that the flags were accepted.

### Tests Executed and Results

- Target-container recreate/start: **PASS** for the two named services only.
- Inside-container safety environment verification: **FAIL** in both target
  containers due to shell syntax error.
- Fresh six-table DagRun: **NOT STARTED**; exact service/run/task: no run and
  no task exist for this attempt.

### Risks and Limitations

The runtime gate remains **BLOCKED / OPEN**. No retry, reset, cleanup, state
change, checkpoint/watermark mutation, Gold launch, or data inventory scan was
performed after verification failed. A corrected read-only verification command
and a separately approved fresh run are required before any DagRun trigger.

## Independent QA verdict — bounded scheduler/processor operation (2026-11-16)

### 1. Review Scope

Reviewed the latest bounded-operation record above, the actual plan diff, and
the repository state relevant to the operation. Scope is limited to the
recreation of `airflow-scheduler` and `airflow-dag-processor` and the attempted
read-only safety-environment verification. No source/configuration change was
reviewed as part of this operation.

### 2. Commit or Diff Reviewed

The actual working-tree diff reviewed is the appended operational evidence in
`docs/plans/semantic-runtime-certification-plan.md`. The working tree contains
many unrelated tracked and untracked changes; this QA does not approve those
changes or infer whole-repository safety from them.

### 3. Acceptance Criteria Results

- **PASS** — only the two named Airflow services were recreated; both are
  recorded as `ecommerce-airflow:3.1.7`, `running`, `exit=0`, `oom=false`.
- **PASS** — no DagRun/task was triggered, and no data scan, manifest read,
  external data mutation, checkpoint/watermark change, cleanup, retry, reset,
  Gold WRITE, or cutover was recorded.
- **FAIL / NOT CERTIFIED** — safety environment verification failed in both
  containers with `/bin/sh: 1: Syntax error: "(" unexpected`. This proves a
  malformed verification command only; it does not prove the safety variables
  were present or correct.
- **PASS** — the operation stopped after the verification failure and did not
  proceed to a fresh six-table run.

### 4. Findings by Severity

- **HIGH — gate-blocking verification failure:** the safety precondition could
  not be verified because the shell command was syntactically invalid. Until a
  corrected read-only check succeeds, bounded runtime certification and any
  DagRun trigger must remain blocked. This is not evidence of data loss or an
  unsafe write having occurred.
- **CRITICAL/MEDIUM/LOW:** none identified for the bounded operation itself.

### 5. Tests Executed

- Read-only review of the recorded service recreation and verification result.
- No new Docker, Airflow, API, database, MinIO, Nessie, Spark, or DAG command
  was executed during this QA review, by design.

### 6. Test Results

The recorded service recreation passed for the two scoped services. The
recorded safety-env check failed due to shell syntax. No independent runtime
replay was performed, so the evidence remains plan-recorded rather than
byte-for-byte re-collected by this reviewer.

### 7. Regression Risks

The operation provides no evidence for six-table DAG behavior, Silver lineage,
manifest/checksum integrity, Iceberg snapshot lineage, reconciliation, or
write idempotency. A healthy scheduler and DAG processor do not close those
runtime gates.

### 8. Security and Data Safety

No unsafe certification is granted. The failed safety check was treated as a
hard stop. No DagRun, data inventory scan, external write, checkpoint or
watermark mutation, cleanup, rollback, maintenance, or cutover was performed
according to the reviewed evidence. Preserve all prior failed runs untouched.

### 9. Final QA Verdict

**PASS WITH WARNINGS** for safe handling of the bounded operation: the scoped
services were recreated and the operation stopped without launching work.
**Runtime certification: NOT CERTIFIED; OPEN/BLOCKED.** The HIGH gate-blocking
verification finding prevents approval of any fresh bounded DAG run.

### 10. Required Fixes

No source-code fix is requested by this QA. Correct the read-only container
environment inspection command and capture direct output for both target
containers. Do not treat `exit=0`/`oom=false` as a substitute for verifying
the safety variables.

### Exact Next Step

After any required approval for external-service inspection, run only a
corrected read-only POSIX-compatible safety-env verification inside the two
already-running target containers; do not restart services, trigger a DAG, or
scan/mutate data. Confirm the exact bounded variables and immutable run-ID
relationship. If and only if that check passes, request a separate explicit
approval for one fresh immutable six-table run; never reuse or retry the failed
run, and keep Gold WRITE/cutover disabled until complete lineage, exact-run
identity, read-back, and integrity evidence passes independent QA.

## Corrected POSIX safety-env check â€” STOPPED (2026-11-16)

### Approved Task and Scope

Read-only inspection of non-secret safety flags in the two already-running
Airflow containers only: `ecommerce-data-platform-airflow-scheduler-1` and
`ecommerce-data-platform-airflow-dag-processor-1`. No restart, DAG trigger,
data-store inspection, configuration change, retry, reset, cleanup, checkpoint
or watermark operation was performed.

### Sanitized Evidence and Result

- The literal service labels `airflow-scheduler` and
  `airflow-dag-processor` were not container names; no container was changed.
- The runtime container names were used for the read-only `docker exec` check.
- The check stopped on the first target because `/bin/sh` returned:
  `Syntax error: end of file unexpected (expecting "fi")`.
- Exit code: `2`.
- No environment flag value was emitted, and no secret was read or exposed.
- Therefore the required flag state is **UNVERIFIED**; this is a command
  syntax failure, not evidence of a flag mismatch.

### Gate Verdict

**FAIL / NOT CERTIFIED.** Per the stop-on-failure rule, no retry or follow-on
runtime/DAG/data operation was attempted. Ingestion write authorization and
all Gold/cutover/publish/checkpoint/watermark/cleanup disabled-state flags
remain unverified.

### Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` (this evidence only)

### QA Reviewer Handoff

Review that the operation stopped after the POSIX command parse failure, that
no flag values or secrets were exposed, and that no DAG or data-store action
followed. A future corrected check requires separate approval and must remain
read-only and limited to the same two running containers.

## Independent QA verdict — latest read-only safety-env check (2026-11-16)

### 1. Review Scope

Reviewed the latest recorded read-only safety-environment check for the two
Airflow runtime containers only. No Docker, Airflow, DAG, database, MinIO,
Nessie, Spark, or data operation was executed during this QA review.

### 2. Commit or Diff Reviewed

Reviewed the actual working-tree addition in
`docs/plans/semantic-runtime-certification-plan.md`. The repository has many
unrelated tracked and untracked changes; this verdict is limited to the
recorded safety-env operation and does not approve the broader working tree.

### 3. Acceptance Criteria Results

- **PASS** — the check targeted only `airflow-scheduler` and
  `airflow-dag-processor` runtime containers.
- **FAIL / UNVERIFIED** — both `docker exec` checks returned exit code `2`
  because `/bin/sh` reported a syntax error expecting `fi`.
- **PASS** — no flag values or secrets were emitted or exposed.
- **PASS** — the operation stopped safely; no DAG/task, data-store read/write,
  retry, reset, cleanup, checkpoint, watermark, Gold WRITE, or cutover action
  followed the failed verification.
- **FAIL** — the safety flags themselves were not verified, so no runtime
  safety certification can be granted.

### 4. Findings by Severity

- **HIGH — gate-blocking verification failure:** the malformed shell command
  leaves the required safety flags unverified. This is not evidence of data
  loss, corruption, or an unsafe write; it is evidence that the precondition
  check did not complete.
- **CRITICAL/MEDIUM/LOW:** none identified from the recorded operation.

### 5. Tests Executed

- Read-only review of the recorded command result and plan diff.
- No new Docker/DAG/runtime command was executed, as explicitly required.

### 6. Test Results

The recorded safety-env check failed in both target containers with shell
syntax error and exit code `2`. No flag values were exposed. Safe stop behavior
is confirmed by the record; direct runtime flag state remains unverified.

### 7. Regression Risks

The failed check provides no evidence for ingestion write authorization,
Gold/cutover/publish guards, checkpoint/watermark protections, or six-table DAG
behavior. Runtime certification therefore remains open.

### 8. Security and Data Safety

No secret was printed, no data operation was initiated after the failure, and
no mutation or destructive action is recorded. Preserve the existing failed
run and runtime state; do not retry or reuse it.

### 9. Final QA Verdict

**PASS WITH WARNINGS** for safe handling of the failed read-only check.
**Runtime certification: NOT CERTIFIED; BLOCKED / OPEN.** The stop-on-failure
behavior was correct, but the safety precondition remains unverified.

### 10. Required Fixes

No source-code or configuration fix is requested by this QA. Before any
runtime/DAG action, obtain separate approval for a safer read-only inspection
using host-side Docker metadata only: inspect the exact container names and
allowlist only non-secret safety-env keys from Docker metadata, without
`docker exec`, shell parsing, service restart, DAG trigger, or data access.
Redact all values except explicitly approved non-secret flags. If metadata
inspection cannot prove the required values and immutable run-ID relationship,
keep the gate blocked and stop.

### Exact Next Step

After separate approval, run one host-side, read-only `docker inspect` metadata
check against the two exact runtime container names, capture image/state and
only the allowlisted non-secret safety flags, then stop. Do not execute Docker
commands in the container, restart services, trigger a DAG, inspect or mutate
data, retry/reuse the failed run, enable Gold WRITE/cutover, or perform
cleanup. Runtime certification may advance only after the metadata check
verifies the complete safety contract and receives a separate QA review.

## Approved host-side Docker metadata inspection — NOT CERTIFIED (2026-11-16)

### 1. Approved Task and Scope

Perform one host-side, read-only `docker inspect` metadata/env inspection for
only the existing runtime containers matching `airflow-scheduler` and
`airflow-dag-processor`. Print only image/status/oom and the explicitly
allowlisted non-secret safety keys. Do not use `docker exec`, restart services,
trigger a DAG, read or write MinIO/Nessie/PostgreSQL/Spark stores, retry/reset/
cleanup, alter `.env`, or expose credentials, tokens, or URLs.

### 2. Sanitized Evidence

Both inspected containers returned identical metadata:

| Container | Image | Status | OOM killed |
|---|---|---|---|
| `ecommerce-data-platform-airflow-scheduler-1` | `ecommerce-airflow:3.1.7` | `running` | `false` |
| `ecommerce-data-platform-airflow-dag-processor-1` | `ecommerce-airflow:3.1.7` | `running` | `false` |

| Allowlisted safety key | scheduler | dag-processor |
|---|---:|---:|
| `INGESTION_WRITE_ENABLED` | `true` | `true` |
| `WAREHOUSE_WRITE_ENABLED` | `<absent>` | `<absent>` |
| `LAKEHOUSE_WRITE_ENABLED` | `<absent>` | `<absent>` |
| `GOLD_DIMENSIONAL_ENABLED` | `false` | `false` |
| `GOLD_DIMENSIONAL_MODE` | `DRY_RUN` | `DRY_RUN` |
| `GOLD_DIMENSIONAL_WRITE_ENABLED` | `false` | `false` |
| `BI_ACTIVE_PUBLISH_APPLY` | `0` | `0` |
| `HISTORICAL_BACKFILL_WRITE_APPROVED` | `0` | `0` |
| `HISTORICAL_BACKFILL_MODE` | `DRY_RUN` | `DRY_RUN` |

No environment keys matching `*_ICEBERG_CUTOVER_ENABLED` were present in
either inspected container. No secret, credential, token, or URL was printed.

### 3. Verdict

**PASS** for the bounded host-side metadata inspection and container
comparison. The two containers are aligned for all keys that were present.

**NOT CERTIFIED / BLOCKED** for runtime no-write safety or Gold runtime
certification. `INGESTION_WRITE_ENABLED=true` is an explicit write-enabled
state; missing warehouse/lakehouse keys and absent cutover keys do not prove a
disabled default. Gold dimensional execution remains explicitly disabled and
in `DRY_RUN`, but that is insufficient to certify the broader runtime safety
contract.

### 4. Tests Executed and Results

- Host-side `docker ps` name resolution: **PASS**, exactly one matching
  container selected for each requested component.
- Host-side read-only `docker inspect` metadata/env extraction: **PASS**.
- Sanitized cross-container comparison: **PASS**, no differences in the
  inspected allowlisted values or image/runtime status.
- No `docker exec`, DAG, external-service, data-store, checkpoint, watermark,
  cleanup, retry, reset, write, or cutover operation was executed.

### 5. Risks and Limitations

- Docker metadata cannot prove the effective value of absent environment keys;
  application defaults remain unknown.
- The explicit ingestion write enablement and absent warehouse/lakehouse flags
  prevent a no-write runtime certification.
- This inspection does not certify DAG behavior, lineage, manifests/checksums,
  Iceberg snapshots, reconciliation, idempotency, or production safety.

### 6. QA Reviewer Handoff

Review the sanitized evidence and confirm that the operation stayed limited to
the two requested containers and did not expose secrets or touch data stores.
Keep runtime certification **OPEN/BLOCKED**. Any change to write flags or any
runtime/DAG/data operation requires separate explicit approval; do not infer
disabled behavior from absent environment keys.

## Independent QA verdict — host-side Docker inspect evidence (2026-11-16)

### 1. Review Scope

Reviewed the latest sanitized, host-side read-only Docker metadata evidence for
exactly `airflow-scheduler` and `airflow-dag-processor`. No Docker, Airflow,
DAG, database, MinIO, Nessie, Spark, or data operation was executed during
this QA review.

### 2. Commit or Diff Reviewed

Reviewed the actual working-tree diff for this plan, including the latest
section `Approved host-side Docker metadata inspection — NOT CERTIFIED`.
The working tree contains unrelated changes; this verdict is limited to the
recorded evidence and does not approve the broader tree.

### 3. Acceptance Criteria Results

- **PASS** — only the two requested Airflow containers were inspected.
- **PASS** — both report image `ecommerce-airflow:3.1.7`, status `running`,
  and `oom=false`.
- **PASS** — `INGESTION_WRITE_ENABLED=true` is recorded for both containers;
  Gold is explicitly `false`/`DRY_RUN`; historical approval is `0`/`DRY_RUN`.
- **PASS** — `WAREHOUSE_WRITE_ENABLED`, `LAKEHOUSE_WRITE_ENABLED`, and all
  `*_ICEBERG_CUTOVER_ENABLED` keys are recorded as absent.
- **PASS** — evidence is sanitized: no secret, credential, token, or URL is
  printed.
- **FAIL / UNVERIFIED** — absent environment keys cannot establish their
  effective disabled defaults; broader no-write runtime safety is not proven.

### 4. Findings by Severity

- **HIGH — runtime gate remains unverified:** explicit ingestion write enablement
  plus absent warehouse/lakehouse/cutover keys prevents certification of the
  complete no-write contract. This is a gate-blocking verification gap, not
  evidence of observed data loss or corruption.
- **CRITICAL/MEDIUM/LOW:** none identified from this evidence review.

### 5. Tests Executed

- Read-only review of the recorded sanitized evidence and plan diff.
- No Docker/DAG/runtime command was executed.

### 6. Test Results

The bounded metadata inspection and cross-container comparison are accepted as
recorded. Sanitization and scope constraints are satisfied. Runtime safety and
Gold certification remain unverified.

### 7. Regression Risks

This evidence does not certify DAG behavior, lineage, manifests/checksums,
Iceberg snapshots, reconciliation, idempotency, or production safety. Future
changes to environment composition may alter application defaults without
appearing as explicit keys.

### 8. Security and Data Safety

No secret was exposed and no runtime/data mutation is recorded. Preserve the
current runtime and failed/blocked run state; do not trigger, retry, reset,
clean up, or enable writes/cutover from this evidence.

### 9. Final QA Verdict

**PASS WITH WARNINGS** for the bounded, sanitized host-side inspection only.
**Runtime certification: NOT CERTIFIED / BLOCKED.** The recorded evidence is
insufficient to treat absent keys as disabled settings.

### 10. Required Fixes

No source-code or configuration fix is requested by this QA. Do not advance the
runtime gate based on absent environment keys. Any stronger safety verification
or runtime/DAG/data operation requires separate explicit approval and must use
an allowlisted, read-only method that proves the effective configuration.

### Exact Next Step

Keep the runtime gate **OPEN/BLOCKED**. Obtain separate approval before any
further runtime action; until effective warehouse/lakehouse write and cutover
defaults are independently proven, do not run a DAG, enable Gold WRITE,
perform Iceberg cutover, retry/reuse a run, or mutate external state.

## Host-side effective-default validation — NOT RUNTIME CERTIFIED (2026-11-16)

### 1. Approved Task and Scope

Validate the resolved Compose configuration for `airflow-scheduler` and
`airflow-dag-processor`, inspect only the allowlisted non-secret safety keys,
review source defaults where a key is absent, and record sanitized evidence.
No container restart, `docker exec`, DAG trigger, data-store access, `.env`
change, retry/reset/cleanup, write, or cutover was permitted.

### 2. Sanitized Effective-Default Evidence

`docker compose config --format json` resolved both requested services from
the repository Compose configuration. Both services resolve to the same
commands and allowlisted values:

| Service | Resolved command | INGESTION_WRITE_ENABLED | GOLD_DIMENSIONAL_ENABLED | GOLD_DIMENSIONAL_MODE | GOLD_DIMENSIONAL_WRITE_ENABLED | BI_ACTIVE_PUBLISH_APPLY | HISTORICAL_BACKFILL_WRITE_APPROVED | HISTORICAL_BACKFILL_MODE |
|---|---|---:|---:|---|---:|---:|---:|---|
| `airflow-scheduler` | `scheduler` | `false` | `false` | `DRY_RUN` | `false` | `0` | `0` | `DRY_RUN` |
| `airflow-dag-processor` | `dag-processor` | `false` | `false` | `DRY_RUN` | `false` | `0` | `0` | `DRY_RUN` |

`WAREHOUSE_WRITE_ENABLED`, `LAKEHOUSE_WRITE_ENABLED`, and all matching
`*_ICEBERG_CUTOVER_ENABLED` keys are absent from the resolved service
environment. The source default in `lakehouse/migration/run.py` is
`LAKEHOUSE_WRITE_ENABLED=false` when unset. No repository Compose or source
default for `WAREHOUSE_WRITE_ENABLED` or an Iceberg cutover key was found;
their effective values remain **unverified**, not inferred as disabled.

Source Compose defaults also explicitly define ingestion as `false`, Gold as
disabled/`DRY_RUN`, active BI publication as `0`, and historical backfill as
approval `0`/`DRY_RUN` (`docker-compose.yml`, shared Airflow environment).
No credentials, tokens, URLs, or full environment output were recorded.

### 3. Docker Inspect Result

Host-side `docker inspect` was attempted for the matching Airflow containers,
but Docker Engine access failed with permission denied on the local Docker
named pipe. No retry or privilege escalation was performed. Therefore live
container command/entrypoint/labels/status were not used as evidence; the
resolved Compose result above is the only effective-default evidence in this
validation.

### 4. Verdict

**PASS** for sanitized, host-side Compose effective-default validation of the
values shown above. **NOT RUNTIME CERTIFIED / OPEN** because live container
metadata could not be inspected and the absent warehouse/cutover keys are not
proven disabled. This validation does not certify DAG execution, manifests,
checksums, watermarks, checkpoints, Iceberg snapshots, reconciliation,
idempotency, or external-service safety.

### 5. Tests Executed and Results

- `docker compose config --format json`: **PASS**; both requested services
  resolved and only allowlisted values were extracted.
- Source-default search with `rg`: **PASS**; defaults and absent keys were
  recorded without printing secret-bearing configuration.
- `docker inspect`: **UNAVAILABLE**; Docker Engine named-pipe permission
  denied before container metadata could be read.
- No external-service, data-store, Airflow, DAG, write, cleanup, reset, or
  cutover operation was executed.

### 6. Risks and Limitations

- Compose resolution is host configuration evidence, not proof of the live
  container environment or process state.
- Missing `WAREHOUSE_WRITE_ENABLED` and cutover keys remain an explicit
  verification gap.
- The validation intentionally does not certify runtime lineage, recovery,
  manifest/checksum integrity, snapshot lineage, or reconciliation guards.

### 7. QA Reviewer Handoff

Review this section against the actual working-tree diff. Confirm that output
was limited to the allowlisted safety settings, that Docker access failure was
handled without escalation or retry, and that the verdict remains **OPEN / NOT
RUNTIME CERTIFIED**. Do not trigger or mutate any runtime state from this
evidence.

## Independent QA closure — latest host-side static validation (2026-11-16)

### 1. Review Scope

Reviewed the latest section, `Host-side effective-default validation — NOT
RUNTIME CERTIFIED`, and its actual working-tree diff. Scope is limited to
sanitized host-side Compose resolution for `airflow-scheduler` and
`airflow-dag-processor`; unrelated dirty-worktree changes are out of scope.

### 2. Commit or Diff Reviewed

Reviewed `docs/plans/semantic-runtime-certification-plan.md` through the latest
host-side validation section. No source code, Compose file, `.env`, DAG,
checkpoint, watermark, object, database row, Nessie ref, or Iceberg table was
modified by this QA review.

### 3. Acceptance Criteria Results

- **PASS** — `docker compose config --format json` resolved both requested
  services and the sanitized defaults show ingestion `false`, Gold disabled/
  `DRY_RUN`, active publication `0`, and historical backfill `0`/`DRY_RUN`.
- **PASS** — source default inspection records `LAKEHOUSE_WRITE_ENABLED=false`
  when unset.
- **UNVERIFIED** — `WAREHOUSE_WRITE_ENABLED` and all matching
  `*_ICEBERG_CUTOVER_ENABLED` keys are absent; no effective disabled default was
  established for them.
- **UNAVAILABLE** — `docker inspect` could not read live metadata because of
  local Docker named-pipe permission; no retry or escalation was performed.
- **PASS** — the evidence is sanitized and the scope stayed host-side/static.

### 4. Findings by Severity

- **HIGH — runtime certification gate remains open:** absent warehouse and
  cutover settings are not proof of disabled behavior, and live container
  metadata is unavailable. This is a verification gap, not observed data loss
  or corruption.
- **CRITICAL/MEDIUM/LOW:** none identified in the reviewed evidence.

### 5. Tests Executed

- Read-only inspection of the recorded Compose/source-default evidence and
  actual plan diff.
- No Docker, Airflow, DAG, Spark, MinIO, Nessie, PostgreSQL, or data operation
  was executed by this QA review.

### 6. Test Results

The recorded Compose validation is accepted as **PASS** for the stated
allowlisted static scope. `docker inspect` is recorded as **UNAVAILABLE**.
There is no runtime test result and no runtime certification evidence.

### 7. Regression Risks

Compose resolution does not prove the live process environment or runtime
behavior. Missing `WAREHOUSE_WRITE_ENABLED` and cutover defaults remain a
configuration verification gap; no DAG, lineage, manifest/checksum, snapshot,
reconciliation, idempotency, or production-safety claim follows from this
validation.

### 8. Security and Data Safety

No secrets were exposed. No DAG was triggered, no runtime was started or
retried, and no database, MinIO, Nessie, Spark, Iceberg, checkpoint, watermark,
or other data state was mutated.

### 9. Final QA Verdict

**PASS WITH WARNINGS** for the bounded host-side static validation only.
**Runtime certification: NOT CERTIFIED / OPEN.** The plan makes no runtime
certification claim from this evidence.

### 10. Required Fixes

No source-code or configuration fix is requested by this QA review. Keep the
runtime gate open; do not infer disabled warehouse/cutover behavior from absent
keys and do not run a DAG or data operation from this static evidence.

### Exact Next Step

Obtain separate approval for an allowlisted read-only method that proves the
effective `WAREHOUSE_WRITE_ENABLED` and Iceberg cutover defaults (and, if
needed, live container metadata). Until that evidence exists, preserve the
**NOT CERTIFIED / OPEN** state and perform no DAG execution, retry, write,
cutover, cleanup, reset, or external data mutation.

## Read-only source/config flag inventory — NOT CERTIFIED (2026-11-16)

### 1. Approved Task and Scope

Inspect repository source, Compose files, env templates, and documentation for
`WAREHOUSE_WRITE_ENABLED` and every key matching
`*_ICEBERG_CUTOVER_ENABLED`. Classify each result as an explicit false default,
required/missing, or runtime override. This inspection was read-only: no source
or `.env` file was changed, no Docker/Airflow/DAG command was run, and no
MinIO/Nessie/PostgreSQL/Spark data store was accessed.

### 2. Sanitized File:Line Evidence

| Area | Evidence | Classification |
|---|---|---|
| Compose shared Airflow environment | `docker-compose.yml:13-18` explicitly defaults `INGESTION_WRITE_ENABLED`, Gold enable/write flags to false, `GOLD_DIMENSIONAL_MODE` to `DRY_RUN`, and `BI_ACTIVE_PUBLISH_APPLY` to `0`; neither target family is declared | Explicit false defaults exist for adjacent guards; target keys missing |
| Default env template | `.env.example:22-39` states data-changing stages are disabled by default and sets ingestion/Gold guards to false, but contains no `WAREHOUSE_WRITE_ENABLED` or cutover key | Explicit false defaults for adjacent guards; target keys missing |
| Quality-it env template | `.env.quality-it.local.example:12-15` overrides `INGESTION_WRITE_ENABLED=true` and `GOLD_DIMENSIONAL_ENABLED=true`, while `GOLD_DIMENSIONAL_WRITE_ENABLED=false`; no target key is present | Runtime override exists only for adjacent local quality-it flags; no target override |
| Lakehouse writer guard | `lakehouse/migration/run.py:23-24` uses source default `LAKEHOUSE_WRITE_ENABLED=false` and requires an explicit `true` to write | Explicit false default; not a `WAREHOUSE_WRITE_ENABLED` consumer |
| Architecture documentation | `docs/ARCHITECTURE.md:32-35` documents false defaults for ingestion/Gold and a separate Gold write guard; no target key is documented | Explicit false defaults for adjacent guards; target keys missing |
| Repository-wide target search | Read-only `rg` search across source, Compose, env templates, and docs found zero matches for `WAREHOUSE_WRITE_ENABLED` and zero matches for `_ICEBERG_CUTOVER_ENABLED` outside this plan | Required/missing or unverified; no consumer/default/override found |

The search did not print `.env` contents or secret-bearing values. Existing
working-tree modifications in `.env.example`, `.env.quality-it.local.example`,
and `docker-compose.yml` were preserved and not edited by this inspection.

### 3. Verdict

**Independent QA verdict: PASS WITH WARNINGS** for bounded, sanitized
read-only inventory and evidence capture.
**NOT CERTIFIED / OPEN** for the target safety contract: the repository has no
declared default or consumer for `WAREHOUSE_WRITE_ENABLED`, and no declared
default or consumer for any `*_ICEBERG_CUTOVER_ENABLED` key. Missing keys must
remain classified as **unverified**, not treated as explicit `false`; an
effective runtime override cannot be established from source/config search.

### 4. Tests Executed and Results

- Targeted `rg` searches over source, Compose, env templates, and docs: **PASS**;
  zero target matches outside this plan.
- Read-only line inspection of the cited files: **PASS**; adjacent explicit
  defaults and the quality-it runtime override were recorded.
- No Docker, Airflow, DAG, Spark, MinIO, Nessie, PostgreSQL, write, cleanup,
  reset, checkpoint, watermark, or cutover operation was executed.

### 5. Risks and Limitations

- An absent environment key may still have an application or image-level
  default that is not represented in this repository; this inspection cannot
  certify its effective value.
- The absence of a cutover key does not prove cutover is disabled.
- This evidence does not certify manifests/checksums, Iceberg snapshots,
  reconciliation, idempotency, or runtime process state.

### 6. QA Reviewer Handoff

Review the cited file:line evidence and confirm the distinction between
explicit false defaults, missing/required target keys, and the adjacent
quality-it runtime override. Keep the runtime certification gate **OPEN / NOT
CERTIFIED** until a separately approved, sanitized method proves effective
warehouse write and Iceberg cutover behavior. Do not edit `.env`, run Docker or
DAG commands, access data stores, enable writes, or enable cutover from this
inventory alone.

## Approved narrow safety-default remediation — 2026-11-16

### 1. Approved Task and Scope

Add explicit deny-by-default configuration for `WAREHOUSE_WRITE_ENABLED` and
the six Silver cutover flags:
`CUSTOMERS_ICEBERG_CUTOVER_ENABLED`, `PRODUCTS_ICEBERG_CUTOVER_ENABLED`,
`ORDERS_ICEBERG_CUTOVER_ENABLED`, `ORDER_ITEMS_ICEBERG_CUTOVER_ENABLED`,
`PAYMENTS_ICEBERG_CUTOVER_ENABLED`, and `SHIPMENTS_ICEBERG_CUTOVER_ENABLED`.
The approved files are limited to `docker-compose.yml`, `.env.example`,
`.env.quality-it.local.example`, `tests/test_quality_it_local_compose.py`, and
this plan. `.env` and backups are explicitly excluded. No consumers, service
restarts, DAG triggers, data-store access, or Iceberg cutover are in scope.

### 2. Acceptance Criteria

- Compose shared Airflow environment declares all seven keys with the exact
  `${VAR:-false}` default form.
- Both env templates declare all seven keys with literal `false` values.
- Focused regression tests assert the Compose defaults and template values.
- Existing unrelated dirty-worktree changes remain untouched.
- No commit, external-service mutation, service restart, DAG trigger, or data
  operation is performed.

### 3. Files Changed

- `docker-compose.yml`
- `.env.example`
- `.env.quality-it.local.example`
- `tests/test_quality_it_local_compose.py`
- `docs/plans/semantic-runtime-certification-plan.md`

### 4. Implementation Summary

Added the seven explicit safety settings to the shared Airflow environment and
to both templates. The quality-it template retains its existing local ingestion
and Gold enablement values; warehouse writes and all six Silver Iceberg cutovers
remain literal `false`. No `.env` or backup file was edited.

### 5. Tests Executed and Results

- `python -m pytest tests/test_quality_it_local_compose.py`: **UNAVAILABLE**;
  the environment has no `pytest` module (`No module named pytest`). No
  dependency installation or external-service test was attempted.
- `docker compose config --quiet`: **PASS** with Docker CLI warnings that the
  local Docker config was inaccessible; Compose validation exited `0` and did
  not start services.
- Read-only `docker compose config --format json` inspection: **PASS**; all
  seven resolved Airflow scheduler values were `false`:
  `WAREHOUSE_WRITE_ENABLED`, `CUSTOMERS_ICEBERG_CUTOVER_ENABLED`,
  `PRODUCTS_ICEBERG_CUTOVER_ENABLED`, `ORDERS_ICEBERG_CUTOVER_ENABLED`,
  `ORDER_ITEMS_ICEBERG_CUTOVER_ENABLED`, `PAYMENTS_ICEBERG_CUTOVER_ENABLED`,
  and `SHIPMENTS_ICEBERG_CUTOVER_ENABLED`.
- `python -m compileall -q tests/test_quality_it_local_compose.py`: **PASS**.
- `git diff --check`: **PASS**.
- Git scope check: exactly the five approved files are changed among the
  allowlist; `.env` and backup patterns have no status entries. No service,
  DAG, runtime, data-store, checkpoint, watermark, manifest, checksum,
  snapshot, or cutover operation was run.

### 6. Risks and Limitations

These settings establish explicit configuration defaults only; they do not add
or change consumers and therefore do not by themselves certify runtime behavior
or prevent an application from interpreting another independent setting. No
Compose service was started or restarted, and no database, MinIO, Nessie, Spark,
Iceberg, checkpoint, watermark, manifest, or checksum state was inspected or
mutated. Runtime certification remains **NOT CERTIFIED / OPEN**.

### 7. QA Reviewer Handoff

Review the five-file diff against this approved scope. Confirm all seven Compose
expressions use `${VAR:-false}`, both templates use literal `false`, `.env` and
backups are unchanged, and the focused tests cover every key. Treat Compose
rendering as static evidence only; do not trigger services, DAGs, consumers, or
cutover. No commit is requested or authorized.

## Independent QA verdict — approved configuration-only change (2026-11-16)

### Review Scope

Independent QA reviewed only the approved configuration/test diff in
`docker-compose.yml`, `.env.example`, `.env.quality-it.local.example`,
`tests/test_quality_it_local_compose.py`, and this plan. Existing unrelated
working-tree changes were not attributed to this change.

### Acceptance Criteria Results

- **PASS:** all seven safety keys use the exact quoted Compose form
  `${VAR:-false}` at `docker-compose.yml:14-20`.
- **PASS:** both env templates define all seven keys as literal `false` at
  `.env.example:25-31` and `.env.quality-it.local.example:13-19`.
- **PASS:** focused regression assertions cover every key and both templates.
- **PASS:** read-only Compose rendering resolved all seven keys to `false` for
  `airflow-scheduler`, `airflow-dag-processor`, and `airflow-api-server`.
- **PASS:** no `.env` or backup path appears as changed; no runtime/data
  operation was performed by this QA review.

### Tests and Evidence

- `docker compose config --format json`: **PASS**; only Docker config access
  warnings were emitted, and no service was started.
- `python -m compileall -q tests/test_quality_it_local_compose.py`: **PASS**.
- `git diff --check`: **PASS**.
- `python -m pytest tests/test_quality_it_local_compose.py`: **UNAVAILABLE**;
  `No module named pytest`. No dependency installation was attempted.

### Final QA Verdict

**PASS WITH WARNINGS** for the bounded configuration-only change. The warning
is limited to the unavailable pytest module; static assertions, Compose
rendering, compileall, diff check, and scope checks passed. This verdict does
not certify runtime DAG behavior, data lineage, or external-service safety.

### Required Fixes / Next Step

No source or configuration fix is required from this QA review. Keep the
pytest limitation disclosed and install/use the approved test environment
before claiming focused pytest execution. Do not restart services, trigger a
DAG, enable cutover, or mutate runtime/data state from this evidence.

## Approved Airflow safety-config recreation and runtime verification — 2026-11-16

### 1. Approved Task and Scope

Recreate only `airflow-scheduler` and `airflow-dag-processor` so the approved
Compose deny-by-default settings take effect. `.env` was not edited. No other
service was restarted, no DAG was triggered, and no retry, reset, cleanup,
checkpoint, watermark, MinIO, PostgreSQL, Nessie, Spark, manifest, checksum,
or Iceberg operation was performed.

### 2. Operation Evidence

- Command executed:
  `docker compose up -d --force-recreate --no-deps airflow-scheduler airflow-dag-processor`
- Both requested services were recreated and started successfully.
- Compose reported the existing `ecommerce-spark` orphan; `--remove-orphans`
  was not used and the orphan was not modified.
- The first sandboxed Docker attempt was blocked by Docker named-pipe
  permission before any operation. The approved command was then run with
  Docker host access; no additional service was targeted.

### 3. Sanitized Read-only Runtime Verification

Host-side `docker inspect` was run only against the two recreated container
names. Output was restricted to image, status, OOM state, and the explicitly
allowlisted keys below; no complete environment, credentials, tokens, URLs, or
other secret-bearing values were printed.

| Container | Image | Status | OOMKilled |
|---|---|---|---|
| `ecommerce-data-platform-airflow-scheduler-1` | `ecommerce-airflow:3.1.7` | `running` | `false` |
| `ecommerce-data-platform-airflow-dag-processor-1` | `ecommerce-airflow:3.1.7` | `running` | `false` |

| Allowlisted flag | scheduler | dag-processor |
|---|---:|---:|
| `WAREHOUSE_WRITE_ENABLED` | `false` | `false` |
| `CUSTOMERS_ICEBERG_CUTOVER_ENABLED` | `false` | `false` |
| `PRODUCTS_ICEBERG_CUTOVER_ENABLED` | `false` | `false` |
| `ORDERS_ICEBERG_CUTOVER_ENABLED` | `false` | `false` |
| `ORDER_ITEMS_ICEBERG_CUTOVER_ENABLED` | `false` | `false` |
| `PAYMENTS_ICEBERG_CUTOVER_ENABLED` | `false` | `false` |
| `SHIPMENTS_ICEBERG_CUTOVER_ENABLED` | `false` | `false` |
| `INGESTION_WRITE_ENABLED` | `false` | `false` |
| `GOLD_DIMENSIONAL_ENABLED` | `false` | `false` |
| `GOLD_DIMENSIONAL_MODE` | `DRY_RUN` | `DRY_RUN` |
| `GOLD_DIMENSIONAL_WRITE_ENABLED` | `false` | `false` |
| `HISTORICAL_BACKFILL_WRITE_APPROVED` | `0` | `0` |
| `HISTORICAL_BACKFILL_MODE` | `DRY_RUN` | `DRY_RUN` |

### 4. Tests Executed and Results

- Targeted Compose recreate: **PASS**; exactly the two requested services
  reported `Recreated` and `Started`.
- Sanitized host-side `docker inspect`: **PASS**; both containers are running,
  use `ecommerce-airflow:3.1.7`, and report `OOMKilled=False`; all 13
  allowlisted values match the approved deny-by-default configuration.
- Secret exposure check: **PASS**; only the requested metadata and flags were
  emitted, with no secrets printed.
- No DAG or data-plane test was run because the scope explicitly forbids DAG
  execution and MinIO/PostgreSQL/Nessie/Spark access.

### 5. Risks and Limitations

This evidence certifies container metadata and environment values only. It does
not certify DAG parsing, task execution, data lineage, manifests/checksums,
Iceberg snapshots, reconciliation, or downstream write behavior. Compose's
orphan warning remains informational; no orphan cleanup was performed.

### 6. QA Reviewer Handoff

Review the command scope, the two-container runtime table, and the 13-key
allowlist against this plan. Confirm that no other service, DAG run, retry,
reset, cleanup, checkpoint, watermark, or data store was touched. Keep any
broader runtime certification gate separate from this bounded configuration
verification. No commit is requested or authorized.

## Independent QA verdict — bounded Airflow restart (2026-11-16)

### Review Scope

Reviewed the bounded restart evidence above and the reported latest runtime
state only. Scope is limited to `airflow-scheduler` and
`airflow-dag-processor`, recreated with `--no-deps`; unrelated worktree changes
and the warned `ecommerce-spark` orphan are out of scope.

### Acceptance Criteria Results

- **PASS:** only the two allowlisted Airflow services were recreated; no DAG was
  triggered and no retry, reset, cleanup, checkpoint, watermark, MinIO,
  PostgreSQL, Nessie, Spark, manifest, checksum, Iceberg, or cutover operation
  was performed.
- **PASS:** both containers run `ecommerce-airflow:3.1.7` and report
  `OOMKilled=false`.
- **PASS:** all 13 allowlisted safety flags remain deny-by-default, including
  `DRY_RUN` for Gold and historical backfill, and no secrets were emitted.
- **PASS WITH WARNINGS:** the `ecommerce-spark` orphan was only reported and was
  not touched; this is safe for the bounded scope but remains an operational
  warning.

### Final QA Verdict

**PASS WITH WARNINGS** for bounded environment-safety verification only.
Runtime certification remains limited to effective environment safety and does
not certify DAG parsing/execution, Silver lineage, Gold behavior, manifests,
checksums, Iceberg snapshots, reconciliation, or downstream writes. Gold and
historical modes remain `DRY_RUN`; no cutover or write enablement is certified.

### Required Fixes / Next Step

No fix is required for this bounded restart evidence. Preserve the orphan
warning without cleanup, and keep the broader DAG/Silver/Gold runtime gate
open. Any DAG/data-plane certification or write/cutover action requires a
separate explicit approval and a new independent QA review.

## Fresh six-table Silver batch preflight — STOPPED (2026-11-16)

### 1. Approved Task and Scope

Attempted the requested single fresh six-table Bronze/Silver batch using the
currently running Airflow scheduler and dag-processor safety configuration.
Gold dimensional write/DRY_RUN, Gold publication, Nessie cutover, checkpoint
and watermark reset, retry, and cleanup were out of scope and remained
disabled. No DagRun ID was allocated or reused because the pre-trigger write
gate failed.

### 2. Pre-trigger Safety Evidence

The read-only check targeted exactly these containers:

- `ecommerce-data-platform-airflow-scheduler-1`
- `ecommerce-data-platform-airflow-dag-processor-1`

Both returned the same allowlisted values:

| Flag | scheduler | dag-processor |
|---|---:|---:|
| `INGESTION_WRITE_ENABLED` | `false` | `false` |
| `GOLD_DIMENSIONAL_ENABLED` | `false` | `false` |
| `GOLD_DIMENSIONAL_MODE` | `DRY_RUN` | `DRY_RUN` |
| `GOLD_DIMENSIONAL_WRITE_ENABLED` | `false` | `false` |
| `WAREHOUSE_WRITE_ENABLED` | `false` | `false` |
| all six `*_ICEBERG_CUTOVER_ENABLED` flags | `false` | `false` |
| `HISTORICAL_BACKFILL_WRITE_APPROVED` | `0` | `0` |
| `HISTORICAL_BACKFILL_MODE` | `DRY_RUN` | `DRY_RUN` |
| `LAKEHOUSE_E2E_APPROVED` | `0` | `0` |
| `LAKEHOUSE_E2E_TEST_BUCKET` | `UNSET` | `UNSET` |
| `LAKEHOUSE_E2E_RUN_ID` | present, value redacted | present, value redacted |
| `LAKEHOUSE_E2E_PREFIX` | `UNSET` | `UNSET` |

No secret-bearing values were printed. The bounded E2E contract therefore did
not validate, and the ingestion write guard was explicitly disabled.

### 3. Execution Result

**STOPPED BEFORE TRIGGER.** No unique DagRun ID was created, no old ID was
reused, and no Airflow API/DAG trigger, task execution, MinIO inventory,
manifest read, schema fingerprint, quality/reconciliation check, or prefix
scan was performed. There are no new DagRun/task states or API/worker errors
for this attempt. No Bronze/Silver/Gold/Nessie/PostgreSQL/Spark state changed.

### 4. Tests Executed and Results

- Read-only Docker safety-flag inspection in both named Airflow containers:
  **PASS**, with sanitized output only.
- Pre-trigger write authorization gate: **FAIL / NOT ELIGIBLE** because
  `INGESTION_WRITE_ENABLED=false` and `LAKEHOUSE_E2E_APPROVED=0` with bucket and
  prefix unset.
- Fresh six-table DagRun: **NOT STARTED**.

### 5. Risks and Limitations

This attempt provides only safety-preflight evidence. It cannot certify
Silver lineage, manifests/checksums, schema fingerprints, MinIO exact-run
containment, quality, reconciliation, or task recovery. Enabling write flags
or changing the bounded E2E environment requires a separate explicit approval;
do not retry this attempt, reset state, or launch Gold from this evidence.

### 6. QA Reviewer Handoff

Review the exact two-container allowlisted output and confirm the hard stop
occurred before any DagRun or data-plane access. Verdict: **PASS WITH WARNINGS
for safe preflight handling; runtime certification OPEN/BLOCKED**. The next
action requires explicit authorization and a valid immutable bounded E2E
configuration with Bronze/Silver write enabled while all Gold/cutover/reset/
cleanup guards remain disabled.

## Fresh six-table Silver batch preflight re-check — STOPPED BEFORE TRIGGER (2026-11-16)

### Approved Task and Scope

The user approved exactly one new unique DagRun for a Bronze/Silver-only
six-table batch, with no Gold launch, retry, reset, cleanup, checkpoint or
watermark mutation, and requested this evidence in the current plan. No
implementation/source files were in scope.

### Exact host inspection evidence

Read-only `docker inspect` targeted exactly these three existing containers:

- `ecommerce-data-platform-airflow-scheduler-1`
- `ecommerce-data-platform-airflow-dag-processor-1`
- `ecommerce-data-platform-airflow-api-server-1`

All three reported image `ecommerce-airflow:3.1.7`, status `running`, and
`OOMKilled=false`. The allowlisted safety values were identical across all
three services:

| Flag | scheduler | dag-processor | api-server |
|---|---:|---:|---:|
| `INGESTION_WRITE_ENABLED` | `false` | `false` | `false` |
| `WAREHOUSE_WRITE_ENABLED` | `false` | `false` | `false` |
| `LAKEHOUSE_WRITE_ENABLED` | `false` | `false` | `false` |
| `GOLD_DIMENSIONAL_ENABLED` | `false` | `false` | `false` |
| `GOLD_DIMENSIONAL_MODE` | `DRY_RUN` | `DRY_RUN` | `DRY_RUN` |
| `GOLD_DIMENSIONAL_WRITE_ENABLED` | `false` | `false` | `false` |
| `HISTORICAL_BACKFILL_WRITE_APPROVED` | `0` | `0` | `0` |
| `HISTORICAL_BACKFILL_MODE` | `DRY_RUN` | `DRY_RUN` | `DRY_RUN` |
| `LAKEHOUSE_E2E_APPROVED` | `0` | `0` | `0` |
| `LAKEHOUSE_E2E_TEST_BUCKET` | unset | unset | unset |
| `LAKEHOUSE_E2E_PREFIX` | unset | unset | unset |
| `LAKEHOUSE_E2E_RUN_ID` | unset | unset | unset |
| all six `*_ICEBERG_CUTOVER_ENABLED` flags | `false` | `false` | `false` |

No secret-bearing values were emitted.

### Result and test evidence

The bounded write gate **FAIL / NOT ELIGIBLE** because
`INGESTION_WRITE_ENABLED=false`, `LAKEHOUSE_E2E_APPROVED=0`, and the required
immutable bucket/prefix/run-id contract is unset. The process therefore
**STOPPED BEFORE TRIGGER**. No unique DagRun ID was created or reused; no Airflow
API trigger, task execution, MinIO inventory, manifest read, schema fingerprint,
quality/reconciliation check, prefix scan, retry, reset or cleanup was run.

- Exact three-container sanitized `docker inspect`: **PASS**.
- Pre-trigger authorization gate: **FAIL / NOT ELIGIBLE**.
- Fresh six-table DagRun: **NOT STARTED**.
- Bronze/Silver/Gold/Nessie/PostgreSQL/Spark state mutation: **NONE**.

### Risks, limitations and next step

This evidence does not certify DagRun/task states, API/worker errors, Bronze or
Silver objects, six Silver manifests, schema fingerprints, quality,
reconciliation, read-back, or exact-prefix containment. Do not retry this
attempt, reset state, clean up, or launch Gold. The next action requires the
approved three-service runtime to expose the exact enabled bounded E2E contract
and `INGESTION_WRITE_ENABLED=true`; after that, perform one fresh unique trigger
only and stop immediately on any failure.

### QA Reviewer Handoff

Review the three-container allowlisted evidence and confirm the hard stop before
any DagRun or data-plane access. Runtime certification remains **OPEN/BLOCKED**;
this is a safe-preflight verdict only.

### Independent QA Verdict (2026-11-16)

- **PASS** for safe hard-stop handling. The three-service host inspection is
  internally consistent: `INGESTION_WRITE_ENABLED=false`, E2E approval is `0`,
  the bounded bucket/prefix/run-id contract is unset, and Gold, warehouse,
  lakehouse, historical backfill, and all six cutover guards remain disabled.
- The evidence supports **STOPPED BEFORE TRIGGER** and no DagRun, task,
  worker execution, data-store access, or data mutation. This is a verified
  safety conclusion from the recorded allowlisted evidence; it is not runtime
  certification of Silver.
- Required next step: with separate explicit approval, recreate only the
  needed scheduler/worker service using command-scoped
  `INGESTION_WRITE_ENABLED=true`; then repeat sanitized host inspection before
  any trigger. Do not run Docker/DAG as part of this QA review, and stop if the
  post-recreate flags are not exactly approved.
## Bounded Silver scheduler recreate and trigger gate (2026-11-16)

### Approved Task and Scope

- Recreate only `airflow-scheduler` and `airflow-dag-processor` with
  process-scoped `INGESTION_WRITE_ENABLED=true` and `--no-deps`.
- Do not edit `.env`, restart other services, retry/reset/cleanup, launch Gold,
  or mutate checkpoints/watermarks.
- After a passing allowlisted host inspection, trigger exactly one fresh unique
  six-table Bronze/Silver DagRun and capture runtime evidence.

### Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` (this execution record)

### Implementation Summary

- Exact Compose action:
  `INGESTION_WRITE_ENABLED=true docker compose --env-file .env up -d --no-deps --force-recreate airflow-scheduler airflow-dag-processor`.
- Only the two requested services were recreated and started. The existing
  `ecommerce-spark` orphan warning was informational; `--remove-orphans` was
  not used.
- Post-recreate filtered `docker inspect` passed on both services:
  `status=running`, `image=ecommerce-airflow:3.1.7`, `oom=false`, ingestion
  `true`, warehouse/lakehouse `false`, Gold write `false`, Gold mode `DRY_RUN`,
  `BI_ACTIVE_PUBLISH_APPLY=0`, historical write approval `0`, historical mode
  `DRY_RUN`, and all six cutover flags `false`.
- The approved single trigger was not created. The read-only collision-check
  request to `http://127.0.0.1:8085/api/v2/dags/ecommerce_incremental_ingestion/dagRuns`
  returned `401 Not authenticated`; the attempted POST was likewise rejected
  with `401`, and no `dag_run_id` was returned. Execution stopped immediately.

### Tests Executed and Results

- Host-side allowlisted Docker recreate: **PASS**.
- Host-side filtered inspect gate: **PASS**.
- Airflow API collision check/trigger: **FAIL / STOPPED** (`401 Not authenticated`).
- Fresh six-table DagRun: **NOT CREATED**; no task/API/worker/data-plane
  evidence exists for this attempt.
- No retry, reset, cleanup, Gold launch, MinIO/Nessie/PostgreSQL/Spark access,
  checkpoint mutation, or watermark mutation was performed.

### Risks and Limitations

Airflow API authentication is unresolved. The failure occurred before a DagRun
was created, so Bronze/Silver manifests, checksums, read-back, task states,
worker logs, and snapshot lineage are **NOT CERTIFIED**. The run must not be
retried or inferred to have executed.

### QA Reviewer Handoff

Review the exact two-container recreate, filtered flag evidence, and the two
`401 Not authenticated` API responses. Verdict: **SAFE HARD STOP; RUNTIME
CERTIFICATION OPEN/BLOCKED**. Any future trigger requires a separately approved
authentication correction and a new preflight; do not reuse an uncreated run ID.

## Approved Airflow auth correction preflight — no DAG trigger (2026-11-16)

### Approved Task and Scope

The user approved only a sanitized Airflow API authentication preflight after
the previous `401 Not authenticated` result. Existing Airflow admin credentials
were read only inside the `airflow-api-server` process; a short-lived JWT was
held in memory, never printed, persisted, or copied into repository/config
files. The scope was limited to the health endpoint and one protected DAG
endpoint. No DAG trigger, restart, config/.env edit, or data-store access was
allowed.

### Exact runtime evidence

- Token acquisition: `POST /auth/token` returned **HTTP 201**;
  `token_acquired=true`. Credential and token values were not emitted.
- Health probe: `GET /api/v2/monitor/health` returned **HTTP 200**. Body was
  redacted to non-secret key names only:
  `dag_processor`, `metadatabase`, `scheduler`, `triggerer`.
- Protected DAG probe: `GET /api/v2/dags/ecommerce_incremental_ingestion`
  with `Authorization: Bearer <in-memory-token>` returned **HTTP 200**. Body
  was redacted to non-secret key names only; no DAG payload values were
  recorded.
- HTTP correlation/request ID headers: **none emitted** by either probe.
- No `dag_run_id`, TI ID, POST trigger, task execution, retry, reset, or
  data-plane request was performed.

### Scheduler and DAG-processor safety evidence

Read-only `docker exec` inspected only allowlisted safety flags in the existing
`airflow-scheduler` and `airflow-dag-processor` processes. Both matched:

| Flag | scheduler | dag-processor |
|---|---:|---:|
| `INGESTION_WRITE_ENABLED` | `true` | `true` |
| `WAREHOUSE_WRITE_ENABLED` | `false` | `false` |
| `LAKEHOUSE_WRITE_ENABLED` | `false` | `false` |
| `GOLD_DIMENSIONAL_ENABLED` | `false` | `false` |
| `GOLD_DIMENSIONAL_MODE` | `DRY_RUN` | `DRY_RUN` |
| `GOLD_DIMENSIONAL_WRITE_ENABLED` | `false` | `false` |
| `HISTORICAL_BACKFILL_WRITE_APPROVED` | `0` | `0` |
| `HISTORICAL_BACKFILL_MODE` | `DRY_RUN` | `DRY_RUN` |
| `LAKEHOUSE_E2E_APPROVED` | `0` | `0` |
| `LAKEHOUSE_E2E_TEST_BUCKET/PREFIX/RUN_ID` | present, redacted | present, redacted |
| all six `*_ICEBERG_CUTOVER_ENABLED` flags | `false` | `false` |
| `BI_ACTIVE_PUBLISH_APPLY` | `0` | `0` |

No secret-bearing environment values were read or emitted. Because ingestion
write is enabled and the bounded E2E values were not validated in this
preflight, no trigger is authorized by this evidence.

### Result, risks and next step

- Auth correction preflight: **PASS**.
- Health/protected API routing: **PASS** (`200` / `200`).
- DAG trigger: **NOT PERFORMED**, by explicit scope.
- Runtime Silver/Gold certification: **OPEN / NOT CERTIFIED**. This evidence
  proves only authentication and protected read routing; it does not prove
  task execution, manifests, checksums, lineage, reconciliation, or recovery.

Exact next step: stop here and obtain a separate explicit approval for any
future bounded DAG trigger, including validation of the immutable E2E contract
and a fresh run ID. Do not trigger, retry, reset, clean up, or access data
stores from this auth evidence.

### QA Reviewer Handoff

Review the in-container credential boundary, `201` token acquisition,
redacted `200` health/protected responses, absence of correlation IDs, and the
allowlisted scheduler/dag-processor safety flags. Verdict for this scope:
**PASS WITH WARNINGS — auth preflight closed; runtime certification remains
OPEN / NOT CERTIFIED**.

## Approved bounded fresh-trigger attempt — STOPPED BEFORE CLIENT/TRIGGER (2026-11-16)

### 1. Approved Task and Scope

The user approved exactly one new unique six-table Bronze/Silver DagRun, using
the current Airflow auth JWT in memory only. Gold, warehouse, lakehouse write,
cutover, publish, backfill, checkpoint/watermark mutation, retry, reset and
cleanup were excluded, except for the explicitly bounded E2E write under the
exact immutable prefix. No commit was requested or made.

### 2. Pre-client immutable E2E contract evidence

An opaque fresh run ID was generated and validated host-side before any
Airflow, MinIO, Nessie, PostgreSQL or Spark client/trigger:

- `LAKEHOUSE_E2E_APPROVED=1`: PASS
- `LAKEHOUSE_E2E_TEST_BUCKET=ecommerce-data-lake`: PASS
- `LAKEHOUSE_E2E_RUN_ID=qualityit_20261116232139_0a5f832a06fc`: PASS against
  the immutable allowed run-ID regex
- `LAKEHOUSE_E2E_PREFIX=quality-it/qualityit_20261116232139_0a5f832a06fc/`:
  PASS; exact `quality-it/<run-id>/` derivation
- Pre-client validation result: **PASS**

No Airflow or storage client was created during this validation.

### 3. Runtime guard inspection and hard stop

Read-only `docker inspect` was performed for the existing scheduler,
dag-processor and API server. The scheduler and dag-processor were running and
had `INGESTION_WRITE_ENABLED=true`, while all inspected Gold/warehouse/
lakehouse/cutover guards were disabled. However, both execution services had
`LAKEHOUSE_E2E_APPROVED=0` and an empty `LAKEHOUSE_E2E_TEST_BUCKET`; therefore
the exact immutable bounded contract was not present in the processes that
would execute the tasks. The API server also had the bounded contract unset and
`INGESTION_WRITE_ENABLED=false`.

The required process-scoped service recreate was not performed: an escalation
request for `docker compose ... up -d --no-deps --force-recreate
airflow-scheduler airflow-dag-processor` was rejected because it would mutate
orchestration services beyond the currently effective runtime contract.

Result: **STOPPED BEFORE AIRFLOW CLIENT/TRIGGER**. No JWT was acquired or
printed, no DagRun ID was reserved or reused, and no Airflow API POST, task
execution, MinIO inventory/readback, manifest/schema/quality/reconciliation
check, prefix scan, retry, reset, cleanup or Gold launch occurred.

### 4. Tests executed and results

- Host-side immutable E2E contract validation: **PASS**.
- Read-only three-service Docker inspection with secret/run-ID redaction:
  **PASS**.
- Runtime bounded write gate: **FAIL / NOT ELIGIBLE** because the existing
  scheduler/dag-processor do not expose `LAKEHOUSE_E2E_APPROVED=1` and the
  exact test bucket.
- Fresh six-table DagRun: **NOT CREATED**.

### 5. Risks and limitations

This attempt provides no task states, errors, object inventory, readback,
manifests, checksums, schema fingerprints, quality/reconciliation evidence or
prefix-containment evidence. The requested runtime certification remains
**OPEN / BLOCKED**. Do not retry, reset, clean up or infer a run from this
preflight.

### 6. QA Reviewer Handoff

Review the host-side contract PASS, the redacted read-only service inspection,
the rejected service-mutation escalation, and the hard stop before any
Airflow/storage client or trigger. Verdict: **SAFE HARD STOP; runtime
certification OPEN/BLOCKED**. The exact next step requires explicit approval
for a process-scoped recreate (or equivalent already-approved runtime change)
 that exposes the validated immutable contract to scheduler and dag-processor;
 only then may one fresh unique six-table trigger be attempted.

## Approved process-scoped E2E runtime injection — verified, no trigger (2026-11-16)

### Approved Task and Scope

- Recreate only `airflow-scheduler` and `airflow-dag-processor`.
- Use the exact immutable process-scoped contract:
  `LAKEHOUSE_E2E_APPROVED=1`, `LAKEHOUSE_E2E_TEST_BUCKET=ecommerce-data-lake`,
  `LAKEHOUSE_E2E_RUN_ID=qualityit_20261116232139_0a5f832a06fc`,
  `LAKEHOUSE_E2E_PREFIX=quality-it/qualityit_20261116232139_0a5f832a06fc/`.
- Preserve `INGESTION_WRITE_ENABLED=true`; do not edit `.env`, restart other
  services, trigger a DAG, or access/mutate data-plane state.

### Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` (this execution record)

### Implementation Summary

- Executed with child-process command-scoped overrides:
  `INGESTION_WRITE_ENABLED=true LAKEHOUSE_E2E_APPROVED=1 LAKEHOUSE_E2E_TEST_BUCKET=ecommerce-data-lake LAKEHOUSE_E2E_RUN_ID=qualityit_20261116232139_0a5f832a06fc LAKEHOUSE_E2E_PREFIX=quality-it/qualityit_20261116232139_0a5f832a06fc/ docker compose up -d --no-deps --force-recreate airflow-scheduler airflow-dag-processor`.
- Exactly the two requested services were recreated and started. The existing
  `ecommerce-spark` orphan warning was informational; no orphan removal was
  performed.
- Filtered host-side `docker inspect` emitted only status, image, OOM, and
  allowlisted contract/safety values; no `docker exec`, full environment,
  credentials, or secrets were used.

### Tests Executed and Results

- Process-scoped Docker recreate: **PASS**.
- Filtered host-side inspection: **PASS** for both services:
  `status=running`, `image=ecommerce-airflow:3.1.7`, `oom=False`.
- Exact contract: **PASS** on both services for approval, bucket, run ID,
  prefix, and `INGESTION_WRITE_ENABLED=true`.
- Safety guards: **PASS** — warehouse/lakehouse/Gold writes disabled, Gold
  mode `DRY_RUN`, historical write approval `0`, historical mode `DRY_RUN`,
  `BI_ACTIVE_PUBLISH_APPLY=0`, and all six Iceberg cutover flags `false`.
- DAG trigger: **NOT RUN**, per instruction. No Airflow API request/token,
  DagRun, task execution, storage access, retry, reset, cleanup, or Gold
  launch occurred.

### Risks and Limitations

Runtime certification remains **OPEN / NOT CERTIFIED**. This evidence proves
only that the approved bounded contract is present in the two execution
services; it does not provide Bronze/Silver task, manifest, checksum, schema,
quality, reconciliation, read-back, or prefix-containment evidence.

### QA Reviewer Handoff

Review the exact two-service recreate command, filtered inspection evidence,
and explicit no-trigger stop. The next action requires separate authorization
for a fresh trigger and bounded runtime verification.

## Approved fresh bounded trigger - STOPPED AT API VALIDATION (2026-11-17)

### 1. Approved Task and Scope

The user explicitly approved exactly one fresh bounded trigger for
`ecommerce_incremental_ingestion` using the already verified process-scoped
contract:

- `LAKEHOUSE_E2E_APPROVED=1`
- `LAKEHOUSE_E2E_TEST_BUCKET=ecommerce-data-lake`
- `LAKEHOUSE_E2E_RUN_ID=qualityit_20261116232139_0a5f832a06fc`
- `LAKEHOUSE_E2E_PREFIX=quality-it/qualityit_20261116232139_0a5f832a06fc/`
- `INGESTION_WRITE_ENABLED=true`
- warehouse/lakehouse/Gold writes and all Iceberg cutover flags disabled

The requested run was Bronze/Silver only for exactly the six tables
`customers`, `products`, `orders`, `order_items`, `payments`, and `shipments`.
Gold was not launched. Retry, reset and cleanup were out of scope.

### 2. Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` (this execution record)

### 3. Implementation Summary

Filtered read-only inspection confirmed both execution services were running
with the exact bounded contract and safety guards. A short-lived Airflow JWT
was acquired in memory inside the API container and was never printed or
persisted. The client generated the fresh candidate run ID
`qualityit__20261117T011310Z__a0ff54e7fb474bb79a9b792cb0c5c752` and verified it
was absent from the recent DagRun listing before the single trigger attempt.

The one POST attempt to
`/api/v2/dags/ecommerce_incremental_ingestion/dagRuns` returned HTTP `422`
before DagRun creation because Airflow 3.1.7 requires `logical_date` in the
request body. No corrected request was sent.

### 4. Tests Executed and Results

- Filtered Airflow service/guard inspection: **PASS**; scheduler and
  dag-processor were running with the approved contract; Gold, warehouse,
  lakehouse and cutover guards were disabled.
- JWT acquisition: **PASS**; memory-only, not emitted or persisted.
- Fresh-ID preflight: **PASS**; candidate ID was not present in the read-only
  DagRun listing.
- Single DagRun trigger: **FAIL / NOT CREATED**; HTTP `422`, missing required
  `logical_date`.
- DagRun/task-state read-back: **NOT APPLICABLE**; no DagRun was created, so
  there are no task states or Airflow task errors to read back.
- Exact-run MinIO inventory and six-table manifest/schema/quality/
  reconciliation/prefix evidence: **NOT PERFORMED**; the failed API validation
  occurred before task execution and data-plane access.
- Retry/reset/cleanup/Gold launch: **NOT PERFORMED**.

### 5. Risks and Limitations

This session does not certify the bounded runtime. The candidate run ID is
recorded for audit only and must not be reused or retried under this approval.
No artifact evidence exists for this candidate because no DagRun was created.
The Airflow trigger contract requires a future separately approved attempt to
include a valid `logical_date`; that is a new operational action and is not
performed here.

### 6. QA Reviewer Handoff

Review the exact one-attempt boundary: service guards passed, JWT remained
memory-only, candidate ID was fresh, and Airflow rejected the sole POST with
HTTP `422` for missing `logical_date` before persistence. Verdict: **FAIL /
BLOCKED at trigger validation**. Runtime certification remains **OPEN / NOT
CERTIFIED**; no retry, reset, cleanup, or Gold action is authorized by this
record.

## Approved new bounded trigger attempt - STOPPED AT POST (2026-11-17)

### 1. Approved Task and Scope

The user approved one completely new bounded trigger attempt for
`ecommerce_incremental_ingestion`, using the currently verified process-scoped
E2E contract and an Airflow JWT held in memory only. Scope was exactly Bronze /
Silver for `customers`, `products`, `orders`, `order_items`, `payments`, and
`shipments`. Gold, warehouse, lakehouse, cutover, publish, backfill,
checkpoint/watermark mutation, retry, reset, and cleanup remained disabled or
out of scope. No commit was requested or made.

### 2. Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` (this execution record)

### 3. Implementation Summary

Read-only filtered Docker inspection confirmed the scheduler and dag-processor
were running with the verified process-scoped values:

- `LAKEHOUSE_E2E_APPROVED=1`
- `LAKEHOUSE_E2E_TEST_BUCKET=ecommerce-data-lake`
- `LAKEHOUSE_E2E_RUN_ID=qualityit_20261116232139_0a5f832a06fc`
- `LAKEHOUSE_E2E_PREFIX=quality-it/qualityit_20261116232139_0a5f832a06fc/`
- `INGESTION_WRITE_ENABLED=true`
- warehouse/lakehouse/Gold writes, publish, historical write approval and all
  six Iceberg cutover flags disabled

The API process acquired a short-lived JWT from its in-container auth file;
the credential and token were kept memory-only and were not printed or
persisted. Fresh-ID preflight listed recent DagRuns and confirmed the new
candidate was absent. The exact candidate and logical date were:

- `dag_run_id=qualityit__20261117T013204Z__ffb6aa89314c4f7e8318c321f50e5a67`
- `logical_date=2026-11-17T01:32:04Z`
- POST endpoint: `/api/v2/dags/ecommerce_incremental_ingestion/dagRuns`
- POST body included `dag_run_id`, the valid ISO UTC `logical_date`, and
  `conf.mode=WRITE` with exactly the six approved tables

The single POST returned HTTP `404`. No DagRun was created; no corrected or
repeat request was sent. Execution stopped at the POST failure boundary.

### 4. Tests Executed and Results

- Process-scoped service/guard inspection: **PASS**.
- JWT acquisition: **PASS**; memory-only and not emitted/persisted.
- Fresh-ID preflight: **PASS**; candidate absent from the `200` DagRun listing.
- Single DagRun POST: **FAIL / NOT CREATED**; HTTP `404`.
- DagRun/task-state read-back: **NOT PERFORMED** because creation failed.
- Exact-run MinIO six-table inventory/manifests/schema/quality/
  reconciliation/prefix evidence: **NOT PERFORMED** because no task executed.
- Retry/reset/cleanup/Gold launch: **NOT PERFORMED**.

### 5. Risks and Limitations

Runtime certification remains **OPEN / NOT CERTIFIED**. The candidate is
recorded for audit only and must not be reused or retried. No Bronze/Silver
artifact, manifest, checksum, schema, quality, reconciliation, prefix, or
task-state evidence exists for this attempt. The HTTP 404 POST boundary needs
separate diagnosis and approval before any later operational action.

### 6. QA Reviewer Handoff

Review the process-scoped guard evidence, memory-only JWT boundary, fresh
candidate/logical-date payload, and the sole HTTP 404 POST result. Verdict:
**FAIL / BLOCKED AT POST**. Confirm that no DagRun, task execution, MinIO
read-back, retry, reset, cleanup, or Gold launch occurred. The phase remains
open and no further trigger is authorized by this record.

## Approved new Airflow OpenAPI-confirmed trigger attempt — STOPPED AT POST (2026-11-17)

### 1. Approved Task and Scope

The user approved exactly one new POST attempt for
`ecommerce_incremental_ingestion` using the confirmed Airflow OpenAPI contract:
`POST /api/v2/dags/{dag_id}/dagRuns` with
`TriggerDAGRunPostBody`. The scope was Bronze/Silver only for exactly
`customers`, `products`, `orders`, `order_items`, `payments`, and `shipments`.
Gold, warehouse, lakehouse, cutover, publish, backfill,
checkpoint/watermark mutation, retry, reset, and cleanup were disabled or out
of scope. No commit was made.

### 2. Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` (this execution record)

### 3. Implementation Summary

Read-only Docker inspection passed for both execution services. The exact
bounded contract was present:

- `LAKEHOUSE_E2E_APPROVED=1`
- `LAKEHOUSE_E2E_TEST_BUCKET=ecommerce-data-lake`
- `LAKEHOUSE_E2E_RUN_ID=qualityit_20261116232139_0a5f832a06fc`
- `LAKEHOUSE_E2E_PREFIX=quality-it/qualityit_20261116232139_0a5f832a06fc/`
- `INGESTION_WRITE_ENABLED=true`

The warehouse/lakehouse/Gold write flags, all six Iceberg cutover flags,
historical backfill approval, and active publish flag were disabled; Gold mode
was `DRY_RUN`. A JWT was acquired and held in memory only; it was not printed
or persisted. The pre-trigger DagRun listing returned successfully and the
candidate was absent from that listing.

The sole POST used the required endpoint and a valid UTC `logical_date`:

- `dag_run_id=qualityit__20261117T__`
- `logical_date=2026-11-17T02:01:00Z`
- `conf.mode=WRITE`
- `conf.tables` contained exactly the six approved tables

Airflow returned HTTP `404 Not Found`. Execution stopped immediately at the
POST boundary. No corrected request was sent and no data-plane client was
created.

### 4. Tests Executed and Results

- Process-scoped service/guard inspection: **PASS**.
- Bounded E2E contract validation: **PASS**.
- JWT acquisition: **PASS**; memory-only and not emitted/persisted.
- Fresh-ID preflight: **PASS**; candidate absent from the DagRun listing.
- Single OpenAPI-confirmed DagRun POST: **FAIL / NOT CREATED**; HTTP `404`.
- DagRun/task-state read-back: **NOT PERFORMED** because POST creation failed.
- Exact-run MinIO manifests/schema/quality/reconciliation/prefix evidence:
  **NOT PERFORMED** because no DagRun was created and the safety rule requires
  stopping on failure.
- Retry/reset/cleanup/Gold launch: **NOT PERFORMED**.

### 5. Risks and Limitations

Runtime certification remains **OPEN / NOT CERTIFIED**. The candidate is
recorded for audit only and must not be reused or retried. The POST `404`
remains a route/runtime-specific blocker despite the prior OpenAPI confirmation
of the advertised operation. No task, manifest, checksum, schema, quality,
reconciliation, prefix, or state evidence exists for this attempt.

### 6. QA Reviewer Handoff

Review the exact bounded flags, memory-only JWT boundary, fresh candidate,
valid UTC logical date, exact six-table payload, and the sole HTTP `404` POST.
Confirm the immediate hard-stop and absence of retry, reset, cleanup, Gold
launch, task execution, or data-plane access. Verdict: **FAIL / BLOCKED AT
POST**. Any further diagnosis or trigger requires separate explicit approval
and a completely new candidate ID.

## Independent QA review - prior bounded trigger attempt (2026-11-17, 01:32Z)

- Scope reviewed: the candidate evidence above and the session-local diagnosis
  scope for this bounded trigger attempt; no Docker, DAG, API retry, database,
  MinIO, Nessie, Spark, checkpoint,
  watermark, cleanup, or Gold operation was executed by this review.
- Evidence confirmed: fresh candidate
  `qualityit__20261117T013204Z__ffb6aa89314c4f7e8318c321f50e5a67`, logical date
  `2026-11-17T01:32:04Z`, memory-only JWT, one POST to the recorded Airflow
  route, HTTP `404`, and no recorded DagRun/task/data mutation.
- Acceptance result: **PASS** for safe hard-stop and bounded safety behavior;
  **FAIL / BLOCKED** for runtime certification.
- Blocker classification: **endpoint/DAG discovery blocker** at the POST
  boundary. The evidence does not distinguish a wrong/unavailable API route
  from an undiscovered/mismatched DAG resource, so no narrower root cause is
  claimed. It is not evidence of a task, ingestion, MinIO, Iceberg, or Gold
  failure.
- Required disposition: do not retry or reuse this candidate; do not run
  Docker/DAG or perform reset/cleanup. A future endpoint/DAG discovery
  diagnosis and any subsequent fresh bounded trigger require separate explicit
  approval. Runtime certification remains **OPEN / NOT CERTIFIED**.

## Independent QA review - latest OpenAPI-confirmed trigger attempt (2026-11-17, 02:01Z)

### 1. Review Scope

Independent QA of the latest semantic plan evidence only. No POST, DAG,
Docker, database, MinIO, Nessie, Spark, checkpoint, watermark, retry, reset,
cleanup, Gold, or cutover operation was executed by this review.

### 2. Commit or Diff Reviewed

Reviewed the working-tree evidence in `docs/plans/semantic-runtime-certification-plan.md`
(`:3103-3176`) and the roadmap entry in `docs/GOLD_LAKEHOUSE_PLAN.md` (`:3-21`).
The working tree contains unrelated changes; this verdict does not approve them.

### 3. Acceptance Criteria Results

- **PASS** — bounded flags and exact six-table scope are recorded; Gold,
  warehouse/lakehouse writes, publish, backfill, and six Iceberg cutovers are
  disabled or out of scope.
- **PASS** — JWT was memory-only and was not printed or persisted.
- **PASS** — fresh candidate `qualityit__20261117T__` was absent in the
  pre-trigger listing; it must not be reused.
- **PASS** — logical date is `2026-11-17T02:01:00Z` and the recorded POST uses
  `/api/v2/dags/ecommerce_incremental_ingestion/dagRuns`.
- **PASS** — the single POST returned HTTP `404 Not Found`; execution stopped
  immediately with no corrected request.
- **PASS** — evidence records no DagRun/task/artifact/data mutation and no
  retry, reset, cleanup, or Gold action.
- **FAIL / NOT CERTIFIED** — no runtime task, manifest, checksum, schema,
  quality, reconciliation, or read-back evidence exists.

### 4. Findings by Severity

- **HIGH** — runtime certification cannot advance because the exact POST
  boundary returned `404`; the root cause remains route/contract-specific and
  is not evidence of a data-plane failure. Evidence: semantic plan `:3144-3167`.
- **LOW** — corrected stale “latest attempt” wording for the prior `01:32Z`
  candidate in this plan and roadmap; no runtime behavior was changed.

### 5. Tests Executed

- Read-only document/diff inspection and `git diff --check` only.
- No POST, DAG, or runtime test was run.

### 6. Test Results

`git diff --check`: PASS. Runtime test execution: NOT PERFORMED by explicit
scope.

### 7. Regression Risks

The trigger contract/route remains unresolved. No claim can be made about
ingestion, manifests, checksums, lineage, reconciliation, idempotency, or
Iceberg snapshots from this failed POST.

### 8. Security and Data Safety

The evidence supports memory-only JWT handling and a safe hard-stop. Do not
retry or reuse `qualityit__20261117T__`; do not reset, clean up, enable Gold,
or perform cutover.

### 9. Final QA Verdict

**PASS** for safe hard-stop and bounded data safety. **FAIL / BLOCKED** for
runtime certification. Runtime remains **OPEN / NOT CERTIFIED**.

### 10. Required Fixes

Resolve the POST route/contract-specific `404` through a separately approved
read-only diagnosis or a separately approved fresh bounded attempt. Use a new
immutable candidate ID; do not reuse the failed candidate. Obtain independent
QA again before any runtime certification claim.

## Approved read-only Airflow endpoint/DAG discovery diagnosis (2026-11-17)

### Approved Task and Scope

The user approved a read-only diagnosis of the prior Airflow `404` boundary.
Only GET probes were performed for health, DAG collection, filtered DAG search,
and exact DAG metadata. An admin JWT was acquired through the auth endpoint and
held in memory only; the credential and token were not printed or persisted.
No DAG-trigger POST, Docker
exec/restart, MinIO/PostgreSQL/Nessie/Spark access, retry, reset, or cleanup
was performed.

### Sanitized probe evidence

- Token acquisition: **PASS**; `token_acquired=true` (value omitted).
- `GET /api/v2/monitor/health`: **HTTP 200**; body keys only:
  `dag_processor`, `metadatabase`, `scheduler`, `triggerer`.
- `GET /api/v2/dags`: **HTTP 200**; body keys `dags`, `total_entries`; returned
  DAG identity `ecommerce_historical_production_backfill` in the visible list.
- `GET /api/v2/dags?dag_id=ecommerce_incremental_ingestion`: **HTTP 200**;
  returned the same visible historical DAG, so this query parameter did not
  filter to the requested identity in this runtime.
- `GET /api/v2/dags?dag_id_pattern=ecommerce_incremental_ingestion`:
  **HTTP 200**, body keys `dags`, `total_entries`, `total_entries=0`.
- `GET /api/v2/dags/ecommerce_incremental_ingestion`: **HTTP 200**; sanitized
  body keys include `dag_id`, `fileloc`, `has_import_errors`, `is_paused`,
  `is_stale`, owners/tags, parse metadata, and scheduling metadata.

### Diagnosis and disposition

The Airflow `/api/v2` route is reachable and the exact
`ecommerce_incremental_ingestion` resource identity resolves successfully via
its metadata endpoint. Therefore the prior `404` from the single POST is **not
evidence of a missing DAG identity** and is not a base API-route outage. The
remaining classification is a **POST route/contract-specific blocker** (the
GET evidence cannot identify which POST contract detail caused the `404`, and
no POST is authorized in this diagnosis). The inconsistent collection-filter
behavior is recorded as an API/runtime discovery caveat, not treated as proof
that the DAG is absent.

### Tests Executed and Results

- Health, collection, both filtered-search forms, and exact metadata: **PASS**
  as GET-only probes with sanitized status/body summaries.
- Prior trigger candidate/DagRun: **not retried or reused**; no DAG-trigger
  POST, new DagRun, or
  data-plane activity occurred.

### Risks and Limitations

Runtime certification remains **OPEN / NOT CERTIFIED**. The GET evidence proves
endpoint reachability and exact DAG metadata identity only; it does not prove
trigger acceptance, task execution, manifests, checksums, lineage, recovery, or
data-store behavior. The filtered collection endpoint should not be used alone
to decide DAG existence in this runtime.

### QA Reviewer Handoff

Review the five GET-only probes across four categories (health, collection,
filtered search, and exact metadata), memory-only JWT handling, and the
classification that exact DAG metadata is present while the prior POST `404`
remains unresolved at the POST contract/route layer. Exact next step: obtain
separate explicit approval for a new, fresh, bounded trigger diagnosis or
inspect the Airflow API contract/configuration read-only; do not retry the
previous candidate, trigger, reset, clean up, or access data stores from this
evidence.

## GET-only Airflow OpenAPI/route diagnosis (2026-11-17)

### 1. Approved Task and Scope

The approved scope was limited to GET-only discovery of Airflow 3.1.7
documentation/OpenAPI routes and the DagRun creation contract, using a JWT
only if already available in memory. No authentication POST, DagRun POST, DAG
trigger, Docker command/restart, data-store access, secret readback, or full
response dump was permitted. No commit was made.

### 2. Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` — added this sanitized
  route/OpenAPI evidence and QA handoff.

### 3. Implementation Summary

Read-only GET probes against the local Airflow API server returned:

- `GET /docs`: **HTTP 200**, `text/html`, response size recorded only as
  `935` bytes.
- `GET /redoc`: **HTTP 200**, `text/html`, response size recorded only as
  `892` bytes.
- `GET /openapi.json`: **HTTP 200**, `application/json`, response size
  recorded only as `264426` bytes. The response was parsed in memory; no body
  was printed or persisted.
- `GET /api/v2/openapi.json`: **HTTP 404**.
- `GET /api/v2/docs`: **HTTP 404**.
- `GET /api/v2/redoc`: **HTTP 404**.
- `GET /api/v2/monitor/health`: **HTTP 200**; sanitized top-level keys were
  `dag_processor`, `metadatabase`, `scheduler`, `triggerer`.
- Unauthenticated `GET /api/v2/dags/ecommerce_incremental_ingestion`:
  **HTTP 401**, confirming that protected DAG metadata requires
  authentication. No new auth request was sent because acquiring a new JWT
  would require a non-GET auth flow; no JWT value was printed or persisted.

The runtime OpenAPI document identifies the exact DagRun creation contract:

| Field | Sanitized value |
|---|---|
| operationId | `trigger_dag_run` |
| method | `POST` |
| path | `/api/v2/dags/{dag_id}/dagRuns` |
| concrete prior path | `/api/v2/dags/ecommerce_incremental_ingestion/dagRuns` |
| request content type | `application/json` |
| request schema | `#/components/schemas/TriggerDAGRunPostBody` |
| required request fields | `logical_date` |
| optional request fields | `dag_run_id`, `data_interval_start`, `data_interval_end`, `run_after`, `conf`, `note` |
| documented responses | `200`, `400`, `401`, `403`, `404`, `409`, `422` |

This matches the prior `404` attempt's path and explains the separate prior
`422` validation result when `logical_date` was omitted. The OpenAPI contract
does not convert the operation into a GET: creating a DagRun remains a POST
operation. The prior `404` therefore remains unresolved as a route/runtime
behavior issue even though the local OpenAPI contract contains the operation.

Reference: [Airflow 3.1.6 Stable REST API](https://airflow.apache.org/docs/apache-airflow/3.1.6/stable-rest-api-ref.html)
(the live runtime `/openapi.json` was treated as authoritative for the
installed service contract).

### 4. Tests Added or Updated

No source or test files were changed in this read-only runtime diagnosis.
Within this session, documentation changes are limited to this plan record.

### 5. Tests Executed and Results

- GET-only route probe: **PASS** for `/docs`, `/redoc`, `/openapi.json`, and
  `/api/v2/monitor/health`; **404** recorded for `/api/v2/openapi.json`,
  `/api/v2/docs`, and `/api/v2/redoc`.
- In-memory OpenAPI parsing: **PASS**; `trigger_dag_run` and
  `TriggerDAGRunPostBody` were extracted with `logical_date` as the only
  required field.
- Protected DAG metadata without JWT: **HTTP 401**, recorded without body
  dump. No auth POST or token acquisition was attempted in this session.
- `git diff --check`: **PASS**.

### 6. Risks and Limitations

- GET/OpenAPI evidence proves that the installed API advertises the DagRun
  POST operation and its schema; it does not prove that a POST would be
  accepted, nor does it explain the prior `404` at the request execution
  boundary.
- No new JWT was acquired because the permitted method set was GET-only and
  the protected DAG route returned `401` without one. Existing prior
  memory-only JWT evidence was not reused as a persisted artifact.
- No DagRun, task, MinIO object, PostgreSQL row, Nessie ref, Spark job,
  checkpoint, watermark, retry, reset, cleanup, or cutover was performed.
- Runtime certification remains **OPEN / NOT CERTIFIED**.

### 7. QA Reviewer Handoff

Independently verify the changed plan section against the actual diff and the
sanitized command evidence. Confirm that only GET requests were issued, the
OpenAPI response was parsed without a full response dump, no JWT/secret was
printed or persisted, and the exact contract is recorded as
`trigger_dag_run` / `POST /api/v2/dags/{dag_id}/dagRuns` /
`TriggerDAGRunPostBody` with required `logical_date`. Do not treat this
diagnosis as authorization to retry the prior POST or trigger a DAG.

## Read-only API-server log and route diagnosis for latest 404 (2026-11-17, 02:01Z)

### 1. Approved Task and Scope

The approved task was read-only diagnosis of the latest candidate
`qualityit__20261117T__` attempt with `logical_date=2026-11-17T02:01:00Z`.
Only host-side Docker metadata, filtered `airflow-api-server` logs, and GET
OpenAPI metadata were inspected. No POST, DAG trigger, Docker exec, restart,
data-store access, secret/full-environment read, retry, reset, cleanup, or
cutover was performed.

### 2. Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` — appended this diagnosis
  and QA handoff record only.

### 3. Implementation Summary / Evidence

- The API container `ecommerce-data-platform-airflow-api-server-1` was
  running `ecommerce-airflow:3.1.7`, with `api-server` as its container
  command and host port `8085` published directly to container port `8080`.
  The container was running and not OOM-killed or restarting.
- Timestamped API-server logs correlate the latest attempt exactly:
  `GET /api/v2/dags/ecommerce_incremental_ingestion/dagRuns?limit=100`
  returned `200 OK` at `2026-11-17T02:00:59.847Z`, followed by
  `POST /api/v2/dags/ecommerce_incremental_ingestion/dagRuns` returning
  `404 Not Found` at `2026-11-17T02:00:59.929Z`. The logs contained no
  traceback/error line for this request and no token material was recorded.
- GET `/openapi.json` returned route metadata identifying
  `trigger_dag_run` on `POST /api/v2/dags/{dag_id}/dagRuns`, with
  `TriggerDAGRunPostBody` and required field `logical_date`. The API server
  therefore advertises the same POST operation used by the failed attempt.
- Classification: **not a base proxy/routing outage** (the request reached the
  API process and adjacent GET routes returned `200`); **not method handling**
  (`POST` is the advertised operation, so a method mismatch would normally be
  `405`, not this POST-specific `404`); **not a data-plane/application-task
  error** (no task execution occurred and no traceback/data-store evidence
  exists). The evidence narrows the failure to **application-level
  POST route/contract/state handling inside the Airflow API operation**. GET
  evidence cannot identify which POST-specific condition produced the 404.

### 4. Tests Added or Updated

No source or test files were changed. This was a read-only runtime diagnosis;
only this plan record was updated.

### 5. Tests Executed and Results

- Filtered timestamped `docker logs` for the API server: **PASS**; latest
  `02:01Z` GET/POST sequence and `404` were correlated.
- Host-side `docker inspect` state/command/labels/mount metadata: **PASS**;
  no restart/OOM state and direct `api-server` command were observed. No env
  or secret fields were read.
- GET `/openapi.json` route metadata extraction: **PASS**; operation, path,
  schema, required field, and documented response set were extracted without
  dumping the document.
- `git diff --check`: **PASS**; no whitespace errors were reported. Git emitted
  only existing LF/CRLF normalization warnings for modified working-tree files.

### 6. Risks and Limitations

- Uvicorn access logs do not include the response body or a request
  correlation ID, so they cannot distinguish DAG-state handling from another
  POST-specific Airflow condition.
- No POST is authorized by this diagnosis; no claim is made that a corrected
  trigger would succeed. Runtime certification remains **OPEN / NOT
  CERTIFIED**.
- Do not reuse the failed candidate or infer any ingestion, manifest,
  checksum, checkpoint, Iceberg, or Gold outcome from this 404.

### 7. QA Reviewer Handoff

Verify the timestamped log correlation, direct API-server route metadata, and
the classification boundaries above against the diff. Confirm that the
session issued no POST, Docker exec, restart, trigger, data-store operation,
or secret/full-environment read. Any follow-up POST or deeper application
diagnosis requires separate explicit approval and a fresh immutable candidate.

## Deeper read-only POST-boundary root-cause finding (2026-11-17)

### Approved Task and Scope

Read-only inspection of Airflow 3.1.7 route implementation/configuration,
DAG registration evidence, and runtime metadata to explain exact-DAG GET `200`
versus DagRun POST `404`. No code/config edit, POST, DAG trigger, Docker exec,
restart, data-store access, secret/full-environment read, retry, reset, cleanup,
or commit was performed.

### Root-cause finding

Airflow 3.1.7 has intentionally different predicates:

- `GET /api/v2/dags/{dag_id}` loads the latest DAG and returns `DagModel` by
  primary key; it does not exclude `is_stale`.
- `POST /api/v2/dags/{dag_id}/dagRuns` first selects the same `DagModel` with
  `~DagModel.is_stale`; if no non-stale row exists, it returns `404` before
  creating a DagRun.

This source-proven distinction explains the observed behavior. The actionable
root-cause classification is **stale/non-current DAG registration at the POST
boundary**, most likely dag-processor freshness/registration state. The live
`is_stale` value was not exposed in the sanitized evidence, so it remains the
required confirmation rather than an asserted database observation.

Supporting repo evidence: `docker-compose.yml:172-176` runs the API directly as
`api-server` on `8085:8080`, with no reverse-proxy service found; scheduler and
dag-processor mount `./airflow/dags` (`:190-218`), while API-server does not.
That mount asymmetry is a validation target, not yet proven as the fix because
Airflow 3 resolves the public route through serialized/metadata DAG state.
`airflow/dags/ecommerce_incremental_ingestion.py:26-35` declares the exact DAG
id, and local dag-processor logs show repeated DagBag loading for that file.

### Minimal approved fix proposal (not implemented)

1. Obtain separately approved sanitized read-only evidence for `is_stale`, DAG
   freshness/registration timestamp, and dag-processor/API process health.
2. If stale, restore the registration boundary: make dag-processor healthy,
   ensure it sees the same DAG source, and republish current serialized DAG
   metadata. Add an API-server DAG mount only if pinned-runtime evidence proves
   its DAG resolver requires it.
3. Add a focused regression/static contract test documenting that stale DAGs can
   be GET-readable but are not POST-triggerable. Preserve all fail-closed data,
   retry, watermark, checkpoint, Iceberg, and Gold gates.

### Tests / limitations / exact next step

- `rg`/file inspection and official Airflow 3.1.7 route-source inspection:
  **PASS**. No test or runtime mutation was run.
- Fresh `docker inspect/logs`: **BLOCKED** in this sandbox by Docker named-pipe
  permission; no `docker exec` or full-environment fallback was attempted.
- Runtime certification remains **OPEN / NOT CERTIFIED**. Do not reuse the
  failed candidate.
- Exact next step: obtain approved sanitized host-side Docker/API evidence for
  `is_stale` and freshness/registration health; then prepare the smallest
  registration correction and regression test for a new user approval gate.

## Approved sanitized Airflow runtime evidence — stale DAG confirmed (2026-11-17)

### Approved Task and Scope

The user approved an early, sanitized runtime evidence snapshot for
`ecommerce_incremental_ingestion`, using a short-lived Airflow admin JWT in
memory only. The scope was limited to authentication, GET-only Airflow health,
DAG metadata, DAG details/version metadata, and serialized-DAG route
availability. No DAG-trigger POST, retry, reset, cleanup, Docker exec/restart,
or data-store query was performed. No secret, JWT, `file_token`, or full
response was recorded.

### Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` — appended this
  sanitized evidence record only.

### Sanitized Runtime Evidence

- Auth token acquisition: **PASS**, `POST /auth/token` returned `201` and
  `token_acquired=true`; credential/JWT values were not emitted or persisted.
- `GET /api/v2/monitor/health`: **HTTP 200**. `dag_processor=healthy` with
  heartbeat `2026-11-17T02:26:35.904812+00:00`; `scheduler=healthy` with
  heartbeat `2026-11-17T02:26:36.223109+00:00`; `metadatabase=healthy`.
- `GET /api/v2/dags/ecommerce_incremental_ingestion/details`: **HTTP 200**.
  Relevant fields:

  | Field | Sanitized value |
  |---|---|
  | `dag_id` | `ecommerce_incremental_ingestion` |
  | `is_paused` | `false` |
  | `is_stale` | `true` |
  | `has_import_errors` | `true` |
  | `last_parsed_time` | `2026-11-16T16:40:28.248838Z` |
  | `last_parse_duration` | `0.35123939998447895` seconds |
  | `last_parsed` | `2026-11-17T02:26:43.892124Z` |
  | `bundle_name` | `dags-folder` |
  | `bundle_version` | empty |
  | `fileloc` | `/opt/airflow/dags/ecommerce_incremental_ingestion.py` |
  | `timetable_description` | `Never, external triggers only` |

- `latest_dag_version` was present in the details response with
  `version_number=32`, `created_at=2026-11-16T11:26:41.533570Z`, and
  `bundle_name=dags-folder`. `GET
  /api/v2/dags/ecommerce_incremental_ingestion/dagVersions/1` returned
  **HTTP 200**, proving the DAG-version metadata endpoint is available.
- `GET /api/v2/serialized-dags/ecommerce_incremental_ingestion` returned
  **HTTP 404**. The public serialized-DAG route was therefore not available at
  that path; the available DAG details/version metadata is not treated as a
  serialized payload read-back.
- API response fields `last_pickled` and `processor_subdir` were **absent**;
  their live values remain **unconfirmed**. No metadata DB query was opened in
  this early snapshot.

### Classification: Confirmed vs Hypothesis

**Confirmed:** the exact DAG identity is registered and GET-readable; it is
not paused; Airflow reports `is_stale=true`; Airflow reports
`has_import_errors=true`; the scheduler and dag-processor health endpoint
reports both components healthy with current heartbeats; parse/version
metadata exists; the direct serialized-DAG route tested is unavailable.

**Hypothesis, not runtime fact:** the prior DagRun POST `404` is explained by
the stale-registration predicate at the POST boundary, as established by the
earlier Airflow 3.1.7 route-source inspection. The current evidence strongly
supports that classification because `is_stale=true`, but does not identify
the underlying import error or prove why the dag-processor heartbeat remains
healthy while the DAG row is stale. `last_pickled` and `processor_subdir` must
not be inferred from the absent API fields.

### Tests Executed and Results

- Memory-only auth plus health and protected DAG GET probes: **PASS**.
- DAG details and DAG-version GET probes: **PASS**.
- Serialized-DAG route availability probe: **404 recorded**, no fallback write
  or mutation attempted.
- No test, trigger, retry, reset, cleanup, or data-store operation was run.

### Risks and Limitations

Runtime certification remains **OPEN / NOT CERTIFIED**. This evidence does not
prove task execution, ingestion output, manifests, checksums, lineage,
recovery, or data-store behavior. `has_import_errors=true` is confirmed but
the error details were intentionally not retrieved or printed. No conclusion
is made about `last_pickled` or `processor_subdir`.

### QA Reviewer Handoff

Review the exact sanitized values above against the GET-only command evidence,
verify that the JWT and secret-bearing fields were not persisted, and confirm
the boundary that stale registration is **confirmed** while its underlying
import/processor cause remains a **hypothesis**. The next action requires
separate approval; do not POST/trigger, restart, retry, reset, cleanup, or
query data stores from this evidence.

## Production config reconcile and GET-only stale/import verification (2026-11-17)

### Approved Task and Scope

User approved recreating only `airflow-scheduler` and
`airflow-dag-processor` with process-scoped production configuration. No
`.env` edit, other-service restart, DAG trigger, data-store write, checkpoint
or watermark mutation was performed.

### Sanitized host evidence

- Both named containers were recreated with `--no-deps --force-recreate` and
  are `running`, `oom=false`, image `ecommerce-airflow:3.1.7`.
- Both match `BRONZE_BUCKET=ecommerce-lakehouse-prod`,
  `NESSIE_REF=gold_prod_current`, `LAKEHOUSE_STORAGE_ENV=production`, and
  `INGESTION_WRITE_ENABLED=true`.
- Lakehouse/warehouse/Gold writes, publication, historical backfill, and all
  six Iceberg cutover flags are disabled; historical mode is `DRY_RUN`.
- `AIRFLOW_LOCAL_E2E_MODE`, all `LAKEHOUSE_E2E_*`, and all `QUALITY_IT_*`
  keys are **UNSET** in both recreated processes. API server was not touched.

### GET-only Airflow evidence

- Auth token acquisition: **PASS**; token held in memory only and not emitted.
- `GET /api/v2/monitor/health`: **HTTP 200**; dag processor, scheduler, and
  metadatabase are `healthy`.
- `GET /api/v2/importErrors?limit=100&offset=0`: **HTTP 200**,
  `total_entries=0`; import error is cleared.
- `GET /api/v2/dags/ecommerce_incremental_ingestion/details`: **HTTP 200**;
  `is_stale=false`, `has_import_errors=false`, `is_paused=false`, with fresh
  parse metadata (`last_parsed=2026-11-17T02:39:36.134936Z`).

### Tests, risks, and QA handoff

- Verification executed: sanitized Docker inspect, Compose config validation,
  service recreate, and authenticated GET-only probes. No DAG trigger or
  external data-service test was run.
- Runtime certification remains **OPEN / NOT CERTIFIED**: this proves config
  reconciliation and import/stale recovery only, not ingestion/Gold output,
  manifests, checksums, lineage, or recovery.
- QA reviewer: independently verify the allowlisted host evidence,
  `importErrors=0`, `is_stale=false`, no API-server restart, and no trigger.

## Approved GET-only Airflow import-error diagnosis (2026-11-17)

### Approved Task and Scope

Chốt read-only diagnosis cho `ecommerce_incremental_ingestion` bằng Airflow
admin JWT memory-only và GET-only endpoints sau bước auth. Không trigger/retry,
reset/cleanup, Docker exec/restart, metadata/data-store query, hoặc runtime
mutation. Không lưu/emitted credential, JWT, full environment hay full traceback.

### Files Changed

- `docs/plans/semantic-runtime-certification-plan.md` — appended sanitized
  evidence only.

### Exact Sanitized Import-Error Evidence

- Auth: **PASS**, `POST /auth/token` trả `201`; JWT chỉ tồn tại trong memory và
  được clear sau probe.
- `GET /api/v2/monitor/health`: **HTTP 200**.
- `GET /api/v2/importErrors?limit=100&offset=0`: **HTTP 200**,
  `total_entries=1`, đúng một item.
- Item: `import_error_id=6`, `timestamp=2026-11-17T02:32:37.357974Z`,
  `bundle_name=dags-folder`, `filename=ecommerce_incremental_ingestion.py`.
- Sanitized summary: exception type
  `common.storage_layout.StorageLayoutError`; source `line 222`, trong
  `create_gold_dimensional_operator`; message:
  `Production cannot use quality-it or E2E flags`.
- Không ghi lại credentials, JWT, host/container path, secret value hoặc full
  traceback.

### Filtering, Likely Fix Scope, and Limits

Response chỉ có `filename`, không có `dag_id`. Query `dag_id=...`, exact
filename và filename không tồn tại đều trả cùng một item; server-side exact
DAG filtering **không được endpoint hỗ trợ/không được verify**. Attribution
dựa trên exact returned filename.

Likely fix scope (chưa implement): reconcile redacted production runtime
configuration để dùng production bucket/layout/ref và không truyền quality-it
hoặc E2E flags; giữ nguyên fail-closed guard tại
`airflow/dags/pipeline_operators.py:222` và `common/storage_layout.py:97-104`.
Tên flag vi phạm chưa được xác định vì không mở full environment. Không sửa
guard, không enable writes, và không mutate storage/checkpoint/ref state.

Runtime certification vẫn **OPEN / NOT CERTIFIED**. GET-only import-error probe
và sanitized evidence: **PASS**; không có unit/DagBag test, trigger, retry,
reset, cleanup hay external data-service test. QA reviewer chỉ cần verify
evidence, redaction, filtering claim và fix scope; mọi config edit hoặc
recheck tiếp theo cần explicit approval mới.
## Fresh production Silver identity verification — completed (2026-11-17, 03:17Z)

The user separately approved one fresh production Silver run after the
canonical manifest identity remediation. The prior candidate was not reused;
no retry, checkpoint reset, cleanup, Gold launch, cutover, commit, or push was
performed.

### Runtime evidence

- Candidate: `production_silver__20261117T031723Z__3d525ed68c967f928f85538a3f7091db`
- DagRun state: `success`; start `2026-11-17T03:17:24.224471Z`; end
  `2026-11-17T03:17:55.104630Z`.
- Task read-back: 59 total; 42 `success`; 17 `skipped`; 0 failed or upstream
  failed.
- `customers` and `products`: `SKIPPED / NO_DATA`, consistent with the
  independently reviewed effective-checkpoint diagnosis.
- `orders`, `order_items`, `payments`, and `shipments`: `PASS`.

### Manifest identity read-back

For each committed table, the exact `manifest_key` returned by the Silver
result XCom was read from MinIO and checked with the shared canonical helper.
All four rows satisfied `XCom identity = stored MinIO identity = canonical
identity`:

| Table | Row count | Identity result |
|---|---:|---|
| `orders` | 705 | PASS |
| `order_items` | 1000 | PASS |
| `payments` | 880 | PASS |
| `shipments` | 659 | PASS |

The two `NO_DATA` tables correctly produced no manifest. This closes the
previous manifest-identity blocker for the Silver boundary. Gold and consumer
cutover remain disabled and are not certified by this evidence.

### Verification and QA handoff

- Airflow state and task counts: GET-only read-back, PASS.
- XCom/MinIO/canonical identity comparison: read-only, PASS for all committed
  tables.
- No credentials, JWT, full source rows, or secrets were recorded.
- Required next step: independent reviewer validation of this runtime record;
  if QA passes, prepare a separate approval gate for any Gold runtime run.

### Independent QA verdict

**PASS WITH WARNINGS.** The reviewer confirmed the candidate identity, terminal
Airflow state, task counts, expected `NO_DATA` semantics, and all four
XCom/MinIO/canonical identity matches. The Silver identity blocker is closed
for the four committed tables. The reviewer found no Critical, High, or Medium
finding; Gold and consumer cutover remain uncertified and require a separate
approval gate.
