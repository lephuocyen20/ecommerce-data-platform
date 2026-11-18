# Production Lakehouse Layout Plan

## Scope

Introduce an environment-separated production layout without migrating or
deleting existing E2E objects.

- Production bucket: `ecommerce-lakehouse-prod`
- Quality bucket: `ecommerce-lakehouse-quality-it`
- Legacy bounded E2E bucket: `ecommerce-data-lake`, test-only and deny-by-default
- Production layout: `bronze/<table>/`, `silver/<table>/`, `gold/<table>/`
- Production Nessie ref: `gold_prod_current`
- Quality E2E layout remains `quality-it/<run-id>/`

## Safety boundaries

- No object copy, rename, delete, overwrite, checkpoint reset or watermark reset.
- No production bucket creation or production write in the code/config phase.
- No E2E-to-production promotion.
- No Iceberg consumer cutover or stable-ref promotion in this phase.
- Existing legacy batch recovery must continue to use the location recorded in
  batch metadata rather than infer a new path.

## Implementation scope

1. Add a canonical storage-layout contract and environment-aware bucket guards.
2. Add production object-key builders while retaining legacy reader support.
3. Include bucket/layout identity in batch and manifest contracts where needed.
4. Update Silver manifest/data prefixes and preserve parent coverage checks.
5. Separate production Gold warehouse/ref/control metadata from quality E2E.
6. Update Airflow/Spark/Trino/Compose examples and documentation.
7. Add unit/contract tests for isolation, legacy recovery, path validation and
   pre-client validation.

## Acceptance criteria

- Production paths are `bronze/<table>/...`, `silver/<table>/...` and
  `gold/<table>/...` under `ecommerce-lakehouse-prod`.
- Quality E2E remains bounded to its approved run prefix.
- Production rejects quality buckets and quality mode flags.
- Legacy batches recover from persisted object locations.
- Watermarks/checkpoints are not reset or advanced before commit.
- Gold production ref/schema/warehouse are isolated from E2E.
- No runtime production write, backfill or cutover occurs before separate
  approval gates.

## Verification

- Static path/safety audit with `rg`.
- `compileall` for common, ingestion, silver, gold and airflow.
- Focused unit/contract tests plus `git diff --check`.
- Airflow container DAG import check.
- Compose/config validation only; no production service startup.
- Independent QA against this plan and the implementation diff.

## Approval gates

- Gate A: target bucket/ref/layout approved — **approved** (`ecommerce-lakehouse-prod`).
- Gate B: implementation diff and tests — required before merge.
- Gate C: production shadow validation — separate approval.
- Gate D: production backfill/write scope — separate approval.
- Gate E: stable-ref promotion/consumer cutover — separate approval.

## Current status

Plan approved for implementation. Storage contract/config implementation is
complete in this session; no production storage mutation has been performed.

### Session verification evidence

- Added pure `common/storage_layout.py` validation and layer key builders.
- Preserved legacy `bronze_incremental/...` and `silver_incremental/...`
  behavior when no canonical environment is selected, including bounded E2E
  scope handling.
- Updated production and quality examples plus the smallest Airflow/Silver/
  Gold config consumers.
- Focused tests: `.venv\\Scripts\\python.exe -m pytest tests/test_storage_layout.py tests/test_bounded_e2e_scope.py tests/test_extractor.py tests/test_recovery.py -q` — **38 passed**.
- Compile check: `.venv\\Scripts\\python.exe -m compileall -q common ingestion silver gold airflow` — **passed**.
- `git diff --check` — **passed** (only pre-existing line-ending warnings).

Independent QA of the implementation diff remains the next gate; production
shadow validation, writes, cutover and runtime service checks remain blocked
until separately approved.

### QA correction session evidence

- Recovery validates and uses persisted `metadata['object_key']`; it does not
  derive a canonical path for legacy batches. `common.paths` now consistently
  honors the passed `environ` for legacy bounded scopes.
- Gold CLI, Silver entrypoint and Gold Airflow manifest client paths validate
  storage configuration before Spark/MinIO client creation and fail closed.
- Production Gold `WRITE` remains denied unless
  `PRODUCTION_LAKEHOUSE_WRITE_APPROVED=1` is explicitly present.
- `QUALITY_IT_RUN_ID` and `QUALITY_IT_PREFIX` are propagated into Spark
  operator environments; stale quality bucket test references were corrected.
- Focused tests: `.venv\\Scripts\\python.exe -m pytest -q tests/test_storage_layout.py tests/test_recovery.py tests/test_gold_orchestration.py tests/test_airflow_parse_imports.py tests/test_lakehouse_safety_guards.py` — **71 passed**.
- Independent QA verdict and any runtime certification remain pending; no
 external service or production mutation was performed.

### QA round-2 correction evidence

- Silver manifest verification now validates the storage environment before
  creating a MinIO client, resolves canonical production `silver/<table>/`
  paths through the shared path helper, and retains bounded quality/legacy
  scope handling.
- Spark child operators now default `NESSIE_REF` to `gold_prod_current`.
- Focused tests: `.venv\\Scripts\\python.exe -m pytest tests/test_airflow_parse_imports.py tests/test_storage_layout.py tests/test_silver_coverage_producer.py -q` — **51 passed**.
- Compile check: `.venv\\Scripts\\python.exe -m compileall -q airflow/dags common` — **passed**.
- `git diff --check` — **passed** with pre-existing line-ending warnings only.
- No external service, storage, database, watermark, checkpoint, cutover,
  commit or merge operation was performed. Independent QA review remains the
  next handoff.

### Final QA round-4 evidence

- Silver summary validation now uses shared storage-layout helpers for
  production, quality and legacy paths; the undefined helper regression is
  covered by behavioral WRITE tests.
- Independent QA round 4: **PASS WITH WARNINGS**, with no
  Critical/High/Medium/Low findings.
- Evidence: correction suite **32 passed**, prior layout/recovery/safety
  suite **73 passed**, compileall **PASS**, and `git diff --check` **PASS**.
- No production bucket creation, external write, backfill, checkpoint or
  watermark reset, stable-ref promotion, or consumer cutover was performed.
- Remaining gate: production DRY_RUN/shadow validation requires separate
  approval; production WRITE and cutover remain disabled.

### Production DRY_RUN shadow validation

- Approved read-only validation completed with no bucket creation, client-side
  data write, backfill, checkpoint/watermark mutation or cutover.
- Evidence: **76 tests passed**; compileall **PASS**; production and quality
  Compose config validation **PASS**; direct Airflow-container DAG import with
  `LAKEHOUSE_STORAGE_ENV=production`, bucket
  `ecommerce-lakehouse-prod`, ref `gold_prod_current` and all WRITE flags off
  returned `PRODUCTION_DAG_IMPORT_OK`.
- Production Gold WRITE guard was verified to remain deny-by-default without
  `PRODUCTION_LAKEHOUSE_WRITE_APPROVED=1`.
- The running scheduler still carries the existing quality E2E environment;
  its `airflow dags list-import-errors` output is not used as production
  shadow evidence. A production service startup or WRITE run remains a
  separate approval gate.
