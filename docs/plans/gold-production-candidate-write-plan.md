# Plan: Isolated production Gold candidate WRITE

## Gate C artifact bundle correction — 2026-11-17

Status: **IMPLEMENTED — STATIC/UNIT QA PASS; RUNTIME NOT CERTIFIED**

### Approved scope and evidence

- Added pure `gold/implementation/gold_candidate_artifacts.py`; it creates no
  Spark, MinIO, PostgreSQL or Nessie client and performs no writes.
- `build_silver_fact_bundle()` requires exactly `orders`, `order_items`,
  `payments`, and `shipments` from one approved `dag_run_id` and one matching
  `source_run_identity`. It validates/enriches checksum, exact manifest key
  selection, canonical manifest identity when the manifest is supplied,
  coverage binding and lineage identity.
- `build_gold_snapshot_bundle()` delegates only to the existing pure
  `GoldSnapshotReuseAdapter`, preserving validation of schema/graph/manifest
  identities, snapshot ID, Gold catalog/namespace/ref, stale policy and
  readback identity. Missing or fabricated evidence fails closed.
- Candidate preflight now accepts the canonical `{silver_run,
  gold_snapshots}` envelope through `validate_fact_and_dimension_bundle()`;
  the legacy flat contract remains unchanged for compatibility.
- Production flags remain disabled; no Nessie ref, object, database,
  checkpoint or watermark was created or modified.

### Changed files

- `gold/implementation/gold_candidate_artifacts.py`
- `gold/implementation/gold_candidate_contract.py`
- `tests/test_gold_candidate_artifact_bundle.py`
- this plan

### Verification evidence

- Focused candidate/snapshot suite: **62 passed**.
- `python -m compileall -q gold airflow tests`: **PASS**.
- Scoped `git diff --check`: **PASS**.
- Tests are pure unit tests; no external-service test marker or client was
  used.

### QA verdict and handoff

Static diff review: **PASS WITH WARNINGS**. The Gate C artifact contract is
closed for JSON evidence and the preflight boundary is fail-closed. Runtime
certification remains **NOT CERTIFIED**: do not trigger a DagRun, create a
Nessie ref, enable Gold WRITE, or perform any external read/write as part of
this session. Next step is an independently approved read-only Gate C
preflight using real metadata, followed by independent QA of that evidence.

Status: **IMPLEMENTATION COMPLETE - QA PASS WITH WARNINGS; RUNTIME NOT CERTIFIED**

## Objective

Create a candidate-only production Gold runner that writes exactly seven Gold
tables to a new Nessie ref/namespace derived from `gold_prod_current`, without
writing or advancing the stable ref and without invoking publication/cutover.

## Hard invariants

- Target ref is explicit, immutable and never `gold_prod_current`.
- Candidate branch is created from the read-back `gold_prod_current` HEAD and
  read back with matching name/type/hash; source HEAD is re-read before WRITE.
- Approved Silver run supplies exactly four current `PASS` fact artifacts:
  `orders`, `order_items`, `payments`, `shipments`.
- `dim_customer` and `dim_product` use pinned, verified, non-stale snapshot
  reuse artifacts. No fact snapshot reuse.
- Preflight rejects wrong bucket/ref/namespace, missing or mismatched
  artifacts, active publication/cutover/cleanup/checkpoint flags, and partial
  candidate reuse before creating Spark/MinIO/Nessie clients where possible.
- WRITE produces exactly 7 tables, 7 table manifests and one completed graph
  manifest under the candidate target, followed by exact read-back.
- No stable publish, consumer cutover, cleanup, delete, rollback, compaction,
  expiration, checkpoint or watermark mutation.

## Planned implementation scope

- New candidate contract/orchestrator module for identity, target validation,
  preflight, lifecycle and read-back status.
- Nessie adapter support for candidate branch creation from an explicit source
  hash with read-back and source-head race detection.
- Explicit candidate ref/namespace propagation through dimensional
  `run_pipeline.py`, writer and snapshot reuse reader.
- Immutable approved Silver artifact bundle loader/validator.
- Candidate-only Gold execution path with publication task disconnected.
- Unit/regression tests for ref isolation, CAS/race cases, artifact bundle,
  snapshot pinning, seven-table read-back and forbidden operations.

### Implementation evidence (candidate-only QA correction)

- Candidate WRITE uses `ProductionGoldCandidateAdapter` and the native
  dimensional writer's `CandidateDimensionalWriteGuard`; arbitrary writer
  callbacks are rejected.
- Readback is bound to candidate ref/namespace and validates seven non-empty
  table manifests, graph references, manifest identities and snapshot IDs.
- Silver facts require one current DagRun, manifest key/identity, checksum and
  lineage evidence; fact snapshot reuse is rejected.
- Candidate ref validation is applied at the contract and Nessie helper;
  metadata persistence and mutation flags are denied.

## Acceptance criteria

- Unit/static tests pass for all hard invariants and failure paths.
- No production runtime write occurs during implementation or QA.
- Independent QA passes the implementation diff and plan.
- Only after a separate runtime gate: candidate branch creation, candidate
  Gold WRITE, seven-table read-back and `CANDIDATE_VERIFIED` evidence.
- Stable publish/cutover remains a separate later phase.

## Verification

```powershell
.venv\Scripts\python.exe -m pytest -q tests/test_gold_candidate_runner.py tests/test_gold_candidate_nessie.py tests/test_gold_candidate_artifacts.py tests/test_gold_candidate_readback.py
.venv\Scripts\python.exe -m pytest -q tests/test_gold_snapshot_reuse_contract.py tests/test_gold_dimensional_lineage.py tests/test_gold_dimensional_verification.py tests/test_gold_orchestration.py
.venv\Scripts\python.exe -m compileall -q gold airflow tests
git diff --check
```

## Risks and rollback

The highest risk is accidental routing to `gold_prod_current`; reject it at
contract and runtime target layers. Partial candidates are quarantined and
never deleted or rolled back; future attempts use a new immutable identity.
Rollback is configuration-only: disable the runner and write approval. No
Nessie/Iceberg/object/checkpoint destructive recovery is allowed.

## Independent implementation QA verdict

**PASS WITH WARNINGS.** Candidate isolation, native writer integration, source
HEAD race handling, four current Silver fact validation, pinned dimension
reuse, exact seven-table readback, and mutation/publication guards passed
static QA and focused tests. The runtime candidate WRITE gate remains open and
has not been executed; no Nessie ref or production data was mutated.

## Gate C — production preflight result

Read-only preflight inspected the approved Silver DagRun
`production_silver__20261117T031723Z__3d525ed68c967f928f85538a3f7091db`, its
four fact XCom results, the `gold_prod_current` Nessie ref, and existing Gold
manifests. Airflow and Nessie health/readback were available, but the gate is
**BLOCKED before candidate creation**:

- The four fact XCom envelopes do not yet carry the checksum and lineage fields
  required by the candidate bundle contract. The corresponding stored Silver
  manifests do carry checksum, lineage evidence and producer `run_id`, so a
  read-only bundle builder may enrich the XCom result only from those exact
  manifest keys; it must not invent values.
- Existing `dim_customer`/`dim_product` Gold manifests expose snapshot and
  basic readback evidence, but not the complete pinned reuse contract required
  here: schema identity, per-table graph identity, stale policy and ref-bound
  readback.

No Nessie branch/ref was created and no Spark, Iceberg, MinIO, PostgreSQL,
checkpoint or watermark mutation occurred. The next implementation step is to
produce a verified immutable artifact bundle from read-only source/Gold
metadata, then repeat Gate C; do not bypass these missing fields or run WRITE.

## Full-Silver baseline preflight and correction scope (2026-11-18)

- Read-only XCom inspection confirmed source run
  `manual__gold_v2_silver_recovery_current_20261117` has `PASS` artifacts and
  deterministic manifest keys/identities for all six Silver tables, including
  `customers` and `products`.
- The current candidate CLI cannot consume those two dimension Silver
  artifacts: it requires them to be represented as `NO_DATA` with verified
  pinned Gold snapshot reuse. Existing Gold snapshot metadata is not suitable
  for that role.
- Before runtime, the approved implementation correction must add a verified
  full-Silver dimension path for candidate baseline writes. The incremental
  pinned-snapshot reuse path remains unchanged and fail-closed.
- No candidate ref, Iceberg table, MinIO object, PostgreSQL row, checkpoint,
  watermark, stable ref or publication was mutated by this preflight.

## Legacy flat compatibility HIGH finding remediation (2026-11-18)

### Approved Task and Scope

Harden only `gold/implementation/gold_candidate_contract.py` legacy flat
compatibility validation. Embedded manifests must match the declared
`manifest_identity` through the project canonical manifest identity helper,
including non-circular self-identity handling. The four legacy facts must
carry one shared, non-empty `source_run_identity`. The full-Silver and
incremental snapshot-reuse paths remain unchanged, and all pre-client guards
remain pure and fail-closed.

### Implementation and verification evidence

- Added canonical embedded-manifest validation using
  `common.manifest.canonical_manifest_identity`.
- Added a shared non-empty source-run identity check across all four legacy
  facts.
- Added regressions for embedded-manifest tampering and missing/empty/mixed
  source identities; updated the legacy fixture to include the valid identity.
- Scoped candidate/reuse/lineage/orchestration suite: **163 passed**.
- `.venv\\Scripts\\python.exe -m compileall -q gold/implementation tests/test_gold_candidate_artifact_bundle.py tests/test_gold_candidate_artifacts.py`: **PASS**.
- `git diff --check`: **PASS** (pre-existing LF/CRLF warnings only).
- No runtime, DAG, external client, candidate ref, WRITE, flag, checkpoint,
  watermark or Iceberg operation was executed.

### QA handoff

Implementation verification is complete. Independent reviewer QA remains the
next gate; review the actual contract/test diff against this scope, with
special attention to legacy caller compatibility and preservation of the
pre-client boundary. Runtime certification and cutover remain out of scope.

## Runtime candidate attempt — blocked by partial candidate state (2026-11-18)

- The approved bundle was built from the six exact Silver manifests and passed
  the full-Silver contract with `parquet_lineage`.
- The CLI date contract fix and `CandidateScope.key()` fix passed independent
  QA with warnings; the targeted local suite passed **37 tests**.
- The bounded Spark WRITE created/advanced the approved candidate ref and
  candidate metadata, but did not produce a verified `GOLD_CANDIDATE_RESULT`.
  Read-only Nessie entries show only `dim_date` under
  `gold_candidate_v2_cert_20261118_r01`; the candidate state is therefore
  partial and **not certified**.
- Stable ref remains unchanged at
  `a4274ec5b703fc37a34ecb8efab17931773aa82987e37ca324a7c798cbfb59e7`.
- The candidate ref is now at
  `cb863ccc8e831ab6423caec569e815c277f9a3f8bcd3f32964d6e5847f3d049e`.
  Re-running the same ref is correctly blocked by the source-head/CAS guard
  because the candidate branch is no longer equal to the stable source head.
- No cleanup, rollback, stable-ref update, publication or consumer cutover was
  performed. The next action requires explicit approval for a new immutable
  candidate ref/namespace (for example `..._r02`), leaving the partial `r01`
  state preserved for audit.

## Candidate r02 attempt - blocked by persistent graph fence (2026-11-18)

- User approved and the exact full-Silver bundle was rebuilt for
  `gold_prod_candidate_v2_cert_20261118_r02` /
  `gold_candidate_v2_cert_20261118_r02`.
- The stable source HEAD remained unchanged and r02 was created as a new
  candidate branch. No stable publication or cutover was attempted.
- The writer was blocked before new table writes by the existing immutable
  graph fence:
  `gold/dimensional/locks/v1/d676b32f01497e88fd30d48e4641f9f40f9f25745fa7b944074a9bcfbc4a37c4.json`.
  Its payload is a matching `gold_dimensional_graph_fence` for the graph
  identity, so this is `GRAPH_FENCE_ALREADY_HELD`, not a lock conflict.
- Read-only inspection confirms the lock is present and the global fence is
  absent. The implementation intentionally requires operator recovery and
  never auto-deletes this lock. Separate explicit approval is required for
  lock/partial-state recovery; no deletion, overwrite, rollback or cleanup was
  performed.

## Recovery attempt result - r02 partial again (2026-11-18)

- The explicitly approved graph-fence recovery removed only the verified
  `d676...37c4.json` fence object; stable ref and all source/checkpoint state
  were untouched.
- r02 passed source bundle validation, ref creation and Spark read-only source
  checks. The writer persisted only the `dim_date` Iceberg entry and stopped
  without emitting `GOLD_CANDIDATE_RESULT`; r02 is **not certified**.
- Read-only verification: stable ref remains
  `a4274ec5b703fc37a34ecb8efab17931773aa82987e37ca324a7c798cbfb59e7`;
  r02 is `2fbc6f5920f459aa8b387d5f7d31b84dd4edfa24c721508ad4ea7e3c32eb7983`
  with only `gold_candidate_v2_cert_20261118_r02.dim_date`.
- Exact six Silver Parquet reads and all seven frame/quality transformations
  passed read-only diagnosis. No further WRITE retry will be issued until the
  write-stage failure is isolated and corrected; another retry would only
  create another partial state.

## Candidate r03 runtime diagnosis - repeated single-table partial (2026-11-18)

- r03 was created from the unchanged stable HEAD with the exact six-table
  bundle. The runtime again persisted only `dim_date` and emitted no candidate
  result; stable ref remained unchanged.
- Read-only source reads and all seven frame/quality transformations passed.
- An isolated candidate-scope `dim_customer` CREATE, MERGE, snapshot lookup and
  pinned readback also passed, so the remaining defect is in the multi-table
  writer/session orchestration boundary rather than source schema or the
  individual dim_customer Iceberg operation.
- No further candidate retry is authorized by this plan until the writer is
  corrected and independently reviewed; repeated retries would create more
  partial candidate branches/entries.

## Candidate r04 runtime certification - PASS pending independent QA (2026-11-18)

- Fixed `captured_at` propagation through candidate, historical and local WRITE
  callers. Independent code QA verdict: **PASS**.
- r04 completed the graph: 7/7 Nessie Iceberg table entries and the graph
  manifest are present under the candidate ref/namespace.
- Independent Spark readback passed for all seven pinned snapshots, including
  positive file/byte/row evidence. Evidence is persisted at
  `docs/evidence/gold-production-candidate-r04-runtime-20261118.json`.
- Stable ref hash remains unchanged; no publication, cutover or cleanup was
  performed. Runtime certification remains pending independent evidence QA.

### Independent runtime QA closure

- Verdict: **PASS WITH WARNINGS**.
- QA independently confirmed 7/7 tables, positive snapshot/file/byte/row
  readbacks, candidate ref/namespace binding, unchanged stable ref, and
  `published=false`, `cutover=false`, `cleanup=false`.
- Warning: the candidate process did not emit captured `GOLD_CANDIDATE_RESULT`
  stdout; independent MinIO/Nessie/Spark verification is the certification
  evidence. Future runs should persist stdout for traceability.
- No blocking Critical/High/Medium finding remains. Stable publication and
  consumer cutover remain separate approval gates.

## Stable promotion and continuous incremental WRITE (2026-11-18)

- Read-only parity passed for all seven r04 tables against frames rebuilt from
  the exact six Silver manifests.
- Explicitly approved CAS promotion advanced `gold_prod_current` from
  `a4274ec5b703fc37a34ecb8efab17931773aa82987e37ca324a7c798cbfb59e7` to
  candidate r04 hash
  `a94321570216b4cfae1e81306400c063bccdb82177242722f2c165289569c6a5`.
- Post-promotion stable readback passed for all seven tables.
- Continuous incremental WRITE was enabled in active `.env` and Airflow was
  started/unpaused with `GOLD_DIMENSIONAL_ENABLED=true`,
  `GOLD_DIMENSIONAL_MODE=WRITE`, `GOLD_DIMENSIONAL_WRITE_ENABLED=true`,
  `INGESTION_WRITE_ENABLED=true`, and
  `PRODUCTION_LAKEHOUSE_WRITE_APPROVED=1`.
- `BI_ACTIVE_PUBLISH_APPLY` remains `0`; no further automatic stable
  promotion/cutover is enabled by the DAG.
