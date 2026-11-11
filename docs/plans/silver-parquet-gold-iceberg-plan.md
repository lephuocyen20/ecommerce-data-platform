# Silver Parquet to Gold Iceberg Refactor

Date: 2026-11-11

## Approved scope

Change the supported main path to `Bronze -> Silver Parquet -> Gold Iceberg`.
The Airflow DAG must no longer execute Silver Parquet-to-Iceberg migration,
Iceberg attestation, Iceberg reconciliation, or Iceberg quality tasks.

## Implementation

- Build a strict Gold source lineage contract from verified Silver completion
  manifests instead of Silver Iceberg snapshots.
- Treat each Silver manifest as an incremental batch: Gold assembles all
  completed manifests per table and keeps the newest row per business key and
  `updated_at` watermark before dimensional transformation.
- Make the Gold dimensional reader revalidate the manifest and Parquet object
  inventory, then read the immutable Silver Parquet prefix.
- Keep Iceberg initialization and writes inside the Gold dimensional writer.
- Remove the four Iceberg Silver operators from the main DAG while retaining
  their modules for explicit legacy/backfill use.
- Update local configuration, documentation, and focused tests to reflect the
  new route.

## Acceptance criteria

- Main DAG contains no migration, attestation, reconciliation, or Iceberg
  quality task between Silver and Gold.
- Gold lineage has `source_type=parquet_manifest` and contains no Silver
  snapshot, Nessie ref, or Silver attestation requirement.
- Gold reads only manifest-verified Silver Parquet data and still fails closed
  on missing, tampered, or schema-drifting inputs.
- Gold Iceberg output behavior and write guards remain unchanged.
- Focused unit/static tests pass and runtime Python compilation passes.

## Risks

- Existing historical tests and local evidence fixtures use Silver Iceberg
  lineage and must be migrated or explicitly retained as legacy coverage.
- This change does not delete old migration code or existing Iceberg tables;
  cleanup of external data remains out of scope.

## Verification

- Airflow source/parse tests.
- Gold lineage and Parquet reader tests.
- Gold writer/metadata regression tests.
- `python -m compileall` for runtime packages.

## QA verdict

Independent QA: **PASS**. The reviewer specifically rechecked that every
historical manifest key and data prefix is bounded before any Parquet read, and
the negative regression test passed without a bucket-mismatch false positive.

Final static/unit evidence: `345 passed, 78 deselected, 15 subtests passed`,
compileall PASS, and `git diff --check` PASS. Runtime Docker E2E and Gold WRITE
cutover remain separate approval gates.

## DAG UI lineage follow-up

The Airflow graph now exposes table-level lineage while preserving task IDs
and the approved runtime dependency contract:

- `bronze/<table>` groups source ingestion and validation.
- `silver_parquet/<table>` groups eligibility, transformation, verification,
  result and Parquet artifact creation.
- `gold_dimensional_write` is the root-level Gold orchestration task.
- Six `dimensional_parquet_artifact_*` tasks are upstream of
  `gold_dimensional_write`.
- `gold_dimensions` and `gold_facts` are non-writing marker groups for the
  seven downstream confirmations: three dimensions and four facts.

Verification: Airflow container DagBag reports `No data found` for import
errors, `12 passed`, `git diff --check` PASS, compileall PASS, and independent
QA PASS. The local venv still lacks `airflow.sdk`; runtime verification was
performed in the Airflow 3 container.

Visual follow-up: the six `dimensional_parquet_artifact_*` tasks now connect
directly to `gold_dimensional_write`, while `gold_dimensional_write` is at DAG
root and `gold_dimensions`/`gold_facts` expose the seven downstream
confirmation markers. This removes the obsolete `gold_iceberg` topology and
keeps the Dim/Fact outputs visible in Graph View without changing task IDs or
the approved runtime dependency contract.

Historical note: earlier plan revisions described the confirmation markers as
nested under `gold_iceberg/dimensions` and `gold_iceberg/facts`. That was the
former UI topology and is retained only as historical context; operational
topology is the root writer plus `gold_dimensions` and `gold_facts` markers
described above.

The control-only nodes `pipeline_summary`, `silver_completion_barrier`, and
`skip_gold_dimensional` are no longer separate DAG tasks. Summary validation,
completion gating, and disabled-Gold handling now execute inside the single
Gold gate task. The visible path remains the data flow and Gold
processing/output groups.

## Gold UI consolidation follow-up

The remaining Gold orchestration nodes are consolidated into one Airflow task,
`gold_dimensional_write`. Its custom Docker operator performs the fail-closed
Silver summary/artifact checks before Spark starts, while the Spark runner
performs lineage construction, Iceberg write, and completed-graph verification
within the same task lifecycle. This intentionally trades separate Gold
gate/lineage/write/verify log, retry, and status nodes for a compact data-table
oriented UI.

Verification: Airflow container DagBag `No data found`, targeted Gold
regression `66 passed`, compileall PASS, diff-check PASS, and independent QA
PASS. After QA identified a missing scheduling edge, the explicit dependency
from all six `dimensional_parquet_artifact_*` tasks to `gold_dimensional_write`
was added and reverified. Final targeted evidence: `52 passed`.

The seven downstream output markers display as
`gold_dimensional_write_dim_*` and `gold_dimensional_write_fact_*` to make the
Dim/Fact targets explicit in Graph View while
retaining the historical `confirm_gold_dimensional_*` task IDs. They remain
non-writing markers; the upstream `gold_dimensional_write` task owns the
atomic Iceberg publication.
