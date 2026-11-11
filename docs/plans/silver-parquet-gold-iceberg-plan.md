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
