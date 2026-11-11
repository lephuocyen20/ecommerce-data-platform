# Cleanup Audit Plan

Date: 2026-11-11

## Scope

Remove ignored local artifacts that still describe the retired Warehouse/dbt
branch or generate references to the deleted `warehouse/` package. Preserve
runtime compatibility objects and active local landing data.

## Acceptance criteria

- Retired ignored docs, presentation assets, and stale helper scripts are gone.
- `source/database/migrations/003_add_iceberg_warehouse_lineage.sql`,
  `warehouse.loaded_batches`, and `staging/` are preserved.
- No tracked runtime file references the deleted aggregate Gold branch.
- Working tree and ignored-file inventory are rechecked after cleanup.

## Risks and decision

The legacy `loaded_batches` schema and migration remain because the current
lakehouse documentation explicitly reserves them for controlled backfill and
reconciliation. Removing them requires a separate database compatibility
decision and is outside this cleanup.

## Verification

- `git status --short`
- `rg` scan for retired Gold/dbt references
- Python compile check for tracked runtime sources

## Result

Completed. The ignored legacy docs, presentation bundle, and helper scripts
were removed. The compatibility migration, `warehouse.loaded_batches`, and
active `staging/` data were preserved. Runtime reference scan and Python
compile validation passed.

## Follow-up cleanup result

After the Silver Parquet → Gold Iceberg refactor, the following unsupported
main-path artifacts were removed:

- Silver Iceberg Airflow factories and the obsolete dependency helper.
- `lakehouse/customers_e2e.py` and `lakehouse/customers_manifest_e2e.py`, plus
  their retired E2E test cases.
- The unused Silver Iceberg cutover policy and configuration flags.
- The obsolete Gold artifact builder that consumed attestation/reconciliation/
  quality task payloads.
- The retired `silver/snapshots/` package, `silver/run_snapshot.py`, its
  snapshot-only tests, and the unused `silver_snapshot_paths` helper.

Legacy migration/attestation/quality modules remain only for explicit
compatibility/backfill coverage. Independent QA: **PASS**. Final evidence:
`337 passed, 76 deselected, 15 subtests passed`, compileall PASS, diff-check
PASS, and no stale supported-path references remain.

This follow-up cleanup retains the same compatibility boundary. The full
repository run is environment-limited in this workspace by Windows temp/Spark
permissions; the isolated regression suite remains the release evidence.

## Current-flow follow-up verification

- Removed the retired Silver snapshot package and snapshot-only tests.
- Removed `silver_snapshot_paths` and the obsolete snapshot test root from
  `pytest.ini`.
- Active-reference scan: **PASS** (`silver/run_snapshot.py`,
  `silver/snapshots/*`, `silver_snapshot_paths`: none).
- Focused regression: **44 passed, 1 skipped**.
- Python compileall: **PASS**.
- `git diff --check`: **PASS**.
- Independent QA verdict: **PASS**.
