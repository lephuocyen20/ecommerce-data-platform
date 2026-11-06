# Lakehouse layout

- `models/` — table-specific schemas, merge rules and quality gates.
- `migration/` — Parquet manifest migration, command entrypoints and reconciliation.
- `warehouse/` — pinned Iceberg readers and Warehouse loaders.
- `diagnostics/` — operational smoke and S3A diagnostics.

Airflow invokes scripts from `migration/` and `warehouse/`; table task IDs and
cutover controls remain unchanged.
