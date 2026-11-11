# Lakehouse

This package contains the Iceberg/Nessie migration, attestation, quality, and diagnostic utilities used by the Silver → Gold lakehouse path.

- `migration/` — bounded Silver-to-Iceberg migration and reconciliation
- `quality/` — read-only Iceberg quality contracts
- `attestation_cli.py` — manifest and snapshot lineage attestation
- `diagnostics/` — read-only metadata diagnostics

Raw Warehouse loaders and dbt projects were retired in Session A. Airflow invokes only supported Bronze → Silver → Gold tasks. Writes remain guarded by explicit local policy flags.
