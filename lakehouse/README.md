# Lakehouse

This package contains lakehouse diagnostics and explicitly invoked legacy
Iceberg migration/attestation/quality utilities. The supported main path is
Bronze → verified Silver Parquet → Gold Iceberg; these utilities are not part
of the main Silver-to-Gold Airflow branch.

- `migration/` — bounded Silver-to-Iceberg migration and reconciliation
- `quality/` — read-only Iceberg quality contracts
- `attestation_cli.py` — manifest and snapshot lineage attestation
- `diagnostics/` — read-only metadata diagnostics

Raw Warehouse loaders and dbt projects were retired in Session A. Airflow invokes only supported Bronze → Silver → Gold tasks. Writes remain guarded by explicit local policy flags.
