# Project Structure

The platform is organized as a Bronze → Silver → Gold lakehouse pipeline. PostgreSQL is retained for OLTP source data and Airflow metadata; no separate analytical Warehouse/dbt project is maintained.

```text
source/                 PostgreSQL schema and source-side bootstrap
ingestion/              Incremental PostgreSQL extraction and Bronze writes
validation/             Batch, checksum, and manifest validation
silver/                 Spark Silver transformations
lakehouse/              Iceberg migration, quality, attestation, and diagnostics
gold/                   Gold contracts, transformations, quality, and guarded writers
airflow/dags/            Bronze/Silver/Gold orchestration and local policies
common/                 Shared contracts and storage helpers
transformation/         Spark transformation entrypoints
tests/                  Unit and static regression tests
docs/                   Architecture, runbooks, and phase evidence
```

PostgreSQL services and volumes in Compose are intentionally preserved. Gold defaults to read-only/DRY_RUN-safe modes until an approved cutover enables writes.
