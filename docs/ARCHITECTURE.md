# Architecture

## Supported path

```text
PostgreSQL (OLTP source) -> incremental ingestion -> Bronze (MinIO)
    -> Silver (Spark/Parquet) -> Iceberg/Nessie (optional local write)
    -> Gold (guarded analytical datasets)
```

Airflow orchestrates validation, Spark transformations, lineage attestation, and Gold quality gates. PostgreSQL also hosts Airflow metadata. The former Raw Warehouse and dbt analytical layer has been retired; no analytical loader, dbt project, or dbt service is required.

## Reliability boundaries

- Incremental ingestion is checksum- and watermark-aware and remains idempotent.
- Silver manifests and Iceberg snapshots are verified before Gold source resolution.
- Gold writes are fail-closed and default to `DRY_RUN`/read-only policy.
- Local Compose keeps PostgreSQL, MinIO, Nessie, and Airflow volumes intact.

## Runtime configuration

`INGESTION_WRITE_ENABLED`, `LAKEHOUSE_WRITE_ENABLED`, and `GOLD_SNAPSHOT_WRITE_ENABLED` default to false. `GOLD_SOURCE_MODE=parquet_snapshot` is the default. Production cutover and operational certification require separate approval and evidence.

## Repository map

See `docs/PROJECT_STRUCTURE.md` for the current directory map and `docs/RUNBOOK.md` for local validation commands.
