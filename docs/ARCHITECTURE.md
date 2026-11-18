# Architecture

## Supported path

```text
PostgreSQL (OLTP source) -> incremental ingestion -> Bronze (MinIO)
    -> Silver (Spark/Parquet) -> Gold (guarded Iceberg dimensional datasets)
```

Airflow orchestrates validation, Spark transformations, Silver manifest
verification, and the Gold dimensional quality/write gate. PostgreSQL also
hosts Airflow metadata. The former Raw Warehouse and dbt analytical layer has
been retired; no analytical loader, dbt project, or dbt service is required.

## Reliability boundaries

- Incremental ingestion is checksum- and watermark-aware and remains idempotent.
- Silver completion manifests and Parquet inventories are verified before Gold source resolution.
- Gold writes are fail-closed and default to `DRY_RUN`/read-only policy.
- Local Compose keeps PostgreSQL, MinIO, Nessie, and Airflow volumes intact.

## Runtime configuration

The canonical production storage contract uses bucket
`ecommerce-lakehouse-prod` with `bronze/<table>/`, `silver/<table>/`, and
`gold/<table>/` keys and Nessie ref `gold_prod_current`. Quality validation is
isolated in `ecommerce-lakehouse-quality-it`; the legacy
`ecommerce-data-lake` namespace remains bounded and test-only. Layout
validation is pure and fail-closed before any MinIO, Spark, PostgreSQL, or
Nessie client is created.

`INGESTION_WRITE_ENABLED` and `GOLD_DIMENSIONAL_ENABLED` default to false;
dimensional writes have a separate
`GOLD_DIMENSIONAL_WRITE_ENABLED` guard. Production cutover and operational
certification require separate approval and evidence.

## Repository map

See `docs/PROJECT_STRUCTURE.md` for the current directory map and `docs/RUNBOOK.md` for local validation commands.
