# Local Pipeline Runbook

The supported local path is `PostgreSQL (source) → Bronze (MinIO) → Silver (Spark) → Gold (Iceberg/Parquet)`. PostgreSQL remains the source and Airflow metadata database; the retired Raw Warehouse/dbt layer is not part of the pipeline.

## Safety defaults

- `INGESTION_WRITE_ENABLED=false`, `LAKEHOUSE_WRITE_ENABLED=false`, and `GOLD_SNAPSHOT_WRITE_ENABLED=false` by default.
- `GOLD_SOURCE_MODE=parquet_snapshot` remains the default. Any Gold `WRITE` requires its explicit snapshot guards.
- Do not remove PostgreSQL, MinIO, Nessie, or Airflow volumes as part of application cleanup.

## Compose services

`postgres`, `minio`, `nessie`, and Airflow services are defined in `docker-compose.yml`. The Compose file intentionally contains no Warehouse/dbt service or Warehouse-only bind mount.

Validate configuration without starting services:

```powershell
docker compose config
```

For native Gold local-only work, use the dedicated environment file
`.env.quality-it.local` (copied from the committed example) with
`docker compose --env-file .env.quality-it.local config` and, only when the
user has approved a local stack run, recreate the Airflow containers so their
environment receives `ecommerce-quality-it` and the explicit date bounds. Do
not copy secrets into the example or point this configuration at production.
The approved local E2E profile explicitly sets `INGESTION_WRITE_ENABLED=true`
and `LAKEHOUSE_WRITE_ENABLED=true`
for Silver-to-Iceberg artifact production while keeping
`GOLD_DIMENSIONAL_WRITE_ENABLED=false` until the Gold write gate is separately
approved.
It also sets all six `*_SILVER_SOURCE=iceberg` values; `iceberg` is the only
allowed non-Parquet source enum for the local migration branch. The legacy
aggregate Gold write remains disabled.
`AIRFLOW_LOCAL_E2E_MODE=WRITE` supplies the explicit six-table local run
configuration only when `BRONZE_BUCKET=ecommerce-quality-it`; normal runs still
require explicit Airflow `dag_run.conf`.

Before the approved local E2E run, seed one deterministic, FK-valid delta in
each PostgreSQL source table (the command is idempotent and never deletes
data):

```powershell
$env:BRONZE_BUCKET="ecommerce-quality-it"
$env:AIRFLOW_LOCAL_E2E_MODE="WRITE"
$env:INGESTION_WRITE_ENABLED="true"
$env:QUALITY_IT_RUN_ID="r5-local-a"
.\.venv\Scripts\python.exe tests\seed_local_postgres_e2e.py
```

The seed uses only `INSERT ... ON CONFLICT DO UPDATE` against existing
`customers`, `products`, `orders`, `order_items`, `payments`, and `shipments`
tables. Its target must remain PostgreSQL host `postgres`, `localhost`, or
`127.0.0.1` and database `ecommerce_platform`; external hosts and other
database names are rejected before connection. Do not run it against a
production/shared bucket or database.
Use the same `QUALITY_IT_RUN_ID` to replay idempotently; choose a new run ID
for a new deterministic six-table delta. The generated timestamp remains in
the bounded local Gold date window.

## Pipeline flow

1. `ingest_*` reads PostgreSQL incrementally and writes validated Bronze objects when enabled.
2. `validate_*` verifies checksums and batch contracts.
3. `bronze_to_silver_*` runs Spark Silver transformation; manifest verification gates downstream tasks.
4. Gold tasks resolve verified Silver lineage and write only when the Gold snapshot guard is explicitly enabled.

There is no `warehouse_gate`, raw PostgreSQL analytical loader, or `dbt_build` task.

## Diagnostics

```powershell
.\.venv\Scripts\python.exe -m py_compile airflow\dags\ecommerce_incremental_ingestion.py airflow\dags\pipeline_operators.py
.\.venv\Scripts\python.exe -m pytest -q tests\test_gold_orchestration.py tests\test_lakehouse_safety_guards.py
```

Runtime writes remain subject to approved local-only quality and cutover plans. Never point local guards at production or shared storage.
