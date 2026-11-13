# Local Pipeline Runbook

## Native Metabase → Trino local BI

The active local BI path is native Metabase → Trino. The repository-local
Streamlit presentation and `gold.query` semantic query layer were retired in
Batch 3. Gold pipeline, Trino/Nessie catalog, stable ref, and read-only access
policy remain unchanged.

## Archived: registered-query/driver local BI boundary

The custom Metabase driver, plugin loader, and registered-query HTTP runtime
were retired on 2026-11-13. The historical notes below are retained for audit
context only. The active local BI path is native Metabase → Trino; use the
Trino contract below and do not recreate the removed Compose/runtime assets.

The optional `docker-compose.metabase.local.yml` exposes only the loopback-bound
Metabase UI and separate `metabase-db` metadata store/volume. It contains no
custom plugin loader or plugin volume. Metabase metadata is not stored in Gold
schemas or Gold object storage. Validate without starting services:

```powershell
docker compose -f docker-compose.yml -f docker-compose.metabase.local.yml --profile bi config
```

No custom driver, registered-query HTTP runtime, or repository-local semantic
query adapter is active. Metabase metadata remains isolated in its PostgreSQL
metadata store and is not stored in Gold schemas or object storage.

## Gate 1: local Trino native BI contract

`docker-compose.trino.local.yml` is a separate, local-only overlay. It pins
`trinodb/trino:476`, joins the existing quality-it and Metabase networks, and
also joins the existing external root network `ecommerce-data-platform_default`
so the catalog endpoint `http://minio:9000` resolves without changing root
Compose. It publishes only `127.0.0.1:${TRINO_PORT:-8081}`. It is not merged into
`docker-compose.yml`; native Metabase → Trino is now the active local BI path.

Validate the merged contract without starting services:

```powershell
docker compose --env-file .env.quality-it.local `
  -f docker-compose.quality-it.local.yml `
  -f docker-compose.metabase.local.yml `
  -f docker-compose.trino.local.yml `
  config --quiet
```

The Trino catalog is fixed to `lakehouse.gold_run_20261112_gold_06` on stable
Nessie ref `bi_gold_current`, using the required
`TRINO_NESSIE_URI=http://host.docker.internal:19132/api/v2` and MinIO
S3-compatible storage via `http://minio:9000`. The
file access policy allows only user `metabase` and `SELECT` on the seven Gold
tables; DDL/DML, ownership, and Iceberg maintenance procedures are not
granted. Set `TRINO_S3_READONLY_ACCESS_KEY` and
`TRINO_S3_READONLY_SECRET_KEY` only to an externally provisioned MinIO
read-only credential. Do not use MinIO root credentials.

For the Metabase native Trino connection, use host `trino`, port `8080`, user
`metabase`, catalog `lakehouse`, and schema `gold_run_20261112_gold_06` while
Metabase and Trino share the local network. Runtime startup and query/read-back are outside Gate
1; no MinIO/Nessie/PostgreSQL/Gold mutation is permitted.
The Trino coordinator also accepts the `Presto` protocol header required by
Metabase's bundled Presto JDBC driver; this is only a local compatibility
setting and does not change the read-only policy.

## REST Catalog local validation

The obsolete SQLite REST fixture and inactive REST config trees were removed
in the approved cleanup. The retained path is the persistent PostgreSQL REST
Catalog overlay `docker-compose.iceberg-rest-catalog-postgres.local.yml`, its
separate Trino validation service, and the isolated Metabase REST trial.
Active Nessie/Trino/Metabase configuration remains unchanged.

Do not remove the persistent REST PostgreSQL volume. Registration and BI trial
commands remain bounded to run-06 and read-only Trino validation.

Stable BI ref promotion is a separate, explicit local operation. The tool
validates a `VERIFIED` run-06 manifest and defaults to dry-run:

```powershell
$env:TRINO_NESSIE_URI = "http://host.docker.internal:19132/api/v2"
powershell -File scripts/promote_bi_gold_current.ps1 `
  -ManifestPath .\path\verified-gold-manifest.json `
  -CandidateHash <candidate-nessie-hash>
```

Applying requires both `-Apply`, `BI_REF_PROMOTION_APPLY=1`, and an explicit
CAS state: the exact current hash for an existing target, or `ABSENT` when the
target GET was verified as missing. A missing target is created only as an
immutable TAG at the verified candidate hash; existing branches are rejected,
and existing matching TAGs are a no-op. Do not run apply as part of static or
runtime smoke verification. The tool has no Spark/MinIO client and does not
write objects or execute Gold maintenance.

The retired Gate 2 executor/JSON service and query-runtime Compose overlay are
historical only; no runtime HTTP listener is active.

## Archived: custom Metabase driver packaging

The custom `metabase-driver/` source, build script, plugin JAR, and runtime
loader were retired in the approved cleanup batch on 2026-11-13. Historical
No historical driver packaging document is active;
no build or plugin-loading command is active. Native Metabase → Trino is the
only supported local BI integration path.

The supported local path is `PostgreSQL (source) → Bronze (MinIO) → Silver (Spark) → Gold (Iceberg/Parquet)`. PostgreSQL remains the source and Airflow metadata database; the retired Raw Warehouse/dbt layer is not part of the pipeline.

## Safety defaults

- `INGESTION_WRITE_ENABLED=false` and `GOLD_DIMENSIONAL_ENABLED=false` by
  default.
- Native dimensional Gold remains `DRY_RUN` by default; any Gold `WRITE`
  requires `GOLD_DIMENSIONAL_WRITE_ENABLED=true` and bounded scope guards.
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
for Silver Parquet artifact production while keeping
`GOLD_DIMENSIONAL_WRITE_ENABLED=false` until the Gold write gate is separately
approved.
It sets all six `*_SILVER_SOURCE=parquet` values. Iceberg/Nessie is initialized
only by the Gold dimensional writer; the legacy Silver migration branch is not
part of the main DAG. The legacy aggregate Gold write remains disabled.
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
3. `bronze_to_silver_*` runs Spark Silver transformation and publishes verified Parquet manifests.
4. Gold revalidates the Parquet manifests, reads Silver Parquet, and writes only when the Gold snapshot guard is explicitly enabled.

There is no `warehouse_gate`, raw PostgreSQL analytical loader, or `dbt_build` task.

## Diagnostics

```powershell
.\.venv\Scripts\python.exe -m py_compile airflow\dags\ecommerce_incremental_ingestion.py airflow\dags\pipeline_operators.py
.\.venv\Scripts\python.exe -m pytest -q tests\test_gold_orchestration.py tests\test_lakehouse_safety_guards.py
```

Runtime writes remain subject to approved local-only quality and cutover plans. Never point local guards at production or shared storage.
