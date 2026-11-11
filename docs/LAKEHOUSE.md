# Lakehouse Foundation

The Lakehouse foundation runs alongside the existing Bronze, Silver, Warehouse,
and dbt pipeline. Existing Parquet and warehouse flows are intentionally
unchanged in this milestone.

## Components

- **MinIO** stores Iceberg table data and metadata below
  `s3://$BRONZE_BUCKET/lakehouse`.
- **Nessie** is the versioned Iceberg REST catalog. Its single-node RocksDB
  store is persisted in the `nessie_data` Docker volume.
- **Spark 3.5** receives the Iceberg runtime and AWS bundle during image build.
  The `lakehouse` catalog is configured in `spark/conf/spark-defaults.conf`.

The local catalog URL from containers is `http://nessie:19120/api/v2`. A host
client should instead use `http://localhost:19120/api/v2`.

The local Nessie service intentionally runs **without authentication or
authorization**. It is suitable only for an isolated development environment;
production requires an authenticated Nessie deployment, encrypted credentials,
and an object-store access policy.

## Start and verify

```powershell
docker compose -f docker-compose.yml -f spark/docker-compose.spark.yml build spark
docker compose -f docker-compose.yml -f spark/docker-compose.spark.yml up -d minio nessie spark
docker exec ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/smoke_test.py
docker exec ecommerce-spark python3 -m pytest -q -m e2e /opt/spark/work-dir/tests/test_lakehouse_e2e.py
```

The final command creates a disposable `lakehouse.validation.iceberg_smoke`
table, writes two rows, reads them back through Nessie, and prints
`ICEBERG_NESSIE_SMOKE_TEST: PASS`.

The pytest E2E test invokes this same bounded Spark submission, so it validates
the actual image, Nessie catalog, and MinIO object store instead of mocks.

## Migration and cutover

Production migration and cutover procedures are intentionally omitted from this
local-only runbook. Use the bounded smoke test above for local Iceberg validation.

Cutover readiness is represented by the read-only immutable preflight in
`gold/implementation/cutover.py`. It defaults to the existing Parquet route and
`DRY_RUN`; it cannot route consumers or enable writes. Runtime certification,
semantic sign-off, operational proof, and consumer parity remain separate gates.

## Read-only Iceberg metadata metrics

`lakehouse/diagnostics/iceberg_metrics.py` collects the latest snapshot and
file/record/byte totals from Iceberg metadata tables only. It is observability
only: no `MERGE`, `DELETE`, `UPDATE`, `ALTER`, `DROP`, `VACUUM`, cleanup or
snapshot-expiration statement is allowed. Run it only against an explicitly
scoped catalog/table list; it does not authorize maintenance or cutover.

## Products and order-items migration

Migration, cutover, rollback, and Airflow dual-run procedures are not available
in this local-only runbook. Use only the bounded smoke test above; no executable
cutover flags or write commands are documented here.

## Regression coverage

`test_iceberg_customers_manifest_migration` creates a fresh Silver Parquet
batch and manifest in MinIO, verifies object inventory/ETag integrity,
migrates it into a disposable Iceberg table, verifies progress replay is
 idempotent, then removes all test objects. This covers the disposable local manifest
path rather than only in-memory merge behavior.
