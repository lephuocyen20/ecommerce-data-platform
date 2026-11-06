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

## Customers cutover controls

Warehouse loading remains on legacy Parquet by default. Set
`CUSTOMERS_SILVER_SOURCE=iceberg` only after successful dual-run reconciliation,
and also set `CUSTOMERS_ICEBERG_CUTOVER_ENABLED=true` for the reviewed cutover
run. Set `CUSTOMERS_SILVER_SOURCE=parquet` to roll back immediately; no table
data is deleted by this switch.

## Customers migration and cutover

Customers migrate from **verified Silver Parquet**, not directly from Bronze:

```text
Silver Parquet manifest -> Iceberg customers -> cumulative reconciliation
```

Backfill all historical manifests (explicit write permission required):

```powershell
docker exec -e LAKEHOUSE_WRITE_ENABLED=true ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/run_customers.py --backfill
```

Migrate one verified batch:

```powershell
docker exec -e LAKEHOUSE_WRITE_ENABLED=true ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/run_customers.py --manifest-key silver_incremental/customers/batch_id=<batch-id>/_BATCH_METADATA.json
```

Reconcile cumulative Parquet and Iceberg state:

```powershell
docker exec ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/reconcile_customers.py
```

Warehouse remains on Parquet by default. After a reviewed reconciliation PASS,
set `CUSTOMERS_SILVER_SOURCE=iceberg` and
`CUSTOMERS_ICEBERG_CUTOVER_ENABLED=true`. Roll back by setting source back to
`parquet`; neither Parquet nor Iceberg data is deleted.
