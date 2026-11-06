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

## Cutover controls

Warehouse loading remains on legacy Parquet by default. Set
`CUSTOMERS_SILVER_SOURCE=iceberg` only after successful dual-run reconciliation,
and also set `CUSTOMERS_ICEBERG_CUTOVER_ENABLED=true` for the reviewed cutover
run. Set `CUSTOMERS_SILVER_SOURCE=parquet` to roll back immediately; no table
data is deleted by this switch.

Products use the identical opt-in controls:
`PRODUCTS_SILVER_SOURCE=iceberg` and
`PRODUCTS_ICEBERG_CUTOVER_ENABLED=true`.  Both sources default to `parquet`.
The Airflow Warehouse gate removes a cut-over table from the legacy loader, so
one verified batch can never be loaded from both Parquet and Iceberg.

## Customers migration and cutover

Customers migrate from **verified Silver Parquet**, not directly from Bronze:

```text
Silver Parquet manifest -> Iceberg customers -> cumulative reconciliation
```

Backfill all historical manifests (explicit write permission required):

```powershell
docker exec -e LAKEHOUSE_WRITE_ENABLED=true ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/migration/run_customers.py --backfill
```

Migrate one verified batch:

```powershell
docker exec -e LAKEHOUSE_WRITE_ENABLED=true ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/migration/run_customers.py --manifest-key silver_incremental/customers/batch_id=<batch-id>/_BATCH_METADATA.json
```

Reconcile cumulative Parquet and Iceberg state:

```powershell
docker exec ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/migration/reconcile_customers.py
```

Warehouse remains on Parquet by default. After a reviewed reconciliation PASS,
set `CUSTOMERS_SILVER_SOURCE=iceberg` and
`CUSTOMERS_ICEBERG_CUTOVER_ENABLED=true`. Roll back by setting source back to
`parquet`; neither Parquet nor Iceberg data is deleted.

## Products migration and cutover

Products follow the same verified migration contract:

```text
Silver Parquet manifest -> Iceberg products -> cumulative reconciliation
```

```powershell
# Historical backfill (explicit write permission)
docker exec -e LAKEHOUSE_WRITE_ENABLED=true ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/migration/run_products.py --backfill

# One incremental manifest
docker exec -e LAKEHOUSE_WRITE_ENABLED=true ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/migration/run_products.py --manifest-key silver_incremental/products/batch_id=<batch-id>/_BATCH_METADATA.json

# Read-only cumulative reconciliation
docker exec ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/migration/reconcile_products.py
```

After reconciliation passes, enable `PRODUCTS_SILVER_SOURCE=iceberg` plus
`PRODUCTS_ICEBERG_CUTOVER_ENABLED=true` for a reviewed run. Restore
`PRODUCTS_SILVER_SOURCE=parquet` and disable the cutover flag to roll back.
Parquet remains the migration and rollback source during this window.

## Order-items migration and cutover

Order items use the same manifest-protected migration path and are reconciled
against the complete Silver Parquet history before they can be used by the
Warehouse:

```powershell
# Historical backfill (explicit write permission)
docker exec -e LAKEHOUSE_WRITE_ENABLED=true ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/migration/run_order_items.py --backfill

# One verified incremental manifest
docker exec -e LAKEHOUSE_WRITE_ENABLED=true ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/migration/run_order_items.py --manifest-key silver_incremental/order_items/batch_id=<batch-id>/_BATCH_METADATA.json

# Read-only reconciliation: rows, business-column hashes, and Order/Product FKs
docker exec ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/migration/reconcile_order_items.py
```

The Airflow dual-run performs migration and reconciliation while the Warehouse
continues loading Parquet. To cut over a reviewed run, set both
`ORDER_ITEMS_SILVER_SOURCE=iceberg` and
`ORDER_ITEMS_ICEBERG_CUTOVER_ENABLED=true`. The DAG then loads the pinned
Iceberg snapshot and records its immutable snapshot lineage in
`warehouse.loaded_batches`. Revert the source to `parquet` to roll back; this
does not delete either source.

## Regression coverage

`test_iceberg_customers_manifest_migration` creates a fresh Silver Parquet
batch and manifest in MinIO, verifies object inventory/ETag integrity,
migrates it into a disposable Iceberg table, verifies progress replay is
idempotent, then removes all test objects. This covers the production manifest
path rather than only in-memory merge behavior.
