# Lakehouse Foundation

The supported path is `Bronze → verified Silver Parquet → Gold Iceberg`.
The former Raw Warehouse/dbt flow is retired; legacy checkpoint and migration
utilities remain only for controlled backfill compatibility.

## Components

- **MinIO** stores Bronze objects, verified Silver Parquet, and Gold Iceberg
  data below the configured bucket.
- **Nessie** is the versioned Iceberg REST catalog for Gold targets. Its single-node RocksDB
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
docker exec ecommerce-spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/diagnostics/smoke_test.py
docker exec ecommerce-spark python3 -m pytest -q -m e2e /opt/spark/work-dir/tests/test_lakehouse_e2e.py
```

The final command creates a disposable `lakehouse.validation.iceberg_smoke`
table, writes two rows, reads them back through Nessie, and prints
`ICEBERG_NESSIE_SMOKE_TEST: PASS`.

The pytest E2E test invokes this same bounded Spark submission, so it validates
the actual image, Nessie catalog, and MinIO object store instead of mocks.

## Gold Iceberg validation

Production migration and consumer cutover procedures are intentionally omitted
from this local-only runbook. Use the bounded smoke test above for local Gold
Iceberg validation.

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

## Regression coverage

`tests/test_lakehouse_e2e.py` covers the disposable Nessie/Iceberg smoke test.
The supported Silver-to-Gold boundary is covered by
`tests/test_gold_parquet_source.py`, including manifest identity, object
inventory, cumulative batch assembly, and bounded-prefix rejection.
