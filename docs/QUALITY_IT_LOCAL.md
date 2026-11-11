# Local quality-it smoke stack

## Phase C local runtime proof (DRY_RUN)

The bounded runner `tests/run_phase_c_local_runtime.py` validates late-correction
overlap/reprocess intent, rejects schema drift, and enforces tombstone-only
policy. It is hard-scoped to bucket `ecommerce-quality-it`, a local quality-it
Nessie endpoint, and ref `main`; it emits `DRY_RUN` with `can_write=false` and
does not start Spark or call MinIO/Nessie. Production/disposable certification
and operational replay remain separate approval gates.

This stack is disposable and local-only for Windows + Docker Desktop. It reuses
the existing local MinIO container, writes only to the dedicated
`ecommerce-quality-it` bucket, and never reads production credentials or
evidence.

Start the root Compose MinIO service first (`ecommerce-minio`). The quality-it
services reach it through Docker Desktop's `host.docker.internal:9000`; no
second MinIO container or MinIO volume is created. Bucket initialization runs
from the existing Spark image, which already contains the Python MinIO SDK.

```powershell
Copy-Item .env.quality-it.local.example .env.quality-it.local
docker compose --env-file .env.quality-it.local -f docker-compose.quality-it.local.yml config --quiet
docker compose --env-file .env.quality-it.local -f docker-compose.quality-it.local.yml up -d --build --wait
docker compose --env-file .env.quality-it.local -f docker-compose.quality-it.local.yml ps
```

MinIO console: `http://127.0.0.1:9001`
Nessie host endpoint: `http://127.0.0.1:19130`

Spark mounts `spark/conf/spark-defaults.quality-it.local.conf`, which points
Iceberg at the local Nessie service and `s3a://ecommerce-quality-it/lakehouse`.
Readiness is gated by Python HTTP probes in `minio-init` and `nessie-ready`;
the minimal MinIO/Nessie images are not assumed to contain `curl`.

After services are healthy, run the bounded Iceberg smoke test inside Spark:

```powershell
docker compose --env-file .env.quality-it.local -f docker-compose.quality-it.local.yml exec spark /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/diagnostics/smoke_test.py
```

Do not use `down -v` unless you explicitly intend to delete this run's local
volumes. This stack does not provide production certification.

## Current Silver-to-Gold boundary

The supported Airflow path does not create Silver Iceberg tables. Silver writes
immutable Parquet batches and completion manifests; Gold revalidates the
cumulative manifest set, assembles the latest state by business key and
`updated_at`, then performs a guarded Iceberg `DRY_RUN` or `WRITE`.

The contract is covered without external writes by:

```powershell
.venv\Scripts\python.exe -m pytest -q `
  tests/test_gold_parquet_source.py `
  tests/test_gold_dimensional_writer.py
```

The previous local fixture runners that constructed Silver Iceberg tables are
legacy compatibility tools and are not part of the supported main flow.

## Gold WRITE gate

Gold `WRITE` remains a separate approval gate. Keep
`GOLD_DIMENSIONAL_WRITE_ENABLED=false` for normal local validation; no local
write/cutover command is documented here.

## Recovery contract checks

The Gold dimensional writer classifies recovery without deleting objects:

- `ALREADY_PUBLISHED` when the immutable graph manifest matches.
- `PARTIAL_REQUIRES_RECONCILIATION` when only some table manifests exist.
- Fence conflicts and immutable manifest mismatches fail closed.
- Fence release is allowed only for the verified owner.

Run the contract suite without Docker or object-store mutation:

```powershell
.venv\Scripts\python.exe -m pytest -q --basetemp=.tmp-pytest-recovery `
  tests/test_gold_dimensional_recovery_contract.py `
  tests/test_gold_dimensional_writer.py
```

Do not inject failures into the live local bucket; use the fake-client tests for
recovery behavior. These checks do not constitute production certification.
