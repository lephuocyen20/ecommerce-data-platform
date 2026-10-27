# Project Structure

This is the canonical navigation map for the repository.

```text
common/                 Cross-layer infrastructure
  database.py           PostgreSQL connection and transaction checks
  minio.py              MinIO client and Spark S3A setup
  manifest.py           Immutable JSON manifest primitives
  object_integrity.py   Object discovery and checksum verification
  paths.py              Bronze/Silver object-key conventions
  runtime_policy.py     DRY_RUN/WRITE and feature-flag policy
  table_registry.py     Six supported business entities

ingestion/              PostgreSQL source -> Bronze/MinIO
silver/                 Public Silver-layer entrypoints
  incremental.py        Bronze batch -> verified Silver batch implementation
  run_incremental.py    Bronze batch -> verified Silver batch
  snapshots/            Immutable snapshot implementation and helpers
  run_snapshot.py       Silver batches -> immutable Silver snapshot

gold/                   Public Gold-layer entrypoints
  implementation/       Gold transformations, storage and contracts
  lineage.py            Silver-source lineage for the four Gold datasets
  run_pipeline.py       Gold validation runner
  contracts.py          Gold data contracts and Silver lineage

warehouse/              Silver -> PostgreSQL warehouse -> dbt marts
airflow/dags/           Orchestration graph, config and operator factory
tests/                  Automated unit, integration and E2E tests
docs/                   Architecture and operating documentation
```

## Legacy area

`transformation/` contains only the legacy Bronze batch implementation and
its shared transform helpers. Silver, Snapshot and Gold runtime code must use
`silver/`, `gold/` and `common/`; the old import paths were removed.

The legacy batch pipeline remains separate until an explicit decision to retire
it. See `CLEANUP_CANDIDATES.md` for its removal prerequisites.
