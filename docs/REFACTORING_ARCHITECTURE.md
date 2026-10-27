# Target Architecture and Migration Status

The repository is being migrated without a big-bang rename. Runtime imports
remain compatible while cross-layer infrastructure moves into `common/`.

```text
common/          # DB, MinIO/S3A, manifests, integrity, paths and flags
ingestion/       # PostgreSQL -> Bronze batches
silver/          # Bronze -> Silver batches and immutable snapshots
gold/            # Four Gold datasets, contracts and lineage
transformation/  # Legacy Bronze batch flow and transform helpers
warehouse/       # Silver -> PostgreSQL warehouse and dbt
airflow/dags/    # Orchestration and DockerOperator factories
tests/           # Unit and integration coverage
```

## Completed

- Shared MinIO/Spark setup, manifests, paths, object integrity and runtime
  policy were extracted.
- Cross-layer database, feature-flag and path primitives now have canonical
  root-level modules in `common/`.
- Airflow and Warehouse use the shared `SUPPORTED_TABLES` registry.
- `transformation/shared/` has been retired; cross-layer helpers live in
  `common/`.
- New Airflow Spark entrypoints use the top-level `silver/` and `gold/`
  packages, which now hold their canonical implementation.
- DAG run configuration validation is isolated in `airflow/dags/pipeline_config.py`.

## Next migrations

1. Continue splitting the incremental DAG into task and graph modules.
2. Move manual test scripts to explicit `tests/manual/` and `tests/e2e/`
   groups without changing their runtime semantics.
3. Retire the legacy batch flow only after an explicit operational decision.

## Migration rule

Runtime code and tests use canonical `common/`, `silver/` and `gold/` imports.
