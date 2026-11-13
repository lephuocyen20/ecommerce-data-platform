# Ecommerce Data Platform

A portfolio-grade local data platform demonstrating a dimensional Iceberg lakehouse.
The supported path is:

`PostgreSQL source → Bronze (MinIO) → verified Silver Parquet → Gold Iceberg dimensions/facts`

Airflow orchestrates the pipeline, while manifest verification, replay
controls, tombstones, and run metadata make each stage auditable and safe to
retry. Gold performs the final dimensional quality validation before Iceberg
writes.

## Current roadmap

This is a reproducible local portfolio demonstration. Enterprise production
deployment and consumer cutover are optional future work.

The version-controlled roadmap is [`docs/GOLD_LAKEHOUSE_PLAN.md`](docs/GOLD_LAKEHOUSE_PLAN.md).
The current scope is local validation and bounded runtime evidence for the
Bronze → Silver → Gold lakehouse. Production routing, consumer cutover, and
unbounded writes remain explicitly out of scope.

The former PostgreSQL analytical Warehouse/dbt path is retired. Its legacy
checkpoint structures and compatibility modules are retained only where they
are still needed for controlled backfill or reconciliation.

## Main components

| Component | Responsibility |
|---|---|
| PostgreSQL | Source transactions and Airflow metadata |
| MinIO | Bounded Bronze, Silver Parquet, and Gold object storage |
| Spark | Silver transformations and Gold processing |
| Nessie | Versioned Iceberg REST catalog |
| Airflow | Dependency-aware orchestration and runtime evidence |
| pytest | Unit, integration, and explicitly gated E2E checks |

## Repository layout

```text
airflow/dags/       Airflow DAGs and task operators
common/             Shared safety, scope, and storage helpers
gold/               Gold dimensional contracts and transformations
ingestion/          Source-to-Bronze ingestion and replay logic
lakehouse/          Iceberg diagnostics and legacy compatibility utilities
silver/             Silver validation and incremental Parquet writers
source/             Source generators, database schema, and author-only SQL
tests/              Safe regression and bounded integration tests
docs/               Roadmap, architecture, runbook, and evidence
```

## Local verification

Read [`docs/RUNBOOK.md`](docs/RUNBOOK.md) before starting services. The safe
regression suite does not write to shared lakehouse data:

```powershell
python -m pytest -q -m "unit or integration" -p no:cacheprovider
```

Bounded Iceberg E2E writes require the explicit `LAKEHOUSE_E2E_*` approval and
scope described in [`AGENTS.md`](AGENTS.md). The certified run-04 is a local
portfolio demonstration, not a production deployment claim.

The read-only local demo contract can be checked without Docker, Spark, Nessie,
MinIO, PostgreSQL, or any runtime write:

```powershell
$env:QUALITY_IT_RUN_ID = 'local-demo-01'
$env:QUALITY_IT_BUCKET = 'ecommerce-quality-it'
$env:QUALITY_IT_PREFIX = 'quality-it/local-demo-01/'
$env:NESSIE_URI = 'http://127.0.0.1:19130/api/v2'
$env:NESSIE_REF = 'main'
$env:GOLD_DIMENSIONAL_WRITE_ENABLED = 'false'
$env:GOLD_RUN_MODE = 'DRY_RUN'
.\scripts\run_local_e2e_demo.ps1 -Mode preflight
```

Local BI uses native Metabase → Trino against the pinned local Gold catalog.
The native Metabase contract and Trino overlay are documented in
`docs/RUNBOOK.md`; no Streamlit or repository-local semantic query layer is
active.

## Development status

The implementation is organized into independently reviewable local commits.
Check the roadmap and runbook for demo evidence and verification commands.
Production hardening notes are optional future work.
