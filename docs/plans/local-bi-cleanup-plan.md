# Local BI cleanup plan

## Scope approved 2026-11-13

Clean redundant local BI paths after native Metabase -> Trino ->
`bi_gold_current` is verified. This is limited to the local
`ecommerce-data-lake` workspace; it does not touch production, Gold data,
MinIO objects, Nessie snapshots/refs, volumes, or PostgreSQL metadata.

## Superseded gates (historical)

1. Native Metabase Presto/Trino database is connected to `lakehouse` and the
   stable schema.
2. Native Metabase Query Builder and a representative dashboard read Gold
   successfully; no custom driver datasource is active.
3. Streamlit/dashboard imports and reads remained valid through the shared
   semantic contract.
4. Focused tests, compile checks, and independent QA pass.

## Historical cleanup order

- Before the gates: remove only explicitly identified generated QA temp
  directories; preserve all source, data, snapshots, and fallback runtimes.
- After the gates: remove custom driver source/build wiring and its Metabase
  plugin loader path.
- After a separate fallback-retirement verification: remove the registered
  query HTTP runtime and its Compose overlay/tests.
- Archive/update historical docs and plans; do not erase audit history.

## Historical acceptance

- Trino keeps read-only policy and `bi_gold_current`.
- Native Metabase reads the verified Gold run through Trino.
- Streamlit/dashboard parity remained intact before Batch 3.
- No active Compose path references the retired plugin/runtime.
- `pytest`, `compileall`, and `git diff --check` pass.
- No MinIO/Nessie/Iceberg/PostgreSQL data mutation or destructive maintenance.

## Current status — Trino-only

Plan approved and executed. Stable Nessie ref, direct Trino SELECT, and the
Trino-only cleanup are PASS. Native Metabase remains the sole BI UI path;
Streamlit, `gold.query`, custom driver, and registered-query runtime are no
longer part of the workspace.

## Batch 1 result — 2026-11-13

**PASS (independent QA).** Removed 27 explicitly identified generated
`.pytest-*`/`.qa-*` temporary directories. No source, fallback runtime,
Metabase plugin, Gold data, MinIO object, Nessie ref/snapshot, volume, or
PostgreSQL metadata was touched. Focused BI/dashboard regression: `69 passed`
with 14 existing pandas/PySpark compatibility warnings. `git diff --check`
passed.

This batch is historical; native Metabase parity is now an operational
follow-up, not a reason to retain retired repository components.

## Batch 2 result — 2026-11-13

**PASS (independent QA).** Retired the custom Metabase driver/build assets,
plugin loader wiring, registered-query HTTP runtime, query-runtime Compose
overlay, and their obsolete tests. Preserved the shared semantic contract
(`bi_schema.py`, `bi_limits.py`, `sql_adapter.py`), dashboard/Streamlit,
native Trino, Nessie, Gold, and Metabase metadata storage. Focused regression
passed (`79 passed`, 14 existing pandas/PySpark warnings), `compileall`
passed, and `git diff --check` passed.

The former parity caveat was superseded by the explicit Trino-only decision in
Batch 3; native Metabase parity is an operational follow-up.

## Approved retirement batch (2026-11-13)

This earlier batch retired the obsolete custom Metabase driver and registered-query
HTTP runtime/Compose overlay after the local BI direction moved to native
Metabase → Trino. It removes only source/build assets, plugin wiring, runtime
modules and obsolete tests; Batch 3 later removed the remaining legacy BI
layer. Trino/Nessie/Gold configuration, Metabase core DB, and all data/volumes
remain untouched.

Historical driver/runtime details are retained only where useful in plan
history; obsolete driver/runtime plans and packaging docs were deleted in
Batch 3. Verification commands are focused regression tests,
Python compileall, and `git diff --check`. No Docker startup, Nessie/MinIO
operation, PostgreSQL metadata operation, or volume cleanup is permitted.

The retirement batch was superseded by Batch 3: the custom driver, plugin
wiring, registered-query HTTP runtime, Compose overlay, Streamlit, and
repository-local semantic query layer are removed. Native Trino is the only
active BI path.

## Batch 3 — Trino-only BI decision (approved 2026-11-13)

The user selected Trino + native Metabase as the only BI path. Remove the
legacy Streamlit/`gold.query` presentation and semantic query layer, its
dashboard dependency/tests, and registered-query historical plans. Preserve
the Trino catalog, native Metabase Compose, Gold/Nessie/MinIO evidence and
promotion tooling. This batch performs no data or metadata-store mutation.

### Batch 3 implementation result

Removed `dashboard/`, `requirements-dashboard.txt`, the legacy `gold/query`
semantic/presentation layer and query templates, dashboard/gold-query tests,
obsolete registered-query historical plans, and custom-driver historical docs.
Native Metabase Compose, Trino/Nessie/Gold configuration, promotion
tooling/tests, evidence, and Gold pipeline files were preserved. Focused
verification: **64 passed**, `compileall` PASS, and `git diff --check` PASS.

Independent QA initially found stale plan wording; this plan correction is the
only remaining re-review item.

### Approved cleanup follow-up: obsolete local artifacts (2026-11-13)

Retired Hive Metastore smoke/Phase 2 assets, the SQLite REST proof and its
obsolete Trino validation overlay, and exited legacy runtime/plugin-loader
containers. Preserved active Nessie/Trino/Metabase, persistent PostgreSQL REST
Catalog and validation, Metabase REST trial, Gold/MinIO objects, and all
protected database/metadata volumes. No broad volume or data cleanup was
performed; only the obsolete Hive/plugins volumes were removed explicitly.

### Cleanup batch verification — 2026-11-13

- Obsolete custom driver/registered-query, Hive smoke/Phase 2, SQLite REST
  proof/validation assets and their obsolete containers were removed.
- Retained persistent PostgreSQL REST Catalog, Trino REST validation, isolated
  Metabase REST trial, active Nessie/Trino/Metabase, Gold/MinIO and all
  protected metadata volumes were verified present/healthy. Obsolete Hive and
  Metabase plugin volumes are the only volumes removed in the follow-up below.
- Focused retained-path contract tests: **15 passed**; `compileall` PASS;
  `git diff --check` PASS.
- Independent QA: **PASS WITH WARNINGS / QA CLOSED**. No protected metadata
  volume, checkpoint, Gold file, MinIO object or Nessie state was deleted.
- Only broader unrelated Windows pytest temp ACL errors remain; they do not
  block retained-path verification and no documentation follow-up remains.
- Follow-up volume cleanup: removed the obsolete Hive Metastore volume and
  detached/recreated active Metabase without the stale plugins mount, then
  removed `ecommerce-metabase-plugins-dev`. Active and trial Metabase health
  checks returned `{"status":"ok"}`; metadata volumes were retained.
