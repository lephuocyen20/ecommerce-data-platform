# Local Iceberg REST Catalog migration — Phase 1

## Scope approved 2026-11-13

Prepare a local-only Apache Iceberg REST Catalog path for the
`ecommerce-data-lake` project. This phase is config-only: it must not start
containers, register tables, write PostgreSQL metadata, mutate MinIO/Nessie,
rewrite Iceberg files, or cut over Trino/Metabase.

## Target architecture

```text
Spark / Trino -> Iceberg REST Catalog -> MinIO Iceberg files
                         |
                 local catalog metadata DB
```

The existing Nessie path remains the active path and rollback profile until a
separate runtime migration gate is approved.

## Deliverables

- Local REST Catalog Compose overlay with pinned image/config.
- REST catalog metadata backend configuration isolated from Metabase metadata.
- Trino REST catalog properties and Spark REST catalog properties, inactive by
  default and not replacing current Nessie config.
- Contract tests for image pin, private/local network boundary, warehouse,
  read-only Trino policy, and rollback-preserving Nessie config.
- Runbook and migration notes describing the later runtime registration gate.

## Acceptance

- Compose config validates without starting services.
- REST image is pinned, host exposure is loopback-only or absent.
- REST metadata DB is separate from Metabase metadata DB.
- No production/root Compose change, data write, catalog registration, or
  Trino/Metabase cutover occurs.
- Current Nessie/Spark/Trino configs remain intact for rollback.
- Focused tests, compileall, and `git diff --check` pass.

## Explicitly deferred

Runtime REST startup, registering the seven run-06 tables, parity checks,
Metabase cutover, promotion automation, and removal of Nessie require a new
approval after this phase passes independent QA.

## Phase 1 result — 2026-11-13

**PASS (independent QA, config-only scope).** The overlay is inactive and
profile-gated, uses the pinned `apache/iceberg-rest-fixture:1.10.0` image, has
no host port, an internal network, and a separate PostgreSQL metadata service.
Trino/Spark REST configs are outside active Nessie config paths. Focused REST
contract tests passed (`5 passed` in QA; `30 passed` including regression
suites), `compileall` passed, and `git diff --check` passed.

No Docker startup, REST registration, Trino/Metabase cutover, MinIO/Nessie/
PostgreSQL operation, or data mutation was performed. Image availability,
JDBC compatibility, fixture startup, registration, and runtime parity remain
`DEFERRED_UNVERIFIED` and require a separate approval.

## Phase 1 implementation (2026-11-13)

Added an inactive local overlay using the Apache-published
`apache/iceberg-rest-fixture:1.10.0` image. Apache Iceberg documents this image
as the local REST Catalog fixture and its published Dockerfile exposes the
`JdbcCatalog`/`CATALOG_URI` configuration contract. The overlay declares an
isolated PostgreSQL metadata service, but does not publish a host port and is
profile-gated under `iceberg-rest-catalog`.

Trino properties are stored under `trino/rest-catalog-inactive/`, not the
active `trino/etc/catalog/` directory. Spark properties are stored under
`spark/conf/inactive/`, not the active Nessie defaults. Both use the official
REST catalog property shape (`catalog.type=rest`, REST URI, warehouse) and
remain read-only/config-only. PostgreSQL JDBC driver compatibility and actual
fixture startup are **DEFERRED_UNVERIFIED**; no support is claimed from static
configuration alone.

Static contract coverage is in `tests/test_iceberg_rest_catalog_contract.py`.
No Docker service, REST registration, Trino/Metabase cutover, MinIO/Nessie
operation, PostgreSQL metadata operation, or production/root Compose file was
executed or changed.

## Phase 2 approval — 2026-11-13

User approved runtime validation and bounded registration of the certified
run-06 tables into the isolated REST Catalog metadata backend. The active
Nessie/Trino/Metabase path remains unchanged until parity and rollback checks
pass.

## Phase 2 first runtime attempt — historical — 2026-11-13

**BLOCKED / UNVERIFIED, fail-closed QA PASS.** The initial PostgreSQL
metadata service started, but `apache/iceberg-rest-fixture:1.10.0` failed at
`JdbcCatalog` initialization with:

```text
java.sql.SQLException: No suitable driver found for
jdbc:postgresql://rest-catalog-db:5432/iceberg_rest_catalog
```

The run stopped before registration, parity, or cutover. Only the two bounded
containers and ephemeral network were removed; no volume, MinIO, Nessie, Gold,
active Trino/Metabase, or production/root Compose state was changed. Static
regression evidence remains `30 passed`, `compileall` PASS, and
`git diff --check` PASS. Independent QA marked the fail-closed handling PASS.

Next step requires a new implementation decision: choose a REST Catalog
server/image with verified PostgreSQL JDBC support (or use an explicitly
supported metadata backend), update the Phase 2 plan, and obtain approval
before retrying runtime startup. Do not inject an unverified driver or switch
images implicitly.

## Phase 2 SQLite runtime evidence — 2026-11-13

The approved local retry used the fixture's supported SQLite JDBC backend and
an isolated REST Catalog container. The container passed `/v1/config` health,
was connected only to the local MinIO read path plus its private network, and
created namespace `gold_run_20261112_gold_06`.

All seven certified run-06 tables were registered using their existing
metadata locations; no Parquet or Iceberg data file was copied or rewritten.
REST table metadata returned snapshot IDs matching the certified evidence for
all seven tables. A separate validation-only Trino container then returned the
exact seven-table set, DESCRIBE results, and row counts:

```text
dim_customer=2, dim_date=7, dim_product=2,
fact_order_items=1, fact_orders=1, fact_payments=1, fact_shipments=1
fact_orders: net_amount=24.00, physicalInputBytes=3088
```

The active Nessie-backed Trino/Metabase path was not changed. The validation
Trino policy is static SELECT-only; no DDL/DML statement was sent. The SQLite
fixture metadata is a local runtime proof, not yet a durable shared catalog
backend. Persistent REST backend selection and active catalog cutover remain
deferred.

## Phase 2 startup gate result — historical first attempt (2026-11-13)

The bounded startup command was:

```powershell
$env:ICEBERG_REST_DB_PASSWORD = "phase2-local-only"
$env:ICEBERG_REST_RUN_ID = "run-20261112-gold-06"
docker compose -f docker-compose.iceberg-rest-catalog.local.yml `
  --profile iceberg-rest-catalog up -d rest-catalog-db rest-catalog
```

The isolated `postgres:16.4` service initialized successfully. The pinned
Apache fixture then failed closed during `JdbcCatalog` initialization with:
`java.sql.SQLException: No suitable driver found for
jdbc:postgresql://rest-catalog-db:5432/iceberg_rest_catalog`.

Per the approved stop condition, that first attempt made no registration or
parity query. It was superseded by the bounded SQLite runtime proof above;
the PostgreSQL JDBC limitation remains documented and is not treated as a
production backend.

Independent QA for the SQLite proof: **PASS**. Contract coverage asserts
`object_data_mutation: false` and explicit bounded catalog metadata
registration. Focused suite passed (`34 passed`), `compileall` and
`git diff --check` passed. Durable REST backend selection and active cutover
remain deferred.

## Persistent PostgreSQL REST candidate â€” static implementation (2026-11-13)

The next approved direction is represented by the separate
`docker-compose.iceberg-rest-catalog-postgres.local.yml` overlay. It uses
pinned `apache/iceberg-rest-fixture:1.10.0` and `postgres:16.4`, a dedicated
metadata volume and an internal-only network. It does not join the root
network, expose a host port, alter the SQLite proof, or replace the active
Nessie/Trino/Metabase path.

The candidate was initially **DEFERRED_UNVERIFIED_JDBC**. The prior bounded
runtime attempt with the upstream fixture failed before registration with:

```text
java.sql.SQLException: No suitable driver found for
jdbc:postgresql://rest-catalog-db:5432/iceberg_rest_catalog
```

That historical failure was corrected by the custom JDBC image documented
below. A future registration/parity phase must still prove a deterministic
classpath (custom image or officially supported image), then run health,
registration and parity checks under separate approval. The later bounded
PostgreSQL runtime health proof is recorded below; registration and parity
remain deferred.

Static verification for this correction covers the pinned candidate,
isolated PostgreSQL metadata volume, fail-closed JDBC status, no active config
or cutover, Python compileall, and `git diff --check`. No Docker/runtime,
PostgreSQL metadata operation, MinIO/Nessie mutation, or Gold operation was
performed.

## Custom PostgreSQL JDBC image â€” static implementation (2026-11-13)

The candidate now builds `local/iceberg-rest-fixture-postgres:1.10.0-jdbc42.7.4`
from the pinned upstream `apache/iceberg-rest-fixture:1.10.0` base. The
PostgreSQL 42.7.4 driver is fetched in a pinned `busybox:1.36.1` stage with
Dockerfile `ADD --checksum`, then copied into the final image with mode 0644.

The official Iceberg fixture source documents the runtime main class
`org.apache.iceberg.rest.RESTCatalogServer`. The custom image uses an explicit
classpath containing both the adapter JAR and the JDBC JAR because `java -jar`
does not load an externally copied dependency. This is a static/source-based
classpath correction. The bounded Docker build and runtime startup later
passed; the evidence is recorded in the runtime section below.

Later bounded runtime command, not executed here:

```powershell
$env:ICEBERG_REST_RUN_ID = "run-20261112-gold-06"
$env:ICEBERG_REST_DB_PASSWORD = "<local-secret>"
docker compose -f docker-compose.iceberg-rest-catalog-postgres.local.yml `
  --profile iceberg-rest-catalog-postgres build rest-catalog-postgres
docker compose -f docker-compose.iceberg-rest-catalog-postgres.local.yml `
  --profile iceberg-rest-catalog-postgres up -d rest-catalog-postgres-db rest-catalog-postgres
```

The runtime gate must verify `/v1/config`, JDBC initialization, and bounded
run-06 registration/parity before any persistent REST backend is considered
ready for Gold use. The health portion passed below; active
Nessie/Trino/Metabase stay unchanged.

## Bounded run-06 registration/parity implementation (2026-11-13)

Added a separate registration/validation path without changing the SQLite
proof or active Nessie catalog:

- `scripts/register_run06_rest_catalog.py` reads the existing SQLite REST
  table metadata, requires the exact seven-table set and exact
  `quality-it/run-20261112-gold-06/` metadata prefix, and POSTs only
  metadata-only `register` requests to the isolated PostgreSQL REST service.
  It is dry-run by default; `--apply` requires
  `ICEBERG_REST_PHASE2_APPLY=1` and refuses a non-empty target namespace.
- `trino/rest-catalog-postgres-validation/etc/` is a separate Trino 476
  read-only catalog/policy. It exposes only the seven run-06 tables and is
  attached to the root local network solely for read-only MinIO access; it is
  not mounted into active Trino and has a loopback-only host port.
- `scripts/validate_postgres_rest_trino_run06.py` compares source SQLite REST
  and persistent PostgreSQL REST reads through Trino for exact table set,
  schema output, row count, snapshot ID, file count, and physical file bytes.
  Expected run-06 row counts and snapshot IDs come from the verified manifest.

Historical static status: **IMPLEMENTED / UNVERIFIED RUNTIME**. At the time
this implementation section was written, no registration, REST metadata write,
MinIO read, Trino query, Nessie mutation, Gold mutation, or cutover had been
performed. The later bounded runtime evidence and final QA below supersede the
runtime portion of this historical status.

Historical bounded commands from the superseded SQLite/initial PostgreSQL
attempts (the referenced SQLite compose file has since been retired):

```powershell
$env:ICEBERG_REST_RUN_ID = "run-20261112-gold-06"
$env:ICEBERG_REST_DB_PASSWORD = "<local-secret>"
$env:TRINO_S3_READONLY_ACCESS_KEY = "<readonly-key>"
$env:TRINO_S3_READONLY_SECRET_KEY = "<readonly-secret>"
docker compose -f docker-compose.iceberg-rest-catalog-postgres.local.yml --profile iceberg-rest-catalog-postgres up -d rest-catalog-postgres-db rest-catalog-postgres
python scripts/register_run06_rest_catalog.py --source http://127.0.0.1:8181 --target http://127.0.0.1:8182
$env:ICEBERG_REST_PHASE2_APPLY = "1"
python scripts/register_run06_rest_catalog.py --apply --source http://127.0.0.1:8181 --target http://127.0.0.1:8182
docker compose -f docker-compose.iceberg-rest-catalog-postgres.local.yml --profile iceberg-rest-postgres-validation up -d trino-rest-postgres-validation
python scripts/validate_postgres_rest_trino_run06.py --source http://127.0.0.1:18081 --target http://127.0.0.1:18085
```

Runtime risks: the source SQLite REST endpoint and persistent PostgreSQL
endpoint must both expose the same metadata locations; Trino must be able to
read existing MinIO objects with the read-only credential; and the target
namespace must be empty. Any mismatch fails closed before registration.

## Isolated BI consumer trial (approved 2026-11-13)

Added `docker-compose.metabase-rest-trial.local.yml` for a separate Metabase
v0.55.3 instance with its own PostgreSQL metadata DB/volume. It connects only
to the loopback-published Trino validation endpoint at `host.docker.internal:
18085`, catalog `lakehouse_rest_postgres`, schema
`gold_run_20261112_gold_06`. The active Metabase compose, active Nessie/Trino
configuration, dashboards, and production are untouched.

`scripts/validate_metabase_rest_trial.py` validates the configured local
Presto/Trino database connection and submits one bounded native read-only
query for `fact_orders` count, requiring result `1`. It requires a trial
session and database ID, rejects non-local connection targets, and rejects
mutation/DDL/maintenance tokens before the request. It does not create a
database, question, dashboard, or alter data.

Acceptance: isolated trial compose config, connection target, and read-only
query contract pass static tests; later runtime evidence must show the trial
Metabase connection and query succeed without changing active BI paths.

Historical trial commands (runtime evidence is recorded below):

```powershell
$env:METABASE_TRIAL_RUN_ID = "run-20261112-gold-06"
$env:METABASE_TRIAL_DB_PASSWORD = "<trial-secret>"
docker compose -f docker-compose.metabase-rest-trial.local.yml --profile metabase-rest-trial up -d metabase-rest-trial-db metabase-rest-trial
# Configure one trial database in the isolated UI/API only:
# host=host.docker.internal, port=18085, dbname=lakehouse_rest_postgres,
# schema=gold_run_20261112_gold_06, read-only user=metabase.
$env:METABASE_TRIAL_SESSION = "<trial-session>"
$env:METABASE_TRIAL_DATABASE_ID = "<trial-database-id>"
python scripts/validate_metabase_rest_trial.py
```

Historical runtime status: **DEFERRED / UNVERIFIED** before the trial was
started. No active Metabase/Nessie/Trino change, dashboard change, DDL/DML, or
cutover is implied.

Changed files for this phase: `scripts/register_run06_rest_catalog.py`,
`scripts/validate_postgres_rest_trino_run06.py`,
`docker-compose.iceberg-rest-catalog-postgres.local.yml`,
`trino/rest-catalog-postgres-validation/etc/**`,
`tests/test_iceberg_rest_catalog_contract.py`, and this plan.

Static evidence: focused REST contract suites **13 passed**; Python
`compileall` **PASS**; `git diff --check` **PASS**; and
`docker compose ... config --quiet` **PASS** with placeholder contract
environment values only. The first config check failed closed as expected when
required environment variables were absent. This paragraph is historical
static evidence; runtime registration/parity evidence is recorded below.

## Isolated Metabase REST trial runtime evidence — 2026-11-13

- Isolated Metabase `v0.55.3` and PostgreSQL metadata DB started on the trial
  network; active Metabase port `3000` and metadata volume were untouched.
- Native `presto-jdbc` database connection created in the trial instance with
  host `trino-rest-postgres-validation:8080`, catalog
  `lakehouse_rest_postgres`, and schema `gold_run_20261112_gold_06`.
- Read-only native query through Metabase returned `fact_orders` row count `1`.
- Runtime validation: **PASS**. No dashboard, active datasource, Nessie path,
  Gold data file, DDL or DML was changed.
- Focused BI/REST contract tests passed (`16 passed`), `compileall` passed and
  `git diff --check` passed.

## Independent QA verdict — isolated Metabase REST trial — 2026-11-13

- Verdict: **PASS**. No findings remain open.
- Evidence: `16 passed`, `compileall` PASS, `git diff --check` PASS, endpoint
  guard verified before any trial API request, and runtime smoke returned
  `fact_orders` row count `1` through native `presto-jdbc`.
- The trial remains isolated; no active BI cutover is authorized by this
  phase. Any move from trial datasource to active Metabase requires separate
  approval.

## Persistent PostgreSQL REST runtime evidence (2026-11-13)

- Custom image `local/iceberg-rest-fixture-postgres:1.10.0-jdbc42.7.4` built
  successfully with checksum-pinned PostgreSQL JDBC `42.7.4`.
- PostgreSQL `16.4` healthcheck reached **healthy** before REST startup.
- REST Catalog connected to PostgreSQL and initialized the JDBC catalog without
  `ClassNotFoundException`.
- Jetty REST service started on internal port `8181`; read-only `/v1/config`
  probe returned successfully.
- Persistent REST runtime health: **PASS**. Gold table registration, Trino
  parity, active catalog selection and Metabase cutover remain deferred; the
  active Nessie path is unchanged.

## Independent QA verdict — runtime health scope — 2026-11-13

- Verdict: **PASS** for the bounded persistent REST runtime-health scope.
- Evidence: contract fields match the verified runtime state; PostgreSQL
  healthcheck gates REST startup; focused contract tests passed (`8 passed`);
  `compileall` and `git diff --check` passed; Docker evidence confirms custom
  image build, JDBC initialization and internal `/v1/config` success.
- No correction required. Gold registration, Trino parity, active catalog
  selection and Metabase cutover remain explicitly deferred.
- Historical next step: obtain separate approval for bounded run-06
  registration and Trino parity. That approval was subsequently granted and
  the phase is recorded below.

## Phase 2 registration and parity evidence — 2026-11-13

- Approval: granted in-session for bounded local run-06 registration and
  Trino parity only.
- Registration: **PASS**. The runner accepted only the seven certified tables,
  validated the exact `quality-it/run-20261112-gold-06/` metadata prefix, refused
  overwrite of non-empty namespaces, and registered metadata locations only.
  No Parquet/Iceberg data file was copied, rewritten, deleted, or compacted.
- Target: persistent PostgreSQL REST Catalog, isolated metadata volume and
  private network. MinIO access was read-only for metadata inspection.
- Trino parity: **PASS**. Validation compared source REST SQLite and target
  PostgreSQL REST through separate Trino validation services. All seven table
  sets, schemas, row counts, snapshot IDs, file counts and physical file bytes
  matched. `fact_orders` remained 1 row with 3,088 physical bytes.
- Validation controls: target Trino uses file-based read-only access control;
  the validation runner issued only `SHOW`, `DESCRIBE` and `SELECT` statements.
- Focused tests passed (`13 passed`), `compileall` passed, and `git diff --check`
  passed. Active Nessie/Trino/Metabase configuration was not changed and no
  consumer cutover was performed.

## Independent QA verdict — registration and parity — 2026-11-13

- Verdict: **PASS**. No findings remain open.
- Endpoint guards in both runtime scripts reject non-HTTP endpoints,
  credentials, paths, and hosts/ports outside the approved local allowlist.
- Evidence reviewed: exactly seven tables, metadata-only registration,
  read-only Trino validation, `13 passed`, `compileall` PASS and
  `git diff --check` PASS.
- No Gold data-file mutation, Nessie mutation, active Trino/Metabase change or
  consumer cutover occurred. The next phase, if desired, is a separately
  approved BI consumer trial; this REST registration/parity phase is complete.

## Approved cleanup status (2026-11-13)

The obsolete SQLite REST fixture/compose/config and its SQLite Trino
validation container were retired. The persistent PostgreSQL REST Catalog,
PostgreSQL REST Trino validation, and Metabase REST trial remain retained.
Historical SQLite evidence is preserved in this plan; no REST PostgreSQL
volume, MinIO object, Nessie ref, Gold table, or active BI service was deleted
or reset.

## Shared run-06 contract refactor (2026-11-13)

Added `gold/implementation/gold_table_registry.py` as the source of truth for
the certified seven-table order, run/schema/prefix/ref identity, snapshot IDs,
and expected row counts. REST registration/parity, BI promotion, dimensional
Gold write order, and Trino/contract tests reuse these values where behavior is
unchanged. Added `gold/implementation/local_endpoint.py` for the shared plain
local HTTP host/port/path guard used by REST and Metabase trial validators;
Nessie `/api/v2` validation remains specialized.

Verification: registry compatibility and endpoint regression tests were added;
focused compatibility suite passed (`42 passed`), `compileall` passed, and
`git diff --check` passed. No runtime/data mutation was performed.

## Local HTTP/JSON client refactor (2026-11-13)

Added `common/http_json.py` for the duplicated local REST JSON request path and
Trino `nextUri` pagination used by the retained registration/parity and
Metabase trial scripts. Callers retain their endpoint allowlists, read-only
checks, metadata-only registration, apply gate, active/fallback boundaries,
and exact timeout values (`20s` registration; `30s` Metabase and Trino). No
Compose file or runtime behavior was changed.

Focused unit coverage is in `tests/test_http_json.py`. `compileall`,
`git diff --check`, and the standard-library fake-opener smoke passed. Focused
pytest is **BLOCKED** in the current interpreter because `pytest` is not
installed; no runtime/data mutation was run. Independent QA must rerun the
focused suite in the project test environment before closing this refactor.
