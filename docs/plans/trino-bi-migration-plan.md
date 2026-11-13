# Gate 1 — Local Trino BI migration

## Scope

Add a local-only, read-only Trino service contract for the existing MinIO +
Iceberg + Nessie quality-it environment. Pin `trinodb/trino:476` (not
`latest`), use the existing Nessie v2 endpoint, and expose the `lakehouse`
catalog with schema `gold_run_20261112_gold_06` on stable Nessie ref
`bi_gold_current`.

Metabase connects through its native Trino connector over the private local
Compose network. The published Trino HTTP port is loopback-only for host tools.
The access policy grants `SELECT` only to user `metabase` on the seven existing
Gold tables. No write credentials, DDL/DML, maintenance procedures, table
creation, snapshot expiry, orphan cleanup, or production/root Compose changes
are included.

## Acceptance criteria

1. `docker-compose.trino.local.yml` is a standalone local overlay and does not
   modify `docker-compose.yml` or delete/replace the custom driver/query runtime.
2. Trino is pinned to `trinodb/trino:476`; the Iceberg catalog is Nessie v2,
   `lakehouse.gold_run_20261112_gold_06`, ref `bi_gold_current`, with MinIO
   S3-compatible endpoint settings.
3. Trino binds host port `8081` to `127.0.0.1` only and joins only the named
   quality-it and Metabase local networks.
4. File access control denies by default, permits only user `metabase`, the
   `lakehouse` catalog, schema `gold_run_20261112_gold_06`, and the seven allowlisted tables with
   `SELECT`; no write privilege or procedure grant exists.
5. S3 credentials are explicitly named read-only credentials and supplied only
   through environment placeholders; no root/admin credential is embedded.
6. Static tests validate Compose/config/catalog/access-rule shape, exact table
   and ref scope, private binding, no maintenance/write configuration, and the
   native Metabase connection contract.

## Compatibility evidence

Trino release 476 is retained because it is a fixed non-latest release and its
official release notes document improved S3-compatible storage compatibility.
The Trino Iceberg catalog documentation documents Nessie API v2 configuration
through `iceberg.catalog.type=nessie`, URI, and ref properties. The
repository's Nessie service is pinned to `0.108.8` and exposes `/api/v2`; this
phase records configuration compatibility only and does not start either
service.

## Risks and runtime gaps

- The read-only MinIO credential must be provisioned externally with object
  `GET`/`LIST` access for the quality-it warehouse; this phase does not create
  or mutate that credential or policy.
- The exact Nessie ref is intentionally fixed to the certified run-06 ref;
  run-scoped ref switching is out of Gate 1 and must not be added by
  environment override.
- Native Metabase connection, Trino startup, catalog discovery, and query
  results remain unverified because runtime is explicitly out of scope.
- The Trino image and connector dependency are not added to root Compose.

## Files

- `docker-compose.trino.local.yml`
- `trino/etc/config.properties`, `jvm.config`, `node.properties`
- `trino/etc/catalog/lakehouse.properties`
- `trino/etc/access-control.properties`, `trino/etc/rules.json`
- `tests/test_trino_local_contract.py`
- `docs/RUNBOOK.md` and archived `docs/metabase-driver.md`

## Verification commands

```powershell
.venv\Scripts\python.exe -m pytest -q tests/test_trino_local_contract.py
.venv\Scripts\python.exe -m compileall -q tests
git diff --check
docker compose -f docker-compose.quality-it.local.yml `
  -f docker-compose.metabase.local.yml `
  -f docker-compose.trino.local.yml config --quiet
```

The Compose command is validation-only. Do not run `up`, query Trino, write
MinIO/Nessie/PostgreSQL, or execute Gold maintenance.

## Verification evidence (2026-11-13)

Implementation completed for the static Gate 1 contract. The focused test
`tests/test_trino_local_contract.py` passed with `6 passed`; Python
`compileall -q tests` passed; and `git diff --check` passed. Merged Compose
validation passed with exit code `0` using the quality-it, Metabase, and Trino
overlays. Placeholder credentials were supplied only as process environment
values for interpolation; no file or service data was changed.

No `up`, Trino query, Metabase startup, MinIO/Nessie/PostgreSQL write, Gold
write, snapshot maintenance, or production/root Compose change was performed.
Runtime catalog discovery and native Metabase query execution remain
**UNVERIFIED**.

## Status

Gate 1 static/config contract: **PASS**. Runtime and data certification:
**UNVERIFIED / not attempted**.

## Runtime smoke correction: writable Trino node data path (2026-11-13)

The first local runtime smoke reached Trino startup but failed because
`node.data-dir=/var/trino/data` was not writable by the image user. The bounded
correction changes only the node data path to `/tmp/trino-data`, which is a
container-local writable path and does not persist or mutate Gold, MinIO,
Nessie, or PostgreSQL data. Catalog, Nessie ref, access policy, and local-only
network scope are unchanged. Runtime smoke remains outside this static-only
verification turn.

The next startup correction is limited to the Trino node identity: the image
rejects the hyphenated environment value, so it must use the valid lowercase
underscore value `local_readonly`. No catalog, access policy, network, or data
configuration changes are part of this correction.

The next startup correction adds the required Nessie default warehouse URI to
the Trino Iceberg catalog. It is configuration-only and does not create,
rewrite, or otherwise mutate any MinIO/Nessie/Iceberg data; the SELECT-only
policy remains unchanged.

## Certified run-06 scope correction (2026-11-13)

The runtime read-only smoke identified that the certified local Gold run is not
on `main`/`gold`. The candidate manifest remains pinned to Nessie ref
`quality_it_run_20261112_gold_06`; after approved promotion, the Trino catalog
and exact table policy use stable ref `bi_gold_current` with schema
`gold_run_20261112_gold_06`. Metabase must target that schema explicitly. No
write, catalog mutation, or root/production Compose change is included.

## Runtime smoke correction: MinIO network reachability (2026-11-13)

The read-only SELECT smoke showed that the Trino container could not resolve
the `minio` service because the root local MinIO service is on
`ecommerce-data-platform_default`. The local overlay now attaches Trino to
that existing external network without editing root Compose. The catalog,
certified run-06 ref/schema, SELECT-only policy, and no-write boundary remain
unchanged.

## Runtime smoke correction: Nessie endpoint collision (2026-11-13)

After adding the root local network for MinIO, the `nessie` hostname resolved
to the root Nessie service instead of the certified run-06 service. The Trino
catalog now uses the bounded host-published endpoint
the required `TRINO_NESSIE_URI` environment value; for the certified run-06
runtime it must be set to `http://host.docker.internal:19132/api/v2`. MinIO
remains on the root-network service endpoint `http://minio:9000`. Ref/schema
and SELECT-only policy are unchanged, and no root Compose or data mutation is
part of this correction.

## Metabase Presto JDBC compatibility correction (2026-11-13)

The local Trino HTTP server must accept the `Presto` protocol header emitted by
Metabase's bundled Presto JDBC driver. Add
`protocol.v1.alternate-header-name=Presto` to the coordinator config only; the
catalog, Nessie ref/schema, SELECT-only access policy, and local-only boundary
remain unchanged.

## Stable BI ref promotion phase (approved, 2026-11-13)

Add a pure local promotion tool that validates a candidate `VERIFIED` manifest
for the certified run-06 ref/schema and exact seven-table set before planning
or assigning the stable Nessie ref `bi_gold_current`. Dry-run is the default;
`--apply` is an explicit guard and requires an expected current target hash for
CAS. The REST adapter uses only Nessie reference GET and immutable TAG-create
operations. It
does not import Spark/MinIO clients, write objects, execute SQL, or perform
maintenance.

Trino will read `bi_gold_current` while keeping schema
`gold_run_20261112_gold_06`. The PowerShell wrapper validates a local Nessie
endpoint and required candidate/expected-hash environment values. This phase
does not execute Nessie apply, runtime, or data mutation.

## QA HIGH correction: Python CLI promotion guards (2026-11-13)

The Python CLI must enforce the same fail-closed boundary as the PowerShell
wrapper. `--apply` now requires `BI_REF_PROMOTION_APPLY=1`, and the Nessie URI
is restricted to HTTP local hosts with `/api/v2` and the approved local port
set; the certified run-06 endpoint is
`http://host.docker.internal:19132/api/v2`. Arbitrary HTTP(S) or production
URIs are rejected before any REST request. Dry-run remains mutation-free.

Static evidence for this phase (2026-11-13):
`tests/test_bi_ref_promotion.py` and `tests/test_trino_local_contract.py`
passed with `17 passed`; `python -m compileall -q gold tests` passed; and
`git diff --check` passed. No Nessie REST request, `--apply`, runtime startup,
or data mutation was executed.

QA HIGH correction: the Python CLI now requires `BI_REF_PROMOTION_APPLY=1`
when `--apply` is present, and both the CLI and REST adapter reject non-local,
non-HTTP, non-`/api/v2`, or non-approved-port Nessie URIs before any request.

## Stable ref missing-target correction (2026-11-13)

The current local blocker is that `bi_gold_current` is absent. The previous
promotion implementation only attempted a PUT branch assignment, which cannot
create the required immutable stable target. The correction uses the Nessie v2
reference-create endpoint to create a `TAG` at the verified candidate hash;
there is no branch assignment path for the stable target. An existing branch is
rejected, an existing tag with a different hash is rejected, and an existing
tag at the candidate hash is an idempotent no-op.

For apply, the missing-target state must be explicit: the caller supplies
`expected_current_hash=ABSENT`, which is the CAS assertion that the target GET
returned 404. Existing targets continue to require their exact current hash.
Dry-run performs only reference reads and never creates or assigns a ref.

Independent QA after the missing-target and response-shape corrections: **PASS**.
Focused promotion/Trino contract tests passed (`17` promotion tests plus the
Trino local contract suite), `compileall` and `git diff --check` passed. The
local apply then created the immutable TAG `bi_gold_current` at candidate hash
`6e5d384db96bbe8798ae270dd8f2416be396c7523ae21df5c70763f2caebd46e` using the
Nessie v2 create-reference contract (`type=TAG` query, `DETACHED` hash body).

Runtime evidence: read-only GET returned `TAG/bi_gold_current` at the exact
candidate hash; Trino read through `lakehouse.gold_run_20261112_gold_06` and
returned `fact_orders` `row_count=1`, `net_amount=24.00`, and
`physicalInputBytes=3088`. No MinIO/Iceberg data objects were written.

Next step: sync the already-connected native Metabase Presto database schema
and verify Query Builder/dashboard reads through the stable ref. The legacy
driver/runtime and repository-local dashboard/query layer have now been
retired by the approved Trino-only cleanup batch.

## Approved cleanup supersession (2026-11-13)

The later approved local BI cleanup batch supersedes the earlier retention
wording above: the custom driver, registered-query HTTP runtime, and Streamlit
were retired from the workspace. Native Metabase → Trino is the only supported
local BI path, and the stable `bi_gold_current`/run-06 scope remains.
