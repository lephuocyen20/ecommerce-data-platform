# Hive Metastore local smoke plan (retired)

This smoke/Phase 2 implementation was retired by the approved local BI
cleanup. Historical runtime evidence below is retained for audit only; the
Hive compose/config/scripts/tests are no longer operational and no volume was
removed.

## Approved scope

Create a static/config-only local smoke contract for an isolated Hive
Metastore backed by a dedicated PostgreSQL metadata database and a separate
read-only Trino validation configuration. This phase does not start Docker,
connect to active Trino/Metabase/Nessie/Gold/MinIO, create tables, or mutate
external services.

## Pinned images and isolation

- Hive Metastore image base: `apache/hive:4.0.1`.
- Metadata database: `postgres:16.4` with a dedicated named volume.
- Validation engine: `trinodb/trino:476` in a separate config tree.
- Compose network is private/internal and named for this smoke only.
- No root Compose file, active Trino catalog, Metabase config, Nessie ref,
  MinIO bucket/object, Gold table, or existing PostgreSQL service is changed.

## Deliverables and acceptance

- `docker-compose.hive-metastore.smoke.local.yml` defines only the isolated
  metastore, metadata DB, and optional validation Trino service.
- `hive-metastore/Dockerfile` and `hive-site.xml` use the pinned Hive image
  and isolated PostgreSQL JDBC metadata contract.
- `trino/hive-smoke/etc/catalog/hive.properties` is not mounted into active
  Trino and uses Hive Metastore only.
- `trino/hive-smoke/etc/rules.json` permits only SELECT for the seven named
  run-06 tables and denies DDL/DML/maintenance by omission/default deny.
- Static tests reject active config mounts, public ports, writes, and root
  Compose references.
- Focused pytest, compileall, and `git diff --check` pass.

## Explicitly deferred

Container startup, schema initialization, table registration, Trino queries,
parity, and any external metadata/data mutation require a separate approval.

## Runtime correction: PostgreSQL JDBC dependency (2026-11-13)

The approved local smoke runtime was attempted and failed during Hive schema
tool initialization. Exact blocker evidence was:

```text
ClassNotFoundException: org.postgresql.Driver
```

The pinned `apache/hive:4.0.1` image does not contain the PostgreSQL JDBC
driver. The correction adds hard-pinned PostgreSQL JDBC `42.7.4` to the smoke
image with Dockerfile `ADD --checksum` from a fixed Maven Central URL and
SHA256 verification; dependency resolution fails closed. No runtime retry or
Docker build was run in this correction; Gold/MinIO/Nessie/active services and
the dedicated metadata volume were not touched.

Runtime status remains **FAIL / UNVERIFIED** until a separately approved build
and smoke retry succeeds.

## Reviewer correction: non-overridable JDBC pin (2026-11-13)

The PostgreSQL JDBC dependency is now hardcoded as version `42.7.4`; there is
no Docker build argument or environment override. The Dockerfile downloads the
fixed Maven Central URL with Dockerfile `ADD --checksum` verification and
verifies SHA256
`188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e` before
copying the Hive configuration without requiring an extra downloader in the
Hive base image. Runtime/build remains unverified; no Docker start or external
data operation was performed.

## Reviewer correction: Hive auxiliary JDBC classpath (2026-11-13)

Read-only image inspection showed the Hive launcher uses
`HIVE_AUX_JARS_PATH`; placing the JAR under `/opt/hive/lib` alone is not
sufficient for this base image. The Dockerfile now sets
`HIVE_AUX_JARS_PATH=/opt/hive/lib/postgresql-42.7.4.jar` explicitly. No
container was restarted and no volume or external data was changed.

## Reviewer correction: schematool Hadoop classpath (2026-11-13)

Read-only inspection showed `/opt/hive/bin/schematool` delegates to
`hive --service schemaTool`, so `HIVE_AUX_JARS_PATH` alone did not make the
PostgreSQL driver visible during schema-tool startup. The Dockerfile now keeps
the auxiliary path and also sets
`HADOOP_CLASSPATH=/opt/hive/lib/postgresql-42.7.4.jar`. No container start,
volume reset, or external data operation was performed.

## Runtime correction: JDBC JAR permissions (2026-11-13)

Read-only image inspection identified the remaining root cause: Docker remote
`ADD` created the JDBC JAR as mode `0600` owned by root, while Hive runs as a
non-root user. The Dockerfile now applies `chmod 0644` before the non-empty
file check, preserving both classpath environment variables. No container was
started and no volume or external data was changed.

## Reviewer correction: immutable remote ADD staging (2026-11-13)

BuildKit kept a remote `ADD` artifact mode-immutable (`0600`) in the Hive
stage, so chmod there failed with `Operation not permitted`. The Dockerfile
now uses a pinned `busybox:1.36.1` fetch stage, applies `chmod 0644` and
`test -s` to `/tmp/postgresql-42.7.4.jar`, then copies it into the pinned
`apache/hive:4.0.1` stage. Classpath ENV settings remain unchanged. No runtime
start, volume reset, or external data operation was performed.

## Runtime correction: Hive JVM password override (2026-11-13)

The JDBC driver loaded successfully, but schemaTool rejected the PostgreSQL
password because this Hive image did not resolve the `${env:...}` value in
`hive-site.xml`. The Compose contract now passes the secret only through the
Hive service `SERVICE_OPTS` JVM property
`-Djavax.jdo.option.ConnectionPassword=...`; the password remains absent from
tracked files. No volume reset, container start, or external data operation was
performed.

## Historical static QA verdict (superseded by final runtime evidence)

Static/config-only QA: **PASS**.

- Pinned Hive/PostgreSQL/Trino images, isolated internal network and dedicated
  metadata volume were verified.
- Active Trino, Metabase, Nessie, Gold, MinIO, root Compose and existing
  PostgreSQL paths are not referenced by the smoke stack.
- Hive schema verification is enabled and automatic schema creation is disabled.
- Validation Trino is separate, Hive-only, read-only, and allowlists only the
  seven run-06 table names with `SELECT`.
- Reviewer evidence: focused contract tests `6 passed`, `compileall PASS`, and
  `git diff --check PASS`.
- At the time of this static review, runtime startup, JDBC compatibility, Hive
  schema initialization, table registration, and data parity were **UNVERIFIED**.

The following next-step wording is historical and superseded by the final
runtime evidence below: obtain approval, start the Metastore, and verify
startup. That smoke has since completed for the bounded run recorded below.

## Final runtime evidence (2026-11-13)

**Runtime smoke: PASS** for bounded run `20261113-hive-smoke-01`.

- Image build completed successfully with `apache/hive:4.0.1` final stage and
  `busybox:1.36.1` JDBC fetch stage.
- PostgreSQL JDBC `42.7.4` was checksum-verified against
  `188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e`.
- PostgreSQL metastore schema initialization completed.
- Hive Metastore process started successfully.
- TCP probe `hive-metastore:9083`: **PASS**.

Historical corrections retained in this plan: missing PostgreSQL driver in the
Hive base image; unavailable `curl`; JDBC classpath discovery through
`HIVE_AUX_JARS_PATH`/`HADOOP_CLASSPATH`; remote-ADD file permissions; and Hive
password environment substitution requiring the `SERVICE_OPTS` JVM override.

This PASS is limited to the isolated Hive Metastore smoke. Trino validation,
run-06 table registration, MinIO/Gold parity, active Trino/Metabase cutover,
and any production/data-path migration remain **DEFERRED / UNVERIFIED**.

Current next step: obtain separate approval for Trino validation, exact run-06
table registration, MinIO/Gold parity checks, and any active Trino/Metabase
cutover. No cutover is implied by the Hive smoke PASS.

## Phase 2 static implementation: Hive Catalog validation (2026-11-13)

This phase adds the implementation contract for a later bounded runtime run.
It does not start Docker or contact Hive, Trino, REST, Nessie, MinIO, or Gold.

The later run is limited to the exact verified run `run-20261112-gold-06`,
schema `gold_run_20261112_gold_06`, and the seven certified tables. Registration
uses existing Iceberg metadata locations only; it must not copy/rewrite Parquet
or Iceberg objects and must not mutate Nessie.

The bounded script defaults to dry-run. `--apply` additionally requires
`HIVE_PHASE2_APPLY=1`, validates every REST metadata location against the exact
run-06 object prefix, rejects foreign/missing/duplicate tables, and invokes
Iceberg `register_table` only against the isolated Hive Metastore catalog.
Trino parity is SELECT-only and compares exact table set, schema, row counts,
and run-06 snapshot IDs from the verified manifest. Active Nessie Trino,
Metabase, and their configs remain unchanged.

Static acceptance: focused contract tests, Python compileall, and
`git diff --check` pass. Runtime acceptance remains deferred until a separate
bounded runtime approval and environment with Docker access.

Later runtime commands (not executed in this phase):

```powershell
$env:HIVE_SMOKE_RUN_ID = "run-20261112-gold-06"
docker compose -f docker-compose.hive-metastore.smoke.local.yml --profile hive-metastore-smoke up -d hive-metastore-db hive-metastore
docker compose -f docker-compose.hive-metastore.smoke.local.yml --profile hive-metastore-smoke-registration run --rm hive-register-run06 --dry-run
$env:HIVE_PHASE2_APPLY = "1"
docker compose -f docker-compose.hive-metastore.smoke.local.yml --profile hive-metastore-smoke-registration run --rm hive-register-run06 --apply
docker compose -f docker-compose.hive-metastore.smoke.local.yml --profile hive-metastore-smoke-validation up -d trino-hive-validation
python scripts/validate_hive_metastore_run06.py
```

## Phase 2 runtime attempt (2026-11-13)

- Trino Iceberg Hive validation node on `18083`: **PASS / healthy**.
- Dedicated registration node: **PASS / healthy**, with no host port exposure.
- REST metadata dry-run: **PASS**; exact seven run-06 tables and canonical
  `s3://ecommerce-data-lake/quality-it/run-20261112-gold-06/` locations were
  discovered without writing.
- Apply: **BLOCKED / NOT APPLIED**. Trino access control rejected
  `system.register_table` because creating the bounded schema/table metadata is
  not permitted by the least-privilege registration policy. No Hive table
  registration, MinIO object write, Iceberg file rewrite, or Nessie mutation
  occurred.
- The previous least-privilege attempt is retained as history. The current
  bounded retry uses a private one-shot runner and no exposed registration
  port; its registration profile must still pass independent QA before apply.

## Phase 2 final runtime blocker (2026-11-13)

- Trino validation starts healthy and REST dry-run resolves the exact seven
  run-06 metadata files.
- Hive registration is **BLOCKED**, not PASS: certified Iceberg metadata uses
  `s3a://`, and Trino registration still fails to resolve
  `org.apache.hadoop.fs.s3a.S3AFileSystem` in the isolated custom image.
- Spark fallback reached Hive Metastore but failed with Hive 4 Thrift
  incompatibility (`Invalid method name: get_table`).
- No Hive table registration, MinIO object write, Iceberg metadata rewrite,
  Gold mutation, or Nessie mutation occurred; registration was stopped.

Do not mark this phase PASS without a compatible Hive 4 client/S3A runtime or
separate approval for metadata normalization. Keep active Trino/Metabase on
Nessie.
