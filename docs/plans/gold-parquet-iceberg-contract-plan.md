# Gold Parquet Data Files Managed by Iceberg — Session Plan

Date: 2026-11-11
Status: IMPLEMENTED — QA PASS WITH WARNINGS

## Scope

Make the Gold storage contract explicit: physical Gold data files are Parquet,
managed by Apache Iceberg format v2 through the `lakehouse` Nessie catalog.
Keep the existing seven-table atomic writer, `MERGE` semantics, fences,
manifests, recovery behavior, `DRY_RUN` default, and cutover guards unchanged.

## Files likely to change

- `gold/implementation/dimensional/writer.py`
- Focused Gold writer/catalog contract tests under `tests/`
- Documentation describing Gold Iceberg-managed Parquet
- This plan with implementation evidence and independent QA verdict

Do not modify Silver writers, cutover routing, checkpoints, MinIO/Nessie data,
or Airflow dependencies in this scope.

## Acceptance criteria

- New Gold tables declare `format-version=2` and
  `write.format.default=parquet`.
- Existing table property drift fails closed; no automatic migration or
  destructive maintenance is introduced.
- Gold writes remain catalog-backed Iceberg writes, not raw path-based Parquet
  writes.
- Existing schema, merge-key, fence, manifest, and partial-recovery contracts
  remain intact.
- Focused tests, compilation, and diff checks pass.
- No runtime Gold write or cutover is performed in this session.

## Risks

- Existing tables may not carry the new explicit property; migration is out of
  scope and must be handled by a separately approved preflight/migration.
- Unit tests cannot prove physical file format; bounded runtime metadata
  verification remains a separate approval gate.
- The working tree already contains unrelated/uncommitted changes; edits must
  be limited to the approved files and preserve those changes.

## Verification commands

- Focused Gold writer/catalog contract pytest suite
- `python -m compileall gold airflow tests`
- `git diff --check`
- Airflow import/DagBag test only if implementation touches Airflow code

## Required independent QA

Reviewer must inspect the implementation diff against this plan and verify:

- Explicit Iceberg v2 + Parquet properties
- Fail-closed drift behavior
- No change to write guards, recovery, or cutover
- Test evidence and unresolved runtime-certification risks

## Implementation evidence

- `gold/implementation/dimensional/writer.py` now declares the Iceberg target
  properties explicitly as `format-version=2` and
  `write.format.default=parquet` during `CREATE TABLE IF NOT EXISTS`.
- Existing target validation requires both exact properties and fails closed on
  missing or mismatched values; it does not issue migration or maintenance SQL.
- `tests/test_gold_dimensional_writer.py` covers CREATE TABLE properties,
  format-version drift, Parquet property drift, schema drift, merge SQL,
  fence ordering, and partial recovery behavior.

## Verification evidence

- `.venv\Scripts\python.exe -m pytest tests/test_gold_dimensional_writer.py -q`
  — `11 passed in 0.18s`
- `python -m compileall -q gold/implementation/dimensional tests/test_gold_dimensional_writer.py`
  — passed with no output.
- `git diff --check` — passed; existing line-ending warnings were reported for
  unrelated dirty files only.
- No runtime Gold write, cutover, external service test, or destructive
  Iceberg/object-store operation was run.

## QA status and next step

Independent QA verdict: **PASS WITH WARNINGS**.

- No critical, high, medium, or low implementation findings.
- Reviewer confirmed the explicit Iceberg v2/Parquet properties, fail-closed
  drift behavior, catalog-backed writes, and unchanged schema/MERGE/fence/
  manifest/recovery/cutover behavior.
- Reviewer re-ran the focused suite: `11 passed in 0.13s`.
- Reviewer confirmed `python -m compileall -q gold tests` passed and the scoped
  `git diff --check` passed.
- Runtime physical Parquet/Nessie metadata and snapshot lineage remain
  unverified; this is a separate approval gate and not a reason to claim runtime
  certification.

The implementation phase is complete. The next step is an explicitly approved
bounded runtime/read-back verification, if desired; no commit, push, merge,
cutover, cleanup, rollback, compaction, or snapshot expiration has been done.
