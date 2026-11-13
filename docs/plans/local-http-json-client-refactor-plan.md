# Local HTTP/JSON client refactor

## Approved scope — 2026-11-13

Refactor duplicated standard-library HTTP request, JSON response validation,
bounded HTTP error detail, timeout forwarding, and Trino `nextUri` pagination
used by the isolated REST registration/parity and Metabase validation scripts.

The refactor preserves each caller's existing timeout (`20s` for REST
registration and `30s` for Metabase/Trino), endpoint allowlists, read-only
query guards, metadata-only registration behavior, `--apply` environment gate,
exact query results, and active/fallback boundaries. Compose files, runtime
services, databases, object storage, Nessie refs, and checkpoints are out of
scope.

## Acceptance criteria

- Shared utility handles request serialization, response object validation,
  HTTP status/detail, explicit timeout forwarding, and Trino pagination.
- Existing endpoint validators, allowlists, read-only guards, apply gates and
  query/result contracts remain in their original callers.
- Focused unit tests cover serialization/timeout, pagination and bounded HTTP
  errors without external services.
- No Compose/runtime/data mutation is executed.

## Verification evidence

- Focused pytest in repository `.venv`: **38 passed** with workspace basetemp.
- `compileall -q common scripts gold tests`: **PASS**.
- `git diff --check`: **PASS**.
- Standard-library fake-opener JSON/pagination smoke: **PASS**.
- HTTP error detail is bounded at read/store time to 500 characters, with a
  regression assertion on the stored exception detail.

## QA handoff

Independent QA must inspect the diff against this scope and confirm the
focused pytest evidence. The phase remains open until QA closes it.

## Project-wide hygiene pass — 2026-11-13

Removed UTF-8 BOM markers from six test modules that prevented standard-library
AST parsing. No production logic, safety guard, runtime configuration, or data
contract changed.

Verification: all 168 project Python files under maintained source/test roots
parse successfully; targeted duplicate-import audit passed; focused REST/HTTP
compatibility tests passed (`18 passed`). A broader non-runtime run collected
successfully but reported `260 passed`, `67 deselected`, `5 failed`, and
`38 errors` due to bounded-scope environment assumptions, Windows pytest temp
directory permissions, and a dynamic Airflow import-path fixture. No
runtime/data mutation was performed.
