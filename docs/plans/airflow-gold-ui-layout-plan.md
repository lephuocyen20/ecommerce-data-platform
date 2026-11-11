# Airflow Gold UI Layout — Session Plan

Date: 2026-11-11
Status: IMPLEMENTED — QA PASS WITH WARNINGS

## Scope

Remove the `gold_iceberg` TaskGroup wrapper and place the existing
`gold_dimensional_write` task at DAG root. This is a Gold orchestration
boundary change with UI/grouping changes: the task remains the runtime owner
of the fail-closed Gold gate, dimensional write lifecycle, and WRITE
verification.

Keep unchanged:

- task IDs and operator arguments;
- all six Silver artifact upstream dependencies;
- all seven Gold confirmation downstream dependencies;
- atomic Gold writer, retries, XCom, trigger rules, and the approved runtime
  validation/verification contract;
- MinIO, Nessie, Spark, PostgreSQL, checkpoints and cutover behavior.

## Acceptance criteria

- No `gold_iceberg` TaskGroup remains.
- `gold_dimensional_write` remains the same task ID and is at DAG root.
- Six `dimensional_parquet_artifact_*` tasks remain upstream.
- Seven `confirm_gold_dimensional_*` tasks remain downstream.
- No second Gold writer or data-writing task is introduced.
- Airflow/static structural tests pass.

## Risks

- Existing dirty working-tree changes must not be overwritten or included.
- UI grouping changes may affect documentation or snapshot-based UI checks;
  runtime task identity and approved dependency boundaries must remain stable.

## Verification

- Focused Airflow/import/orchestration tests where available.
- Airflow-compatible DAG import or structural inspection if local dependencies
  are unavailable.
- `git diff --check` on the scoped file.

## QA requirement

Independent reviewer must inspect the implementation diff and verify task ID,
group placement, upstream/downstream sets, and absence of runtime changes.

## Implementation evidence

- Removed the `gold_iceberg` TaskGroup wrapper from
  `airflow/dags/ecommerce_incremental_ingestion.py`.
- Kept `gold_dimensional_write` at task ID root with the same operator
  arguments and atomic writer implementation.
- Kept the six dimensional artifact upstreams and seven Gold confirmation
  downstreams.
- Updated the structural group assertion in
  `tests/test_airflow_parse_imports.py` to remove the obsolete
  `gold_iceberg` expectation.
- No storage, database, checkpoint, cutover, or external-service operation was
  performed.

## Verification and QA

- Python compilation: PASS.
- AST/source structural checks: PASS.
- Scoped `git diff --check`: PASS.
- Focused pytest and Airflow DAG import: not available in this environment;
  host `pytest` and `airflow.sdk` are missing.
- Airflow container verification completed:
  - `airflow dags list-import-errors`: `No data found`.
  - `airflow dags show ecommerce_incremental_ingestion`: PASS; the rendered
    graph shows `gold_dimensional_write` at root, no `gold_iceberg` cluster,
    six artifact upstream edges, and seven confirmation downstream edges.
- Container pytest remains unavailable because the image does not include the
  `pytest` module.
- Independent QA verdict: **PASS WITH WARNINGS**. Reviewer confirmed the
  scoped UI delta, task ID, six upstreams, seven downstream markers, and no
  required code fixes. Runtime Airflow instantiation remains unverified.

The Airflow container DagBag/import verification above removes the runtime
instantiation warning for this DAG. Focused pytest coverage remains pending in
an environment with the test dependency installed.

The runtime/operator changes are part of the approved
`gold-runtime-orchestration-boundary` session and must be reviewed together
with this layout change; this is not a UI-only scope.

## Next step

After implementation and QA, update this plan with changed files, actual test
results, QA verdict, unresolved risks and the next handoff. Do not commit or
merge this change without separate user approval.

## Handoff

The layout change is not UI-only: the approved runtime orchestration boundary
also hardens canonical six-artifact input validation and WRITE verification.
A separate user approval is required before commit, push, or merge.
Recommended next verification is Airflow
DagBag/import in the project container with `airflow.sdk` and focused pytest
available.
