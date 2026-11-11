# Gold lakehouse commit plan

Branch: `feature/iceberg-audit-improvement`

The current validated working tree is split into dependency-ordered commits. Each
commit is staged explicitly, checked, and remains local until separately approved
for push or merge.

1. Safety and bounded quality-it runtime scaffolding.
   Check: compose config, bounded-scope tests, compileall.
2. Incremental replay, mutable-source signals, tombstones and migrations.
   Check: extractor/replay/source-change/transaction tests.
3. Lakehouse contracts, evidence, reconciliation and diagnostics.
   Check: lakehouse quality/safety/reconcile tests.
4. Gold dimensional contracts, writer, metadata and verification.
   Check: Gold dimensional/metadata/recovery tests.
5. Airflow orchestration and project documentation/plan closure.
   Check: Airflow parse tests plus full safe regression.

Rollback point: each commit can be reverted independently before push. No
production database, shared lakehouse, consumer route, or Gold WRITE path is
enabled by this plan.
