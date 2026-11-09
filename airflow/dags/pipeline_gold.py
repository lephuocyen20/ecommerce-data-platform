"""Gold-specific lineage helpers used by the incremental Airflow DAG."""

import json
import os
import sys

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from pipeline_contracts import GOLD_DATASET_SOURCES, TABLES
from gold.implementation.source_lineage import ICEBERG_SNAPSHOT, canonical_lineage, validate_source_lineage


def resolve_silver_snapshot_lineage():
    """Return deterministic Silver snapshot IDs for all Gold source tables."""
    from ingestion.incremental.database import get_db_connection
    from silver.snapshots.batch_discovery import get_committed_batches
    from silver.snapshots.continuity import validate_batch_continuity
    from silver.snapshots.identity import generate_snapshot_id

    conn = get_db_connection()
    try:
        snapshots = {}
        for table_name in TABLES:
            batches = get_committed_batches(conn, table_name)
            if not batches:
                raise RuntimeError(
                    f"Gold requires committed Silver source batches: {table_name}"
                )
            validate_batch_continuity(batches)
            snapshots[table_name] = generate_snapshot_id(table_name, batches)
        return snapshots
    finally:
        conn.close()


def build_gold_source_snapshot_payload(dataset_name, silver_snapshots):
    """Build a canonical JSON lineage payload for one Gold dataset."""
    if dataset_name not in GOLD_DATASET_SOURCES:
        raise ValueError(f"Unsupported Gold dataset: {dataset_name}")
    if not isinstance(silver_snapshots, dict) or set(silver_snapshots) != set(TABLES):
        raise ValueError("Incomplete Silver snapshot lineage for Gold")

    return json.dumps(
        {
            source: silver_snapshots[source]
            for source in GOLD_DATASET_SOURCES[dataset_name]
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def build_gold_source_lineage_payload(dataset_name, lineage):
    """Subset a verified all-table v2 lineage to one Gold dataset."""
    validate_source_lineage(lineage)
    if lineage["source_type"] != ICEBERG_SNAPSHOT:
        raise ValueError("Iceberg Gold payload requires iceberg_snapshot lineage")
    payload = {"version": 2, "source_type": ICEBERG_SNAPSHOT,
               "sources": {table: lineage["sources"][table] for table in GOLD_DATASET_SOURCES[dataset_name]}}
    validate_source_lineage(payload, dataset_name)
    return canonical_lineage(payload)


def load_iceberg_gold_lineage(raw_json):
    """Load explicit attested lineage; never discover mutable/latest snapshots."""
    try:
        lineage = json.loads(raw_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("GOLD_ICEBERG_SOURCE_LINEAGE must be valid JSON") from exc
    validate_source_lineage(lineage)
    if lineage["source_type"] != ICEBERG_SNAPSHOT or set(lineage["sources"]) != set(TABLES):
        raise ValueError("Iceberg Gold lineage must cover every Silver table")
    return lineage


def link_gold_orchestration(
    *,
    pipeline_summary_task,
    gold_gate_task,
    gold_skip_task,
    gold_lineage_task,
    snapshot_tasks,
    dataset_source_tasks,
    gold_write_tasks,
):
    """Wire Gold only to verified Silver lineage, never Warehouse/dbt tasks."""
    if set(snapshot_tasks) != set(TABLES):
        raise ValueError("Gold requires a snapshot task for every Silver table")
    if set(dataset_source_tasks) != set(GOLD_DATASET_SOURCES):
        raise ValueError("Gold requires a source task for every dataset")
    if set(gold_write_tasks) != set(GOLD_DATASET_SOURCES):
        raise ValueError("Gold requires a write task for every dataset")

    pipeline_summary_task >> gold_gate_task
    gold_gate_task >> gold_skip_task
    gold_gate_task >> gold_lineage_task

    for table_name in TABLES:
        gold_lineage_task >> snapshot_tasks[table_name]

    for dataset_name, sources in GOLD_DATASET_SOURCES.items():
        source_task = dataset_source_tasks[dataset_name]
        for source_table in sources:
            snapshot_tasks[source_table] >> source_task
        source_task >> gold_write_tasks[dataset_name]


def link_gold_iceberg_orchestration(*, pipeline_summary_task, gold_gate_task,
                                    gold_skip_task, gold_lineage_task,
                                    evidence_tasks, dataset_source_tasks,
                                    gold_write_tasks):
    """Wire Gold to attested Iceberg evidence, never to Warehouse/dbt."""
    if set(evidence_tasks) != set(TABLES):
        raise ValueError("Iceberg Gold requires evidence completion for every Silver table")
    if set(dataset_source_tasks) != set(GOLD_DATASET_SOURCES) or set(gold_write_tasks) != set(GOLD_DATASET_SOURCES):
        raise ValueError("Incomplete Iceberg Gold dataset wiring")
    pipeline_summary_task >> gold_gate_task
    gold_gate_task >> gold_skip_task
    gold_gate_task >> gold_lineage_task
    for task in evidence_tasks.values():
        task >> gold_lineage_task
    for dataset_name in GOLD_DATASET_SOURCES:
        gold_lineage_task >> dataset_source_tasks[dataset_name] >> gold_write_tasks[dataset_name]
