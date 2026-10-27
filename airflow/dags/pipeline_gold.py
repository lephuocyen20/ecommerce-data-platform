"""Gold-specific lineage helpers used by the incremental Airflow DAG."""

import json
import sys

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from pipeline_contracts import GOLD_DATASET_SOURCES, TABLES


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
