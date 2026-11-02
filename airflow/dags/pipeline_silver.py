"""Silver-layer orchestration helpers shared by the incremental DAG tasks."""

import re


def get_silver_manifest_key(table_name, batch_id):
    """Return the only valid completion-manifest key for a Silver batch."""
    if not isinstance(table_name, str) or not table_name:
        raise ValueError("Invalid Silver table name")
    if not isinstance(batch_id, str) or not re.fullmatch(r"[0-9a-f]{32}", batch_id):
        raise ValueError("Invalid Bronze batch ID")

    return (
        f"silver_incremental/{table_name}/"
        f"batch_id={batch_id}/_BATCH_METADATA.json"
    )


def get_silver_data_prefix(table_name, batch_id):
    """Return the immutable run-prefix base for a Silver batch."""
    get_silver_manifest_key(table_name, batch_id)
    return f"silver_incremental/{table_name}/batch_id={batch_id}/runs/"
