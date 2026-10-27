import hashlib
import json

from ingestion.incremental.config import get_table_config


def generate_snapshot_id(table_name, batches):
    """
    Generate a deterministic snapshot ID from
    the table name and ordered source batch IDs.
    """

    get_table_config(table_name)

    if not batches:
        raise ValueError(
            "Cannot generate snapshot ID without batches"
        )

    batch_ids = []

    for batch in batches:
        if batch["table_name"] != table_name:
            raise ValueError(
                "Snapshot batches belong to different tables"
            )

        batch_id = batch["batch_id"]

        if not isinstance(batch_id, str) or not batch_id:
            raise ValueError("Invalid batch ID")

        batch_ids.append(batch_id)

    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("Duplicate batch IDs")

    payload = {
        "table_name": table_name,
        "source_batch_ids": batch_ids,
    }

    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    digest = hashlib.sha256(encoded).hexdigest()

    return f"snap_{digest[:32]}"