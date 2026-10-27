from ingestion.incremental.config import get_table_config

from silver.snapshots.assembler import (
    assemble_silver_batches,
)
from silver.snapshots.merge import (
    merge_silver_batches,
)


def build_silver_snapshot(
    spark,
    client,
    bucket,
    batches,
    table_name,
):
    """
    Build a consolidated Silver snapshot DataFrame.

    Primary key and watermark configuration are resolved
    from the central ingestion table configuration.

    This function does not publish or persist the snapshot.
    """

    config = get_table_config(table_name)

    if not batches:
        raise ValueError(
            "Cannot build snapshot without committed batches"
        )

    if any(
        batch["table_name"] != table_name
        for batch in batches
    ):
        raise ValueError(
            "Snapshot batches do not match requested table"
        )

    if any(
        batch["watermark_type"] != config["watermark_type"]
        for batch in batches
    ):
        raise ValueError(
            "Snapshot watermark type does not match table configuration"
        )

    combined_df = assemble_silver_batches(
        spark=spark,
        client=client,
        bucket=bucket,
        batches=batches,
    )

    snapshot_df = merge_silver_batches(
        df=combined_df,
        primary_key=config["primary_key"],
        watermark_column=config["watermark_column"],
    )

    return snapshot_df
