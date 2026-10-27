from functools import reduce

from silver.snapshots.continuity import (
    validate_batch_continuity,
)
from silver.snapshots.silver_reader import (
    read_verified_silver_batch,
)


def assemble_silver_batches(
    spark,
    client,
    bucket,
    batches,
):
    """Assemble verified Silver batches in watermark order."""

    if not batches:
        raise ValueError(
            "Cannot assemble snapshot without committed batches"
        )

    batch_ids = [batch["batch_id"] for batch in batches]

    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("Duplicate batch IDs")

    expected_order = sorted(
        batches,
        key=lambda batch: (
            batch["end_timestamp"] is not None,
            batch["end_timestamp"]
            if batch["end_timestamp"] is not None
            else "",
            batch["end_id"],
            batch["batch_id"],
        ),
    )

    if batch_ids != [
        batch["batch_id"] for batch in expected_order
    ]:
        raise ValueError("Batches are not in watermark order")

    validate_batch_continuity(batches)

    table_name = batches[0]["table_name"]

    if any(
        batch["table_name"] != table_name
        for batch in batches
    ):
        raise ValueError(
            "Snapshot batches belong to different tables"
        )

    dataframes = []

    for sequence, batch in enumerate(batches, start=1):
        df = read_verified_silver_batch(
            spark=spark,
            client=client,
            bucket=bucket,
            batch=batch,
            batch_sequence=sequence,
        )

        if dataframes:
            expected_schema = dataframes[0].schema
            actual_schema = df.schema

            if actual_schema != expected_schema:
                raise ValueError(
                    f"Silver schema mismatch in batch {batch['batch_id']}"
                )

        dataframes.append(df)

    return reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=False,
        ),
        dataframes,
    )
