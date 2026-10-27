
from gold.implementation.contracts.datasets import (
    get_gold_contract,
)
from gold.implementation.contracts.validator import (
    validate_gold_contract,
)
from silver.snapshots.publisher import (
    get_snapshot_manifest_key,
    read_published_manifest,
)
from silver.snapshots.recovery import (
    inspect_snapshot_state,
)
from silver.snapshots.verifier import (
    verify_snapshot,
)


def read_gold_source(
    spark,
    client,
    bucket,
    dataset_name,
    table_name,
    snapshot_id,
):
    """
    Read a verified, published Silver Snapshot
    as an input to a Gold dataset.

    Read-only. Does not publish or modify objects.
    """

    validate_gold_contract(dataset_name)
    contract = get_gold_contract(dataset_name)

    if table_name not in contract["sources"]:
        raise ValueError(
            f"{table_name} is not a source of {dataset_name}"
        )

    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise ValueError("Invalid snapshot ID")

    state = inspect_snapshot_state(
        client=client,
        bucket=bucket,
        table_name=table_name,
        snapshot_id=snapshot_id,
    )

    if state["state"] != "PUBLISHED":
        raise ValueError(
            f"Silver Snapshot is not published: "
            f"{table_name}/{snapshot_id} "
            f"state={state['state']}"
        )

    manifest_key = get_snapshot_manifest_key(
        table_name=table_name,
        snapshot_id=snapshot_id,
    )

    manifest = read_published_manifest(
        client=client,
        bucket=bucket,
        manifest_key=manifest_key,
    )

    if manifest.get("snapshot_id") != snapshot_id:
        raise ValueError("Snapshot ID mismatch")

    if manifest.get("table_name") != table_name:
        raise ValueError("Snapshot table mismatch")

    verify_snapshot(
        spark=spark,
        client=client,
        bucket=bucket,
        manifest=manifest,
    )

    data_prefix = manifest["data_prefix"]

    df = spark.read.parquet(
        f"s3a://{bucket}/{data_prefix}"
    )

    return {
        "dataframe": df,
        "manifest": manifest,
        "table_name": table_name,
        "snapshot_id": snapshot_id,
    }


def read_gold_sources(
    spark,
    client,
    bucket,
    dataset_name,
    snapshot_ids,
):
    """
    Read all required Silver Snapshot sources
    for one Gold dataset.
    """

    validate_gold_contract(dataset_name)
    contract = get_gold_contract(dataset_name)

    expected_sources = set(contract["sources"])

    if not isinstance(snapshot_ids, dict):
        raise ValueError("Snapshot IDs must be a dictionary")

    if set(snapshot_ids) != expected_sources:
        raise ValueError(
            f"Snapshot source mismatch for {dataset_name}"
        )

    sources = {}

    for table_name in contract["sources"]:
        sources[table_name] = read_gold_source(
            spark=spark,
            client=client,
            bucket=bucket,
            dataset_name=dataset_name,
            table_name=table_name,
            snapshot_id=snapshot_ids[table_name],
        )

    return sources
