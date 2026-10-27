
from uuid import uuid4

from gold.implementation.storage.contract import (
    GoldStorageContractError,
    get_gold_storage_paths,
    validate_dataset_name,
    validate_gold_snapshot_id,
)
from gold.implementation.contracts.validator import (
    validate_gold_contract,
)


def write_gold_snapshot_parquet(
    spark,
    gold_df,
    bucket,
    dataset_name,
    snapshot_id=None,
):
    """
    Write Gold Parquet into a new immutable snapshot path.

    This function:
    - validates the Gold dataset and snapshot identity
    - uses Spark errorifexists mode
    - writes Parquet only
    - does not publish a manifest
    - does not modify Silver snapshots

    The caller must perform Gold Data Quality validation
    before invoking this function.
    """

    if not isinstance(bucket, str) or not bucket.strip():
        raise GoldStorageContractError(
            "Invalid Gold storage bucket"
        )

    validate_dataset_name(dataset_name)
    validate_gold_contract(dataset_name)

    if snapshot_id is None:
        snapshot_id = "gold_snap_" + uuid4().hex

    validate_gold_snapshot_id(snapshot_id)

    paths = get_gold_storage_paths(
        dataset_name,
        snapshot_id,
    )

    data_prefix = paths["data_prefix"]

    output_path = f"s3a://{bucket}/{data_prefix}"

    gold_df.write.mode("errorifexists").parquet(
        output_path
    )

    return {
        "dataset_name": dataset_name,
        "snapshot_id": snapshot_id,
        "bucket": bucket,
        "snapshot_prefix": paths["snapshot_prefix"],
        "data_prefix": data_prefix,
        "manifest_key": paths["manifest_key"],
        "output_path": output_path,
        "status": "DATA_WRITTEN",
    }
