import uuid

from ingestion.incremental.config import get_table_config
from common.paths import silver_snapshot_paths


def write_snapshot_parquet(
    spark,
    snapshot_df,
    bucket,
    table_name,
    snapshot_id=None,
):
    """
    Write an immutable Silver snapshot to MinIO.

    This function writes Parquet only.
    It does not publish a manifest or update the latest pointer.
    """

    get_table_config(table_name)

    if not bucket or not isinstance(bucket, str):
        raise ValueError("Invalid snapshot bucket")

    if snapshot_id is None:
        snapshot_id = uuid.uuid4().hex

    if (
        not isinstance(snapshot_id, str)
        or not snapshot_id
        or not all(
            char.isalnum() or char in "-_"
            for char in snapshot_id
        )
    ):
        raise ValueError("Invalid snapshot ID")

    data_prefix = silver_snapshot_paths(table_name, snapshot_id)["data_prefix"]

    output_path = f"s3a://{bucket}/{data_prefix}"

    if "_batch_sequence" not in snapshot_df.columns:
        raise ValueError("Missing snapshot batch sequence")

    output_df = snapshot_df.drop("_batch_sequence")

    output_df.write.mode("errorifexists").parquet(output_path)

    return {
        "snapshot_id": snapshot_id,
        "table_name": table_name,
        "data_prefix": data_prefix,
        "output_path": output_path,
    }
