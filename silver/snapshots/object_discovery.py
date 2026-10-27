from common.object_integrity import discover_parquet_objects


def discover_snapshot_parquet_objects(client, bucket, data_prefix):
    """
    Discover Parquet objects belonging to a Silver snapshot.

    Returns a deterministic list of object metadata.
    """

    return discover_parquet_objects(client, bucket, data_prefix)
