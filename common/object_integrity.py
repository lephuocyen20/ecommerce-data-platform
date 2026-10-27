"""Deterministic MinIO object discovery and checksum helpers."""

import hashlib


CHUNK_SIZE = 1024 * 1024


def calculate_object_sha256(client, bucket, object_key):
    """Stream one object and return its SHA-256 without loading it into memory."""
    response = client.get_object(bucket, object_key)
    digest = hashlib.sha256()
    try:
        while True:
            chunk = response.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    finally:
        response.close()
        response.release_conn()
    return digest.hexdigest()


def discover_parquet_objects(client, bucket, data_prefix, *, include_etag=False):
    """Return a sorted, validated inventory of Parquet objects below a prefix."""
    if not isinstance(bucket, str) or not bucket:
        raise ValueError("Invalid snapshot bucket")
    if not isinstance(data_prefix, str) or not data_prefix.endswith("/"):
        raise ValueError("Invalid snapshot data prefix")

    objects = []
    for obj in client.list_objects(bucket, prefix=data_prefix, recursive=True):
        if not (obj.object_name.startswith(data_prefix) and obj.object_name.endswith(".parquet")):
            continue
        item = {"object_key": obj.object_name, "size": obj.size}
        if include_etag:
            if not isinstance(obj.etag, str) or not obj.etag:
                raise ValueError("Invalid Parquet object ETag")
            item["etag"] = obj.etag.strip('"')
        objects.append(item)

    objects.sort(key=lambda item: item["object_key"])
    if not objects:
        raise ValueError("Snapshot Parquet files are missing")
    keys = [item["object_key"] for item in objects]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate snapshot Parquet objects")
    for item in objects:
        size = item["size"]
        if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
            raise ValueError("Invalid snapshot Parquet object size")
    return objects
