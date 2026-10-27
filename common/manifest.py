"""Immutable JSON manifest primitives for MinIO-backed data layers."""

import json


def read_json_object(client, bucket, object_key):
    """Read and decode one JSON object while always releasing its connection."""
    response = client.get_object(bucket, object_key)
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def put_json_object_if_absent(client, bucket, object_key, payload):
    """Publish JSON with create-if-absent semantics; never overwrite a manifest."""
    if not isinstance(payload, dict):
        raise ValueError("JSON manifest payload must be an object")

    data = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return client._put_object(
        bucket_name=bucket,
        object_name=object_key,
        data=data,
        headers={"Content-Type": "application/json", "If-None-Match": "*"},
    )
