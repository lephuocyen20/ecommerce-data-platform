"""Read-only verification of a completed native Gold dimensional graph."""

import json

from .contracts import WRITE_ORDER
from .manifest import read_completed_graph, read_table_manifests


class DimensionalVerificationError(ValueError):
    pass


def parse_writer_result(value):
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or "GOLD_DIMENSIONAL_RESULT=" not in value:
        raise DimensionalVerificationError("Native writer result is not canonical JSON")
    payload = value.rsplit("GOLD_DIMENSIONAL_RESULT=", 1)[1].strip()
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise DimensionalVerificationError("Native writer result JSON is invalid") from exc


def verify_completed_graph(writer_result, client, bucket, *, scope=None):
    result = parse_writer_result(writer_result)
    graph = result.get("graph")
    if not isinstance(graph, dict) or not isinstance(graph.get("identity"), str):
        raise DimensionalVerificationError("Native writer result lacks graph identity")
    completed = read_completed_graph(client, bucket, graph, scope)
    if not isinstance(completed, dict):
        raise DimensionalVerificationError("Completed graph manifest is missing")
    outputs = completed.get("outputs")
    if (
        not isinstance(outputs, list)
        or len(outputs) != len(WRITE_ORDER)
        or any(not isinstance(item, dict) for item in outputs)
        or len({item.get("table") for item in outputs}) != len(WRITE_ORDER)
        or {item.get("table") for item in outputs} != set(WRITE_ORDER)
    ):
        raise DimensionalVerificationError("Completed graph manifest does not cover all seven outputs")
    tables = read_table_manifests(client, bucket, graph, scope)
    if set(tables) != set(WRITE_ORDER):
        raise DimensionalVerificationError("Seven table manifests are not complete")
    for item in outputs:
        manifest = tables[item["table"]]
        if manifest.get("identity") != item.get("identity"):
            raise DimensionalVerificationError(f"Graph/table manifest mismatch: {item['table']}")
    return {"status": "VERIFIED", "graph_identity": graph["identity"], "tables": list(WRITE_ORDER)}
