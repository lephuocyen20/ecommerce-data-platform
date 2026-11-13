"""Read-only verification of a completed native Gold dimensional graph."""

import json

from .contracts import WRITE_ORDER
from .manifest import read_completed_graph, read_table_manifests


class DimensionalVerificationError(ValueError):
    pass


def build_certification_report(*, run_id, code_identity, config_identity, lineage_identity,
                               bucket, prefix, ref, writer_result, verification=None, client=None, scope=None):
    """Build a deterministic, read-only certification record from runtime evidence.

    This function performs no Spark, database, or network calls.  The
    ``ALREADY_PUBLISHED`` path performs only explicit read-only manifest reads
    through the supplied client; DRY_RUN and partial paths remain client-free.
    """
    if not all(isinstance(value, str) and value for value in (run_id, code_identity, config_identity, lineage_identity, bucket, ref)):
        raise DimensionalVerificationError("Certification identity fields are required")
    if prefix is not None and (not isinstance(prefix, str) or not prefix.endswith("/")):
        raise DimensionalVerificationError("Certification prefix must be empty or slash-terminated")
    result = parse_writer_result(writer_result)
    graph = result.get("graph")
    if not isinstance(graph, dict) or not isinstance(graph.get("identity"), str):
        raise DimensionalVerificationError("Certification requires graph identity")
    status = result.get("status")
    recovery = status if status in {"ALREADY_PUBLISHED", "PARTIAL_REQUIRES_RECONCILIATION"} else "NEW"
    verification_status = verification.get("status") if isinstance(verification, dict) else "NOT_APPLICABLE"
    output_by_table = {}
    if status == "ALREADY_PUBLISHED":
        if client is None:
            raise DimensionalVerificationError("ALREADY_PUBLISHED certification requires manifest read-back client")
        completed = read_completed_graph(client, bucket, graph, scope)
        if not isinstance(completed, dict):
            raise DimensionalVerificationError("ALREADY_PUBLISHED completed graph manifest is missing")
        result = {**result, "graph_run": completed}
        manifests = read_table_manifests(client, bucket, graph, scope)
        if set(manifests) != set(WRITE_ORDER):
            raise DimensionalVerificationError("ALREADY_PUBLISHED table manifests are incomplete")
        completed_outputs = completed.get("outputs")
        if (
            not isinstance(completed_outputs, list)
            or len(completed_outputs) != len(WRITE_ORDER)
            or {item.get("table") for item in completed_outputs if isinstance(item, dict)} != set(WRITE_ORDER)
            or any(not isinstance(item, dict) or manifests[item["table"]].get("identity") != item.get("identity") for item in completed_outputs)
        ):
            raise DimensionalVerificationError("ALREADY_PUBLISHED graph/table manifest mismatch")
        for table, manifest in manifests.items():
            output_by_table[table] = {"table": table, "manifest": manifest}
    else:
        for item in result.get("tables", []) if isinstance(result.get("tables"), list) else []:
            if isinstance(item, dict) and isinstance(item.get("table"), str):
                output_by_table[item["table"]] = item
            elif isinstance(item, str):
                output_by_table[item] = {"table": item}
        for table, quality in (result.get("quality") or {}).items():
            if isinstance(quality, dict):
                output_by_table.setdefault(table, {"table": table, "quality": quality})
    if status == "PARTIAL_REQUIRES_RECONCILIATION":
        outputs = [{"table": table, "snapshot_id": None, "file_count": None, "row_count": None, "readback_status": "NOT_AVAILABLE"}
                   for table in sorted(output_by_table)]
        graph_complete = False
        readback_complete = False
    else:
        if set(output_by_table) != set(WRITE_ORDER):
            raise DimensionalVerificationError("Certification report does not cover all seven outputs")
        outputs = []
        readback_complete = True
        for table in WRITE_ORDER:
            item = output_by_table[table]
            manifest = item.get("manifest") if isinstance(item.get("manifest"), dict) else {}
            quality = manifest.get("quality") if isinstance(manifest.get("quality"), dict) else item.get("quality", {})
            readback = quality.get("readback") if isinstance(quality, dict) else None
            valid_readback = (
                isinstance(manifest.get("snapshot_id"), int) and manifest["snapshot_id"] > 0
                and isinstance(readback, dict) and readback.get("status") == "PASS"
                and readback.get("snapshot_id") == manifest.get("snapshot_id")
                and isinstance(readback.get("file_count"), int) and readback["file_count"] >= 0
                and isinstance(readback.get("file_bytes"), int) and readback["file_bytes"] >= 0
                and isinstance(readback.get("row_count"), int) and readback["row_count"] >= 0
            )
            readback_complete = readback_complete and valid_readback
            outputs.append({"table": table, "snapshot_id": manifest.get("snapshot_id"),
                            "file_count": readback.get("file_count") if isinstance(readback, dict) else None,
                            "file_bytes": readback.get("file_bytes") if isinstance(readback, dict) else None,
                            "row_count": readback.get("row_count") if isinstance(readback, dict) else None,
                            "readback_status": readback.get("status") if isinstance(readback, dict) else "MISSING"})
        graph_complete = status in {"PUBLISHED", "ALREADY_PUBLISHED"} or verification_status == "VERIFIED"
    certified = graph_complete and readback_complete and (status in {"PUBLISHED", "ALREADY_PUBLISHED"} or verification_status == "VERIFIED")
    return {
        "kind": "gold_dimensional_runtime_certification",
        "version": 1,
        "run_id": run_id,
        "code_identity": code_identity,
        "config_identity": config_identity,
        "lineage_identity": lineage_identity,
        "bucket": bucket,
        "prefix": prefix,
        "ref": ref,
        "graph_identity": graph["identity"],
        "outputs": outputs,
        "graph_manifest_complete": graph_complete,
        "readback_complete": readback_complete,
        "certified": certified,
        "verification_status": verification_status,
        "recovery_classification": recovery,
        "mode": result.get("mode", "DRY_RUN"),
        "can_write": False if result.get("mode", "DRY_RUN") == "DRY_RUN" else certified,
    }


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
