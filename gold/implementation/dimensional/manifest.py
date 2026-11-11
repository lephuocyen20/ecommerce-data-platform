"""Content-addressed manifests and recovery decisions for one Gold graph run."""

import hashlib
import json
import re
from datetime import date

from common.manifest import put_json_object_if_absent, read_json_object
from .contracts import DIMENSIONAL_GOLD_VERSION, WRITE_ORDER
from .lineage import dimensional_lineage_identity, source_contract_metadata, validate_dimensional_lineage


class DimensionalManifestError(ValueError):
    pass


def _canonical(payload):
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def build_graph_manifest(lineage, *, dim_date_start, dim_date_end, scope=None):
    validate_dimensional_lineage(lineage, scope=scope)
    start = _parse_date(dim_date_start, "dim_date_start")
    end = _parse_date(dim_date_end, "dim_date_end")
    if start > end:
        raise DimensionalManifestError("Invalid dim_date range")
    body = {"version": 1, "kind": "gold_dimensional_graph", "gold_version": DIMENSIONAL_GOLD_VERSION,
            "lineage_identity": dimensional_lineage_identity(lineage, scope=scope), "dim_date_start": start.isoformat(),
            "dim_date_end": end.isoformat(), "write_order": list(WRITE_ORDER),
            "source_contract": source_contract_metadata(lineage)}
    identity = hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()
    return {**body, "identity": identity}


def build_table_manifest(graph, *, table, snapshot_id, quality):
    if table not in WRITE_ORDER or not isinstance(snapshot_id, int) or snapshot_id <= 0:
        raise DimensionalManifestError("Invalid table manifest")
    body = {"version": 1, "kind": "gold_dimensional_table", "graph_identity": graph["identity"],
            "table": table, "snapshot_id": snapshot_id, "quality": quality}
    return {**body, "identity": hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()}


def build_graph_run_manifest(graph, table_manifests):
    """Tie the immutable per-table outputs to one fully completed graph run."""
    if (
        not isinstance(table_manifests, (list, tuple))
        or len(table_manifests) != len(WRITE_ORDER)
        or any(not isinstance(item, dict) for item in table_manifests)
        or len({item.get("table") for item in table_manifests}) != len(WRITE_ORDER)
        or {item.get("table") for item in table_manifests} != set(WRITE_ORDER)
    ):
        raise DimensionalManifestError("Graph run requires every table manifest")
    outputs = [{"table": item["table"], "identity": item["identity"], "snapshot_id": item["snapshot_id"]}
               for item in sorted(table_manifests, key=lambda item: item["table"])]
    body = {"version": 1, "kind": "gold_dimensional_graph_run", "graph_identity": graph["identity"], "outputs": outputs}
    return {**body, "identity": hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()}


def _parse_date(value, field):
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
        raise DimensionalManifestError(f"Invalid {field}; expected YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise DimensionalManifestError(f"Invalid {field}; expected calendar date") from exc


def _scoped(scope, suffix):
    return scope.key(suffix) if scope is not None else suffix


def graph_manifest_key(identity, scope=None): return _scoped(scope, f"gold/dimensional/graphs/v1/{identity}.json")
def table_manifest_key(graph_identity, table, scope=None): return _scoped(scope, f"gold/dimensional/tables/v1/{graph_identity}/{table}.json")
def graph_lock_key(identity, scope=None): return _scoped(scope, f"gold/dimensional/locks/v1/{identity}.json")
def global_lock_key(scope=None): return _scoped(scope, "gold/dimensional/locks/v1/global.json")


def read_completed_graph(client, bucket, graph, scope=None):
    """Find a completed graph by deterministic input identity without listing storage."""
    try:
        document = read_json_object(client, bucket, graph_manifest_key(graph["identity"], scope))
    except Exception as exc:
        if getattr(exc, "code", None) in {"NoSuchKey", "NoSuchObject"}:
            return None
        raise
    if document.get("kind") != "gold_dimensional_graph_run" or document.get("graph_identity") != graph["identity"]:
        raise DimensionalManifestError("Graph recovery manifest mismatch")
    return document


def read_table_manifests(client, bucket, graph, scope=None):
    """Read deterministic table completion markers; never list object storage."""
    found = {}
    for table in WRITE_ORDER:
        try:
            document = read_json_object(client, bucket, table_manifest_key(graph["identity"], table, scope))
        except Exception as exc:
            if getattr(exc, "code", None) in {"NoSuchKey", "NoSuchObject"}:
                continue
            raise
        if document.get("kind") != "gold_dimensional_table" or document.get("graph_identity") != graph["identity"] or document.get("table") != table:
            raise DimensionalManifestError("Table recovery manifest mismatch")
        found[table] = document
    return found


def acquire_graph_fence(client, bucket, graph, scope=None):
    """Create-once fence. It is intentionally never auto-released after a failure."""
    lock = {"version": 1, "kind": "gold_dimensional_graph_fence", "graph_identity": graph["identity"]}
    key = graph_lock_key(graph["identity"], scope)
    try:
        put_json_object_if_absent(client, bucket, key, lock)
    except Exception as exc:
        if getattr(exc, "code", None) not in {"PreconditionFailed", "ConditionalRequestConflict"}:
            raise
        current = read_json_object(client, bucket, key)
        if current != lock:
            raise DimensionalManifestError("Graph fence conflict") from exc
        raise DimensionalManifestError("Graph fence is already held") from exc
    current = read_json_object(client, bucket, key)
    if current != lock:
        raise DimensionalManifestError("Graph fence read-back verification failed")
    return lock


def acquire_global_fence(client, bucket, graph, scope=None):
    """Serialize all dimensional writers, including different graph identities."""
    lock = {"version": 1, "kind": "gold_dimensional_global_fence", "graph_identity": graph["identity"]}
    key = global_lock_key(scope)
    try:
        put_json_object_if_absent(client, bucket, key, lock)
    except Exception as exc:
        if getattr(exc, "code", None) not in {"PreconditionFailed", "ConditionalRequestConflict"}:
            raise
        current = read_json_object(client, bucket, key)
        if current.get("kind") != lock["kind"]:
            raise DimensionalManifestError("Global fence conflict") from exc
        raise DimensionalManifestError("Another dimensional Gold writer holds the global fence") from exc
    current = read_json_object(client, bucket, key)
    if current != lock:
        raise DimensionalManifestError("Global fence read-back verification failed")
    return lock


def assert_global_fence(client, bucket, fence, scope=None):
    if read_json_object(client, bucket, global_lock_key(scope)) != fence:
        raise DimensionalManifestError("Global fence ownership is no longer valid")
    return True


def release_global_fence(client, bucket, fence, scope=None):
    """Release only a verified successful writer's transient global fence."""
    if read_json_object(client, bucket, global_lock_key(scope)) != fence:
        raise DimensionalManifestError("Refusing to release another writer's global fence")
    try:
        client.remove_object(bucket, global_lock_key(scope))
    except Exception as exc:
        raise DimensionalManifestError("Global fence release failed; operator action required") from exc


def assert_graph_fence(client, bucket, graph, fence, scope=None):
    current = read_json_object(client, bucket, graph_lock_key(graph["identity"], scope))
    if current != fence:
        raise DimensionalManifestError("Graph fence ownership is no longer valid")
    return True


def publish_manifest(client, bucket, manifest, key):
    try:
        current = read_json_object(client, bucket, key)
    except Exception as exc:
        if getattr(exc, "code", None) not in {"NoSuchKey", "NoSuchObject"}:
            raise
    else:
        if current != manifest: raise DimensionalManifestError("Conflicting immutable manifest")
        return "ALREADY_PUBLISHED"
    try:
        put_json_object_if_absent(client, bucket, key, manifest)
    except Exception as exc:
        if getattr(exc, "code", None) not in {"PreconditionFailed", "ConditionalRequestConflict"}:
            raise
        current = read_json_object(client, bucket, key)
        if current != manifest:
            raise DimensionalManifestError("Concurrent immutable manifest conflict") from exc
        return "ALREADY_PUBLISHED"
    current = read_json_object(client, bucket, key)
    if current != manifest: raise DimensionalManifestError("Manifest read-back verification failed")
    return "PUBLISHED"
