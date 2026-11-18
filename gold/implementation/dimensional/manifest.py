"""Content-addressed manifests and recovery decisions for one Gold graph run."""

import hashlib
import json
import re
from datetime import date, datetime, timezone

from common.manifest import put_json_object_if_absent, read_json_object
from .contracts import (
    DIMENSIONAL_CATALOG,
    DIMENSIONAL_GOLD_VERSION,
    DIMENSIONAL_NAMESPACE,
    GOLD_SNAPSHOT_METADATA_VERSION,
    WRITE_ORDER,
    canonical_contract_identity,
    table_schema_identity,
)
from .lineage import dimensional_lineage_identity, source_contract_metadata, validate_dimensional_lineage


class DimensionalManifestError(ValueError):
    """Deterministic manifest/fence failure with optional operator metadata."""

    def __init__(self, message, *, code=None, recovery_state=None, lock_key=None, graph_identity=None,
                 lock_metadata=None):
        super().__init__(message)
        self.code = code
        self.recovery_state = recovery_state
        self.lock_key = lock_key
        self.graph_identity = graph_identity
        self.lock_metadata = lock_metadata


def _canonical(payload):
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def build_graph_manifest(lineage, *, dim_date_start, dim_date_end, scope=None,
                         configured_date_start=None, configured_date_end=None,
                         observed_order_date_min=None, observed_order_date_max=None):
    validate_dimensional_lineage(lineage, scope=scope)
    start = _parse_date(dim_date_start, "dim_date_start")
    end = _parse_date(dim_date_end, "dim_date_end")
    if start > end:
        raise DimensionalManifestError("Invalid dim_date range")
    body = {"version": 1, "kind": "gold_dimensional_graph", "gold_version": DIMENSIONAL_GOLD_VERSION,
            "lineage_identity": dimensional_lineage_identity(lineage, scope=scope), "dim_date_start": start.isoformat(),
            "dim_date_end": end.isoformat(), "write_order": list(WRITE_ORDER),
            "source_contract": source_contract_metadata(lineage)}
    if configured_date_start is not None or configured_date_end is not None:
        body["configured_capture_start"] = _parse_date(configured_date_start, "configured_date_start").isoformat()
        body["configured_capture_end"] = _parse_date(configured_date_end, "configured_date_end").isoformat()
    if observed_order_date_min is not None or observed_order_date_max is not None:
        body["observed_order_date_min"] = _parse_date(observed_order_date_min, "observed_order_date_min").isoformat()
        body["observed_order_date_max"] = _parse_date(observed_order_date_max, "observed_order_date_max").isoformat()
    identity = hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()
    return {**body, "identity": identity}


def build_table_manifest(graph, *, table, snapshot_id, quality, catalog=DIMENSIONAL_CATALOG,
                         namespace=DIMENSIONAL_NAMESPACE, ref="main", stale_policy=None,
                         captured_at=None):
    if table not in WRITE_ORDER or not isinstance(snapshot_id, int) or snapshot_id <= 0:
        raise DimensionalManifestError("Invalid table manifest")
    if not all(isinstance(value, str) and value for value in (catalog, namespace, ref)):
        raise DimensionalManifestError("Invalid table catalog coordinates")
    readback = quality.get("readback") if isinstance(quality, dict) else None
    readback_identity = readback.get("identity") if isinstance(readback, dict) else None
    # Historical callers may create a logical recovery marker before physical
    # readback exists. Keep that v1 shape readable; only a physically verified
    # writer output receives reusable v2 metadata.
    if not isinstance(readback_identity, str):
        body = {"version": 1, "kind": "gold_dimensional_table", "graph_identity": graph["identity"],
                "table": table, "snapshot_id": snapshot_id, "quality": quality}
        return {**body, "identity": hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()}
    if stale_policy is None:
        if captured_at is None:
            raise DimensionalManifestError(
                "Reusable table manifest requires explicit captured_at or stale_policy"
            )
        stale_policy = {"captured_at": captured_at, "max_age_seconds": 86400}
    if (
        not isinstance(stale_policy, dict)
        or set(stale_policy) != {"captured_at", "max_age_seconds"}
        or not isinstance(stale_policy["max_age_seconds"], int)
        or isinstance(stale_policy["max_age_seconds"], bool)
        or stale_policy["max_age_seconds"] < 0
        or not isinstance(readback_identity, str)
    ):
        raise DimensionalManifestError("Reusable table manifest requires stale/readback evidence")
    stale_policy = dict(stale_policy)
    stale_policy["captured_at"] = _normalize_captured_at(stale_policy["captured_at"])
    body = {
        "version": GOLD_SNAPSHOT_METADATA_VERSION,
        "kind": "gold_dimensional_table",
        "status": "PASS",
        "graph_identity": graph["identity"],
        "table": table,
        "snapshot_id": snapshot_id,
        "catalog": catalog,
        "namespace": namespace,
        "ref": ref,
        "schema_identity": table_schema_identity(table),
        "manifest_identity": None,
        "stale_policy": stale_policy,
        "readback_identity": readback_identity,
        "quality": quality,
    }
    body["manifest_identity"] = canonical_contract_identity(body, identity_field="manifest_identity")
    # Keep the historical top-level identity as the storage/recovery identity;
    # manifest_identity is the reusable snapshot contract identity.
    body["identity"] = body["manifest_identity"]
    return body


def _normalize_captured_at(value):
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DimensionalManifestError("captured_at must be a valid ISO timestamp") from exc
    else:
        raise DimensionalManifestError("captured_at must be a valid ISO timestamp")
    if parsed.tzinfo is None:
        raise DimensionalManifestError("captured_at must be timezone-aware")
    return parsed.astimezone(timezone.utc).isoformat()


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
            raise DimensionalManifestError(
                f"Graph fence conflict; lock_key={key}; graph_identity={graph['identity']}",
                code="GRAPH_FENCE_CONFLICT",
                lock_key=key,
                graph_identity=graph["identity"],
            ) from exc
        raise DimensionalManifestError(
            "Graph fence is already held; operator recovery required; "
            f"lock_key={key}; graph_identity={graph['identity']}",
            code="GRAPH_FENCE_ALREADY_HELD",
            recovery_state="GRAPH_FENCE_RECOVERY_REQUIRED",
            lock_key=key,
            graph_identity=graph["identity"],
        ) from exc
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
        holder = current.get("graph_identity") if isinstance(current, dict) else None
        raise DimensionalManifestError(
            "Global fence conflict; operator recovery required; "
            f"lock_key={key}; holder_graph_identity={holder}; requested_graph_identity={graph['identity']}",
            code="GLOBAL_FENCE_CONFLICT",
            recovery_state="GLOBAL_FENCE_RECOVERY_REQUIRED",
            lock_key=key,
            graph_identity=graph["identity"],
            lock_metadata=current,
        ) from exc
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
