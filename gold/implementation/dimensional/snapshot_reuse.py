"""Fail-closed contract for incremental Gold snapshot reuse.

This module deliberately resolves inputs only.  It does not read or write
Iceberg/MinIO/Nessie state.  The dimensional writer must not treat a validated
contract as permission to merge until a runtime adapter can read the pinned
snapshot and rebuild the dependent graph.
"""

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone

from .contracts import (
    DIMENSIONAL_SOURCE_TABLES,
    DIMENSIONAL_TABLES,
    TABLE_COLUMNS,
    TABLE_MERGE_KEYS,
    TABLE_SCHEMA,
    WRITE_ORDER,
    GOLD_SNAPSHOT_METADATA_VERSION,
    table_schema_metadata,
    table_schema_identity,
)

SNAPSHOT_REUSE_FEATURE = "GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED"
SILVER_DELTA = "SILVER_DELTA"
GOLD_SNAPSHOT_REUSE = "GOLD_SNAPSHOT_REUSE"
REBUILD_FROM_DELTA_PLUS_REUSED_DIM = "REBUILD_FROM_DELTA_PLUS_REUSED_DIM"
_HEX64 = re.compile(r"^[a-f0-9]{64}$")
_REUSABLE_DIMENSIONS = {"customers": "dim_customer", "products": "dim_product"}


class SnapshotReuseContractError(ValueError):
    """Raised when an incremental reuse input cannot be proven safe."""


def resolve_effective_gold_inputs(
    artifacts, *, reuse_artifacts=None, feature_enabled=False
):
    """Resolve six JSON-safe Silver states before any runtime client exists.

    The all-``NO_DATA`` state is an explicit successful no-op.  It is not a
    manifest, certification, or synthetic ``PASS`` result.  Mixed fact
    ``NO_DATA`` remains fail-closed; mixed dimension reuse delegates to the
    existing pinned-snapshot contract and therefore remains opt-in.
    """
    if not isinstance(artifacts, dict):
        raise SnapshotReuseContractError(
            "Gold effective-input resolution requires all Silver source tables"
        )
    if set(artifacts) != set(DIMENSIONAL_SOURCE_TABLES):
        fact_tables = set(DIMENSIONAL_SOURCE_TABLES) - set(_REUSABLE_DIMENSIONS)
        if any(
            isinstance(artifact, dict)
            and artifact.get("status") == "NO_DATA"
            for table, artifact in artifacts.items()
            if table in fact_tables
        ):
            raise SnapshotReuseContractError(
                "Gold dimensional facts require current Silver PASS artifacts"
            )
        raise SnapshotReuseContractError(
            "Gold effective-input resolution requires all Silver source tables"
        )
    try:
        json.dumps(artifacts, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise SnapshotReuseContractError(
            "Gold effective-input states must be JSON-serializable"
        ) from exc

    states = {}
    for table in DIMENSIONAL_SOURCE_TABLES:
        artifact = artifacts[table]
        if not isinstance(artifact, dict) or artifact.get("table_name") != table:
            raise SnapshotReuseContractError(f"Malformed Silver artifact state: {table}")
        status = artifact.get("status")
        if status == "PASS":
            states[table] = "PASS"
        elif status == "NO_DATA" and artifact.get("reason") in {"NO_DATA", "DRY_RUN"}:
            allowed = {"table_name", "status", "reason"}
            if table in _REUSABLE_DIMENSIONS:
                allowed.add("snapshot_reuse")
            if set(artifact) - allowed:
                raise SnapshotReuseContractError(
                    f"Malformed NO_DATA Silver artifact state: {table}"
                )
            states[table] = "NO_DATA"
        else:
            raise SnapshotReuseContractError(f"Invalid Silver artifact state: {table}")

    if all(state == "NO_DATA" for state in states.values()):
        return {
            "version": 1,
            "status": "GOLD_NO_CHANGE",
            "reason": "NO_CHANGE",
            "states": states,
            "effective_inputs": {
                table: {"state": "NO_DATA"} for table in DIMENSIONAL_SOURCE_TABLES
            },
        }

    fact_tables = set(DIMENSIONAL_SOURCE_TABLES) - set(_REUSABLE_DIMENSIONS)
    if any(states[table] == "NO_DATA" for table in fact_tables):
        raise SnapshotReuseContractError(
            "Gold dimensional facts require current Silver PASS artifacts"
        )

    dimension_no_data = {
        table for table in _REUSABLE_DIMENSIONS if states[table] == "NO_DATA"
    }
    if dimension_no_data:
        resolved = resolve_dimensional_inputs(
            artifacts,
            reuse_artifacts=reuse_artifacts,
            feature_enabled=feature_enabled,
        )
        return {
            "version": 1,
            "status": "GOLD_SNAPSHOT_REUSE_REQUIRED",
            "reason": "MIXED_INPUTS",
            "states": states,
            "effective_inputs": resolved["resolutions"],
        }

    return {
        "version": 1,
        "status": "GOLD_READY",
        "reason": "SILVER_PASS",
        "states": states,
        "effective_inputs": {
            table: {"state": "PASS", "artifact": artifacts[table]}
            for table in DIMENSIONAL_SOURCE_TABLES
        },
    }


class SnapshotReuseReader:
    """Explicit runtime seam for reading a verified Gold snapshot.

    No Iceberg API is assumed here.  A future Spark/PyIceberg implementation
    must provide this boundary and perform read-back verification before
    returning dataframes.  The default implementation is intentionally
    fail-closed and never creates an external client.
    """

    def __init__(self, *, spark=None, catalog_resolver=None, now=None):
        self.spark = spark
        self.catalog_resolver = catalog_resolver
        self.now = _normalize_datetime(
            datetime.now(timezone.utc) if now is None else now, "now"
        )

    def read_verified_snapshot(self, resolved):
        """Read one pinned Iceberg snapshot through injected runtime seams.

        ``catalog_resolver`` is deliberately a callable seam, not a guessed
        Nessie client API.  It receives the validated catalog/namespace/ref/
        table coordinates and must return the Spark table identifier.  Spark
        itself is used only through the existing ``read.format(...).option(...)
        .load(...)`` API already used by the Gold writer.
        """
        if not isinstance(resolved, dict) or resolved.get("resolution") != GOLD_SNAPSHOT_REUSE:
            raise SnapshotReuseContractError(
                "Gold snapshot reader requires a resolved reuse input"
            )
        table = resolved.get("table_name")
        _validate_reuse_artifact(table, resolved, now=self.now)
        snapshot = resolved["snapshot"]
        if self.spark is None or not callable(self.catalog_resolver):
            raise NotImplementedError(
                "Gold snapshot reader is not implemented without injected Spark and catalog resolver"
            )
        captured_at = _parse_timestamp(snapshot["stale_policy"]["captured_at"], "captured_at")
        if captured_at > self.now:
            raise SnapshotReuseContractError("Snapshot reuse captured_at is in the future")
        identifier = self.catalog_resolver(
            catalog=snapshot["catalog"], namespace=snapshot["namespace"],
            ref=snapshot["ref"], table=snapshot["table"],
        )
        expected_identifier = ".".join(
            (snapshot["catalog"], snapshot["namespace"], snapshot["table"])
        )
        if identifier != expected_identifier:
            raise SnapshotReuseContractError("Catalog resolver returned an unpinned table identifier")
        try:
            dataframe = (
                self.spark.read.format("iceberg")
                .option("snapshot-id", snapshot["snapshot_id"])
                .load(identifier)
            )
            files = (
                self.spark.read.format("iceberg")
                .option("snapshot-id", snapshot["snapshot_id"])
                .load(f"{identifier}.files")
            )
            row_count = int(dataframe.count())
            file_row = files.selectExpr(
                "count(*) AS file_count",
                "coalesce(sum(file_size_in_bytes), 0) AS file_bytes",
            ).first()
        except Exception as exc:
            raise SnapshotReuseContractError(
                f"Pinned Gold snapshot read failed: {snapshot['table']}@{snapshot['snapshot_id']}"
            ) from exc
        expected_schema = TABLE_COLUMNS.get(snapshot["table"])
        fields = getattr(getattr(dataframe, "schema", None), "fields", None)
        observed_schema = tuple(getattr(field, "name", None) for field in fields or ())
        if observed_schema != expected_schema:
            raise SnapshotReuseContractError(
                f"Gold snapshot schema mismatch: {snapshot['table']}"
            )
        if fields and all(hasattr(field, "dataType") and hasattr(field, "nullable") for field in fields):
            typed_schema = tuple(
                (field.name, field.dataType.simpleString(), field.nullable) for field in fields
            )
            if typed_schema != TABLE_SCHEMA[snapshot["table"]]:
                raise SnapshotReuseContractError(
                    f"Gold snapshot typed schema mismatch: {snapshot['table']}"
                )
        if file_row is None or int(file_row.file_count) <= 0 or row_count < 0:
            raise SnapshotReuseContractError("Gold snapshot readback is incomplete")
        observed = {
            "status": "PASS", "snapshot_id": snapshot["snapshot_id"],
            "row_count": row_count, "file_count": int(file_row.file_count),
            "file_bytes": int(file_row.file_bytes),
        }
        if "table" in snapshot["readback"]:
            observed["table"] = snapshot["table"]
        observed["identity"] = _canonical_identity(observed, identity_field="identity")
        if "readback_identity" in snapshot["readback"]:
            observed["readback_identity"] = observed["identity"]
        if observed != snapshot["readback"]:
            raise SnapshotReuseContractError("Gold snapshot readback evidence mismatch")
        declared_columns = snapshot["schema"].get("columns")
        if declared_columns != list(expected_schema):
            raise SnapshotReuseContractError("Gold snapshot declared schema mismatch")
        return {
            "status": "PASS", "table": snapshot["table"],
            "snapshot_id": snapshot["snapshot_id"], "dataframe": dataframe,
            "schema": snapshot["schema"], "readback": observed,
        }


def _canonical_identity(value, *, identity_field=None):
    payload = dict(value)
    if identity_field is not None:
        payload.pop(identity_field, None)
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _parse_timestamp(value, field):
    if not isinstance(value, str):
        raise SnapshotReuseContractError(f"Invalid snapshot reuse {field}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotReuseContractError(f"Invalid snapshot reuse {field}") from exc
    if parsed.tzinfo is None:
        raise SnapshotReuseContractError(f"Snapshot reuse {field} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _normalize_datetime(value, field):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise SnapshotReuseContractError(f"Snapshot reuse {field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def snapshot_reuse_enabled(environ=None):
    values = os.environ if environ is None else environ
    return isinstance(values.get(SNAPSHOT_REUSE_FEATURE, ""), str) and values.get(
        SNAPSHOT_REUSE_FEATURE, ""
    ).lower() == "true"


def _identity(value, field):
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise SnapshotReuseContractError(f"Invalid snapshot reuse {field}")


def build_snapshot_reuse_artifact(*, table, graph, manifest, readback, ref=None,
                                  stale_policy, mode="WRITE"):
    """Build a versioned reusable PASS envelope from verified evidence.

    DRY_RUN is deliberately unable to issue reusable metadata. Legacy
    envelopes remain readable, but are never upgraded implicitly.
    """
    if mode == "DRY_RUN":
        raise SnapshotReuseContractError("DRY_RUN cannot issue reusable Gold PASS metadata")
    if table not in _REUSABLE_DIMENSIONS or not all(isinstance(item, dict) for item in (graph, manifest, readback)):
        raise SnapshotReuseContractError(f"Incomplete reusable snapshot evidence: {table}")
    if not isinstance(ref, str) or not ref:
        raise SnapshotReuseContractError("Reusable snapshot ref must be explicit")
    target = _REUSABLE_DIMENSIONS[table]
    if graph.get("table") != target or manifest.get("table") != target:
        raise SnapshotReuseContractError(f"Reusable snapshot table mismatch: {table}")
    if manifest.get("status", "PASS") != "PASS" or readback.get("status") != "PASS":
        raise SnapshotReuseContractError(f"Reusable snapshot evidence is not PASS: {table}")
    snapshot_id = manifest.get("snapshot_id")
    if (not isinstance(snapshot_id, int) or isinstance(snapshot_id, bool)
            or snapshot_id <= 0 or readback.get("snapshot_id") != snapshot_id):
        raise SnapshotReuseContractError(f"Reusable snapshot pin mismatch: {table}")
    schema = table_schema_metadata(target)
    schema["identity"] = table_schema_identity(target)
    readback = dict(readback)
    readback["identity"] = readback.get("identity") or _canonical_identity(readback, identity_field="identity")
    readback["readback_identity"] = readback["identity"]
    snapshot = {
        "metadata_version": GOLD_SNAPSHOT_METADATA_VERSION,
        "catalog": "lakehouse", "namespace": "gold", "ref": ref,
        "table": target, "snapshot_id": snapshot_id,
        "graph_identity": graph.get("identity"), "graph": graph,
        "manifest_identity": manifest.get("manifest_identity", manifest.get("identity")),
        "manifest": manifest, "schema_identity": schema["identity"], "schema": schema,
        "readback_identity": readback["identity"], "readback": readback,
        "stale_policy": dict(stale_policy),
    }
    artifact = {"table_name": table, "status": "PASS", "resolution": GOLD_SNAPSHOT_REUSE, "snapshot": snapshot}
    _validate_reuse_artifact(table, artifact)
    return artifact


def _validate_reuse_artifact(table, artifact, *, now=None):
    if table not in _REUSABLE_DIMENSIONS:
        raise SnapshotReuseContractError(
            f"Snapshot reuse is limited to unchanged dimensions: {table}"
        )
    expected = {"table_name", "status", "resolution", "snapshot"}
    if not isinstance(artifact, dict) or set(artifact) != expected:
        raise SnapshotReuseContractError(f"Malformed snapshot reuse artifact: {table}")
    if artifact["table_name"] != table or artifact["status"] != "PASS":
        raise SnapshotReuseContractError(f"Snapshot reuse status is not PASS: {table}")
    if artifact["resolution"] != GOLD_SNAPSHOT_REUSE:
        raise SnapshotReuseContractError(f"Invalid snapshot reuse resolution: {table}")
    snapshot = artifact["snapshot"]
    required = {
        "catalog", "namespace", "ref", "table", "snapshot_id", "graph_identity",
        "manifest_identity", "schema_identity", "graph", "manifest", "schema", "readback",
        "stale_policy",
    }
    legacy_required = required
    current_required = required | {"metadata_version", "readback_identity"}
    if not isinstance(snapshot, dict) or set(snapshot) not in (legacy_required, current_required):
        raise SnapshotReuseContractError(f"Incomplete pinned snapshot contract: {table}")
    is_current = "metadata_version" in snapshot
    if is_current and snapshot["metadata_version"] != GOLD_SNAPSHOT_METADATA_VERSION:
        raise SnapshotReuseContractError(f"Unsupported pinned snapshot metadata version: {table}")
    if snapshot["catalog"] != "lakehouse" or snapshot["namespace"] != "gold":
        raise SnapshotReuseContractError(f"Snapshot catalog is not Gold lakehouse: {table}")
    if snapshot["table"] != _REUSABLE_DIMENSIONS[table]:
        raise SnapshotReuseContractError(f"Snapshot table mismatch: {table}")
    if (
        not isinstance(snapshot["ref"], str) or not snapshot["ref"]
        or not isinstance(snapshot["snapshot_id"], int)
        or isinstance(snapshot["snapshot_id"], bool) or snapshot["snapshot_id"] <= 0
    ):
        raise SnapshotReuseContractError(f"Invalid pinned snapshot identity: {table}")
    for field in ("graph_identity", "manifest_identity", "schema_identity"):
        _identity(snapshot[field], field)
    graph = snapshot["graph"]
    if (
        not isinstance(graph, dict)
        or graph.get("table") != snapshot["table"]
        or graph.get("identity") != snapshot["graph_identity"]
        or _canonical_identity(graph, identity_field="identity") != snapshot["graph_identity"]
    ):
        raise SnapshotReuseContractError(f"Snapshot graph identity mismatch: {table}")
    _validate_merge_graph(graph)
    manifest = snapshot["manifest"]
    if (
        not isinstance(manifest, dict)
        or manifest.get("status") != "PASS"
        or manifest.get("table") != snapshot["table"]
        or manifest.get("snapshot_id") != snapshot["snapshot_id"]
        or manifest.get("identity") != snapshot["manifest_identity"]
        or _manifest_identity(manifest) != snapshot["manifest_identity"]
    ):
        raise SnapshotReuseContractError(f"Snapshot manifest identity mismatch: {table}")
    if is_current:
        embedded_fields = {
            "catalog", "namespace", "ref", "schema_identity", "readback_identity",
        }
        if (
            not embedded_fields.issubset(manifest)
            or manifest["catalog"] != snapshot["catalog"]
            or manifest["namespace"] != snapshot["namespace"]
            or manifest["ref"] != snapshot["ref"]
            or manifest["schema_identity"] != snapshot["schema_identity"]
            or manifest["readback_identity"] != snapshot["readback_identity"]
        ):
            raise SnapshotReuseContractError(f"Snapshot manifest pin metadata mismatch: {table}")
    schema = snapshot["schema"]
    if (
        not isinstance(schema, dict)
        or schema.get("table") != snapshot["table"]
        or schema.get("identity") != snapshot["schema_identity"]
        or _schema_identity(schema, table=snapshot["table"]) != snapshot["schema_identity"]
    ):
        raise SnapshotReuseContractError(f"Snapshot schema identity mismatch: {table}")
    if schema.get("columns") != list(TABLE_COLUMNS[snapshot["table"]]):
        raise SnapshotReuseContractError(f"Snapshot schema columns mismatch: {table}")
    readback = snapshot["readback"]
    readback_fields = {"status", "snapshot_id", "identity", "row_count", "file_count", "file_bytes"}
    if not isinstance(readback, dict) or set(readback) not in (readback_fields, readback_fields | {"readback_identity"}):
        raise SnapshotReuseContractError(f"Incomplete snapshot readback: {table}")
    if (
        readback["status"] != "PASS"
        or readback["snapshot_id"] != snapshot["snapshot_id"]
        or any(not isinstance(readback[field], int) or readback[field] < 0 for field in ("row_count", "file_count", "file_bytes"))
    ):
        raise SnapshotReuseContractError(f"Snapshot readback does not match pin: {table}")
    _identity(readback["identity"], "readback identity")
    if is_current:
        if readback.get("readback_identity") != readback["identity"] or snapshot["readback_identity"] != readback["identity"]:
            raise SnapshotReuseContractError(f"Snapshot readback identity mismatch: {table}")
    if readback["file_count"] <= 0:
        raise SnapshotReuseContractError(f"Snapshot readback has no files: {table}")
    readback_body = dict(readback)
    readback_body.pop("identity", None)
    readback_body.pop("readback_identity", None)
    if _canonical_identity(readback_body) != readback["identity"]:
        raise SnapshotReuseContractError(f"Snapshot readback identity mismatch: {table}")
    stale_policy = snapshot["stale_policy"]
    if (
        not isinstance(stale_policy, dict)
        or set(stale_policy) != {"captured_at", "max_age_seconds"}
        or not isinstance(stale_policy["max_age_seconds"], int)
        or isinstance(stale_policy["max_age_seconds"], bool)
        or stale_policy["max_age_seconds"] < 0
    ):
        raise SnapshotReuseContractError(f"Invalid snapshot stale policy: {table}")
    captured_at = _parse_timestamp(stale_policy["captured_at"], "captured_at")
    if now is not None:
        now = _normalize_datetime(now, "now")
    if now is not None and captured_at > now:
        raise SnapshotReuseContractError(
            f"Snapshot reuse is stale/future-dated: {table}"
        )
    if now is not None and now > captured_at + timedelta(seconds=stale_policy["max_age_seconds"]):
        raise SnapshotReuseContractError(f"Snapshot reuse is stale: {table}")
    return artifact


def _manifest_identity(manifest):
    """Validate both the v2 self-field and the historical identity field."""
    declared = manifest.get("manifest_identity")
    if declared is not None:
        body = dict(manifest)
        body.pop("identity", None)
        body.pop("manifest_identity", None)
        return _canonical_identity(body)
    return _canonical_identity(manifest, identity_field="identity")


def _schema_identity(schema, *, table):
    if "schema" in schema:
        return table_schema_identity(table) if schema.get("table") == table else ""
    return _canonical_identity(schema, identity_field="identity")


def _validate_merge_graph(graph):
    expected = DIMENSIONAL_TABLES[graph["table"]]
    expected_sources = expected["sources"]
    if tuple(graph.get("sources", ())) != tuple(expected_sources):
        raise SnapshotReuseContractError(
            f"Snapshot graph input mapping mismatch: {graph['table']}"
        )
    if tuple(graph.get("grain", ())) != tuple(expected["grain"]):
        raise SnapshotReuseContractError(
            f"Snapshot graph grain mismatch: {graph['table']}"
        )
    if tuple(graph.get("merge_key", ())) != tuple(expected["merge_key"]):
        raise SnapshotReuseContractError(
            f"Snapshot graph merge key mismatch: {graph['table']}"
        )
    if graph.get("foreign_keys", {}) != expected.get("foreign_keys", {}):
        raise SnapshotReuseContractError(
            f"Snapshot graph foreign-key mapping mismatch: {graph['table']}"
        )


class GoldSnapshotReuseAdapter:
    """Pure semantic resolver; runtime reads remain an explicit seam."""

    def __init__(self, *, reader=None, now=None):
        self.reader = reader or SnapshotReuseReader()
        self.now = _normalize_datetime(
            datetime.now(timezone.utc) if now is None else now, "now"
        )

    def resolve(self, table, artifact):
        validated = _validate_reuse_artifact(table, artifact, now=self.now)
        return {
            "table_name": table,
            "status": "PASS",
            "resolution": GOLD_SNAPSHOT_REUSE,
            "snapshot": validated["snapshot"],
        }

    def read(self, resolved):
        if not isinstance(resolved, dict) or resolved.get("resolution") != GOLD_SNAPSHOT_REUSE:
            raise SnapshotReuseContractError("Gold snapshot read requires a resolved reuse input")
        _validate_reuse_artifact(resolved.get("table_name"), resolved, now=self.now)
        return self.reader.read_verified_snapshot(resolved)


def resolve_dimensional_inputs(artifacts, *, reuse_artifacts=None, feature_enabled=None):
    """Resolve Gold inputs without reading or mutating external services.

    ``NO_DATA`` is never converted to ``PASS``.  It resolves to a separate
    pinned snapshot input only when the feature is explicitly enabled and the
    complete immutable readback contract is present.
    """
    if not isinstance(artifacts, dict) or set(artifacts) != set(DIMENSIONAL_SOURCE_TABLES):
        raise SnapshotReuseContractError("Gold input resolution requires all Silver source tables")
    reuse_artifacts = {} if reuse_artifacts is None else reuse_artifacts
    if not isinstance(reuse_artifacts, dict):
        raise SnapshotReuseContractError("reuse_artifacts must be a mapping")
    enabled = snapshot_reuse_enabled() if feature_enabled is None else feature_enabled
    if not isinstance(enabled, bool):
        raise SnapshotReuseContractError("feature_enabled must be boolean")
    resolutions = {}
    for table in DIMENSIONAL_SOURCE_TABLES:
        artifact = artifacts[table]
        if isinstance(artifact, dict) and artifact.get("table_name") != table:
            raise SnapshotReuseContractError(f"Silver input table mismatch: {table}")
        if isinstance(artifact, dict) and artifact.get("status") == "PASS":
            resolutions[table] = {"resolution": SILVER_DELTA, "artifact": artifact}
            continue
        if not isinstance(artifact, dict) or artifact.get("status") != "NO_DATA":
            raise SnapshotReuseContractError(f"Silver input is not PASS or NO_DATA: {table}")
        if not enabled:
            raise SnapshotReuseContractError(
                f"Snapshot reuse disabled; refusing NO_DATA input: {table}"
            )
        reused = GoldSnapshotReuseAdapter().resolve(table, reuse_artifacts.get(table))
        resolutions[table] = {"resolution": GOLD_SNAPSHOT_REUSE, "artifact": reused}
    if any(item["resolution"] == GOLD_SNAPSHOT_REUSE for item in resolutions.values()):
        for table, item in resolutions.items():
            if item["resolution"] == SILVER_DELTA and table in {"orders", "order_items", "payments", "shipments"}:
                item["resolution"] = REBUILD_FROM_DELTA_PLUS_REUSED_DIM
    return {"version": 1, "feature": SNAPSHOT_REUSE_FEATURE, "resolutions": resolutions}


def snapshot_reuse_contract_identity(contract):
    """Return a deterministic identity for audit/idempotency checks."""
    if not isinstance(contract, dict) or contract.get("version") != 1:
        raise SnapshotReuseContractError("Invalid snapshot reuse contract")
    canonical = json.dumps(contract, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_incremental_merge_plan(resolved_inputs, silver_deltas, reused_dimensions):
    """Build a deterministic, non-writing merge plan for an incremental graph.

    The returned plan names exactly one source for every Gold target.  Reused
    dimensions are already verified reader results; this function never joins,
    writes, or creates a Spark client.  It is therefore safe to unit-test with
    plain fakes and remains fail-closed until a writer explicitly consumes it.
    """
    if not isinstance(resolved_inputs, dict) or resolved_inputs.get("version") != 1:
        raise SnapshotReuseContractError("Invalid resolved snapshot reuse contract")
    resolutions = resolved_inputs.get("resolutions")
    if not isinstance(resolutions, dict) or set(resolutions) != set(DIMENSIONAL_SOURCE_TABLES):
        raise SnapshotReuseContractError("Resolved contract has incomplete source resolutions")
    silver_deltas = {} if silver_deltas is None else silver_deltas
    reused_dimensions = {} if reused_dimensions is None else reused_dimensions
    if not isinstance(silver_deltas, dict) or not isinstance(reused_dimensions, dict):
        raise SnapshotReuseContractError("Merge inputs must be mappings")

    sources = {}
    for source_table in DIMENSIONAL_SOURCE_TABLES:
        item = resolutions[source_table]
        resolution = item.get("resolution") if isinstance(item, dict) else None
        if resolution == GOLD_SNAPSHOT_REUSE:
            reused = reused_dimensions.get(source_table)
            if not isinstance(reused, dict) or reused.get("status") != "PASS":
                raise SnapshotReuseContractError(f"Missing verified reused dimension: {source_table}")
            target = _REUSABLE_DIMENSIONS[source_table]
            if reused.get("table") != target or reused.get("snapshot_id", 0) <= 0:
                raise SnapshotReuseContractError(f"Invalid reused dimension identity: {source_table}")
            if "dataframe" not in reused or reused["dataframe"] is None:
                raise SnapshotReuseContractError(f"Missing reused dimension data: {source_table}")
            sources[source_table] = {"kind": GOLD_SNAPSHOT_REUSE, "target": target,
                                     "snapshot_id": reused["snapshot_id"], "data": reused["dataframe"]}
        elif resolution in {SILVER_DELTA, REBUILD_FROM_DELTA_PLUS_REUSED_DIM}:
            if source_table not in silver_deltas or silver_deltas[source_table] is None:
                raise SnapshotReuseContractError(f"Missing Silver delta: {source_table}")
            sources[source_table] = {"kind": resolution, "data": silver_deltas[source_table]}
        else:
            raise SnapshotReuseContractError(f"Unsupported input resolution: {source_table}")

    target_sources = _TARGET_SOURCES
    _validate_merge_graph_contract(target_sources)
    actions = []
    for target in WRITE_ORDER:
        inputs = target_sources.get(target, ())
        if not inputs:
            raise SnapshotReuseContractError(f"No merge inputs declared for {target}")
        source_kinds = (
            ("GENERATED",) if inputs == ("generated",)
            else tuple(sources[name]["kind"] for name in inputs)
        )
        actions.append({"target": target, "merge_key": TABLE_MERGE_KEYS[target],
                        "inputs": inputs, "source_kinds": source_kinds})
    plan = {"version": 1, "feature": SNAPSHOT_REUSE_FEATURE,
            "sources": sources, "actions": tuple(actions)}
    plan["identity"] = merge_plan_identity(plan)
    return plan


def build_controlled_dry_run_merge_plan(resolved_inputs, silver_deltas, *, snapshot_reader):
    """Resolve reused dimensions through an injected reader and build one plan.

    This is the non-writing orchestration seam for the approved controlled
    DRY_RUN.  Silver deltas are supplied explicitly by the caller; reused
    dimensions must be obtained through the supplied reader, so callers cannot
    silently substitute an unverified snapshot or fabricate a missing input.
    """
    if isinstance(snapshot_reader, GoldSnapshotReuseAdapter):
        read_snapshot = snapshot_reader.read
    elif isinstance(snapshot_reader, SnapshotReuseReader):
        read_snapshot = snapshot_reader.read_verified_snapshot
    else:
        raise SnapshotReuseContractError(
            "Controlled DRY_RUN requires a verified SnapshotReuseReader or "
            "GoldSnapshotReuseAdapter; arbitrary callables are not trusted"
        )
    if not isinstance(resolved_inputs, dict) or resolved_inputs.get("version") != 1:
        raise SnapshotReuseContractError("Invalid resolved snapshot reuse contract")
    resolutions = resolved_inputs.get("resolutions")
    if not isinstance(resolutions, dict) or set(resolutions) != set(DIMENSIONAL_SOURCE_TABLES):
        raise SnapshotReuseContractError("Resolved contract has incomplete source resolutions")
    reused_dimensions = {}
    for source_table, item in resolutions.items():
        if item.get("resolution") != GOLD_SNAPSHOT_REUSE:
            continue
        resolved = item.get("artifact")
        if not isinstance(resolved, dict):
            raise SnapshotReuseContractError(f"Missing resolved reuse artifact: {source_table}")
        result = read_snapshot(resolved)
        if not isinstance(result, dict):
            raise SnapshotReuseContractError(f"Snapshot reader returned no result: {source_table}")
        reused_dimensions[source_table] = result
    return build_incremental_merge_plan(resolved_inputs, silver_deltas, reused_dimensions)


_TARGET_SOURCES = {
    "dim_date": ("generated",),
    "dim_customer": ("customers",), "dim_product": ("products",),
    "fact_orders": ("orders", "order_items", "customers"),
    "fact_order_items": ("order_items", "products"),
    "fact_payments": ("payments",), "fact_shipments": ("shipments",),
}


def _validate_merge_graph_contract(target_sources):
    if set(target_sources) != set(WRITE_ORDER) or len(target_sources) != len(WRITE_ORDER):
        raise SnapshotReuseContractError("Merge graph has duplicate or incomplete targets")
    for target in WRITE_ORDER:
        inputs = target_sources[target]
        contract = DIMENSIONAL_TABLES[target]
        if not inputs or len(inputs) != len(set(inputs)):
            raise SnapshotReuseContractError(f"Merge graph has fan-out inputs: {target}")
        expected = ("generated",) if target == "dim_date" else contract["sources"]
        if inputs != expected:
            raise SnapshotReuseContractError(f"Merge graph input mapping mismatch: {target}")
        merge_key = TABLE_MERGE_KEYS[target]
        if not merge_key or len(merge_key) != len(set(merge_key)) or merge_key != contract["merge_key"]:
            raise SnapshotReuseContractError(f"Invalid merge key/grain mapping: {target}")
        grain = contract.get("grain")
        if not grain or len(grain) != len(set(grain)) or not set(merge_key).issubset(grain):
            raise SnapshotReuseContractError(f"Invalid grain mapping: {target}")
        for foreign_key, dimension in contract.get("foreign_keys", {}).items():
            if not foreign_key or dimension not in DIMENSIONAL_TABLES:
                raise SnapshotReuseContractError(f"Invalid foreign-key mapping: {target}")


def merge_plan_identity(plan):
    """Hash only deterministic plan metadata, excluding dataframe objects."""
    if not isinstance(plan, dict) or plan.get("version") != 1:
        raise SnapshotReuseContractError("Invalid merge plan")
    sources = {
        key: {name: value for name, value in value.items() if name != "data"}
        for key, value in sorted(plan.get("sources", {}).items())
    }
    payload = {"version": plan["version"], "feature": plan.get("feature"),
               "sources": sources, "actions": plan.get("actions")}
    return _canonical_identity(payload)


def validate_incremental_merge_plan(plan):
    """Revalidate a merge plan at the writer boundary without runtime I/O."""
    if not isinstance(plan, dict) or plan.get("version") != 1:
        raise SnapshotReuseContractError("Invalid incremental merge plan")
    if plan.get("feature") != SNAPSHOT_REUSE_FEATURE:
        raise SnapshotReuseContractError("Incremental merge plan feature mismatch")
    if plan.get("identity") != merge_plan_identity(plan):
        raise SnapshotReuseContractError("Incremental merge plan identity mismatch")
    sources = plan.get("sources")
    if not isinstance(sources, dict) or set(sources) != set(DIMENSIONAL_SOURCE_TABLES):
        raise SnapshotReuseContractError("Incremental merge plan has incomplete sources")
    for source_table in DIMENSIONAL_SOURCE_TABLES:
        source = sources[source_table]
        if not isinstance(source, dict) or source.get("data") is None:
            raise SnapshotReuseContractError(f"Incremental merge plan lacks data: {source_table}")
        if source.get("kind") == GOLD_SNAPSHOT_REUSE:
            if source.get("target") != _REUSABLE_DIMENSIONS.get(source_table):
                raise SnapshotReuseContractError(f"Reused dimension target mismatch: {source_table}")
            if not isinstance(source.get("snapshot_id"), int) or source["snapshot_id"] <= 0:
                raise SnapshotReuseContractError(f"Invalid reused snapshot id: {source_table}")
        elif source.get("kind") not in {SILVER_DELTA, REBUILD_FROM_DELTA_PLUS_REUSED_DIM}:
            raise SnapshotReuseContractError(f"Unsupported merge-plan source kind: {source_table}")
    _validate_merge_graph_contract(_TARGET_SOURCES)
    expected_actions = tuple({
        "target": target,
        "merge_key": TABLE_MERGE_KEYS[target],
        "inputs": _TARGET_SOURCES[target],
        "source_kinds": (("GENERATED",) if _TARGET_SOURCES[target] == ("generated",)
                          else tuple(sources[name]["kind"] for name in _TARGET_SOURCES[target])),
    } for target in WRITE_ORDER)
    if tuple(plan.get("actions") or ()) != expected_actions:
        raise SnapshotReuseContractError("Incremental merge plan actions do not match Gold graph contract")
    return plan
