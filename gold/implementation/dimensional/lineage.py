"""Strict, Iceberg-only Silver lineage for the dimensional Gold graph."""

import hashlib
import json
import re

from .contracts import DIMENSIONAL_SOURCE_TABLES
from lakehouse.attestation import verify_snapshot_attestation

ICEBERG_SNAPSHOT = "iceberg_snapshot"
LINEAGE_VERSION = 1
SOURCE_CONTRACT_VERSION = 1
DEFAULT_LATE_DATA_POLICY = {"mode": "overlap_reprocess", "overlap_days": 2, "watermark": "updated_at"}
DEFAULT_DELETE_POLICY = {"mode": "tombstone_only", "tombstone_column": "_is_deleted", "physical_delete": False}
DEFAULT_SCHEMA_POLICY = {"mode": "explicit_additive_only", "allow_additive": False, "allow_rename": False, "fail_on_unknown": True}
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")


def _attestation_publication_key_matches(key, identity):
    """Accept legacy keys and the exact bounded quality-it key envelope."""
    suffix = f"lakehouse/attestations/v1/{identity}.json"
    if key == suffix:
        return True
    return bool(re.fullmatch(r"quality-it/[A-Za-z0-9._-]{1,64}/" + re.escape(suffix), key or ""))


def _is_silver_table_identifier(value, table):
    return value == f"lakehouse.silver.{table}" or bool(
        re.fullmatch(r"lakehouse\.silver_[A-Za-z0-9_]+\." + re.escape(table), value or "")
    )


class DimensionalLineageError(ValueError):
    pass


def source_contract_metadata(lineage):
    """Return explicit, fail-closed source handling policy for manifests."""
    policy = lineage.get("source_contract", {})
    if not isinstance(policy, dict):
        raise DimensionalLineageError("Invalid source contract metadata")
    late = {**DEFAULT_LATE_DATA_POLICY, **policy.get("late_data", {})}
    delete = {**DEFAULT_DELETE_POLICY, **policy.get("delete", {})}
    schema = {**DEFAULT_SCHEMA_POLICY, **policy.get("schema_evolution", {})}
    if late["mode"] != "overlap_reprocess" or not isinstance(late["overlap_days"], int) or late["overlap_days"] < 0:
        raise DimensionalLineageError("Invalid late-data policy")
    if delete["mode"] != "tombstone_only" or delete["physical_delete"] is not False or delete["tombstone_column"] != "_is_deleted":
        raise DimensionalLineageError("Physical deletes are not permitted; use tombstones")
    if schema != DEFAULT_SCHEMA_POLICY:
        raise DimensionalLineageError("Unsupported schema evolution policy")
    return {"version": SOURCE_CONTRACT_VERSION, "late_data": late, "delete": delete, "schema_evolution": schema,
            "sources": {table: {"snapshot_id": lineage["sources"][table]["snapshot_id"],
                                 "delete_count": lineage["sources"][table].get("delete_count", 0),
                                 "reprocess_required": lineage["sources"][table].get("reprocess_required", False)}
                        for table in DIMENSIONAL_SOURCE_TABLES}}


def _identifier(value, field):
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise DimensionalLineageError(f"Invalid {field}")


def _evidence(value, field, source):
    expected = {"identity", "status", "snapshot_id", "table"}
    if not isinstance(value, dict) or set(value) != expected:
        raise DimensionalLineageError(f"Invalid {field} evidence")
    if value["status"] != "PASS" or value["snapshot_id"] != source["snapshot_id"] or value["table"] != source["table"]:
        raise DimensionalLineageError(f"Unauthorized {field} evidence")
    if not isinstance(value["identity"], str) or _SHA256.fullmatch(value["identity"]) is None:
        raise DimensionalLineageError(f"Invalid {field} evidence identity")


def validate_dimensional_lineage(lineage, scope=None):
    if not isinstance(lineage, dict) or not {"version", "source_type", "sources"} <= set(lineage) or set(lineage) - {"version", "source_type", "sources", "source_contract"}:
        raise DimensionalLineageError("Invalid dimensional lineage envelope")
    if lineage["version"] != LINEAGE_VERSION or lineage["source_type"] != ICEBERG_SNAPSHOT:
        raise DimensionalLineageError("Dimensional Gold requires Iceberg snapshot lineage")
    if not isinstance(lineage["sources"], dict) or set(lineage["sources"]) != set(DIMENSIONAL_SOURCE_TABLES):
        raise DimensionalLineageError("Dimensional source set does not match graph contract")
    fields = {"source_type", "table", "catalog", "namespace", "ref", "snapshot_id", "snapshot_attestation_identity", "reconciliation_evidence", "quality_evidence"}
    for table in DIMENSIONAL_SOURCE_TABLES:
        source = lineage["sources"][table]
        if not isinstance(source, dict) or not fields <= set(source) or set(source) - fields - {"schema_fingerprint", "delete_count", "reprocess_required", "bronze_batch_id", "row_count"}:
            raise DimensionalLineageError(f"Unexpected lineage fields for {table}")
        if "delete_count" in source and (not isinstance(source["delete_count"], int) or source["delete_count"] < 0):
            raise DimensionalLineageError(f"Invalid delete_count for {table}")
        if "reprocess_required" in source and not isinstance(source["reprocess_required"], bool):
            raise DimensionalLineageError(f"Invalid reprocess_required for {table}")
        if "bronze_batch_id" in source and source["bronze_batch_id"] is not None and not isinstance(source["bronze_batch_id"], str):
            raise DimensionalLineageError(f"Invalid bronze_batch_id for {table}")
        if "row_count" in source and (not isinstance(source["row_count"], int) or source["row_count"] < 0):
            raise DimensionalLineageError(f"Invalid row_count for {table}")
        if "schema_fingerprint" in source and (not isinstance(source["schema_fingerprint"], str) or _SHA256.fullmatch(source["schema_fingerprint"]) is None):
            raise DimensionalLineageError(f"Invalid schema_fingerprint for {table}")
        if source["source_type"] != ICEBERG_SNAPSHOT or source["table"] != table:
            raise DimensionalLineageError(f"Source type/table mismatch for {table}")
        expected_location = ("lakehouse", scope.silver_namespace, scope.nessie_ref) if scope is not None else ("lakehouse", "silver", "main")
        if (source["catalog"], source["namespace"], source["ref"]) != expected_location:
            if scope is None:
                raise DimensionalLineageError("Lineage location must be lakehouse.silver on main")
            raise DimensionalLineageError("Lineage location does not match the requested catalog scope")
        for field in ("catalog", "namespace", "ref"):
            _identifier(source[field], field)
        if not isinstance(source["snapshot_id"], int) or isinstance(source["snapshot_id"], bool) or source["snapshot_id"] <= 0:
            raise DimensionalLineageError(f"Invalid snapshot_id for {table}")
        if not isinstance(source["snapshot_attestation_identity"], str) or _SHA256.fullmatch(source["snapshot_attestation_identity"]) is None:
            raise DimensionalLineageError(f"Invalid snapshot attestation for {table}")
        _evidence(source["reconciliation_evidence"], "reconciliation", source)
        _evidence(source["quality_evidence"], "quality", source)
    if "source_contract" in lineage:
        source_contract_metadata(lineage)
    return True


def canonical_dimensional_lineage(lineage, *, scope=None):
    validate_dimensional_lineage(lineage, scope=scope)
    return json.dumps(lineage, sort_keys=True, separators=(",", ":"))


def dimensional_lineage_identity(lineage, *, scope=None):
    return hashlib.sha256(canonical_dimensional_lineage(lineage, scope=scope).encode("utf-8")).hexdigest()


def build_dimensional_lineage_from_artifacts(artifacts, *, scope=None):
    """Build lineage only from six verified Silver Iceberg artifact envelopes."""
    if not isinstance(artifacts, dict) or set(artifacts) != set(DIMENSIONAL_SOURCE_TABLES):
        raise DimensionalLineageError("Dimensional Gold requires six Silver artifact envelopes")
    sources = {}
    for table in DIMENSIONAL_SOURCE_TABLES:
        artifact = artifacts[table]
        if not isinstance(artifact, dict) or artifact.get("status") != "PASS":
            raise DimensionalLineageError(f"Silver artifact verification failed: {table}")
        lineage = artifact.get("iceberg_lineage")
        if not isinstance(lineage, dict):
            raise DimensionalLineageError(f"Missing migrated Iceberg lineage artifact: {table}")
        sources[table] = lineage
    envelope = {"version": LINEAGE_VERSION, "source_type": ICEBERG_SNAPSHOT, "sources": sources}
    validate_dimensional_lineage(envelope, scope=scope)
    return envelope


def normalize_dimensional_artifact(table, attestation, reconciliation, quality, *, bronze_batch_id=None, row_count=None):
    """Normalize three local CLI outputs into one pinned lineage artifact."""
    def parse_cli_output(value, name):
        """Extract the last JSON payload from Docker/Spark stdout noise."""
        if not isinstance(value, str):
            return value
        lines = [line.strip() for line in value.splitlines() if line.strip()]
        for line in reversed(lines):
            candidates = [line]
            if "=" in line:
                candidates.append(line.rsplit("=", 1)[1].strip())
            start = line.find("{")
            if start >= 0:
                candidates.append(line[start:])
            start = line.find("[")
            if start >= 0:
                candidates.append(line[start:])
            for candidate in candidates:
                try:
                    parsed = json.loads(candidate)
                except json.JSONDecodeError:
                    continue
                # Docker/XCom can JSON-encode the complete log envelope once
                # more. Decode exactly one additional JSON layer only when the
                # decoded value is an object-like string.
                if isinstance(parsed, str) and parsed.lstrip().startswith(("{", "[")):
                    try:
                        parsed = json.loads(parsed)
                    except json.JSONDecodeError:
                        continue
                # Airflow may persist each stdout line as a structured log
                # envelope. Only unwrap the documented `event` field from an
                # exact envelope shape; never recursively search arbitrary JSON.
                if (
                    isinstance(parsed, dict)
                    and isinstance(parsed.get("event"), str)
                    and set(parsed) <= {"timestamp", "level", "event", "logger", "filename", "lineno"}
                ):
                    try:
                        return json.loads(parsed["event"])
                    except json.JSONDecodeError:
                        continue
                return parsed
        raise DimensionalLineageError(f"Invalid {name} JSON")

    def payload(value, name):
        value = parse_cli_output(value, name)
        def unwrap_transport(item):
            if name != "attestation" or not isinstance(item, dict):
                return item
            for wrapper_key in ("stdout", "return_value", "result"):
                wrapped = item.get(wrapper_key)
                if isinstance(wrapped, (str, dict, list)) and set(item) <= {wrapper_key, "status", "exit_code", "returncode"}:
                    return parse_cli_output(wrapped, name)
            return item
        if isinstance(value, list):
            value = [unwrap_transport(item) for item in value]
            if name == "attestation" and len(value) == 1 and isinstance(value[0], dict) and isinstance(value[0].get("attestation"), dict):
                value = value[0]
        # Airflow providers may wrap DockerOperator XCom in a bounded result
        # envelope. Unwrap only documented transport keys; never search
        # arbitrary nested objects (which could bypass lineage contracts).
        if name == "attestation" and isinstance(value, dict):
            for wrapper_key in ("stdout", "return_value", "result"):
                wrapped = value.get(wrapper_key)
                allowed_transport = {wrapper_key, "status", "exit_code", "returncode"}
                if isinstance(wrapped, (str, dict, list)) and set(value) <= allowed_transport:
                    value = parse_cli_output(wrapped, name)
                    break
        if isinstance(value, list) and value and all(isinstance(item, str) for item in value):
            for item in reversed(value):
                try:
                    value = parse_cli_output(item, name)
                    break
                except DimensionalLineageError:
                    continue
        if isinstance(value, list):
            matches = [
                item for item in value
                if isinstance(item, dict)
                and (item.get("table") == table or _is_silver_table_identifier(item.get("table"), table))
            ]
            if len(matches) != 1:
                raise DimensionalLineageError(f"Missing unique {name} result for {table}")
            value = matches[0]
        nested_attestation = False
        if name == "attestation" and isinstance(value, dict) and isinstance(value.get("attestation"), str):
            # Some DockerOperator/XCom serializers preserve the publication
            # envelope but stringify the nested document. Decode only this
            # bounded field; arbitrary string payloads remain rejected.
            try:
                nested = json.loads(value["attestation"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise DimensionalLineageError(f"{name} nested publication contract is required for {table}") from exc
            if not isinstance(nested, dict):
                raise DimensionalLineageError(f"{name} nested publication contract is required for {table}")
            value = {**value, "attestation": nested}
        if name == "attestation" and isinstance(value, dict) and isinstance(value.get("attestation"), dict):
            nested_attestation = True
            outer_status = value.get("status")
            nested_status = value["attestation"].get("status")
            if outer_status not in {"PUBLISHED", "ALREADY_PUBLISHED", "PASS"}:
                raise DimensionalLineageError(f"{name} publication contract failed for {table}")
            if nested_status is not None and nested_status != "PUBLISHED":
                raise DimensionalLineageError(f"{name} nested publication status invalid for {table}")
            nested_identity = value["attestation"].get("identity") or value.get("identity")
            if not _attestation_publication_key_matches(value.get("key"), nested_identity):
                raise DimensionalLineageError(f"{name} publication key mismatch for {table}")
            # publish_snapshot_attestation() returns publication metadata next
            # to the canonical attestation document; status is outer metadata.
            value = {**value["attestation"], "status": outer_status}
        elif name == "attestation" and isinstance(value, dict):
            # DockerOperator/XCom may deserialize the canonical attestation
            # document and drop the outer publication envelope. Accept only
            # the exact signed document shape; arbitrary status/identity JSON
            # remains rejected below.
            canonical_fields = {
                "catalog", "identity", "kind", "manifest_identities",
                "namespace", "ref", "snapshot_id", "table", "version",
            }
            publication_fields = canonical_fields | {"status", "key"}
            if set(value) not in (canonical_fields, publication_fields) or value.get("kind") != "iceberg_snapshot_attestation":
                raise DimensionalLineageError(f"{name} nested publication contract is required for {table}")
            if set(value) == publication_fields:
                if value.get("status") not in {"PUBLISHED", "ALREADY_PUBLISHED", "PASS"}:
                    raise DimensionalLineageError(f"{name} publication contract failed for {table}")
                if not _attestation_publication_key_matches(value.get("key"), value.get("identity")):
                    raise DimensionalLineageError(f"{name} publication key mismatch for {table}")
                value = {field: value[field] for field in canonical_fields}
            try:
                verify_snapshot_attestation(value, table=table)
            except Exception as exc:
                raise DimensionalLineageError(f"Invalid {name} canonical document for {table}") from exc
            value = {**value, "status": "PUBLISHED"}
        elif name == "attestation":
            raise DimensionalLineageError(f"{name} nested publication contract is required for {table}")
        if not isinstance(value, dict):
            raise DimensionalLineageError(f"{name} payload is invalid for {table}")
        status = value.get("status")
        if name == "attestation":
            if status not in {"PUBLISHED", "ALREADY_PUBLISHED", "PASS"}:
                raise DimensionalLineageError(f"{name} did not publish for {table}")
        elif status != "PASS":
            raise DimensionalLineageError(f"{name} did not PASS for {table}")
        declared_table = value.get("table") or value.get("table_name")
        if not isinstance(declared_table, str):
            raise DimensionalLineageError(f"{name} table is required for {table}")
        if name == "attestation":
            valid_tables = {table}
        else:
            valid_tables = {table, f"lakehouse.silver.{table}"}
        if declared_table not in valid_tables and not _is_silver_table_identifier(declared_table, table):
            raise DimensionalLineageError(f"{name} table mismatch for {table}")
        if name == "reconciliation" and not any(value.get(field) for field in ("identity", "reconciliation_evidence_identity", "evidence_identity")):
            raise DimensionalLineageError(f"{name} evidence identity is required for {table}")
        if name == "quality" and not any(value.get(field) for field in ("identity", "quality_evidence_identity", "evidence_identity")):
            raise DimensionalLineageError(f"{name} evidence identity is required for {table}")
        return value

    attestation, reconciliation, quality = (payload(attestation, "attestation"), payload(reconciliation, "reconciliation"), payload(quality, "quality"))
    snapshot_ids = {item.get("snapshot_id") for item in (attestation, reconciliation, quality)}
    if len(snapshot_ids) != 1 or None in snapshot_ids:
        raise DimensionalLineageError(f"Artifact snapshot mismatch for {table}")
    snapshot_id = snapshot_ids.pop()
    identity = attestation.get("identity") or attestation.get("snapshot_attestation_identity")
    reconciliation_identity = reconciliation.get("identity") or reconciliation.get("reconciliation_evidence_identity") or reconciliation.get("evidence_identity")
    quality_identity = quality.get("identity") or quality.get("quality_evidence_identity") or quality.get("evidence_identity")
    if not all(isinstance(value, str) and value for value in (identity, reconciliation_identity, quality_identity)):
        raise DimensionalLineageError(f"Artifact identities are incomplete for {table}")
    source = {"source_type": ICEBERG_SNAPSHOT, "table": table, "catalog": attestation.get("catalog", "lakehouse"), "namespace": attestation.get("namespace", "silver"), "ref": attestation.get("ref", "main"), "snapshot_id": snapshot_id, "snapshot_attestation_identity": identity,
              "reconciliation_evidence": {"identity": reconciliation_identity, "status": "PASS", "snapshot_id": snapshot_id, "table": table},
              "quality_evidence": {"identity": quality_identity, "status": "PASS", "snapshot_id": snapshot_id, "table": table}}
    if bronze_batch_id is not None:
        source["bronze_batch_id"] = bronze_batch_id
    if row_count is not None:
        source["row_count"] = row_count
    return {"status": "PASS", "iceberg_lineage": source}
