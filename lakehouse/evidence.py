"""Immutable, content-addressed quality/reconciliation evidence helpers."""

import hashlib
import json

from common.manifest import put_json_object_if_absent, read_json_object


class LakehouseEvidenceError(ValueError):
    pass


def build_evidence(*, kind, table, snapshot_id, status, details, catalog="lakehouse", namespace="silver", ref="main", migration_manifest_identity="", snapshot_attestation_identity=""):
    if kind not in {"quality", "reconciliation"}:
        raise LakehouseEvidenceError("Unsupported evidence kind")
    if not isinstance(table, str) or not table:
        raise LakehouseEvidenceError("Invalid evidence table")
    if not isinstance(snapshot_id, int) or isinstance(snapshot_id, bool) or snapshot_id <= 0:
        raise LakehouseEvidenceError("Invalid evidence snapshot_id")
    if status != "PASS":
        raise LakehouseEvidenceError("Only PASS evidence may authorize Gold")
    if not isinstance(details, dict):
        raise LakehouseEvidenceError("Evidence details must be a dictionary")
    identity = snapshot_attestation_identity or migration_manifest_identity
    if not all(isinstance(value, str) and value for value in (catalog, namespace, ref)):
        raise LakehouseEvidenceError("Evidence requires catalog/ref")
    if not isinstance(identity, str) or len(identity) != 64:
        raise LakehouseEvidenceError("Evidence requires attestation identity")
    if not all(character in "0123456789abcdef" for character in identity):
        raise LakehouseEvidenceError("Invalid attestation identity")
    payload = {"version": 2, "kind": kind, "table": table, "catalog": catalog, "namespace": namespace, "ref": ref, "snapshot_id": snapshot_id, "snapshot_attestation_identity": identity, "status": status, "details": details}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return {**payload, "identity": hashlib.sha256(canonical.encode("utf-8")).hexdigest()}


def evidence_key(identity):
    if not isinstance(identity, str) or len(identity) != 64:
        raise LakehouseEvidenceError("Invalid evidence identity")
    return f"lakehouse/evidence/v2/{identity}.json"


def verify_evidence(document, *, kind, table, snapshot_id, catalog="lakehouse", namespace="silver", ref="main", migration_manifest_identity="", snapshot_attestation_identity=""):
    if not isinstance(document, dict):
        raise LakehouseEvidenceError("Invalid evidence document")
    expected = build_evidence(kind=kind, table=table, snapshot_id=snapshot_id, status=document.get("status"), details=document.get("details"), catalog=catalog, namespace=namespace, ref=ref, migration_manifest_identity=migration_manifest_identity, snapshot_attestation_identity=snapshot_attestation_identity)
    if document != expected:
        raise LakehouseEvidenceError("Evidence content or identity mismatch")
    return True


def publish_evidence(client, bucket, evidence, *, scope=None):
    """Create an immutable evidence object; never overwrite a different proof."""
    verify_evidence(evidence, kind=evidence["kind"], table=evidence["table"], snapshot_id=evidence["snapshot_id"], catalog=evidence["catalog"], namespace=evidence["namespace"], ref=evidence["ref"], snapshot_attestation_identity=evidence["snapshot_attestation_identity"])
    key = evidence_key(evidence["identity"])
    if scope is not None:
        key = scope.key(key)
    try:
        actual = read_json_object(client, bucket, key)
        if actual != evidence:
            raise LakehouseEvidenceError("Conflicting evidence already exists")
        return {"key": key, "status": "ALREADY_PUBLISHED", "identity": evidence["identity"]}
    except Exception as exc:
        if getattr(exc, "code", None) not in {"NoSuchKey", "NoSuchObject"}:
            raise
    try:
        put_json_object_if_absent(client, bucket, key, evidence)
    except Exception as exc:
        if getattr(exc, "code", None) not in {"PreconditionFailed", "ConditionalRequestConflict"}:
            raise
        actual = read_json_object(client, bucket, key)
        if actual != evidence:
            raise LakehouseEvidenceError("Concurrent evidence conflict") from exc
        return {"key": key, "status": "ALREADY_PUBLISHED", "identity": evidence["identity"]}
    actual = read_json_object(client, bucket, key)
    if actual != evidence:
        raise LakehouseEvidenceError("Evidence read-back verification failed")
    return {"key": key, "status": "PUBLISHED", "identity": evidence["identity"]}


def publish_pass_evidence_for_attestation(client, bucket, *, kind, table, snapshot_id,
                                          details, attestation_identity,
                                          catalog="lakehouse", namespace="silver", ref="main", scope=None):
    """Publish authorization evidence only after its attestation is read-back verified."""
    from lakehouse.attestation import read_snapshot_attestation
    read_snapshot_attestation(
        client, bucket, attestation_identity, table=table, snapshot_id=snapshot_id,
        catalog=catalog, namespace=namespace, ref=ref,
        scope=scope,
    )
    evidence = build_evidence(
        kind=kind, table=table, snapshot_id=snapshot_id, status="PASS", details=details,
        catalog=catalog, namespace=namespace, ref=ref,
        snapshot_attestation_identity=attestation_identity,
    )
    return {"evidence": evidence, **publish_evidence(client, bucket, evidence, scope=scope)}
