"""Immutable, content-addressed authorization for Iceberg Silver snapshots."""

import hashlib
import json
import re

from common.manifest import put_json_object_if_absent, read_json_object


_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SnapshotAttestationError(ValueError):
    pass


def _identifier(value, field):
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise SnapshotAttestationError(f"Invalid {field}")
    return value


def _sha256(value, field):
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise SnapshotAttestationError(f"Invalid {field}")
    return value


def build_snapshot_attestation(*, table, snapshot_id, manifest_identities,
                               catalog="lakehouse", namespace="silver", ref="main"):
    """Build canonical proof for every manifest contributing to one snapshot."""
    for field, value in (("table", table), ("catalog", catalog),
                         ("namespace", namespace), ("ref", ref)):
        _identifier(value, field)
    if not isinstance(snapshot_id, int) or isinstance(snapshot_id, bool) or snapshot_id <= 0:
        raise SnapshotAttestationError("Invalid snapshot_id")
    if not isinstance(manifest_identities, (list, tuple)) or not manifest_identities:
        raise SnapshotAttestationError("manifest_identities must be non-empty")
    identities = sorted(_sha256(item, "manifest identity") for item in manifest_identities)
    if len(set(identities)) != len(identities):
        raise SnapshotAttestationError("Duplicate manifest identities")
    payload = {
        "version": 1,
        "kind": "iceberg_snapshot_attestation",
        "table": table,
        "catalog": catalog,
        "namespace": namespace,
        "ref": ref,
        "snapshot_id": snapshot_id,
        "manifest_identities": identities,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return {**payload, "identity": hashlib.sha256(canonical.encode("utf-8")).hexdigest()}


def verify_snapshot_attestation(document, **expected):
    if not isinstance(document, dict):
        raise SnapshotAttestationError("Invalid snapshot attestation")
    actual = build_snapshot_attestation(
        table=document.get("table"), snapshot_id=document.get("snapshot_id"),
        manifest_identities=document.get("manifest_identities"),
        catalog=document.get("catalog"), namespace=document.get("namespace"), ref=document.get("ref"),
    )
    if document != actual:
        raise SnapshotAttestationError("Snapshot attestation content or identity mismatch")
    for field, value in expected.items():
        if actual.get(field) != value:
            raise SnapshotAttestationError(f"Snapshot attestation {field} mismatch")
    return True


def attestation_key(identity, scope=None):
    _sha256(identity, "attestation identity")
    key = f"lakehouse/attestations/v1/{identity}.json"
    return scope.key(key) if scope is not None else key


def _read_json(client, bucket, key):
    return read_json_object(client, bucket, key)


def _is_absent(exc):
    return getattr(exc, "code", None) in {"NoSuchKey", "NoSuchObject"}


def _is_precondition_conflict(exc):
    return getattr(exc, "code", None) in {"PreconditionFailed", "ConditionalRequestConflict"}


def publish_snapshot_attestation(client, bucket, attestation, *, scope=None):
    """Idempotently create and read-back verify a content-addressed object."""
    verify_snapshot_attestation(attestation)
    key = attestation_key(attestation["identity"], scope)
    try:
        current = _read_json(client, bucket, key)
    except Exception as exc:
        if not _is_absent(exc):
            raise
    else:
        if current != attestation:
            raise SnapshotAttestationError("Conflicting snapshot attestation")
        return {"status": "ALREADY_PUBLISHED", "key": key, "identity": attestation["identity"]}
    try:
        put_json_object_if_absent(client, bucket, key, attestation)
    except Exception as exc:
        if not _is_precondition_conflict(exc):
            raise
        persisted = _read_json(client, bucket, key)
        if persisted != attestation:
            raise SnapshotAttestationError("Concurrent snapshot attestation conflict") from exc
        return {"status": "ALREADY_PUBLISHED", "key": key, "identity": attestation["identity"]}
    persisted = _read_json(client, bucket, key)
    if persisted != attestation:
        raise SnapshotAttestationError("Snapshot attestation read-back verification failed")
    return {"status": "PUBLISHED", "key": key, "identity": attestation["identity"]}


def read_snapshot_attestation(client, bucket, identity, *, scope=None, **expected):
    document = _read_json(client, bucket, attestation_key(identity, scope))
    verify_snapshot_attestation(document, identity=identity, **expected)
    return document
