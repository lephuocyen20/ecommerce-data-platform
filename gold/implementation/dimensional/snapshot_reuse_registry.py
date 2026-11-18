"""Pure production metadata contract for reusable Gold dimensions.

The registry is a small, JSON-only attestation envelope.  It is deliberately
validated before any Spark, MinIO or Nessie client is constructed.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Mapping

from .snapshot_reuse import (
    GoldSnapshotReuseAdapter,
    SnapshotReuseContractError,
)

REGISTRY_VERSION = 1
REGISTRY_STATUS = "VERIFIED"
PRODUCTION_CATALOG = "lakehouse"
PRODUCTION_NAMESPACE = "gold"
PRODUCTION_REF = "gold_prod_current"
REGISTRY_KEYS = frozenset(
    {
        "version", "registry_status", "catalog", "namespace", "ref",
        "provenance", "stale_policy", "customers", "products",
        "registry_identity",
    }
)
_DIMENSIONS = {"customers": "dim_customer", "products": "dim_product"}


class GoldSnapshotReuseRegistryError(ValueError):
    """Raised when the verified production registry cannot be trusted."""


def _json_safe(value):
    try:
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise GoldSnapshotReuseRegistryError("Registry must be JSON-safe") from exc
    return value


def _canonical_body(registry):
    body = dict(registry)
    body.pop("registry_identity", None)
    return json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)


def registry_identity(registry):
    """Return the content address of a registry, excluding its self-field."""
    _json_safe(registry)
    return hashlib.sha256(_canonical_body(registry).encode("utf-8")).hexdigest()


def _timestamp(value, field):
    if not isinstance(value, str):
        raise GoldSnapshotReuseRegistryError(f"Invalid registry {field}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise GoldSnapshotReuseRegistryError(f"Invalid registry {field}") from exc
    if parsed.tzinfo is None:
        raise GoldSnapshotReuseRegistryError(f"Registry {field} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def validate_gold_snapshot_reuse_registry(registry, *, now=None):
    """Validate and return a detached canonical registry without I/O."""
    _json_safe(registry)
    if not isinstance(registry, dict) or set(registry) != REGISTRY_KEYS:
        raise GoldSnapshotReuseRegistryError("Registry has missing or extra keys")
    if registry["version"] != REGISTRY_VERSION or registry["registry_status"] != REGISTRY_STATUS:
        raise GoldSnapshotReuseRegistryError("Registry is not VERIFIED version 1")
    if (registry["catalog"], registry["namespace"], registry["ref"]) != (
        PRODUCTION_CATALOG, PRODUCTION_NAMESPACE, PRODUCTION_REF
    ):
        raise GoldSnapshotReuseRegistryError("Registry is not bound to production Gold ref")
    provenance = registry["provenance"]
    if not isinstance(provenance, dict) or set(provenance) != {"source_run"}:
        raise GoldSnapshotReuseRegistryError("Registry provenance must contain source_run")
    if not isinstance(provenance["source_run"], str) or not provenance["source_run"]:
        raise GoldSnapshotReuseRegistryError("Registry source_run must be non-empty")
    policy = registry["stale_policy"]
    if not isinstance(policy, dict) or set(policy) != {"captured_at", "max_age_seconds"}:
        raise GoldSnapshotReuseRegistryError("Registry stale_policy is malformed")
    captured_at = _timestamp(policy["captured_at"], "captured_at")
    if not isinstance(policy["max_age_seconds"], int) or isinstance(policy["max_age_seconds"], bool) or policy["max_age_seconds"] < 0:
        raise GoldSnapshotReuseRegistryError("Registry max_age_seconds is invalid")
    effective_now = datetime.now(timezone.utc) if now is None else now
    if not isinstance(effective_now, datetime) or effective_now.tzinfo is None:
        raise GoldSnapshotReuseRegistryError("Registry now must be timezone-aware")
    effective_now = effective_now.astimezone(timezone.utc)
    if captured_at > effective_now:
        raise GoldSnapshotReuseRegistryError("Registry captured_at is in the future")
    from datetime import timedelta
    if effective_now > captured_at + timedelta(seconds=policy["max_age_seconds"]):
        raise GoldSnapshotReuseRegistryError("Registry is stale")
    for source, target in _DIMENSIONS.items():
        artifact = registry[source]
        try:
            GoldSnapshotReuseAdapter(now=effective_now).resolve(source, artifact)
        except SnapshotReuseContractError as exc:
            raise GoldSnapshotReuseRegistryError(f"Invalid {source} registry artifact: {exc}") from exc
        snapshot = artifact["snapshot"]
        if (snapshot["catalog"], snapshot["namespace"], snapshot["ref"], snapshot["table"]) != (
            PRODUCTION_CATALOG, PRODUCTION_NAMESPACE, PRODUCTION_REF, target
        ):
            raise GoldSnapshotReuseRegistryError(f"{source} artifact production pin mismatch")
        if snapshot["stale_policy"] != policy:
            raise GoldSnapshotReuseRegistryError(f"{source} stale_policy mismatch")
    if not isinstance(registry["registry_identity"], str) or registry["registry_identity"] != registry_identity(registry):
        raise GoldSnapshotReuseRegistryError("Registry identity mismatch or tampering")
    return json.loads(json.dumps(registry, sort_keys=True, separators=(",", ":"), allow_nan=False))


def build_gold_snapshot_reuse_registry(
    *, customers, products, source_run=None, captured_at=None,
    max_age_seconds=None, provenance=None, stale_policy=None
):
    """Build a verified production registry from already verified artifacts."""
    if provenance is None:
        provenance = {"source_run": source_run}
    if stale_policy is None:
        stale_policy = {"captured_at": captured_at, "max_age_seconds": max_age_seconds}
    registry = {
        "version": REGISTRY_VERSION,
        "registry_status": REGISTRY_STATUS,
        "catalog": PRODUCTION_CATALOG,
        "namespace": PRODUCTION_NAMESPACE,
        "ref": PRODUCTION_REF,
        "provenance": provenance,
        "stale_policy": stale_policy,
        "customers": customers,
        "products": products,
    }
    registry["registry_identity"] = registry_identity(registry)
    return validate_gold_snapshot_reuse_registry(registry)


def registry_artifacts(registry):
    """Return only the two canonical source-to-artifact mappings."""
    validated = validate_gold_snapshot_reuse_registry(registry)
    return {source: validated[source] for source in _DIMENSIONS}

