"""Fail-closed storage layout contract for production and test environments.

This module is intentionally pure: resolving a layout validates configuration
but never creates clients or mutates storage.  Legacy incremental keys remain
available for recovery of batches whose locations were persisted previously.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from common.bounded_e2e_scope import TEST_BUCKET, BoundedE2EScope, build_bounded_e2e_scope

PRODUCTION_BUCKET = "ecommerce-lakehouse-prod"
QUALITY_BUCKET = "ecommerce-lakehouse-quality-it"
PRODUCTION_REF = "gold_prod_current"
LEGACY_LAYOUT = "legacy"
PRODUCTION_LAYOUT = "production"
QUALITY_LAYOUT = "quality-it"

_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_PRODUCTION_REF = re.compile(r"^gold_prod_(?:backfill|candidate)_[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class StorageLayoutError(ValueError):
    """Raised when storage configuration is unsafe or internally inconsistent."""


@dataclass(frozen=True)
class StorageLayout:
    environment: str
    bucket: str
    nessie_ref: str | None = None
    run_id: str | None = None
    scope: BoundedE2EScope | None = None

    @property
    def is_production(self):
        return self.environment == PRODUCTION_LAYOUT

    def prefix(self, layer: str, table_name: str) -> str:
        _require_name(layer, "layer")
        _require_name(table_name, "table name")
        if layer not in {"bronze", "silver", "gold"}:
            raise StorageLayoutError("layer must be bronze, silver or gold")
        root = self.scope.prefix if self.scope is not None else (
            f"quality-it/{self.run_id}/" if self.environment == QUALITY_LAYOUT else ""
        )
        return f"{root}{layer}/{table_name}/"

    def key(self, layer: str, table_name: str, suffix: str = "") -> str:
        prefix = self.prefix(layer, table_name)
        if not isinstance(suffix, str) or suffix.startswith("/") or ".." in suffix.split("/"):
            raise StorageLayoutError("object suffix escapes the storage layout")
        return prefix + suffix


@dataclass(frozen=True)
class ProductionGoldScope:
    """Explicit production Gold coordinates for an isolated candidate branch.

    Production is not the bounded E2E exception: the scope has no object
    prefix, but it still carries an explicit candidate ref and target namespace
    so a WRITE cannot fall back to the legacy ``gold`` namespace or bucket.
    """

    bucket: str
    candidate_ref: str
    target_namespace: str
    prefix: str = ""

    @property
    def warehouse(self):
        return f"s3a://{self.bucket}/lakehouse"

    @property
    def nessie_ref(self):
        return self.candidate_ref

    @property
    def gold_namespace(self):
        return self.target_namespace

    @property
    def silver_namespace(self):
        return "silver"

    def key(self, suffix):
        if not isinstance(suffix, str) or suffix.startswith("/") or ".." in suffix.split("/"):
            raise StorageLayoutError("Object suffix escapes the production Gold layout")
        return self.prefix + suffix


def _require_name(value, label):
    if not isinstance(value, str) or _NAME.fullmatch(value) is None:
        raise StorageLayoutError(f"Invalid {label}")
    return value


def _production_flags_present(environ):
    return any(
        environ.get(name) not in (None, "", "0", "false", "False")
        for name in (
            "LAKEHOUSE_E2E_APPROVED",
            "AIRFLOW_LOCAL_E2E_MODE",
            "QUALITY_IT_ENABLED",
            "QUALITY_IT_GATE2_APPROVED",
        )
    )


def is_production_ref(value):
    return value == PRODUCTION_REF or bool(_PRODUCTION_REF.fullmatch(value or ""))


def validate_storage_environment(environ=None) -> StorageLayout:
    """Validate bucket/layout identity without creating any external client."""
    values = os.environ if environ is None else environ
    bucket = values.get("BRONZE_BUCKET")
    requested = values.get("LAKEHOUSE_STORAGE_ENV", "").strip().lower()
    if not requested:
        requested = {
            PRODUCTION_BUCKET: PRODUCTION_LAYOUT,
            QUALITY_BUCKET: QUALITY_LAYOUT,
            TEST_BUCKET: LEGACY_LAYOUT,
        }.get(bucket, PRODUCTION_LAYOUT if bucket is None else "")
    if requested not in {PRODUCTION_LAYOUT, QUALITY_LAYOUT, LEGACY_LAYOUT}:
        raise StorageLayoutError("LAKEHOUSE_STORAGE_ENV must be production, quality-it or legacy")

    if requested == PRODUCTION_LAYOUT:
        if bucket != PRODUCTION_BUCKET:
            raise StorageLayoutError("Production must use ecommerce-lakehouse-prod")
        if _production_flags_present(values):
            raise StorageLayoutError("Production cannot use quality-it or E2E flags")
        requested_ref = values.get("NESSIE_REF", PRODUCTION_REF)
        if not is_production_ref(requested_ref):
            raise StorageLayoutError("Production must use gold_prod_current or an approved immutable production candidate ref")
        return StorageLayout(PRODUCTION_LAYOUT, PRODUCTION_BUCKET, requested_ref)

    if requested == QUALITY_LAYOUT:
        if bucket != QUALITY_BUCKET:
            raise StorageLayoutError("quality-it must use ecommerce-lakehouse-quality-it")
        run_id = values.get("QUALITY_IT_RUN_ID", "")
        if _RUN_ID.fullmatch(run_id) is None:
            raise StorageLayoutError("QUALITY_IT_RUN_ID must be an immutable run ID")
        prefix = values.get("QUALITY_IT_PREFIX", f"quality-it/{run_id}/")
        if prefix != f"quality-it/{run_id}/":
            raise StorageLayoutError("QUALITY_IT_PREFIX must match QUALITY_IT_RUN_ID")
        return StorageLayout(QUALITY_LAYOUT, QUALITY_BUCKET, values.get("NESSIE_REF", "quality_it_current"), run_id)

    if bucket != TEST_BUCKET:
        raise StorageLayoutError("Legacy layout must use ecommerce-data-lake")
    scope = build_bounded_e2e_scope(values)
    return StorageLayout(LEGACY_LAYOUT, TEST_BUCKET, scope.nessie_ref, scope=scope)


def build_layer_key(layer, table_name, suffix="", *, layout=None, environ=None):
    selected = layout or validate_storage_environment(environ)
    return selected.key(layer, table_name, suffix)


def bronze_table_key(table_name, suffix="", *, layout=None, environ=None):
    return build_layer_key("bronze", table_name, suffix, layout=layout, environ=environ)


def silver_table_key(table_name, suffix="", *, layout=None, environ=None):
    return build_layer_key("silver", table_name, suffix, layout=layout, environ=environ)


def gold_table_key(table_name, suffix="", *, layout=None, environ=None):
    return build_layer_key("gold", table_name, suffix, layout=layout, environ=environ)


__all__ = [
    "LEGACY_LAYOUT", "PRODUCTION_BUCKET", "PRODUCTION_LAYOUT", "PRODUCTION_REF",
    "QUALITY_BUCKET", "QUALITY_LAYOUT", "ProductionGoldScope", "StorageLayout", "StorageLayoutError",
    "bronze_table_key", "build_layer_key", "gold_table_key", "silver_table_key",
    "validate_storage_environment",
    "is_production_ref",
]
