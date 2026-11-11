"""Fail-closed authorization and derived names for bounded E2E runs."""

import re
from dataclasses import dataclass

TEST_BUCKET = "ecommerce-data-lake"
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class BoundedE2EScopeError(RuntimeError):
    pass


@dataclass(frozen=True)
class BoundedE2EScope:
    """The only storage/catalog namespace a shared-bucket E2E run may use."""

    bucket: str
    run_id: str
    prefix: str

    @property
    def warehouse(self):
        return f"s3a://{self.bucket}/{self.prefix}lakehouse"

    @property
    def nessie_ref(self):
        return "quality_it_" + re.sub(r"[^A-Za-z0-9_]", "_", self.run_id)

    @property
    def gold_namespace(self):
        return "gold_" + re.sub(r"[^A-Za-z0-9_]", "_", self.run_id)

    @property
    def silver_namespace(self):
        return "silver_" + re.sub(r"[^A-Za-z0-9_]", "_", self.run_id)

    def key(self, suffix):
        if not isinstance(suffix, str) or suffix.startswith("/") or ".." in suffix.split("/"):
            raise BoundedE2EScopeError("Object key is outside the approved E2E prefix")
        return f"{self.prefix}{suffix}"


def validate_bounded_e2e_scope(environ, *, object_key=None):
    """Validate before creating any storage, catalog, DB, or Spark client."""
    if environ.get("LAKEHOUSE_E2E_APPROVED") != "1":
        raise BoundedE2EScopeError("LAKEHOUSE_E2E_APPROVED=1 is required")
    if environ.get("LAKEHOUSE_E2E_TEST_BUCKET") != TEST_BUCKET:
        raise BoundedE2EScopeError("Unexpected E2E test bucket")
    run_id = environ.get("LAKEHOUSE_E2E_RUN_ID", "")
    if _RUN_ID.fullmatch(run_id) is None:
        raise BoundedE2EScopeError("Invalid immutable E2E run ID")
    expected = f"quality-it/{run_id}/"
    if environ.get("LAKEHOUSE_E2E_PREFIX") != expected:
        raise BoundedE2EScopeError("E2E prefix must match the immutable run ID")
    if object_key is not None and (not isinstance(object_key, str) or not object_key.startswith(expected) or ".." in object_key.split("/") or object_key.startswith("/")):
        raise BoundedE2EScopeError("Object key is outside the approved E2E prefix")
    return {"bucket": TEST_BUCKET, "prefix": expected, "run_id": run_id}


def build_bounded_e2e_scope(environ):
    """Validate authorization and return deterministic run-scoped names."""
    values = validate_bounded_e2e_scope(environ)
    return BoundedE2EScope(**values)


def spark_scope_configs(scope):
    return {
        "spark.sql.catalog.lakehouse.warehouse": scope.warehouse,
        "spark.sql.catalog.lakehouse.ref": scope.nessie_ref,
    }
