"""Explicit, safe source selection for Lakehouse migrations."""

import os


PARQUET = "parquet"
ICEBERG = "iceberg"


def customers_silver_source(environ=None):
    """Return the selected customers Silver source; legacy Parquet is default."""
    environment = os.environ if environ is None else environ
    source = environment.get("CUSTOMERS_SILVER_SOURCE", PARQUET).lower()
    if source not in (PARQUET, ICEBERG):
        raise ValueError("CUSTOMERS_SILVER_SOURCE must be 'parquet' or 'iceberg'")
    return source


def require_iceberg_cutover_enabled(environ=None):
    """Refuse a cutover unless it is explicitly enabled after reconciliation."""
    environment = os.environ if environ is None else environ
    if customers_silver_source(environment) != ICEBERG:
        return False
    if environment.get("CUSTOMERS_ICEBERG_CUTOVER_ENABLED", "false").lower() != "true":
        raise RuntimeError("Iceberg customers cutover blocked: set CUSTOMERS_ICEBERG_CUTOVER_ENABLED=true")
    return True
