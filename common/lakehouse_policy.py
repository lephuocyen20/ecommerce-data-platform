"""Explicit, safe source selection for Lakehouse migrations."""

import os


PARQUET = "parquet"
ICEBERG = "iceberg"


def silver_source(table_name, environ=None):
    """Return a per-table Silver source, defaulting safely to Parquet."""
    if not isinstance(table_name, str) or not table_name:
        raise ValueError("table_name is required")
    environment = os.environ if environ is None else environ
    source = environment.get(f"{table_name.upper()}_SILVER_SOURCE", PARQUET).lower()
    if source not in (PARQUET, ICEBERG):
        raise ValueError(f"{table_name.upper()}_SILVER_SOURCE must be 'parquet' or 'iceberg'")
    return source


def customers_silver_source(environ=None):
    """Return the selected customers Silver source; legacy Parquet is default."""
    return silver_source("customers", environ)


def require_iceberg_table_cutover(table_name, environ=None):
    environment = os.environ if environ is None else environ
    if silver_source(table_name, environment) != ICEBERG:
        return False
    flag = f"{table_name.upper()}_ICEBERG_CUTOVER_ENABLED"
    if environment.get(flag, "false").lower() != "true":
        raise RuntimeError(f"Iceberg {table_name} cutover blocked: set {flag}=true")
    return True


def require_iceberg_cutover_enabled(environ=None):
    """Refuse a cutover unless it is explicitly enabled after reconciliation."""
    return require_iceberg_table_cutover("customers", environ)
