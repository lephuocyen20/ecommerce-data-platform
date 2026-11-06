"""Central registry for table-specific Lakehouse behavior."""

from importlib import import_module


TABLES = (
    "customers", "products", "orders", "order_items", "payments", "shipments",
)


def migration_module(table_name):
    if table_name not in TABLES:
        raise ValueError(f"Unsupported Lakehouse table: {table_name}")
    return import_module(f"lakehouse.migration.parquet_{table_name}")


def migration_api(table_name):
    """Normalize legacy migration module spellings behind one small interface."""
    module = migration_module(table_name)
    discover = getattr(module, "discover_manifests", None) or getattr(module, "discover")
    migrate = getattr(module, "migrate_manifest", None) or getattr(module, "migrate")
    return discover, migrate
