"""Central registry for table-specific Lakehouse behavior."""



TABLES = (
    "customers", "products", "orders", "order_items", "payments", "shipments",
)


def migration_module(table_name):
    if table_name not in TABLES:
        raise ValueError(f"Unsupported Lakehouse table: {table_name}")
    return None


def migration_api(table_name, scope=None):
    """Normalize legacy migration module spellings behind one small interface."""
    from lakehouse.migration.manifest import discover_manifests, migrate_manifest
    return (lambda client, bucket: discover_manifests(client, bucket, table_name, scope),
            lambda spark, client, bucket, key, manifest=None: migrate_manifest(spark, client, bucket, key, table_name, manifest, scope=scope))
