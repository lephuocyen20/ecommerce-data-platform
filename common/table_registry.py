"""Platform-wide identities for source entities shared across pipeline layers."""

SUPPORTED_TABLES = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
)


def validate_supported_table(table_name):
    if table_name not in SUPPORTED_TABLES:
        raise ValueError(f"Unsupported table: {table_name}")
    return table_name
