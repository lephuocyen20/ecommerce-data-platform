"""Declarative contracts for the first Iceberg Gold star-schema slice."""

DIMENSIONAL_GOLD_VERSION = "1.0"
UNKNOWN_KEY = -1
DIMENSIONAL_CATALOG = "lakehouse"
DIMENSIONAL_NAMESPACE = "gold"

DIMENSIONS = {
    "dim_date": {"grain": ("date_key",), "natural_key": ("calendar_date",), "sources": (), "type": "generated", "timezone":"UTC", "required":("date_key","calendar_date")},
    "dim_customer": {"grain": ("customer_key",), "natural_key": ("customer_id",), "sources": ("customers",), "type": "type_1", "timezone":"UTC", "required":("customer_key","customer_id"), "foreign_keys":{}},
    "dim_product": {"grain": ("product_key",), "natural_key": ("product_id",), "sources": ("products",), "type": "type_1", "timezone":"UTC", "required":("product_key","product_id"), "foreign_keys":{}},
}

FACTS = {
    "fact_orders": {
        "grain": ("order_id",), "primary_key": ("order_id",),
        "sources": ("orders", "order_items", "customers"),
        "dimensions": ("dim_date", "dim_customer"),
        "measures": ("line_count", "item_count", "gross_amount", "discount_amount", "net_amount"),
        "timezone":"UTC", "required":("order_id","order_date_key","customer_key","customer_natural_key","order_status","line_count","item_count","gross_amount","discount_amount","net_amount"), "foreign_keys":{"customer_key":"dim_customer"}, "unknown_key": UNKNOWN_KEY,
    },
}

DIMENSIONAL_TABLES = {**DIMENSIONS, **FACTS}
DIMENSIONAL_SOURCE_TABLES = tuple(sorted({
    source for contract in DIMENSIONAL_TABLES.values() for source in contract["sources"]
}))

# These are deliberately static.  A writer must not infer target schema from a
# mutable source DataFrame: an added/renamed Silver column is a contract change.
TABLE_COLUMNS = {
    "dim_date": ("date_key", "calendar_date", "year", "month", "iso_week"),
    "dim_customer": ("customer_key", "customer_id", "full_name", "email", "created_at", "updated_at"),
    "dim_product": ("product_key", "product_id", "product_name", "stock_quantity", "price", "created_at", "updated_at"),
    "fact_orders": ("order_id", "order_date_key", "customer_key", "customer_natural_key", "order_status", "line_count", "item_count", "gross_amount", "discount_amount", "net_amount"),
}

TABLE_DDLS = {
    "dim_date": "date_key BIGINT, calendar_date DATE, year INT, month INT, iso_week INT",
    "dim_customer": "customer_key BIGINT, customer_id BIGINT, full_name STRING, email STRING, created_at TIMESTAMP, updated_at TIMESTAMP",
    "dim_product": "product_key BIGINT, product_id BIGINT, product_name STRING, stock_quantity INT, price DECIMAL(12,2), created_at TIMESTAMP, updated_at TIMESTAMP",
    "fact_orders": "order_id BIGINT, order_date_key INT, customer_key BIGINT, customer_natural_key BIGINT, order_status STRING, line_count BIGINT, item_count BIGINT, gross_amount DECIMAL(20,2), discount_amount DECIMAL(20,2), net_amount DECIMAL(20,2)",
}

TABLE_SCHEMA = {
    "dim_date": (("date_key", "bigint", True), ("calendar_date", "date", True), ("year", "int", True), ("month", "int", True), ("iso_week", "int", True)),
    "dim_customer": (("customer_key", "bigint", True), ("customer_id", "bigint", True), ("full_name", "string", True), ("email", "string", True), ("created_at", "timestamp", True), ("updated_at", "timestamp", True)),
    "dim_product": (("product_key", "bigint", True), ("product_id", "bigint", True), ("product_name", "string", True), ("stock_quantity", "int", True), ("price", "decimal(12,2)", True), ("created_at", "timestamp", True), ("updated_at", "timestamp", True)),
    "fact_orders": (("order_id", "bigint", True), ("order_date_key", "int", True), ("customer_key", "bigint", True), ("customer_natural_key", "bigint", True), ("order_status", "string", True), ("line_count", "bigint", True), ("item_count", "bigint", True), ("gross_amount", "decimal(20,2)", True), ("discount_amount", "decimal(20,2)", True), ("net_amount", "decimal(20,2)", True)),
}

TABLE_MERGE_KEYS = {
    "dim_date": ("date_key",), "dim_customer": ("customer_key",),
    "dim_product": ("product_key",), "fact_orders": ("order_id",),
}

WRITE_ORDER = ("dim_date", "dim_customer", "dim_product", "fact_orders")

def get_dimensional_contract(name):
    if name in DIMENSIONAL_TABLES: return DIMENSIONAL_TABLES[name]
    raise ValueError(f"Unsupported dimensional Gold table: {name}")
