"""Declarative contracts for the first Iceberg Gold star-schema slice."""

from gold.implementation.gold_table_registry import GOLD_TABLES

DIMENSIONAL_GOLD_VERSION = "1.0"
UNKNOWN_KEY = -1
DIMENSIONAL_CATALOG = "lakehouse"
DIMENSIONAL_NAMESPACE = "gold"

# Semantic metadata is explicit so downstream consumers do not have to infer
# grain, time semantics or measure additivity from a Spark schema.
DIMENSIONAL_SEMANTIC_CONTRACT = {
    "version": DIMENSIONAL_GOLD_VERSION,
    "owner": "analytics",
    "timezone": "UTC",
    "date_semantics": "date_key is YYYYMMDD for the UTC calendar date",
    "unknown_member_key": UNKNOWN_KEY,
    "type_2_dimensions": False,
    "business_signoff_required": True,
}

DIMENSIONS = {
    "dim_date": {"grain": ("date_key",), "merge_key": ("date_key",), "natural_key": ("calendar_date",), "sources": (), "type": "generated", "timezone":"UTC", "required":("date_key","calendar_date")},
    "dim_customer": {"grain": ("customer_key",), "merge_key": ("customer_key",), "natural_key": ("customer_id",), "sources": ("customers",), "type": "type_1", "timezone":"UTC", "required":("customer_key","customer_id"), "foreign_keys":{}},
    "dim_product": {"grain": ("product_key",), "merge_key": ("product_key",), "natural_key": ("product_id",), "sources": ("products",), "type": "type_1", "timezone":"UTC", "required":("product_key","product_id"), "foreign_keys":{}},
}

FACTS = {
    "fact_orders": {
        "grain": ("order_id",), "primary_key": ("order_id",), "merge_key": ("order_id",),
        "sources": ("orders", "order_items", "customers"),
        "dimensions": ("dim_date", "dim_customer"),
        "measures": ("line_count", "item_count", "gross_amount", "discount_amount", "net_amount"),
        "timezone":"UTC", "required":("order_id","order_date_key","customer_key","customer_natural_key","order_status","line_count","item_count","gross_amount","discount_amount","net_amount"), "foreign_keys":{"customer_key":"dim_customer", "order_date_key":"dim_date"}, "unknown_key": UNKNOWN_KEY,
        "measure_rules": {"line_count": "non_negative", "item_count": "non_negative", "gross_amount": "non_negative", "discount_amount": "non_negative", "net_amount": "gross_amount - discount_amount"},
        "measures_metadata": {name: {"additivity": "additive", "lifecycle": "non_negative_base_or_derived"} for name in ("line_count", "item_count", "gross_amount", "discount_amount", "net_amount")},
        "lifecycle": {"status_column": "order_status", "signed_revenue_measure": None, "policy": "preserve_base_amounts; order-level signed_revenue ownership unresolved"},
    },
    "fact_order_items": {"grain": ("order_item_id",), "primary_key": ("order_item_id",), "merge_key": ("order_item_id",), "sources": ("order_items", "products"), "dimensions": ("dim_product",), "measures": ("quantity", "gross_amount", "discount_amount", "net_amount"), "timezone": "UTC", "required": ("order_item_id", "order_id", "product_key", "quantity", "unit_price", "gross_amount", "discount_amount", "net_amount"), "foreign_keys": {"product_key": "dim_product"}, "unknown_key": UNKNOWN_KEY, "measures_metadata": {name: {"additivity": "additive", "lifecycle": "non_negative"} for name in ("quantity", "gross_amount", "discount_amount", "net_amount")}},
    "fact_payments": {"grain": ("payment_id",), "primary_key": ("payment_id",), "merge_key": ("payment_id",), "sources": ("payments",), "dimensions": (), "measures": ("payment_amount", "signed_revenue"), "timezone": "UTC", "required": ("payment_id", "order_id", "payment_status", "payment_method", "payment_amount", "signed_revenue"), "foreign_keys": {"order_id": "fact_orders"}, "unknown_key": UNKNOWN_KEY, "measures_metadata": {"payment_amount": {"additivity": "additive", "lifecycle": "non_negative_base"}, "signed_revenue": {"additivity": "additive", "lifecycle": "REFUNDED_negative_other_statuses_positive"}}, "lifecycle": {"status_column": "payment_status", "signed_revenue_measure": "signed_revenue"}},
    "fact_shipments": {"grain": ("shipment_id",), "primary_key": ("shipment_id",), "merge_key": ("shipment_id",), "sources": ("shipments",), "dimensions": (), "measures": (), "timezone": "UTC", "required": ("shipment_id", "order_id", "shipment_status", "shipped_at", "delivered_at"), "foreign_keys": {"order_id": "fact_orders"}, "unknown_key": UNKNOWN_KEY, "lifecycle": {"status_column": "shipment_status", "policy": "status_preserved; shipment fees remain shipment-grain"}},
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
    "fact_order_items": ("order_item_id", "order_id", "product_key", "quantity", "unit_price", "gross_amount", "discount_amount", "net_amount"),
    "fact_payments": ("payment_id", "order_id", "payment_status", "payment_method", "payment_amount", "signed_revenue"),
    "fact_shipments": ("shipment_id", "order_id", "shipment_status", "shipped_at", "delivered_at"),
}

TABLE_DDLS = {
    "dim_date": "date_key BIGINT, calendar_date DATE, year INT, month INT, iso_week INT",
    "dim_customer": "customer_key BIGINT, customer_id BIGINT, full_name STRING, email STRING, created_at TIMESTAMP, updated_at TIMESTAMP",
    "dim_product": "product_key BIGINT, product_id BIGINT, product_name STRING, stock_quantity INT, price DECIMAL(12,2), created_at TIMESTAMP, updated_at TIMESTAMP",
    "fact_orders": "order_id BIGINT, order_date_key INT, customer_key BIGINT, customer_natural_key BIGINT, order_status STRING, line_count BIGINT, item_count BIGINT, gross_amount DECIMAL(20,2), discount_amount DECIMAL(20,2), net_amount DECIMAL(20,2)",
    "fact_order_items": "order_item_id BIGINT, order_id BIGINT, product_key BIGINT, quantity BIGINT, unit_price DECIMAL(20,2), gross_amount DECIMAL(20,2), discount_amount DECIMAL(20,2), net_amount DECIMAL(20,2)",
    "fact_payments": "payment_id BIGINT, order_id BIGINT, payment_status STRING, payment_method STRING, payment_amount DECIMAL(20,2), signed_revenue DECIMAL(20,2)",
    "fact_shipments": "shipment_id BIGINT, order_id BIGINT, shipment_status STRING, shipped_at TIMESTAMP, delivered_at TIMESTAMP",
}

TABLE_SCHEMA = {
    "dim_date": (("date_key", "bigint", True), ("calendar_date", "date", True), ("year", "int", True), ("month", "int", True), ("iso_week", "int", True)),
    "dim_customer": (("customer_key", "bigint", True), ("customer_id", "bigint", True), ("full_name", "string", True), ("email", "string", True), ("created_at", "timestamp", True), ("updated_at", "timestamp", True)),
    "dim_product": (("product_key", "bigint", True), ("product_id", "bigint", True), ("product_name", "string", True), ("stock_quantity", "int", True), ("price", "decimal(12,2)", True), ("created_at", "timestamp", True), ("updated_at", "timestamp", True)),
    "fact_orders": (("order_id", "bigint", True), ("order_date_key", "int", True), ("customer_key", "bigint", True), ("customer_natural_key", "bigint", True), ("order_status", "string", True), ("line_count", "bigint", True), ("item_count", "bigint", True), ("gross_amount", "decimal(20,2)", True), ("discount_amount", "decimal(20,2)", True), ("net_amount", "decimal(20,2)", True)),
    "fact_order_items": (("order_item_id", "bigint", True), ("order_id", "bigint", True), ("product_key", "bigint", True), ("quantity", "bigint", True), ("unit_price", "decimal(20,2)", True), ("gross_amount", "decimal(20,2)", True), ("discount_amount", "decimal(20,2)", True), ("net_amount", "decimal(20,2)", True)),
    "fact_payments": (("payment_id", "bigint", True), ("order_id", "bigint", True), ("payment_status", "string", True), ("payment_method", "string", True), ("payment_amount", "decimal(20,2)", True), ("signed_revenue", "decimal(20,2)", True)),
    "fact_shipments": (("shipment_id", "bigint", True), ("order_id", "bigint", True), ("shipment_status", "string", True), ("shipped_at", "timestamp", True), ("delivered_at", "timestamp", True)),
}

TABLE_MERGE_KEYS = {
    "dim_date": ("date_key",), "dim_customer": ("customer_key",),
    "dim_product": ("product_key",), "fact_orders": ("order_id",), "fact_order_items": ("order_item_id",), "fact_payments": ("payment_id",), "fact_shipments": ("shipment_id",),
}

WRITE_ORDER = GOLD_TABLES

def get_dimensional_contract(name):
    if name in DIMENSIONAL_TABLES: return DIMENSIONAL_TABLES[name]
    raise ValueError(f"Unsupported dimensional Gold table: {name}")
