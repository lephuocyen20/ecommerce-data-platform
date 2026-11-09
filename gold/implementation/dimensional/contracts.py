"""Declarative contracts for the first Iceberg Gold star-schema slice."""

DIMENSIONAL_GOLD_VERSION = "1.0"
UNKNOWN_KEY = -1

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

def get_dimensional_contract(name):
    if name in DIMENSIONS: return DIMENSIONS[name]
    if name in FACTS: return FACTS[name]
    raise ValueError(f"Unsupported dimensional Gold table: {name}")
