"""Declarative Lakehouse table contracts used by shared engines."""
from dataclasses import dataclass

@dataclass(frozen=True)
class TableSpec:
    name: str; key: str; columns: tuple; casts: dict; ddl: str
    watermark: str | None = None; immutable: bool = False
    required: tuple = (); predicates: tuple = (); foreign_keys: dict = None

def _spec(name, key, columns, casts, ddl, **kwargs):
    return TableSpec(name, key, columns, casts, ddl, **kwargs)

SPECS = {
 "customers": _spec("customers","customer_id",("customer_id","full_name","email","created_at","updated_at"),{"customer_id":"bigint","full_name":"string","email":"string","created_at":"timestamp","updated_at":"timestamp"},"customer_id BIGINT, full_name STRING, email STRING, created_at TIMESTAMP, updated_at TIMESTAMP",watermark="updated_at",required=("customer_id","updated_at")),
 "products": _spec("products","product_id",("product_id","product_name","stock_quantity","price","created_at","updated_at"),{"product_id":"bigint","product_name":"string","stock_quantity":"int","price":"decimal(12,2)","created_at":"timestamp","updated_at":"timestamp"},"product_id BIGINT, product_name STRING, stock_quantity INT, price DECIMAL(12,2), created_at TIMESTAMP, updated_at TIMESTAMP",watermark="updated_at",required=("product_id","updated_at"),predicates=("stock_quantity >= 0","price > 0")),
 "orders": _spec("orders","order_id",("order_id","customer_id","status","order_date","updated_at"),{"order_id":"bigint","customer_id":"bigint","status":"string","order_date":"timestamp","updated_at":"timestamp"},"order_id BIGINT, customer_id BIGINT, status STRING, order_date TIMESTAMP, updated_at TIMESTAMP",watermark="updated_at",required=("order_id","customer_id","order_date","updated_at"),foreign_keys={"customer_id":"customers"}),
 "order_items": _spec("order_items","order_item_id",("order_item_id","order_id","product_id","quantity","unit_price","discount_amount"),{"order_item_id":"bigint","order_id":"bigint","product_id":"bigint","quantity":"int","unit_price":"decimal(12,2)","discount_amount":"decimal(12,2)"},"order_item_id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT, unit_price DECIMAL(12,2), discount_amount DECIMAL(12,2)",immutable=True,required=("order_item_id","order_id","product_id"),predicates=("quantity > 0","unit_price >= 0","discount_amount >= 0"),foreign_keys={"order_id":"orders","product_id":"products"}),
 "payments": _spec("payments","payment_id",("payment_id","order_id","amount","payment_method","status","transaction_id","created_at"),{"payment_id":"bigint","order_id":"bigint","amount":"decimal(12,2)","payment_method":"string","status":"string","transaction_id":"string","created_at":"timestamp"},"payment_id BIGINT, order_id BIGINT, amount DECIMAL(12,2), payment_method STRING, status STRING, transaction_id STRING, created_at TIMESTAMP",watermark="created_at",required=("payment_id","order_id","amount","created_at"),predicates=("amount > 0",),foreign_keys={"order_id":"orders"}),
 "shipments": _spec("shipments","shipment_id",("shipment_id","order_id","carrier","tracking_number","shipping_address","shipping_fee","status","shipped_at","delivered_at","estimated_delivery_at","created_at","updated_at"),{"shipment_id":"bigint","order_id":"bigint","carrier":"string","tracking_number":"string","shipping_address":"string","shipping_fee":"decimal(12,2)","status":"string","shipped_at":"timestamp","delivered_at":"timestamp","estimated_delivery_at":"timestamp","created_at":"timestamp","updated_at":"timestamp"},"shipment_id BIGINT, order_id BIGINT, carrier STRING, tracking_number STRING, shipping_address STRING, shipping_fee DECIMAL(12,2), status STRING, shipped_at TIMESTAMP, delivered_at TIMESTAMP, estimated_delivery_at TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP",watermark="updated_at",required=("shipment_id","order_id","shipping_fee","created_at","updated_at"),predicates=("shipping_fee >= 0",),foreign_keys={"order_id":"orders"}),
}

def get_spec(name): return SPECS[name]
