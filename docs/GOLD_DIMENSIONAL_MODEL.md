# Gold Iceberg dimensional model v1

Gold is the analytical warehouse layer: conformed dimensions and facts are canonical; report marts are derived.

| Table | Grain | Inputs |
|---|---|---|
| `gold.dim_date` | calendar date | deterministic generator |
| `gold.dim_customer` | customer (Type 1) | Silver customers |
| `gold.dim_product` | product (Type 1) | Silver products |
| `gold.fact_orders` | one row per order | Silver orders, order_items, customers |

`fact_orders` holds order-level measures only. Shipping fees remain shipment-grain and must not be copied onto order items. Unknown dimensions use surrogate key `-1`; Type 2 is explicitly out of scope for v1.
