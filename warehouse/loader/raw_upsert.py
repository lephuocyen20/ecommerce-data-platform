"""Transactional PostgreSQL Raw Warehouse UPSERT."""

from psycopg2 import sql
from psycopg2.extras import execute_values


TABLE_COLUMNS = {
    "customers": (
        "customer_id",
        "full_name",
        "email",
        "created_at",
        "updated_at",
    ),
    "products": (
        "product_id",
        "product_name",
        "stock_quantity",
        "price",
        "created_at",
        "updated_at",
    ),
    "orders": (
        "order_id",
        "customer_id",
        "status",
        "order_date",
        "updated_at",
    ),
    "order_items": (
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
    ),
    "payments": (
        "payment_id",
        "order_id",
        "amount",
        "payment_method",
        "status",
        "transaction_id",
        "created_at",
    ),
    "shipments": (
        "shipment_id",
        "order_id",
        "carrier",
        "tracking_number",
        "shipping_address",
        "shipping_fee",
        "status",
        "shipped_at",
        "delivered_at",
        "estimated_delivery_at",
        "created_at",
        "updated_at",
    ),
}

PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "shipments": "shipment_id",
}


def upsert_raw_batch(conn, table_name, batch_id, arrow_table):
    """
    UPSERT one verified Silver batch into PostgreSQL raw.

    The caller owns the transaction.
    This function never commits or rolls back.
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    if table_name not in TABLE_COLUMNS:
        raise ValueError(f"Unsupported table: {table_name}")

    if not isinstance(batch_id, str) or len(batch_id) != 32:
        raise ValueError("Invalid batch_id")

    columns = TABLE_COLUMNS[table_name]
    primary_key = PRIMARY_KEYS[table_name]

    if tuple(arrow_table.column_names) != columns:
        raise ValueError(
            f"Column mismatch for {table_name}: "
            f"{arrow_table.column_names}"
        )

    records = arrow_table.to_pylist()

    values = [
        tuple(record[column] for column in columns)
        + (batch_id,)
        for record in records
    ]

    if not values:
        return 0

    insert_columns = (
        list(columns)
        + ["_silver_batch_id", "_loaded_at"]
    )

    update_columns = [
        column
        for column in insert_columns
        if column != primary_key
    ]

    insert_sql = sql.SQL("""
        INSERT INTO {} ({})
        VALUES %s
        ON CONFLICT ({})
        DO UPDATE SET {}
    """).format(
        sql.Identifier("raw", table_name),
        sql.SQL(", ").join(
            sql.Identifier(column)
            for column in insert_columns
        ),
        sql.Identifier(primary_key),
        sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(column),
                sql.Identifier(column),
            )
            for column in update_columns
        ),
    )

    # Each record contains the Silver columns and batch_id.
    # PostgreSQL computes _loaded_at.
    template = (
        "("
        + ", ".join(["%s"] * (len(columns) + 1))
        + ", NOW())"
    )

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            insert_sql.as_string(conn),
            values,
            template=template,
            page_size=1000,
        )

    return len(values)
