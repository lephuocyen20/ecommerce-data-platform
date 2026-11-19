"""Seed the approved local production-mode PostgreSQL delta.

This command is intentionally narrow: it targets the local PostgreSQL service
while using the production storage identity required by the production-mode
pipeline. All scope checks run before importing or constructing a PostgreSQL
client. Re-running the same immutable run ID replays the same rows.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from typing import Callable, Mapping


PRODUCTION_APPROVAL = "1"
PRODUCTION_BUCKET = "ecommerce-lakehouse-prod"
PRODUCTION_STORAGE_ENV = "production"
LOCAL_DB_HOSTS = frozenset({"postgres", "localhost", "127.0.0.1"})
PRODUCTION_DB_NAME = "ecommerce_platform"
IMMUTABLE_RUN_ID = "prodwrite20261119r06"
DELTA_TIMESTAMP = datetime(2026, 11, 19, 23, 30, 0)

TABLE_ORDER = ("customers", "products", "orders", "order_items", "payments", "shipments")


def validate_scope(environ: Mapping[str, str] | None = None, run_id: str = IMMUTABLE_RUN_ID) -> None:
    """Fail closed before any PostgreSQL client can be created."""

    env = os.environ if environ is None else environ
    expected = {
        "PRODUCTION_INCREMENTAL_WRITE_APPROVED": PRODUCTION_APPROVAL,
        "BRONZE_BUCKET": PRODUCTION_BUCKET,
        "LAKEHOUSE_STORAGE_ENV": PRODUCTION_STORAGE_ENV,
        "DB_NAME": PRODUCTION_DB_NAME,
        "DB_PORT": "5432",
    }
    for name, value in expected.items():
        if env.get(name) != value:
            raise RuntimeError(f"{name} must be explicitly set to {value!r}")

    host = env.get("DB_HOST", "").strip().lower()
    if host not in LOCAL_DB_HOSTS:
        raise RuntimeError(f"DB_HOST must be a local PostgreSQL host, got {host!r}")
    if not env.get("DB_USER") or not env.get("DB_PASSWORD"):
        raise RuntimeError("DB_USER and DB_PASSWORD must be explicitly provided")

    if run_id != IMMUTABLE_RUN_ID:
        raise RuntimeError(f"run_id must equal immutable value {IMMUTABLE_RUN_ID!r}")


def _rows(timestamp: datetime = DELTA_TIMESTAMP) -> dict[str, tuple[tuple[object, ...], ...]]:
    """Build deterministic, FK-valid rows for the approved run."""

    customer_ids = tuple(960601 + index for index in range(5))
    product_ids = tuple(961001 + index for index in range(8))
    order_ids = tuple(962001 + index for index in range(20))
    order_item_ids = tuple(963001 + index for index in range(40))
    payment_ids = tuple(964001 + index for index in range(20))
    shipment_ids = tuple(965001 + index for index in range(20))

    customers = tuple(
        (customer_id, f"Production Delta Customer {index + 1}",
         f"prodwrite20261119r06-customer-{index + 1}@example.test", timestamp, timestamp)
        for index, customer_id in enumerate(customer_ids)
    )
    products = tuple(
        (product_id, f"Production Delta Product {index + 1}", 100 + index, f"{19.99 + index:.2f}", timestamp, timestamp)
        for index, product_id in enumerate(product_ids)
    )
    orders = tuple(
        (order_id, customer_ids[index % len(customer_ids)], "CONFIRMED", timestamp, timestamp)
        for index, order_id in enumerate(order_ids)
    )
    order_items = tuple(
        (item_id, order_ids[index // 2], product_ids[index % len(product_ids)], 1 + index % 3,
         f"{19.99 + (index % len(product_ids)):.2f}", "0.00", timestamp)
        for index, item_id in enumerate(order_item_ids)
    )
    payments = tuple(
        (payment_id, order_id, f"{19.99 + (index % 8):.2f}", "CREDIT_CARD", "SUCCESS",
         f"prodwrite20261119r06-tx-{index + 1:02d}", timestamp, timestamp)
        for index, (payment_id, order_id) in enumerate(zip(payment_ids, order_ids))
    )
    shipments = tuple(
        (shipment_id, order_id, "GHN", f"PRODWRITE20261119R06-{index + 1:02d}",
         f"Production delta address {index + 1}", "0.00", "PENDING", None, None,
         timestamp + timedelta(days=3), timestamp, timestamp)
        for index, (shipment_id, order_id) in enumerate(zip(shipment_ids, order_ids))
    )
    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
        "shipments": shipments,
    }


UPSERTS = {
    "customers": """INSERT INTO public.customers (customer_id, full_name, email, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (customer_id) DO NOTHING""",
    "products": """INSERT INTO public.products (product_id, product_name, stock_quantity, price, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (product_id) DO NOTHING""",
    "orders": """INSERT INTO public.orders (order_id, customer_id, status, order_date, updated_at)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING""",
    "order_items": """INSERT INTO public.order_items
        (order_item_id, order_id, product_id, quantity, unit_price, discount_amount, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (order_item_id) DO NOTHING""",
    "payments": """INSERT INTO public.payments
        (payment_id, order_id, amount, payment_method, status, transaction_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (payment_id) DO NOTHING""",
    "shipments": """INSERT INTO public.shipments
        (shipment_id, order_id, carrier, tracking_number, shipping_address, shipping_fee,
         status, shipped_at, delivered_at, estimated_delivery_at, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (shipment_id) DO NOTHING""",
}


def seed_sources(connect: Callable[[], object], *, run_id: str = IMMUTABLE_RUN_ID) -> dict[str, int]:
    """Insert all rows if absent, in FK order, inside one transaction."""

    validate_scope(run_id=run_id)
    rows = _rows()
    connection = connect()
    try:
        with connection:
            with connection.cursor() as cursor:
                for table in TABLE_ORDER:
                    cursor.executemany(UPSERTS[table], rows[table])
        return {table: len(rows[table]) for table in TABLE_ORDER}
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=IMMUTABLE_RUN_ID)
    args = parser.parse_args(argv)
    validate_scope(run_id=args.run_id)

    # Import only after the complete scope guard has passed.
    import psycopg2

    result = seed_sources(
        lambda: psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ.get("DB_PORT", "5432"),
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
        ),
        run_id=args.run_id,
    )
    counts = ",".join(f"{table}={count}" for table, count in result.items())
    print(f"PRODUCTION_INCREMENTAL_DELTA_SEED=PASS run_id={args.run_id} {counts}")


if __name__ == "__main__":
    main()
