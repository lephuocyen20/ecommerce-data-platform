"""Seed six deterministic PostgreSQL source deltas for the local E2E only.

The script performs idempotent ``INSERT ... ON CONFLICT DO UPDATE`` statements
against existing source tables. It never creates, drops, deletes, or truncates
data. The hard guard requires the dedicated quality-it bucket and explicit
local WRITE mode, so the command cannot be pointed at a shared/prod-looking
run accidentally.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime, timedelta

import psycopg2


LOCAL_BUCKET = "ecommerce-quality-it"
LOCAL_MODE = "WRITE"
LOCAL_DB_HOSTS = frozenset({"postgres", "localhost", "127.0.0.1"})
LOCAL_DB_NAME = "ecommerce_platform"
DEFAULT_RUN_ID = "local-dev"
FIXTURE_IDS = {
    "customer_id": 910001,
    "product_id": 910001,
    "order_id": 910001,
    "order_item_id": 910001,
    "payment_id": 910001,
    "shipment_id": 910001,
}


def fixture_ids(run_id=DEFAULT_RUN_ID):
    if not isinstance(run_id, str) or not run_id.strip() or len(run_id) > 64:
        raise ValueError("QUALITY_IT_RUN_ID must be a non-empty value of at most 64 characters")
    digest = int(hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:10], 16)
    # Keep fixture IDs above long-lived local ingestion watermarks so repeated
    # certification runs cannot become invisible as NO_DATA.  The range stays
    # well within BIGINT and is confined to the dedicated quality-it source.
    base = 2000000000 + (digest % 8000000) * 10
    return {
        "customer_id": base + 1,
        "product_id": base + 2,
        "order_id": base + 3,
        "order_item_id": base + 4,
        "payment_id": base + 5,
        "shipment_id": base + 6,
    }


def validate_local_scope(environ=None):
    env = os.environ if environ is None else environ
    if env.get("BRONZE_BUCKET") != LOCAL_BUCKET:
        raise RuntimeError("Refusing PostgreSQL seed outside ecommerce-quality-it")
    if env.get("AIRFLOW_LOCAL_E2E_MODE") != LOCAL_MODE:
        raise RuntimeError("Set AIRFLOW_LOCAL_E2E_MODE=WRITE for the local E2E seed")
    if env.get("INGESTION_WRITE_ENABLED", "").lower() != "true":
        raise RuntimeError("Set INGESTION_WRITE_ENABLED=true for the local E2E seed")
    host = env.get("DB_HOST", "localhost").strip().lower()
    if host not in LOCAL_DB_HOSTS:
        raise RuntimeError(f"Refusing PostgreSQL host outside local allowlist: {host}")
    database = env.get("DB_NAME", LOCAL_DB_NAME).strip()
    if database != LOCAL_DB_NAME:
        raise RuntimeError(f"Refusing PostgreSQL database outside local allowlist: {database}")


def _fixture_rows(timestamp, run_id=DEFAULT_RUN_ID, *, business_timestamp=None):
    ids = fixture_ids(run_id)
    business_timestamp = timestamp if business_timestamp is None else business_timestamp
    customer_id = ids["customer_id"]
    product_id = ids["product_id"]
    order_id = ids["order_id"]
    return {
        "customers": (
            (customer_id, f"Quality IT Customer {run_id}", f"quality-it-{customer_id}@example.test", business_timestamp, timestamp),
        ),
        "products": (
            (product_id, "Quality IT Product", 10, "19.90", business_timestamp, timestamp),
        ),
        "orders": (
            (order_id, customer_id, "CONFIRMED", business_timestamp, timestamp),
        ),
        "order_items": (
            (ids["order_item_id"], order_id, product_id, 2, "19.90", "1.00"),
        ),
        "payments": (
            (ids["payment_id"], order_id, "38.80", "CREDIT_CARD", "SUCCESS", f"quality-it-tx-{ids['payment_id']}", timestamp),
        ),
        "shipments": (
            (ids["shipment_id"], order_id, "GHN", f"QUALITY-IT-{ids['shipment_id']}", "Local quality-it", "0.00", "PENDING", None, None, business_timestamp, business_timestamp, timestamp),
        ),
    }


UPSERTS = {
    "customers": """INSERT INTO public.customers (customer_id, full_name, email, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s) ON CONFLICT (customer_id) DO UPDATE SET
        full_name=EXCLUDED.full_name,email=EXCLUDED.email,updated_at=EXCLUDED.updated_at""",
    "products": """INSERT INTO public.products (product_id, product_name, stock_quantity, price, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (product_id) DO UPDATE SET
        product_name=EXCLUDED.product_name,stock_quantity=EXCLUDED.stock_quantity,price=EXCLUDED.price,updated_at=EXCLUDED.updated_at""",
    "orders": """INSERT INTO public.orders (order_id, customer_id, status, order_date, updated_at)
        VALUES (%s,%s,%s,%s,%s) ON CONFLICT (order_id) DO UPDATE SET
        customer_id=EXCLUDED.customer_id,status=EXCLUDED.status,order_date=EXCLUDED.order_date,updated_at=EXCLUDED.updated_at""",
    "order_items": """INSERT INTO public.order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount)
        VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (order_item_id) DO UPDATE SET
        order_id=EXCLUDED.order_id,product_id=EXCLUDED.product_id,quantity=EXCLUDED.quantity,unit_price=EXCLUDED.unit_price,discount_amount=EXCLUDED.discount_amount""",
    "payments": """INSERT INTO public.payments (payment_id, order_id, amount, payment_method, status, transaction_id, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (payment_id) DO UPDATE SET
        order_id=EXCLUDED.order_id,amount=EXCLUDED.amount,payment_method=EXCLUDED.payment_method,status=EXCLUDED.status,transaction_id=EXCLUDED.transaction_id,created_at=EXCLUDED.created_at""",
    "shipments": """INSERT INTO public.shipments (shipment_id, order_id, carrier, tracking_number, shipping_address, shipping_fee, status, shipped_at, delivered_at, estimated_delivery_at, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (shipment_id) DO UPDATE SET
        order_id=EXCLUDED.order_id,carrier=EXCLUDED.carrier,tracking_number=EXCLUDED.tracking_number,shipping_address=EXCLUDED.shipping_address,shipping_fee=EXCLUDED.shipping_fee,status=EXCLUDED.status,estimated_delivery_at=EXCLUDED.estimated_delivery_at,updated_at=EXCLUDED.updated_at""",
}


def seed_local_sources(connect, *, timestamp=None, run_id=None):
    validate_local_scope()
    run_id = run_id or os.environ.get("QUALITY_IT_RUN_ID", DEFAULT_RUN_ID)
    fixture_ids(run_id)
    conn = connect()
    try:
        with conn:
            with conn.cursor() as cursor:
                if timestamp is None:
                    # Preserve the original timestamp on a retry of the same
                    # run_id (idempotent seed), while ensuring a genuinely new
                    # fixture is newer than the local ingestion watermark.
                    ids = fixture_ids(run_id)
                    cursor.execute(
                        "SELECT updated_at FROM public.customers WHERE customer_id = %s",
                        (ids["customer_id"],),
                    )
                    existing = cursor.fetchone()
                    if existing and existing[0] is not None:
                        timestamp = existing[0]
                    else:
                        timestamp = datetime.now().replace(microsecond=0) + timedelta(minutes=5)
                offset = int(hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:4], 16) % 30
                business_timestamp = datetime(2026, 1, 1, 12, 0, 0) + timedelta(minutes=offset)
                rows = _fixture_rows(timestamp, run_id, business_timestamp=business_timestamp)
                # Parent rows precede dependent rows to satisfy source FKs.
                for table in ("customers", "products", "orders", "order_items", "payments", "shipments"):
                    values = rows[table][0]
                    cursor.execute(UPSERTS[table], values)
        return {table: 1 for table in rows}
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    result = seed_local_sources(lambda: psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"), port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "ecommerce_platform"),
        user=os.environ.get("DB_USER", "ecommerce_user"),
        password=os.environ.get("DB_PASSWORD", "ecommerce_pass"),
    ))
    print(f"LOCAL_POSTGRES_E2E_SEED=PASS run_id={os.environ.get('QUALITY_IT_RUN_ID', DEFAULT_RUN_ID)} tables={','.join(result)}")


if __name__ == "__main__":
    main()
