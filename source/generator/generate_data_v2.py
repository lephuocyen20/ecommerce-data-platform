import argparse
import os
import random
from datetime import datetime
from pathlib import Path
from faker import Faker
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
from dotenv import load_dotenv

fake = Faker("vi_VN")
ENV_PATH = Path(__file__).resolve().parent / ".env.test"
REFERENCE_DATE = datetime(2026, 10, 1, 0, 0, 0)


def load_config():
    """Load database configuration for test environment."""
    if not ENV_PATH.is_file():
        raise FileNotFoundError(f"Missing config: {ENV_PATH}")

    load_dotenv(ENV_PATH, override=True)

    if os.getenv("DB_NAME") != "ecommerce_test":
        raise RuntimeError(
            "Safety check failed: Generator V2 only supports ecommerce_test"
        )


def get_db_connection():
    """Create PostgreSQL connection to test database."""
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=5,
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="E-commerce Synthetic Data Generator V2"
    )

    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--products", type=int, default=300)
    parser.add_argument("--orders", type=int, default=5000)
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    if min(args.customers, args.products, args.orders, args.months) <= 0:
        parser.error(
            "--customers, --products, --orders and --months must be positive"
        )

    return args

def random_timestamp(start, end):
    """Generate a random timestamp between two datetimes."""
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, seconds))


def generate_customers(cursor, n, start_date, end_date):
    customers = []


    for index in range(n):
        created_at = random_timestamp(start_date, end_date)
        updated_at = random_timestamp(created_at, end_date)


        customers.append(
            (
            fake.name(),
            f"customer_v2_{index}_{uuid.uuid4().hex[:8]}@example.com",
            created_at,
            updated_at,
        ))
    cursor.executemany(
        """
        INSERT INTO customers (
            full_name, email, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s)
        """,
        customers,
    )

    print(f"Generated {n} customers")

def generate_products(cursor, n, start_date, end_date):
    products = []

    for index in range(n):
        created_at = random_timestamp(start_date, end_date)
        updated_at = random_timestamp(created_at, end_date)


    products.append((
        f"Product V2 {index + 1}",
        random.randint(20, 500),
        Decimal(random.randrange(100_000, 30_000_001, 1000)),
        created_at,
        updated_at,
    ))
    cursor.executemany(
        """
        INSERT INTO  products (
            product_name,
            stock_quantity,
            price,
            created_at,
            updated_at
        )
        VALUES( %s, %s, %s, %s, %s)
        """,
        products,    

    )
    print(f"Generate {n} products")


def main():
    args = parse_args()
    load_config()

    random.seed(args.seed)
    Faker.seed(args.seed)

    start_date = REFERENCE_DATE - timedelta(days=args.months * 30)

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            db_name = cursor.fetchone()[0]

            if db_name != "ecommerce_test":
                raise RuntimeError("Unsafe database connection")

            # Avoid duplicate data on rerun
            cursor.execute("""
                SELECT
                    (SELECT COUNT(*) FROM customers),
                    (SELECT COUNT(*) FROM products)
            """)
            customer_count, product_count = cursor.fetchone()

            if customer_count > 0 or product_count > 0:
                raise RuntimeError(
                    "Test database already contains customers/products. "
                    "Refusing to append automatically."
                )

            generate_customers(
                cursor, args.customers, start_date, REFERENCE_DATE
            )

            generate_products(
                cursor, args.products, start_date, REFERENCE_DATE
            )

    print("Task 5.1A.2.2 completed successfully")


if __name__ == "__main__":
    main()