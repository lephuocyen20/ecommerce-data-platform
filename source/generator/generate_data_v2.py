import argparse
import os
import random
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


ENV_PATH = Path(__file__).resolve().parent / ".env.test"

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


def main():
    args = parse_args()

    load_config()
    random.seed(args.seed)

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT current_database(), current_user, CURRENT_TIMESTAMP"
            )
            db_name, db_user, db_time = cursor.fetchone()

    print("=== E-commerce Generator V2 ===")
    print(f"Database: {db_name}")
    print(f"User: {db_user}")
    print(f"Customers: {args.customers}")
    print(f"Products: {args.products}")
    print(f"Orders: {args.orders}")
    print(f"History: {args.months} months")
    print(f"Seed: {args.seed}")
    print(f"Database time: {db_time}")
    print("Configuration check: PASS")
    print("No data generated yet.")


if __name__ == "__main__":
    main()