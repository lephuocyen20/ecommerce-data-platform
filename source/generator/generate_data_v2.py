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
    parser.add_argument("--stage", choices=["master", "orders"], default="master",
)

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

def calculate_discount(quantity, unit_price):
    if unit_price >= Decimal("10000000"):
        rate = [0, 0.05, 0.10]
        wieghts = [65, 25, 10]

    elif unit_price >= Decimal("1000000"):
        rate = [0, 0.05, 0.10, 0.15]
        wieghts = [50, 25, 20, 5]

    else:
        rates = [0, 0.05, 0.10, 0.15]
        weights = [40, 20, 25, 15]


    discount_rate = Decimal(
        str(random.choices(rate, weights=wieghts, k = 1)[0])
    )

    gross_amount = quantity * unit_price

    return (gross_amount * discount_rate).quantize(
        Decimal("0.01")
    )
        

def generate_orders_with_items(cursor, n, end_date):
    #Load customers
    cursor.execute("""SELECT customer_id, created_at FROM customers""")
    customers = cursor.fetchall()

    #Load product
    cursor.execute("""SELECT product_id, price, stock_quantity, created_at FROM products""")
    products = cursor.fetchall()

    if not customers or not products:
        raise RuntimeError("Customer or Products are missing")

    #Track stock in memory

    stock = {
        product_id: quantity
        for product_id, price, quantity, created_at in products
    }

    statuses = [
        "PENDING",
        "CONFIRMED",
        "PROCESSING",
        "SHIPPED",
        "DELIVERED",
        "CANCELLED",
    ]

    status_weights = [10, 15, 20, 15, 35, 5]

    generated_items = 0

    for _ in range(n):
        customer_id, customer_created_at = random.choice(customers)

        eligible_product = [
            product
            for product in products
            if product[3] <= end_date
        ]
        if not eligible_product:
            raise RuntimeError("No eligible products")

        order_date = random_timestamp(customer_created_at, end_date,)

        status = random.choices(statuses, weights = status_weights, k = 1,)[0]

        cursor.execute("""
            INSERT INTO orders (
                customer_id, status, order_date, updated_at
                )
                VALUES (%s, %s, %s, %s)
                RETURNING order_id
                """,
                (
                    customer_id,
                    status,
                    order_date,
                    order_date,
                ),
        )

        order_id = cursor.fetchone()[0]

        #Product must exits at order date

        available_products = [
            product
            for product in products
            if product[3] <= order_date and stock[product[0]] > 0
        ]

        if not available_products:
            raise RuntimeError(f"No available products for order {order_id}")

        item_count = min(random.randint(1, 5),
                         len(available_products),
                         )

        select_products = random.sample(
            available_products,
            item_count,
        )

        for product_id, price, _, _ in select_products:
            quantity = random.randint(1,
                                      min(3, stock[product_id]),
                                      )

            discount = calculate_discount(quantity, price)

            cursor.execute(
                """ INSERT INTO order_items (
                            order_id,
                            product_id,
                            quantity,
                            unit_price,
                            discount_amount
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            order_id,
                            product_id,
                            quantity,
                            price,
                            discount,
                        ),
                        )
            # Pending and cancelled orders do not consume stock
            if status not in ("PENDING","CANCELLED"):
                stock[product_id] -= quantity

            generated_items += 1
    #Persist updated stock
    for product_id, remaining in stock.items():
        cursor.execute(
            """ UPDATE products
            SET stock_quantity = %s
            WHERE product_id = %s
            """,
            (remaining, product_id),
    )

    print(f"Generated {n} orders")
    print(f"Generated {generated_items} order items")
         

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
            if args.stage == "master":
                cursor.execute(
                    """SELECT 
                            (SELECT COUNT(*) FROM customers),
                            (SELECT COUNT(*) FROM products)               
                            """)
                customer_count, product_count = cursor.fetchone()

                if customer_count > 0 or product_count > 0:
                    raise RuntimeError("Master data already exists")

                generate_customers(
                    cursor, args.customers, start_date, REFERENCE_DATE,
                )
                generate_products(
                    cursor, args.products, start_date, REFERENCE_DATE,
                )

            elif args.stage == "orders":
                cursor.execute(
                    """ SELECT
                            (SELECT COUNT(*) FROM orders),
                            (SELECT COUNT(*) FROM order_items)
                """
                )
                order_count, item_count = cursor.fetchone()
                if order_count > 0 or item_count > 0:
                    raise RuntimeError("Orders already exists")

                generate_orders_with_items(
                    cursor,
                    args.orders,
                    REFERENCE_DATE,
                )


if __name__ == "__main__":
    main()