import os
import random
import uuid
from contextlib import closing

import psycopg2
from dotenv import load_dotenv
from faker import Faker


load_dotenv()
fake = Faker("vi_VN")


def get_db_connection():
    """Create and return a new PostgreSQL connection."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "ecommerce"),
        user=os.getenv("DB_USER", "ecommerce_user"),
        password=os.getenv("DB_PASSWORD"),
    )


def generate_customers(cursor, n):
    customers = []

    for _ in range(n):
        name = fake.name()
        email = f"customer_{uuid.uuid4().hex}@example.com"
        customers.append((name, email))

    cursor.executemany(
        "INSERT INTO customers (full_name, email) VALUES (%s, %s)",
        customers,
    )


def generate_products(cursor, n):
    products = [
        (f"Product {random.randint(100, 999)}", random.randint(0, 100),
         random.randint(100_000, 30_000_000))
        for _ in range(n)
    ]
    cursor.executemany(
        "INSERT INTO products (product_name, stock_quantity, price) VALUES (%s, %s, %s)",
        products,
    )


def generate_orders(cursor, n):
    cursor.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    if not customer_ids:
        print("Khong co customer nao trong database.")
        return

    statuses = ["PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
    orders = [(random.choice(customer_ids), random.choice(statuses)) for _ in range(n)]
    cursor.executemany(
        "INSERT INTO orders (customer_id, status) VALUES (%s, %s)", orders
    )


def generate_order_items(cursor, n):
    cursor.execute("SELECT order_id FROM orders")
    order_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT product_id, stock_quantity, price FROM products WHERE stock_quantity > 0")
    products = cursor.fetchall()
    if not order_ids or not products:
        print("Can co order va product con hang de tao order item.")
        return

    order_items = []
    for _ in range(n):
        product_id, stock_quantity, price = random.choice(products)
        order_items.append((
            random.choice(order_ids), product_id,
            random.randint(1, min(5, stock_quantity)), price, 0,
        ))
    cursor.executemany(
        """INSERT INTO order_items
           (order_id, product_id, quantity, unit_price, discount_amount)
           VALUES (%s, %s, %s, %s, %s)""",
        order_items,
    )


def generate_payments(cursor, n):
    cursor.execute("""
        SELECT order_id, SUM(quantity * unit_price - discount_amount)
        FROM order_items
        GROUP BY order_id
        HAVING SUM(quantity * unit_price - discount_amount) > 0
    """)
    orders_with_total = cursor.fetchall()
    if not orders_with_total:
        print("Khong co order nao co tong tien de tao payment.")
        return

    payments = []
    for _ in range(n):
        order_id, total_amount = random.choice(orders_with_total)
        payments.append((
            order_id, total_amount,
            random.choice(["COD", "BANK_TRANSFER", "CREDIT_CARD", "E_WALLET"]),
            random.choice(["PENDING", "SUCCESS", "FAILED"]), str(uuid.uuid4()),
        ))
    cursor.executemany(
        """INSERT INTO payments
           (order_id, amount, payment_method, status, transaction_id)
           VALUES (%s, %s, %s, %s, %s)""",
        payments,
    )


def generate_shipments(cursor, n):
    cursor.execute("""
        SELECT o.order_id, o.status
        FROM orders AS o
        WHERE o.status IN ('PROCESSING', 'SHIPPED', 'DELIVERED')
          AND NOT EXISTS (
              SELECT 1 FROM shipments AS s WHERE s.order_id = o.order_id
          )
    """)
    orders_for_shipping = cursor.fetchall()
    if not orders_for_shipping:
        print("Khong co order nao du dieu kien giao hang.")
        return

    shipment_status_map = {
        "PROCESSING": ["PENDING"],
        "SHIPPED": ["PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY"],
        "DELIVERED": ["DELIVERED"],
    }
    selected_orders = random.sample(orders_for_shipping, min(n, len(orders_for_shipping)))
    shipments = [
        (
            order_id, random.choice(["GHN", "GHTK", "Viettel Post"]),
            str(uuid.uuid4()), fake.address(), random.randint(15_000, 50_000),
            random.choice(shipment_status_map[order_status]),
        )
        for order_id, order_status in selected_orders
    ]
    cursor.executemany(
        """INSERT INTO shipments
           (order_id, carrier, tracking_number, shipping_address, shipping_fee, status,
            shipped_at, delivered_at)
           VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL)""",
        shipments,
    )





if __name__ == "__main__":
    with closing(get_db_connection()) as connection, connection:
        with connection.cursor() as db_cursor:
            # Uncomment the generator you want to run.
            generate_customers(db_cursor, 10)
            # generate_products(db_cursor, 10)
            # generate_orders(db_cursor, 10)
            # generate_order_items(db_cursor, 10)
            # generate_payments(db_cursor, 10)
            # generate_shipments(db_cursor, 10)
            # test_transaction(connection, db_cursor)
            pass
