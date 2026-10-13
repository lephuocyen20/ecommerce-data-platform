import random
from decimal import Decimal

from .time_utils import random_timestamp


def generate_products(cursor, n, start_date, end_date):
    products = []
    for index in range(n):
        created_at = random_timestamp(start_date, end_date)
        products.append((
            f"Product V2 {index + 1}", random.randint(20, 500),
            Decimal(random.randrange(100_000, 30_000_001, 1000)), created_at,
            random_timestamp(created_at, end_date),
        ))
    cursor.executemany(
        """INSERT INTO products
           (product_name, stock_quantity, price, created_at, updated_at)
           VALUES (%s, %s, %s, %s, %s)""",
        products,
    )
    print(f"Generate {n} products")

