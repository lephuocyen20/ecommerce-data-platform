import random
from datetime import timedelta
from decimal import Decimal

from .time_utils import random_timestamp


def calculate_discount(quantity, unit_price):
    if unit_price >= Decimal("10000000"):
        rates, weights = [0, 0.05, 0.10], [65, 25, 10]
    elif unit_price >= Decimal("1000000"):
        rates, weights = [0, 0.05, 0.10, 0.15], [50, 25, 20, 5]
    else:
        rates, weights = [0, 0.05, 0.10, 0.15], [40, 20, 25, 15]
    discount_rate = Decimal(str(random.choices(rates, weights=weights, k=1)[0]))
    return (quantity * unit_price * discount_rate).quantize(Decimal("0.01"))


def generate_order_status(order_date, reference_date):
    age_days = (reference_date - order_date).days
    if age_days <= 2:
        statuses, weights = ["PENDING", "CONFIRMED", "CANCELLED"], [60, 35, 5]
    elif age_days <= 7:
        statuses, weights = ["CONFIRMED", "PROCESSING", "SHIPPED", "CANCELLED"], [15, 45, 35, 5]
    elif age_days <= 14:
        statuses, weights = ["PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"], [10, 35, 50, 5]
    else:
        statuses, weights = ["DELIVERED", "CANCELLED"], [95, 5]
    return random.choices(statuses, weights=weights, k=1)[0]


def generate_order_updated_at(order_date, status, reference_date):
    delay_ranges = {
        "PENDING": (0, 0), "CONFIRMED": (0, 1), "PROCESSING": (1, 3),
        "SHIPPED": (2, 5), "DELIVERED": (4, 10), "CANCELLED": (0, 3),
    }
    min_days, max_days = delay_ranges[status]
    return min(order_date + timedelta(days=random.randint(min_days, max_days)), reference_date)


def generate_orders_with_items(cursor, n, end_date):
    cursor.execute("SELECT customer_id, created_at FROM customers")
    customers = cursor.fetchall()
    cursor.execute("SELECT product_id, price, stock_quantity, created_at FROM products")
    products = cursor.fetchall()
    if not customers or not products:
        raise RuntimeError("Customer or Products are missing")

    stock = {product_id: quantity for product_id, _, quantity, _ in products}
    earliest_product_date = min(product[3] for product in products)
    if earliest_product_date > end_date:
        raise RuntimeError("No products existed before reference date")

    planned_orders = []
    for _ in range(n):
        customer_id, customer_created_at = random.choice(customers)
        order_start_date = max(customer_created_at, earliest_product_date)
        if order_start_date > end_date:
            raise RuntimeError("Customer created after reference date")
        planned_orders.append((random_timestamp(order_start_date, end_date), customer_id))

    generated_items = 0
    for order_date, customer_id in sorted(planned_orders):
        status = generate_order_status(order_date, end_date)
        updated_at = generate_order_updated_at(order_date, status, end_date)
        cursor.execute(
            """INSERT INTO orders (customer_id, status, order_date, updated_at)
               VALUES (%s, %s, %s, %s) RETURNING order_id""",
            (customer_id, status, order_date, updated_at),
        )
        order_id = cursor.fetchone()[0]
        available_products = [p for p in products if p[3] <= order_date and stock[p[0]] > 0]
        if not available_products:
            raise RuntimeError(f"No available products for order {order_id}")

        for product_id, price, _, _ in random.sample(
            available_products, min(random.randint(1, 5), len(available_products))
        ):
            quantity = random.randint(1, min(3, stock[product_id]))
            cursor.execute(
                """INSERT INTO order_items
                   (order_id, product_id, quantity, unit_price, discount_amount)
                   VALUES (%s, %s, %s, %s, %s)""",
                (order_id, product_id, quantity, price, calculate_discount(quantity, price)),
            )
            if status not in ("PENDING", "CANCELLED"):
                stock[product_id] -= quantity
            generated_items += 1

    for product_id, remaining in stock.items():
        cursor.execute("UPDATE products SET stock_quantity = %s WHERE product_id = %s", (remaining, product_id))
    print(f"Generated {n} orders")
    print(f"Generated {generated_items} order items")

