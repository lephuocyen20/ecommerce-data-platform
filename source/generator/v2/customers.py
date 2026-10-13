import uuid

from faker import Faker

from .time_utils import random_timestamp

fake = Faker("vi_VN")


def generate_customers(cursor, n, start_date, end_date):
    customers = []
    for index in range(n):
        created_at = random_timestamp(start_date, end_date)
        customers.append((
            fake.name(),
            f"customer_v2_{index}_{uuid.uuid4().hex[:8]}@example.com",
            created_at,
            random_timestamp(created_at, end_date),
        ))
    cursor.executemany(
        """INSERT INTO customers (full_name, email, created_at, updated_at)
           VALUES (%s, %s, %s, %s)""",
        customers,
    )
    print(f"Generated {n} customers")

