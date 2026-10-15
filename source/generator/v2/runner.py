import random
from datetime import timedelta

from faker import Faker

from .cli import parse_args
from .config import get_db_connection, load_config
from .customers import generate_customers
from .orders import generate_orders_with_items
from .payments import generate_payments, insert_payments
from .products import generate_products
from .shipments import generate_shipments, insert_shipments
from .time_utils import REFERENCE_DATE


def main():
    args = parse_args()
    load_config()
    random.seed(args.seed)
    Faker.seed(args.seed)
    start_date = REFERENCE_DATE - timedelta(days=args.months * 30)

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            if cursor.fetchone()[0] != "ecommerce_test":
                raise RuntimeError("Unsafe database connection")
            if args.stage == "master":
                cursor.execute("""SELECT (SELECT COUNT(*) FROM customers),
                                         (SELECT COUNT(*) FROM products)""")
                if any(cursor.fetchone()):
                    raise RuntimeError("Master data already exists")
                generate_customers(cursor, args.customers, start_date, REFERENCE_DATE)
                generate_products(cursor, args.products, start_date, REFERENCE_DATE)
            elif args.stage == "orders":
                cursor.execute("""SELECT (SELECT COUNT(*) FROM orders),
                                         (SELECT COUNT(*) FROM order_items)""")
                if any(cursor.fetchone()):
                    raise RuntimeError("Orders already exists")
                generate_orders_with_items(cursor, args.orders, REFERENCE_DATE)

            elif args.stage == "payments":
                cursor.execute("SELECT COUNT(*) FROM payments")

                if cursor.fetchone()[0] > 0:
                    raise RuntimeError("Payments already exist. Refusing duplicate generation.")

                records = generate_payments(cursor)

                if not args.insert:
                    print("DRY RUN: No records inserted")
                else:
                    insert_payments(cursor, records)
            import random
from datetime import timedelta

from faker import Faker

from .cli import parse_args
from .config import get_db_connection, load_config
from .customers import generate_customers
from .orders import generate_orders_with_items
from .payments import generate_payments, insert_payments
from .products import generate_products
from .shipments import generate_shipments
from .time_utils import REFERENCE_DATE


def main():
    args = parse_args()
    load_config()
    random.seed(args.seed)
    Faker.seed(args.seed)
    start_date = REFERENCE_DATE - timedelta(days=args.months * 30)

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            if cursor.fetchone()[0] != "ecommerce_test":
                raise RuntimeError("Unsafe database connection")
            if args.stage == "master":
                cursor.execute("""SELECT (SELECT COUNT(*) FROM customers),
                                         (SELECT COUNT(*) FROM products)""")
                if any(cursor.fetchone()):
                    raise RuntimeError("Master data already exists")
                generate_customers(cursor, args.customers, start_date, REFERENCE_DATE)
                generate_products(cursor, args.products, start_date, REFERENCE_DATE)
            elif args.stage == "orders":
                cursor.execute("""SELECT (SELECT COUNT(*) FROM orders),
                                         (SELECT COUNT(*) FROM order_items)""")
                if any(cursor.fetchone()):
                    raise RuntimeError("Orders already exists")
                generate_orders_with_items(cursor, args.orders, REFERENCE_DATE)

            elif args.stage == "payments":
                cursor.execute("SELECT COUNT(*) FROM payments")

                if cursor.fetchone()[0] > 0:
                    raise RuntimeError("Payments already exist. Refusing duplicate generation.")

                records = generate_payments(cursor)

                if not args.insert:
                    print("DRY RUN: No records inserted")
                else:
                    insert_payments(cursor, records)

            elif args.stage == "shipments":
                cursor.execute("SELECT COUNT(*) FROM shipments")

                if cursor.fetchone()[0] > 0:
                    raise RuntimeError("Shipments already exist. Refusing duplicate generation.")

                records = generate_shipments(cursor)

                if not args.insert:
                    print("DRY RUN: No records inserted")
                else:
                    insert_shipments(cursor, records)