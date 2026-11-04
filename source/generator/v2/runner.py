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
            if cursor.fetchone()[0] != "ecommerce_platform":
                raise RuntimeError("Unsafe database connection")

            if args.stage == "master":
                cursor.execute("""SELECT (SELECT COUNT(*) FROM customers),
                                         (SELECT COUNT(*) FROM products)""")
                if any(cursor.fetchone()) and not args.append:
                    raise RuntimeError("Master data already exists")
                cursor.execute(
                    """
                    SELECT
                        (SELECT COALESCE(MAX(customer_id), 0) FROM customers),
                        (SELECT COALESCE(MAX(product_id), 0) FROM products)
                    """
                )
                previous_customer_id, previous_product_id = cursor.fetchone()
                generate_customers(cursor, args.customers, start_date, REFERENCE_DATE)
                generate_products(cursor, args.products, start_date, REFERENCE_DATE)
                if args.append:
                    cursor.execute(
                        """
                        UPDATE customers
                        SET updated_at = CURRENT_TIMESTAMP
                        WHERE customer_id > %s
                        """,
                        (previous_customer_id,),
                    )
                    cursor.execute(
                        """
                        UPDATE products
                        SET updated_at = CURRENT_TIMESTAMP
                        WHERE product_id > %s
                        """,
                        (previous_product_id,),
                    )
            elif args.stage == "orders":
                cursor.execute("""SELECT (SELECT COUNT(*) FROM orders),
                                         (SELECT COUNT(*) FROM order_items)""")
                if any(cursor.fetchone()) and not args.append:
                    raise RuntimeError("Orders already exists")
                cursor.execute("SELECT COALESCE(MAX(order_id), 0) FROM orders")
                previous_order_id = cursor.fetchone()[0]
                generate_orders_with_items(cursor, args.orders, REFERENCE_DATE)
                if args.append:
                    cursor.execute(
                        """
                        UPDATE orders
                        SET updated_at = CURRENT_TIMESTAMP
                        WHERE order_id > %s
                        """,
                        (previous_order_id,),
                    )
            elif args.stage == "payments":
                cursor.execute("SELECT COUNT(*) FROM payments")
                if cursor.fetchone()[0] > 0 and not args.append:
                    raise RuntimeError("Payments already exist. Refusing duplicate generation.")

                records = generate_payments(cursor, only_missing=args.append)
                if not records:
                    print("NO_DATA: No payments inserted")
                elif not args.insert:
                    print("DRY RUN: No records inserted")
                else:
                    insert_payments(cursor, records, allow_append=args.append)
            elif args.stage == "shipments":
                cursor.execute("SELECT COUNT(*) FROM shipments")
                if cursor.fetchone()[0] > 0 and not args.append:
                    raise RuntimeError("Shipments already exist. Refusing duplicate generation.")

                records = generate_shipments(cursor, only_missing=args.append)
                if not records:
                    print("NO_DATA: No shipments inserted")
                elif not args.insert:
                    print("DRY RUN: No records inserted")
                else:
                    insert_shipments(cursor, records, allow_append=args.append)
                    if args.append:
                        cursor.execute(
                            """
                            UPDATE shipments
                            SET updated_at = CURRENT_TIMESTAMP
                            WHERE order_id = ANY(%s)
                            """,
                            ([record["order_id"] for record in records],),
                        )
