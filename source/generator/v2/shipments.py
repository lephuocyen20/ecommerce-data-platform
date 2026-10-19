import random
from datetime import timedelta
from faker import Faker

from .time_utils import REFERENCE_DATE

fake = Faker("vi_VN")


def generate_shipment_status(order_status):
    if order_status == "PROCESSING":
        return "PENDING"

    if order_status == "SHIPPED":
        return random.choice(["PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY"])

    if order_status == "DELIVERED":
        return "DELIVERED"

    raise RuntimeError(f"Unsupported order status: {order_status}")


def generate_shipment_details(order_id):
    carrier = random.choice(
        [
            "GHN",
            "GHTK",
            "Viettel Post",
        ]
    )

    tracking_number = f"SHP-{order_id:08d}"

    shipping_address = fake.address()

    shipping_fee = random.choice(
        [
            15000,
            20000,
            25000,
            30000,
            35000,
            40000,
        ]
    )

    return carrier, tracking_number, shipping_address, shipping_fee


def generate_shipment_timeline(
    order_date,
    order_updated_at,
    shipment_status,
):
    end_date = min(order_updated_at, REFERENCE_DATE)

    if order_date > end_date:
        raise RuntimeError("Order date after allowed timeline")

    duration = end_date - order_date

    # PROCESSING: Shipment chưa xuất kho
    if shipment_status == "PENDING":
        created_at = order_date + duration * 0.2

        return {
            "created_at": created_at,
            "updated_at": created_at,
            "shipped_at": None,
            "delivered_at": None,
            "estimated_delivery_at": created_at + timedelta(days=random.randint(2, 5)),
        }

    # SHIPPED / DELIVERED: phân bổ thời gian trong
    # khoảng từ order_date đến order_updated_at.
    created_at = order_date + duration * 0.2
    shipped_at = order_date + duration * 0.5

    if shipment_status == "DELIVERED":
        delivered_at = end_date
        updated_at = delivered_at
    else:
        delivered_at = None
        updated_at = end_date

    return {
        "created_at": created_at,
        "updated_at": updated_at,
        "shipped_at": shipped_at,
        "delivered_at": delivered_at,
        "estimated_delivery_at": created_at + timedelta(days=random.randint(2, 5)),
    }


def validate_shipments(records, orders, paid_order_ids):

    if not orders or not records:
        raise RuntimeError("DQ FAILED: Empty orders or shipment records")

    if len(records) != len(orders):
        raise RuntimeError(
            f"DQ FAILED: Expected {len(orders)} shipments, " f"generated {len(records)}"
        )

    order_map = {
        order_id: (status, order_date, updated_at)
        for order_id, status, order_date, updated_at in orders
    }

    order_ids = set()
    tracking_numbers = set()

    for record in records:
        order_id = record["order_id"]
        status = record["status"]

        # DQ 01
        if order_id in order_ids:
            raise RuntimeError(f"DQ 01 FAILED: Duplicate order {order_id}")
        order_ids.add(order_id)

        # DQ 02
        tracking = record["tracking_number"]
        if tracking in tracking_numbers:
            raise RuntimeError(f"DQ 02 FAILED: Duplicate tracking {tracking}")
        tracking_numbers.add(tracking)

        # DQ 03
        if record["shipping_fee"] < 0:
            raise RuntimeError(f"DQ 03 FAILED: Negative shipping fee, order {order_id}")

        # DQ 04
        order_status, order_date, order_updated_at = order_map[order_id]

        allowed = {
            "PROCESSING": {"PENDING"},
            "SHIPPED": {"PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY"},
            "DELIVERED": {"DELIVERED"},
        }

        if status not in allowed[order_status]:
            raise RuntimeError(f"DQ 04 FAILED: Invalid status, order {order_id}")

        created = record["created_at"]
        updated = record["updated_at"]
        shipped = record["shipped_at"]
        delivered = record["delivered_at"]
        estimated = record["estimated_delivery_at"]

        # DQ 05
        if created < order_date:
            raise RuntimeError(f"DQ 05 FAILED: Created before order {order_id}")

        # DQ 06
        if not created <= updated <= REFERENCE_DATE:
            raise RuntimeError(
                f"DQ 06 FAILED: Invalid update timeline, order {order_id}"
            )

        # DQ 07
        if estimated < created:
            raise RuntimeError(f"DQ 07 FAILED: Invalid ETA, order {order_id}")

        # DQ 08
        if status == "PENDING":
            if shipped is not None or delivered is not None:
                raise RuntimeError(
                    f"DQ 08 FAILED: Pending shipment has timestamps, order {order_id}"
                )

        # DQ 09
        if status in {"PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"}:
            if shipped is None or not created <= shipped <= updated:
                raise RuntimeError(
                    f"DQ 09 FAILED: Invalid shipped_at, order {order_id}"
                )

        # DQ 10
        if status == "DELIVERED":
            if delivered is None or not shipped <= delivered <= updated:
                raise RuntimeError(
                    f"DQ 10 FAILED: Invalid delivered_at, order {order_id}"
                )
        elif delivered is not None:
            raise RuntimeError(
                f"DQ 10 FAILED: Unexpected delivered_at, order {order_id}"
            )
        # DQ 11: Shipment created before Order completion
        if order_status == "DELIVERED":
            if created > order_updated_at:
                raise RuntimeError(
                    f"DQ 11 FAILED: Shipment created after order completion {order_id}"
                )

        # DQ 12: Delivered timeline consistency
        if order_status == "DELIVERED":
            if delivered != order_updated_at:
                raise RuntimeError(
                    f"DQ 12 FAILED: Delivery timestamp mismatch, order {order_id}"
                )

        # DQ 13: No future actual timestamps
        if shipped is not None and shipped > REFERENCE_DATE:
            raise RuntimeError(f"DQ 13 FAILED: Future shipped_at, order {order_id}")

        if delivered is not None and delivered > REFERENCE_DATE:
            raise RuntimeError(f"DQ 13 FAILED: Future delivered_at, order {order_id}")

        # DQ 14: Delivered shipments require successful payment
        if status == "DELIVERED" and order_id not in paid_order_ids:
            raise RuntimeError(
                f"DQ 14 FAILED: Delivered order without successful payment {order_id}"
            )

        # DQ 15: Estimated delivery must be reasonable
        if estimated is None:
            raise RuntimeError(f"DQ 15 FAILED: Missing ETA, order {order_id}")

        if estimated > created + timedelta(days=7):
            raise RuntimeError(f"DQ 15 FAILED: ETA exceeds 7 days, order {order_id}")

    if len(records) != len(orders):
        raise RuntimeError("DQ FAILED: Shipment count does not match eligible orders")

    print("DQ 01-15: PASS")


def generate_shipments(cursor):
    cursor.execute("""
        SELECT
            o.order_id,
            o.status,
            o.order_date,
            o.updated_at
        FROM orders o
        WHERE o.status IN (
            'PROCESSING',
            'SHIPPED',
            'DELIVERED'
        )
        ORDER BY o.order_id
    """)

    orders = cursor.fetchall()

    if not orders:
        raise RuntimeError("No eligible orders for shipments generation")

    print(f"Loaded {len(orders)} eligible orders for shipments")

    cursor.execute("""
            SELECT DISTINCT order_id
            FROM payments
            WHERE status = 'SUCCESS'
        """)

    paid_order_ids = {row[0] for row in cursor.fetchall()}

    records = []

    for order_id, order_status, order_date, order_updated_at in orders:
        shipment_status = generate_shipment_status(order_status)

        carrier, tracking_number, shipping_address, shipping_fee = (
            generate_shipment_details(order_id)
        )

        timeline = generate_shipment_timeline(
            order_date,
            order_updated_at,
            shipment_status,
        )

        records.append(
            {
                "order_id": order_id,
                "carrier": carrier,
                "tracking_number": tracking_number,
                "shipping_address": shipping_address,
                "shipping_fee": shipping_fee,
                "status": shipment_status,
                **timeline,
            }
        )
    validate_shipments(records, orders, paid_order_ids)

    print(f"Generated and validated {len(records)} shipment records")

    return records


def insert_shipments(cursor, records):
    if not records:
        raise RuntimeError("No shipment records to insert")

    cursor.execute("SELECT COUNT(*) FROM shipments")

    if cursor.fetchone()[0] != 0:
        raise RuntimeError("Shipments already exist. Refusing duplicate INSERT.")

    query = """
        INSERT INTO shipments (
            order_id,
            carrier,
            tracking_number,
            shipping_address,
            shipping_fee,
            status,
            shipped_at,
            delivered_at,
            estimated_delivery_at,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = [
        (
            record["order_id"],
            record["carrier"],
            record["tracking_number"],
            record["shipping_address"],
            record["shipping_fee"],
            record["status"],
            record["shipped_at"],
            record["delivered_at"],
            record["estimated_delivery_at"],
            record["created_at"],
            record["updated_at"],
        )
        for record in records
    ]

    cursor.executemany(query, values)

    cursor.execute("SELECT COUNT(*) FROM shipments")
    actual_count = cursor.fetchone()[0]

    if actual_count != len(records):
        raise RuntimeError(
            f"INSERT mismatch: expected={len(records)}, " f"actual={actual_count}"
        )

    print(f"Inserted {actual_count} shipment records")
