import random
from collections import defaultdict
from datetime import timedelta

from .time_utils import REFERENCE_DATE


def generate_payment_statuses(order_status, payment_method):
    if payment_method == "COD":
        if order_status == "DELIVERED":
            return ["SUCCESS"]
        if order_status == "CANCELLED":
            return []
        return ["PENDING"]
    if order_status == "PENDING":
        return random.choice([[], ["PENDING"]])
    if order_status in ("CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED"):
        failed_attempts = random.choices([0, 1, 2], weights=[75, 20, 5], k=1)[0]
        return ["FAILED"] * failed_attempts + ["SUCCESS"]
    if order_status == "CANCELLED":
        return random.choice([[], ["FAILED"], ["SUCCESS", "REFUNDED"]])
    raise RuntimeError(f"Unsupported order status: {order_status}")


def generate_payment_method():
    return random.choices(
        ["BANK_TRANSFER", "CREDIT_CARD", "E_WALLET", "COD"],
        weights=[35, 25, 25, 15], k=1,
    )[0]


def generate_online_payment_timeline(order_date, order_updated_at, statuses, end_date):
    if not statuses:
        return []
    required_time = timedelta(minutes=len(statuses))
    preferred_latest = min(order_updated_at, end_date)
    latest_allowed = preferred_latest if order_date + required_time <= preferred_latest else end_date
    if order_date + required_time > latest_allowed:
        raise RuntimeError("Insufficient time for payment attempts before reference date")
    times, current_time = [], order_date
    for index in range(len(statuses)):
        max_allowed = latest_allowed - timedelta(minutes=len(statuses) - index - 1)
        current_time = min(current_time + timedelta(minutes=random.randint(1, 10)), max_allowed)
        times.append(current_time)
    return times


def generate_cod_payment_timeline(order_date, order_updated_at, statuses, end_date):
    if not statuses:
        return []
    if statuses[0] == "PENDING":
        return [order_date]
    if statuses[0] == "SUCCESS":
        payment_time = min(order_updated_at, end_date)
        if payment_time < order_date:
            raise RuntimeError("COD payment time before order date")
        return [payment_time]
    raise RuntimeError(f"Unsupported COD payment status: {statuses[0]}")


def generate_refund_payment_timeline(order_date, order_updated_at, end_date):
    success_time = order_date + timedelta(minutes=random.randint(1, 10))
    if success_time > order_updated_at:
        raise RuntimeError("Payment SUCCESS after order cancellation")
    refund_time = order_updated_at + timedelta(minutes=random.randint(1, 60))
    if refund_time > end_date:
        raise RuntimeError("Payment REFUNDED after reference date")
    return [success_time, refund_time]


def _payment_times(order_id, status, method, order_date, updated_at, statuses):
    if statuses == ["SUCCESS", "REFUNDED"]:
        return generate_refund_payment_timeline(order_date, updated_at, REFERENCE_DATE)
    if method == "COD":
        return generate_cod_payment_timeline(order_date, updated_at, statuses, REFERENCE_DATE)
    if status in ("PENDING", "CANCELLED") and statuses == ["PENDING"] or (
        status == "CANCELLED" and statuses == ["FAILED"] and updated_at == order_date
    ):
        return [order_date]
    try:
        return generate_online_payment_timeline(order_date, updated_at, statuses, REFERENCE_DATE)
    except RuntimeError as exc:
        raise RuntimeError(f"Order {order_id}: {exc}") from exc


def _validate_records(records, orders):
    transaction_ids, by_order = set(), defaultdict(list)
    for record in records:
        if record["amount"] <= 0:
            raise RuntimeError(f"DQ 01 FAILED: Invalid payment amount for order {record['order_id']}")
        if not record["order_date"] <= record["created_at"] <= REFERENCE_DATE:
            raise RuntimeError(f"DQ 02/03 FAILED: Invalid timestamp for order {record['order_id']}")
        if record["transaction_id"] in transaction_ids:
            raise RuntimeError(f"DQ 04 FAILED: Duplicate transaction ID for order {record['order_id']}")
        transaction_ids.add(record["transaction_id"])
        by_order[record["order_id"]].append(record)
    amounts = {row[0]: row[4] for row in orders}
    for order_id, entries in by_order.items():
        entries.sort(key=lambda record: record["created_at"])
        if any(entry["amount"] != amounts[order_id] for entry in entries):
            raise RuntimeError(f"DQ 05 FAILED: Invalid amount for order {order_id}")
        if len({entry["payment_method"] for entry in entries}) != 1:
            raise RuntimeError(f"DQ 12 FAILED: Multiple payment methods for order {order_id}")
        for index, entry in enumerate(entries):
            if index and entry["created_at"] <= entries[index - 1]["created_at"]:
                raise RuntimeError(f"DQ 10 FAILED: Non-increasing timeline for order {order_id}")
            if entry["payment_method"] == "COD" and entry["payment_status"] == "SUCCESS" and entry["order_status"] != "DELIVERED":
                raise RuntimeError(f"DQ 06 FAILED: COD success for order {order_id}")
            if entry["order_status"] == "PENDING" and entry["payment_status"] != "PENDING":
                raise RuntimeError(f"DQ 07 FAILED: Invalid pending payment for order {order_id}")
            if entry["order_status"] == "CANCELLED" and entry["payment_status"] == "PENDING":
                raise RuntimeError(f"DQ 08 FAILED: Pending payment for cancelled order {order_id}")
            if entry["payment_status"] == "REFUNDED":
                successes = [previous for previous in entries[:index] if previous["payment_status"] == "SUCCESS" and previous["amount"] == entry["amount"] and previous["payment_method"] == entry["payment_method"]]
                if entry["order_status"] != "CANCELLED" or len(successes) != 1:
                    raise RuntimeError(f"DQ 09 FAILED: Invalid refund for order {order_id}")
    for order_id, status, _, _, _ in orders:
        if status not in ("CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED"):
            continue
        entries = by_order.get(order_id, [])
        if not entries:
            raise RuntimeError(f"DQ 11 FAILED: No payment for order {order_id}")
        method = entries[0]["payment_method"]
        expected = 1 if method != "COD" or status == "DELIVERED" else 0
        if sum(entry["payment_status"] == "SUCCESS" for entry in entries) != expected:
            raise RuntimeError(f"DQ 11 FAILED: Invalid SUCCESS count for order {order_id}")
    print("DQ 01-12: PASS")


def generate_payments(cursor):
    cursor.execute("""
        SELECT o.order_id, o.status, o.order_date, o.updated_at,
               SUM(oi.quantity * oi.unit_price - oi.discount_amount) AS order_amount
        FROM orders o JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, o.status, o.order_date, o.updated_at
        ORDER BY o.order_date
    """)
    orders = cursor.fetchall()
    if not orders:
        raise RuntimeError("No orders available for payments generation")
    print(f"Loaded {len(orders)} orders for payments")
    records = []
    for order_id, status, order_date, updated_at, amount in orders:
        method = generate_payment_method()
        statuses = generate_payment_statuses(status, method)
        if status == "CANCELLED" and updated_at == order_date and statuses == ["SUCCESS", "REFUNDED"]:
            statuses = random.choice([[], ["FAILED"]])
        times = _payment_times(order_id, status, method, order_date, updated_at, statuses)
        if len(statuses) != len(times):
            raise RuntimeError(f"Order {order_id}: Payment statuses and times mismatch")
        for index, (payment_status, payment_time) in enumerate(zip(statuses, times), start=1):
            records.append({
                "order_id": order_id, "order_status": status, "order_date": order_date,
                "amount": amount, "payment_method": method, "payment_status": payment_status,
                "transaction_id": f"TXN-{order_id:08d}-{index:02d}", "created_at": payment_time,
            })
    _validate_records(records, orders)
    print(f"Generated and validated {len(records)} payment records")

