"""Transaction-scoped guards for incremental Warehouse loading."""

from psycopg2 import sql


def acquire_table_lock(conn, table_name):
    """Serialize Warehouse loading for a given source table."""

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    supported = {
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "shipments",
    }

    if table_name not in supported:
        raise ValueError(f"Unsupported table: {table_name}")

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT pg_advisory_xact_lock(
                hashtext('warehouse.raw_loader'),
                hashtext(%s)
            )
            """,
            (table_name,),
        )


def check_batch_loadable(conn, batch):
    """
    Return READY or SKIP.

    Reject missing, overlapping, or out-of-order watermark ranges.

    Must be called after acquire_table_lock() in the same
    transaction.
    """

    if conn.autocommit:
        raise ValueError("Autocommit must be disabled")

    table_name = batch["table_name"]
    batch_id = batch["batch_id"]
    watermark_type = batch["watermark_type"]

    supported = {
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "shipments",
    }

    if table_name not in supported:
        raise ValueError(f"Unsupported table: {table_name}")

    if watermark_type not in ("TIMESTAMP", "ID"):
        raise ValueError("Unsupported watermark type")

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT 1
            FROM warehouse.loaded_batches
            WHERE batch_id = %s
            """,
            (batch_id,),
        )

        if cursor.fetchone() is not None:
            return "SKIP"

        cursor.execute(
            """
            SELECT watermark_type, end_timestamp, end_id
            FROM warehouse.loaded_batches
            WHERE table_name = %s
            ORDER BY
                end_timestamp DESC NULLS LAST,
                end_id DESC
            LIMIT 1
            """,
            (table_name,),
        )

        previous = cursor.fetchone()

    start_id = batch["start_id"]
    end_id = batch["end_id"]

    if start_id is None or end_id is None:
        raise ValueError("Incomplete ID watermark")

    if watermark_type == "TIMESTAMP":
        start_ts = batch["start_timestamp"]
        end_ts = batch["end_timestamp"]

        if end_ts is None:
            raise ValueError("Missing end timestamp")

        if previous is None:
            if start_ts is not None or start_id != 0:
                raise ValueError(
                    f"Missing initial batch for {table_name}"
                )
        else:
            previous_type, previous_ts, previous_id = previous

            if previous_type != watermark_type:
                raise ValueError("Watermark type mismatch")

            if (start_ts, start_id) != (
                previous_ts,
                previous_id,
            ):
                raise ValueError(
                    f"Watermark continuity violation: {batch_id}"
                )

            if (end_ts, end_id) <= (
                previous_ts,
                previous_id,
            ):
                raise ValueError(
                    f"Out-of-order TIMESTAMP batch: {batch_id}"
                )

        if start_ts is not None:
            if (end_ts, end_id) <= (start_ts, start_id):
                raise ValueError("Non-increasing TIMESTAMP watermark")

    else:
        if batch["start_timestamp"] is not None:
            raise ValueError("Unexpected start timestamp")

        if batch["end_timestamp"] is not None:
            raise ValueError("Unexpected end timestamp")

        if previous is None:
            if start_id != 0:
                raise ValueError(
                    f"Missing initial batch for {table_name}"
                )
        else:
            previous_type, _, previous_id = previous

            if previous_type != watermark_type:
                raise ValueError("Watermark type mismatch")

            if start_id != previous_id:
                raise ValueError(
                    f"Watermark continuity violation: {batch_id}"
                )

        if end_id <= start_id:
            raise ValueError("Non-increasing ID watermark")

    return "READY"
