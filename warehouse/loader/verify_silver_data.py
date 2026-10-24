"""Read-only Silver Parquet data verification."""

import io
import os

import pyarrow as pa
import pyarrow.parquet as pq

from incremental_silver_to_warehouse import (
    get_connection,
    get_minio_client,
    get_committed_batches,
    check_silver_manifest,
    read_silver_manifest,
    verify_parquet_objects,
)


EXPECTED_SCHEMAS = {
    "customers": {
        "customer_id": pa.int64(),
        "full_name": pa.string(),
        "email": pa.string(),
        "created_at": pa.timestamp("ns"),
        "updated_at": pa.timestamp("ns"),
    },
    "products": {
        "product_id": pa.int64(),
        "product_name": pa.string(),
        "stock_quantity": pa.int32(),
        "price": pa.decimal128(12, 2),
        "created_at": pa.timestamp("ns"),
        "updated_at": pa.timestamp("ns"),
    },
    "orders": {
        "order_id": pa.int64(),
        "customer_id": pa.int64(),
        "status": pa.string(),
        "order_date": pa.timestamp("ns"),
        "updated_at": pa.timestamp("ns"),
    },
    "order_items": {
        "order_item_id": pa.int64(),
        "order_id": pa.int64(),
        "product_id": pa.int64(),
        "quantity": pa.int32(),
        "unit_price": pa.decimal128(12, 2),
        "discount_amount": pa.decimal128(12, 2),
    },
    "payments": {
        "payment_id": pa.int64(),
        "order_id": pa.int64(),
        "amount": pa.decimal128(12, 2),
        "payment_method": pa.string(),
        "status": pa.string(),
        "transaction_id": pa.string(),
        "created_at": pa.timestamp("ns"),
    },
    "shipments": {
        "shipment_id": pa.int64(),
        "order_id": pa.int64(),
        "carrier": pa.string(),
        "tracking_number": pa.string(),
        "shipping_address": pa.string(),
        "shipping_fee": pa.decimal128(12, 2),
        "status": pa.string(),
        "shipped_at": pa.timestamp("ns"),
        "delivered_at": pa.timestamp("ns"),
        "estimated_delivery_at": pa.timestamp("ns"),
        "created_at": pa.timestamp("ns"),
        "updated_at": pa.timestamp("ns"),
    },
}

PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "shipments": "shipment_id",
}


def read_parquet(client, bucket, key):
    response = client.get_object(bucket, key)

    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()

    return pq.read_table(io.BytesIO(data))


def load_verified_silver_batch(client, bucket, batch):
    manifest_status, manifest_key = check_silver_manifest(
        client, bucket, batch
    )

    if manifest_status != "VALID_METADATA":
        raise ValueError(
            f"Invalid manifest: {batch['batch_id']}"
        )

    manifest = read_silver_manifest(
        client, bucket, manifest_key
    )

    parquet_status = verify_parquet_objects(
        client, bucket, manifest
    )

    if parquet_status != "VERIFIED":
        raise ValueError(
            f"Invalid Parquet objects: {batch['batch_id']}"
        )

    table_name = batch["table_name"]
    expected = EXPECTED_SCHEMAS[table_name]

    tables = [
        read_parquet(client, bucket, obj["object_key"])
        for obj in manifest["parquet_objects"]
    ]

    for table in tables:
        actual = {
            field.name: field.type
            for field in table.schema
        }

        if actual != expected:
            raise ValueError(
                f"Schema mismatch: {table_name}\n"
                f"Expected: {expected}\n"
                f"Actual: {actual}"
            )

    combined = pa.concat_tables(tables)

    if combined.num_rows != manifest["silver_row_count"]:
        raise ValueError(
            f"Row count mismatch: {batch['batch_id']}"
        )

    pk = PRIMARY_KEYS[table_name]
    values = combined.column(pk).to_pylist()

    if any(value is None for value in values):
        raise ValueError(
            f"NULL primary key: {batch['batch_id']}"
        )

    if len(values) != len(set(values)):
        raise ValueError(
            f"Duplicate primary key: {batch['batch_id']}"
        )

    return manifest_key, manifest, combined


def verify_batch(client, bucket, batch):
    """Verify a Silver batch and return its row count."""
    _, _, arrow_table = load_verified_silver_batch(
        client, bucket, batch
    )
    return arrow_table.num_rows


def main():
    bucket = os.environ.get(
        "MINIO_BUCKET", "ecommerce-data-lake"
    )

    client = get_minio_client()

    with get_connection() as conn:
        batches = get_committed_batches(conn)

        if not batches:
            print("NO PENDING BATCHES")
            return

        passed = 0
        total_rows = 0

        for batch in batches:
            try:
                rows = verify_batch(client, bucket, batch)
            except Exception as exc:
                print(
                    f"FAIL table={batch['table_name']} "
                    f"batch_id={batch['batch_id']} "
                    f"error={exc}"
                )
                raise

            passed += 1
            total_rows += rows

            print(
                f"PASS table={batch['table_name']} "
                f"batch_id={batch['batch_id']} "
                f"silver_rows={rows}"
            )

        print(
            f"FINAL RESULT: PASS {passed}/{len(batches)} "
            f"SILVER_ROWS={total_rows}"
        )


if __name__ == "__main__":
    main()
