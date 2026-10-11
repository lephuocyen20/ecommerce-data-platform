import os
import csv
import io
import sys
from psycopg2 import sql
from contextlib import closing
from pathlib import Path
from dotenv import load_dotenv
from minio import Minio

# Allow direct execution with: python validation/validate_bronze.py
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from source.generator.generate_data import get_db_connection

load_dotenv()

client = Minio(
    endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)


PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "shipments": "shipment_id",
}

BUCKET_NAME = os.environ["BRONZE_BUCKET"]


def get_latest_object(client, bucket_name, table_name):
    prefix = f"bronze/{table_name}/"

    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)

    latest = max(
        (obj for obj in objects if obj.object_name.endswith(".csv")),
        key=lambda obj: obj.last_modified,
        default=None,
    )

    if latest is None:
        raise ValueError(f"No CSV found for {table_name}")

    return latest.object_name


def validate_table(table_name, object_name=None):
    """
    Validate Bronze CSV data.

    Airflow:
        validate_table(table_name, object_name)

    Manual execution:
        validate_table(table_name)
    """

    # 1. Validate table name
    if table_name not in PRIMARY_KEYS:
        raise ValueError(f"Unsupported table: {table_name}")

    # 2. Resolve Bronze object
    if object_name is None:
        object_name = get_latest_object(
            client,
            BUCKET_NAME,
            table_name,
        )
    else:
        expected_prefix = f"bronze/{table_name}/"

        if (
            not object_name.startswith(expected_prefix)
            or not object_name.endswith(".csv")
        ):
            raise ValueError(
                f"Invalid Bronze object for {table_name}: {object_name}"
            )

    print(f"Validating Bronze object: {object_name}")

    # 3. Read exact CSV object from MinIO
    response = client.get_object(
        BUCKET_NAME,
        object_name,
    )

    try:
        csv_content = response.read().decode("utf-8")
    finally:
        response.close()
        response.release_conn()

    # 4. Parse CSV
    reader = csv.DictReader(io.StringIO(csv_content))
    rows = list(reader)

    # 5. Primary key validation
    primary_key = PRIMARY_KEYS[table_name]

    if reader.fieldnames is None or primary_key not in reader.fieldnames:
        raise ValueError(
            f"Missing primary key column '{primary_key}' "
            f"in Bronze object: {object_name}"
        )

    primary_key_values = [
        row[primary_key]
        for row in rows
    ]

    duplicate_rows = (
        len(primary_key_values)
        - len(set(primary_key_values))
    )

    # 6. Compare with PostgreSQL
    with closing(get_db_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(table_name)
                )
            )

            postgres_count = cursor.fetchone()[0]

    # 7. Data Quality checks
    row_count_check = postgres_count == len(rows)
    duplicate_check = duplicate_rows == 0

    overall_validation = (
        row_count_check
        and duplicate_check
    )

    # 8. Logging
    print(f"\nTable: {table_name}")
    print(f"Bronze object: {object_name}")
    print(f"PostgreSQL rows: {postgres_count}")
    print(f"MinIO CSV rows: {len(rows)}")
    print(f"Duplicate rows: {duplicate_rows}")

    print(
        "Row Count Check:",
        "PASS" if row_count_check else "FAIL"
    )

    print(
        "Duplicate Check:",
        "PASS" if duplicate_check else "FAIL"
    )

    print(
        "Overall Validation:",
        "PASS" if overall_validation else "FAIL"
    )

    # 9. Fail Airflow task if validation fails
    if not overall_validation:
        raise ValueError(
            f"Data validation failed for table: {table_name}"
        )

    return True


if __name__ == "__main__":
    tables = [
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "shipments",
    ]

    for table_name in tables:
        validate_table(table_name)
