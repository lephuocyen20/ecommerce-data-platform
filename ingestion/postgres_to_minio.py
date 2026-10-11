import csv
import io
import os
import sys
from psycopg2 import sql
from datetime import datetime
from pathlib import Path
from contextlib import closing
import minio
from dotenv import load_dotenv

# Allow direct execution with: python ingestion/postgres_to_minio.py
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from source.generator.generate_data import get_db_connection

load_dotenv()

ALLOWED_TABLES = {
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
}


def validate_table_name(table_name):
    """Raise an error when table_name is outside the project schema."""
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Unsupported table: {table_name}")


def get_minio_client():
    """Create and return a new MinIO client."""
    return minio.Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )


def extract_table(table_name):
    """Extract records from a PostgreSQL table."""
    validate_table_name(table_name)

    with closing(get_db_connection()) as connection, connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
            )
            columns = [column[0] for column in cursor.description]
            records = cursor.fetchall()
            return columns, records


def records_to_csv(columns, records):
    """Convert records to CSV content."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    writer.writerows(records)
    return output.getvalue()


def upload_to_minio(client, table_name, csv_content):
    """Upload CSV content to the Bronze layer in MinIO."""
    validate_table_name(table_name)

    bucket_name = "ecommerce-data-lake"
    extract_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = (
        f"bronze/{table_name}/extract_date={extract_timestamp[:8]}/"
        f"{table_name}_{extract_timestamp}.csv"
    )
    csv_bytes = csv_content.encode("utf-8")

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    return client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv",
    )


def ingest_table(table_name):
    """Extract one PostgreSQL table and upload it to MinIO."""

    # 1. Validate table name.
    validate_table_name(table_name)

    # 2. Connect to MinIO.
    client = get_minio_client()

    # 3. Extract data from PostgreSQL.
    columns, records = extract_table(table_name)

    # 4. Convert records to CSV.
    csv_content = records_to_csv(columns, records)

    # 5. Upload to MinIO.
    result = upload_to_minio(client, table_name, csv_content)

    print(f"{table_name}: {result.object_name}")

    return result.object_name


if __name__ == "__main__":
    tables = [
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "shipments",
    ]

    client = get_minio_client()

    for table_name in tables:
        # 1. Extract data from the current table.
        columns, records = extract_table(table_name)

        # 2. Convert records to CSV.
        csv_content = records_to_csv(columns, records)

        # 3. Upload the CSV to MinIO.
        result = upload_to_minio(client, table_name, csv_content)

        # 4. This runs only when the upload finishes successfully.
        print(f"{table_name}: {result.object_name}")
