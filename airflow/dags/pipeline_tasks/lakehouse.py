"""TaskFlow gates for the optional customers Iceberg dual run."""

import os

from airflow.sdk import task


@task.branch(retries=0)
def check_lakehouse_customers_eligibility(validation_result):
    """Permit an Iceberg write only for a verified, explicitly enabled batch."""
    if not isinstance(validation_result, dict) or validation_result.get("table_name") != "customers":
        raise ValueError("Missing customers Bronze validation")
    if validation_result.get("mode") != "WRITE":
        return "skip_iceberg_customers"
    if validation_result.get("status") == "SKIPPED":
        return "skip_iceberg_customers"
    if validation_result.get("status") != "PASS" or validation_result.get("checksum_verified") is not True:
        raise ValueError("customers Bronze validation did not pass")
    if os.getenv("LAKEHOUSE_WRITE_ENABLED", "false").lower() != "true":
        return "skip_iceberg_customers"
    return "silver_parquet_to_iceberg_customers"


@task(retries=0)
def skip_iceberg_customers(validation_result):
    return {"table_name": "customers", "status": "SKIPPED", "reason": "LAKEHOUSE_DISABLED_OR_NO_DATA"}


@task(retries=0)
def verify_iceberg_customers_checkpoint(validation_result):
    """Verify the Spark Iceberg loader committed its Warehouse checkpoint."""
    if not isinstance(validation_result, dict) or validation_result.get("table_name") != "customers":
        raise ValueError("Missing customers validation result")
    batch_id = validation_result.get("batch_id")
    if not isinstance(batch_id, str) or len(batch_id) != 32:
        raise ValueError("Iceberg checkpoint verification requires a batch ID")
    import psycopg2
    connection = psycopg2.connect(
        host=os.environ["WAREHOUSE_DB_HOST"], port=os.environ["WAREHOUSE_DB_PORT"],
        dbname=os.environ["WAREHOUSE_DB_NAME"], user=os.environ["WAREHOUSE_DB_USER"],
        password=os.environ["WAREHOUSE_DB_PASSWORD"],
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT source_type, iceberg_table, iceberg_catalog_ref, iceberg_snapshot_id
                FROM warehouse.loaded_batches WHERE batch_id = %s
            """, (batch_id,))
            row = cursor.fetchone()
    finally:
        connection.close()
    if not row or row[0] != "iceberg" or not row[1] or not row[2] or not isinstance(row[3], int):
        raise RuntimeError(f"Iceberg customers Warehouse checkpoint missing or invalid: {batch_id}")
    return {"table_name": "customers", "batch_id": batch_id, "source_type": row[0], "iceberg_table": row[1], "iceberg_catalog_ref": row[2], "iceberg_snapshot_id": row[3]}
