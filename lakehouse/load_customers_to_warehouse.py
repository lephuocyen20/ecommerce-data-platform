"""Load the pinned Iceberg customers snapshot into Warehouse Raw."""
import argparse
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/work-dir")
sys.path.insert(0, "/opt/spark/work-dir/warehouse/loader")
from common.minio import configure_s3a
from lakehouse.warehouse_customers_reader import read_customers_snapshot
from incremental_silver_to_warehouse import get_committed_batches, get_connection
from warehouse_batch_loader import load_iceberg_customers_batch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("IcebergCustomersToWarehouse").getOrCreate()
    connection = None
    try:
        configure_s3a(spark)
        connection = get_connection()
        connection.autocommit = False
        batches = get_committed_batches(connection, selected_tables=["customers"], selected_batch_ids=[args.batch_id])
        if len(batches) != 1:
            raise RuntimeError("Expected exactly one pending customers batch for Iceberg Warehouse load")
        result = load_iceberg_customers_batch(connection, batches[0], read_customers_snapshot(spark))
        connection.commit()
        print(result)
    except Exception:
        if connection is not None:
            connection.rollback()
        raise
    finally:
        if connection is not None:
            connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
