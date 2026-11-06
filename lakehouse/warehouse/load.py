"""Generic, transactional Iceberg snapshot loader for Warehouse Raw."""

import argparse
import sys

from pyspark.sql import SparkSession

sys.path[:0] = ["/opt/spark/work-dir", "/opt/spark/work-dir/warehouse/loader"]
from common.minio import configure_s3a
from incremental_silver_to_warehouse import get_committed_batches, get_connection
from lakehouse.migration.registry import TABLES
from lakehouse.warehouse.registry import warehouse_api


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-name", choices=TABLES, required=True)
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName(f"Iceberg{args.table_name.title().replace('_', '')}ToWarehouse").getOrCreate()
    connection = None
    try:
        configure_s3a(spark)
        connection = get_connection()
        connection.autocommit = False
        batches = get_committed_batches(connection, selected_tables=[args.table_name], selected_batch_ids=[args.batch_id])
        if len(batches) != 1:
            raise RuntimeError(f"Expected one pending {args.table_name} batch")
        reader, loader = warehouse_api(args.table_name)
        print(loader(connection, batches[0], reader(spark)))
        connection.commit()
    except Exception:
        if connection:
            connection.rollback()
        raise
    finally:
        if connection:
            connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
