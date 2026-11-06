"""Load pinned Iceberg orders into Warehouse Raw."""
import argparse, sys
from pyspark.sql import SparkSession
sys.path.insert(0, "/opt/spark/work-dir"); sys.path.insert(0, "/opt/spark/work-dir/warehouse/loader")
from common.minio import configure_s3a
from lakehouse.warehouse_orders_reader import read_orders_snapshot
from incremental_silver_to_warehouse import get_committed_batches, get_connection
from warehouse_batch_loader import load_iceberg_orders_batch
def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--batch-id", required=True); args = parser.parse_args(); spark = SparkSession.builder.appName("IcebergOrdersToWarehouse").getOrCreate(); conn = None
    try:
        configure_s3a(spark); conn = get_connection(); conn.autocommit = False; batches = get_committed_batches(conn, selected_tables=["orders"], selected_batch_ids=[args.batch_id])
        if len(batches) != 1: raise RuntimeError("Expected one pending orders batch")
        print(load_iceberg_orders_batch(conn, batches[0], read_orders_snapshot(spark))); conn.commit()
    except Exception:
        if conn is not None: conn.rollback()
        raise
    finally:
        if conn is not None: conn.close()
        spark.stop()
if __name__ == "__main__": main()
