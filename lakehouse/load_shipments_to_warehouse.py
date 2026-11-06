import argparse, sys
from pyspark.sql import SparkSession
sys.path[:0] = ["/opt/spark/work-dir", "/opt/spark/work-dir/warehouse/loader"]
from common.minio import configure_s3a
from lakehouse.warehouse_shipments_reader import read_shipments_snapshot
from incremental_silver_to_warehouse import get_committed_batches, get_connection
from warehouse_batch_loader import load_iceberg_shipments_batch

parser = argparse.ArgumentParser(); parser.add_argument("--batch-id", required=True); args = parser.parse_args(); spark = SparkSession.builder.appName("IcebergShipmentsToWarehouse").getOrCreate(); connection = None
try:
    configure_s3a(spark); connection = get_connection(); connection.autocommit = False; batches = get_committed_batches(connection, selected_tables=["shipments"], selected_batch_ids=[args.batch_id])
    if len(batches) != 1: raise RuntimeError("Expected one pending shipments batch")
    print(load_iceberg_shipments_batch(connection, batches[0], read_shipments_snapshot(spark))); connection.commit()
except Exception:
    if connection: connection.rollback()
    raise
finally:
    if connection: connection.close()
    spark.stop()
