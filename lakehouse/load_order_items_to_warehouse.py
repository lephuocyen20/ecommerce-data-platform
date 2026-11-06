import argparse,sys
from pyspark.sql import SparkSession
sys.path[:0]=["/opt/spark/work-dir","/opt/spark/work-dir/warehouse/loader"]
from common.minio import configure_s3a
from lakehouse.warehouse_order_items_reader import read_order_items_snapshot
from incremental_silver_to_warehouse import get_committed_batches,get_connection
from warehouse_batch_loader import load_iceberg_order_items_batch
p=argparse.ArgumentParser();p.add_argument("--batch-id",required=True);a=p.parse_args();s=SparkSession.builder.appName("IcebergOrderItemsWarehouse").getOrCreate();c=None
try:
 configure_s3a(s);c=get_connection();c.autocommit=False;b=get_committed_batches(c,selected_tables=["order_items"],selected_batch_ids=[a.batch_id]);
 if len(b)!=1: raise RuntimeError("Expected one pending order_items batch")
 print(load_iceberg_order_items_batch(c,b[0],read_order_items_snapshot(s)));c.commit()
except Exception:
 if c:c.rollback()
 raise
finally:
 if c:c.close()
 s.stop()
