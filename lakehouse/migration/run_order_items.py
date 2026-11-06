import argparse,os,sys
from pyspark.sql import SparkSession
sys.path.insert(0,"/opt/spark/work-dir")
from common.minio import configure_s3a,create_minio_client
from lakehouse.migration.parquet_order_items import discover,migrate
p=argparse.ArgumentParser(); p.add_argument("--backfill",action="store_true"); p.add_argument("--manifest-key"); a=p.parse_args()
if os.getenv("LAKEHOUSE_WRITE_ENABLED","false").lower()!="true": raise RuntimeError("Lakehouse write blocked")
s=SparkSession.builder.appName("IcebergOrderItems").getOrCreate()
try:
 configure_s3a(s); c=create_minio_client(); b=os.environ["BRONZE_BUCKET"]; print([migrate(s,c,b,k,m) for k,m in discover(c,b)] if a.backfill else migrate(s,c,b,a.manifest_key))
finally: s.stop()
