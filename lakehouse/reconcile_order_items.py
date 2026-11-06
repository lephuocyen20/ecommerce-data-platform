import os,sys,json
from minio import Minio
from pyspark.sql import SparkSession,functions as F
sys.path.insert(0,"/opt/spark/work-dir")
from common.minio import configure_s3a,get_minio_settings
from lakehouse.parquet_order_items import discover
COLS=["order_item_id","order_id","product_id","quantity","unit_price","discount_amount"]
s=SparkSession.builder.appName("ReconcileIcebergOrderItems").getOrCreate()
try:
 settings=get_minio_settings(); b=os.environ["BRONZE_BUCKET"]; c=Minio(settings["endpoint"],access_key=settings["access_key"],secret_key=settings["secret_key"],secure=False); manifests=discover(c,b)
 configure_s3a(s); frames=[s.read.parquet(f"s3a://{b}/{m['data_prefix']}") for _,m in manifests]; legacy=frames[0]
 for f in frames[1:]: legacy=legacy.unionByName(f)
 ice=s.table("lakehouse.silver.order_items"); h=lambda x:x.select("order_item_id",F.sha2(F.concat_ws("||",*[F.coalesce(F.col(c).cast("string"),F.lit("")) for c in COLS]),256).alias("hash")); diff=h(legacy).exceptAll(h(ice)).unionByName(h(ice).exceptAll(h(legacy)))
 orders=s.table("lakehouse.silver.orders").select("order_id").distinct(); products=s.table("lakehouse.silver.products").select("product_id").distinct(); result={"legacy_rows":legacy.count(),"iceberg_rows":ice.count(),"hash_mismatches":diff.count(),"missing_order_fks":ice.select("order_id").distinct().join(orders,"order_id","left_anti").count(),"missing_product_fks":ice.select("product_id").distinct().join(products,"product_id","left_anti").count(),"manifest_count":len(manifests)}
 if result["legacy_rows"]!=result["iceberg_rows"] or any(result[k] for k in ("hash_mismatches","missing_order_fks","missing_product_fks")): raise RuntimeError(f"order_items reconciliation failed: {result}")
 print(json.dumps(result,sort_keys=True))
finally: s.stop()
