"""Verified Silver Parquet to Iceberg order-items migration."""
import hashlib, json
from common.object_integrity import discover_parquet_objects
from lakehouse.models.order_items import TABLE, merge_order_items, prepare_order_items
PREFIX="silver_incremental/order_items/"; PROGRESS_TABLE="lakehouse.metadata.order_items_parquet_migration_progress"
def identity(m): return hashlib.sha256(json.dumps(m,sort_keys=True,separators=(",",":")).encode()).hexdigest()
def validate(key,m):
    bid,prefix,rows,objects=m.get("batch_id"),m.get("data_prefix"),m.get("silver_row_count"),m.get("parquet_objects")
    if not isinstance(m,dict) or m.get("status")!="COMPLETED" or m.get("table_name")!="order_items" or not isinstance(bid,str) or len(bid)!=32 or key!=f"{PREFIX}batch_id={bid}/_BATCH_METADATA.json" or not isinstance(prefix,str) or not prefix.startswith(f"{PREFIX}batch_id={bid}/runs/") or type(rows) is not int or rows<0 or not isinstance(objects,list) or not objects: raise ValueError("Invalid order_items manifest")
    if any(not isinstance(x,dict) or not isinstance(x.get("object_key"),str) or not x["object_key"].startswith(prefix) or type(x.get("size")) is not int or x["size"]<=0 or not isinstance(x.get("etag"),str) or not x["etag"] for x in objects): raise ValueError("Invalid order_items parquet inventory")
    return bid,prefix
def read(client,bucket,key):
    r=client.get_object(bucket,key)
    try: m=json.loads(r.read().decode())
    finally: r.close(); r.release_conn()
    validate(key,m); return m
def discover(client,bucket): return sorted([(o.object_name,read(client,bucket,o.object_name)) for o in client.list_objects(bucket,prefix=PREFIX,recursive=True) if o.object_name.endswith('/_BATCH_METADATA.json')],key=lambda x:(x[1].get("completed_at",""),x[1]["batch_id"]))
def migrate(spark,client,bucket,key,m=None):
    m=read(client,bucket,key) if m is None else m; bid,prefix=validate(key,m); spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.metadata"); spark.sql(f"CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} (batch_id STRING, manifest_identity STRING, snapshot_id BIGINT, completed_at TIMESTAMP) USING iceberg")
    if discover_parquet_objects(client,bucket,prefix,include_etag=True)!=sorted(m["parquet_objects"],key=lambda x:x["object_key"]): raise ValueError("Order-items inventory mismatch")
    digest=identity(m); old=spark.sql(f"SELECT manifest_identity,snapshot_id FROM {PROGRESS_TABLE} WHERE batch_id='{bid}'").collect()
    if old:
        if old[0].manifest_identity!=digest: raise RuntimeError("Conflicting order-items manifest")
        return {"status":"ALREADY_MIGRATED","batch_id":bid}
    src=spark.read.parquet(f"s3a://{bucket}/{prefix}")
    if src.count()!=m["silver_row_count"]: raise ValueError("Order-items row count mismatch")
    merge_order_items(spark,prepare_order_items(src,bid)); snap=int(spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1").first().snapshot_id); spark.sql(f"INSERT INTO {PROGRESS_TABLE} VALUES ('{bid}','{digest}',{snap},current_timestamp())"); return {"status":"MIGRATED","batch_id":bid,"snapshot_id":snap}
