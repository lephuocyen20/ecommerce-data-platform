"""Read-only operational summary for Lakehouse reconciliation and lineage."""
import json
import sys
from pyspark.sql import SparkSession
sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a
from lakehouse.models.spec import SPECS

spark = SparkSession.builder.appName("LakehouseAudit").getOrCreate()
try:
    configure_s3a(spark)
    result = {}
    for name, spec in SPECS.items():
        table = f"lakehouse.silver.{name}"
        snapshot = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
        result[name] = {"rows": spark.table(table).count(), "snapshot_id": int(snapshot.snapshot_id), "foreign_keys": spec.foreign_keys or {}}
    print(json.dumps(result, sort_keys=True))
finally:
    spark.stop()
