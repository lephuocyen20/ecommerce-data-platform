"""Non-destructive bounded Iceberg/Nessie smoke proof."""
import os
import sys
from pyspark.sql import SparkSession
sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a
from common.bounded_e2e_scope import build_bounded_e2e_scope

def main():
    scope = build_bounded_e2e_scope(os.environ)
    table = f"lakehouse.{scope.silver_namespace}.bounded_smoke"
    spark = (SparkSession.builder.appName("BoundedIcebergSmoke")
             .config("spark.sql.catalog.lakehouse.warehouse", scope.warehouse)
             .config("spark.sql.catalog.lakehouse.ref", scope.nessie_ref)
             .getOrCreate())
    try:
        configure_s3a(spark)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{scope.silver_namespace}")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (id BIGINT, value STRING) USING iceberg")
        spark.sql(f"INSERT INTO {table} VALUES (1, 'bounded')")
        rows = spark.sql(f"SELECT id, value FROM {table} ORDER BY id DESC LIMIT 1").collect()
        assert rows and rows[0].id == 1 and rows[0].value == "bounded"
        print(f"BOUNDED_ICEBERG_SMOKE: PASS table={table} warehouse={scope.warehouse} ref={scope.nessie_ref}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
