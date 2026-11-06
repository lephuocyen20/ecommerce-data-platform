"""Create and query a disposable Iceberg table through the Nessie REST catalog.

Run inside the Spark container after `nessie`, `minio`, and `spark` are ready:

    /opt/spark/bin/spark-submit /opt/spark/work-dir/lakehouse/smoke_test.py
"""

import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a


CATALOG = "lakehouse"
NAMESPACE = "validation_v2"
TABLE = f"{CATALOG}.{NAMESPACE}.iceberg_smoke"


def main():
    spark = SparkSession.builder.appName("IcebergNessieSmokeTest").getOrCreate()
    try:
        configure_s3a(spark)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        spark.sql(f"CREATE TABLE {TABLE} (id BIGINT, value STRING) USING iceberg")
        spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'nessie'), (2, 'iceberg')")
        rows = spark.sql(f"SELECT id, value FROM {TABLE} ORDER BY id").collect()
        actual = [(row.id, row.value) for row in rows]
        expected = [(1, "nessie"), (2, "iceberg")]
        if actual != expected:
            raise RuntimeError(f"Iceberg smoke test failed: {actual!r}")
        print("ICEBERG_NESSIE_SMOKE_TEST: PASS")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
