"""Verify direct Hadoop S3A access to the local MinIO warehouse.

This intentionally does not initialize an Iceberg catalog.  It isolates the
object-store connector when investigating Lakehouse startup failures.
"""

import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a


def main():
    spark = SparkSession.builder.appName("S3ADiagnostic").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        configure_s3a(spark)
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI("s3a://ecommerce-data-lake/")
        filesystem = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        statuses = filesystem.listStatus(jvm.org.apache.hadoop.fs.Path(uri))
        print(f"S3A_DIAGNOSTIC: PASS ({len(statuses)} objects/directories listed)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
