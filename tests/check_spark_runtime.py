import sys

from pyspark.sql import SparkSession

print("PYTHON EXECUTABLE:", sys.executable)
print("PYSPARK IMPORT: PASS")

spark = (
    SparkSession.builder
    .appName("SnapshotE2ERuntimeCheck")
    .getOrCreate()
)

print("SPARK VERSION:", spark.version)
print("SPARK RUNTIME: PASS")

spark.stop()