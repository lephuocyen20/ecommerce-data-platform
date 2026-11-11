from pyspark.sql import SparkSession
import sys

sys.path.insert(0, "/opt/spark/work-dir")

import gold.implementation.dimensional.writer as writer
from gold.implementation.dimensional.transformations import build_dim_date
from gold.implementation.dimensional.contracts import WRITE_ORDER
from tests.run_bounded_gold_parquet_fixture import _typed_fixture_frame


spark = (
    SparkSession.builder.appName("GoldWriterLocalDiagnostic")
    .config("spark.sql.catalog.debug", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.debug.type", "hadoop")
    .config("spark.sql.catalog.debug.warehouse", "file:///tmp/gold-writer-debug-py")
    .getOrCreate()
)
writer.DIMENSIONAL_CATALOG = "debug"
try:
    sources = {table: _typed_fixture_frame(spark, table) for table in ("customers", "products", "orders", "order_items", "payments", "shipments")}
    frames, quality = writer.build_graph_frames(
        spark, sources, dim_date_start="2026-01-01", dim_date_end="2026-01-07"
    )
    print("quality=" + repr(quality))
    for table in WRITE_ORDER:
        print("table=" + table + " schema=" + frames[table].schema.simpleString())
        writer.create_target_table(spark, table)
        print("created=" + table)
        writer.merge_table(spark, table, frames[table])
        print("merged=" + table)
finally:
    spark.stop()
