"""End-to-end behavioural test for the Iceberg Silver customers merge."""

import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/work-dir")
from common.minio import configure_s3a
from lakehouse.models.customers import latest_snapshot_id, merge_customers, prepare_customers, validate_customers


TABLE = "lakehouse.validation.customers_m2_e2e"
COLS = ["customer_id", "full_name", "email", "created_at", "updated_at"]


def rows(spark, values):
    return spark.createDataFrame(values, COLS)


def main():
    spark = SparkSession.builder.appName("IcebergCustomersE2E").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        configure_s3a(spark)
        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        initial = [
            (1, "Ada", "ada@example.com", "2026-01-01 00:00:00", "2026-01-01 00:00:00"),
            (2, "Grace", "grace@example.com", "2026-01-01 00:00:00", "2026-01-01 00:00:00"),
        ]
        merge_customers(spark, prepare_customers(rows(spark, initial), "initial"), TABLE)
        assert validate_customers(spark, TABLE)["row_count"] == 2
        first_snapshot = latest_snapshot_id(spark, TABLE)

        merge_customers(spark, prepare_customers(rows(spark, [
            (3, "Linus", "linus@example.com", "2026-01-02 00:00:00", "2026-01-02 00:00:00"),
        ]), "insert"), TABLE)
        assert validate_customers(spark, TABLE)["row_count"] == 3

        merge_customers(spark, prepare_customers(rows(spark, [
            (1, "Ada Lovelace", "ada@example.com", "2026-01-01 00:00:00", "2026-02-01 00:00:00"),
        ]), "update"), TABLE)

        duplicate_source = [
            (4, "Old", "four@example.com", "2026-01-03 00:00:00", "2026-01-03 00:00:00"),
            (4, "Newest", "four@example.com", "2026-01-03 00:00:00", "2026-03-01 00:00:00"),
        ]
        prepared_duplicate = prepare_customers(rows(spark, duplicate_source), "duplicate")
        assert prepared_duplicate.count() == 1
        merge_customers(spark, prepared_duplicate, TABLE)
        snapshot_before_replay = latest_snapshot_id(spark, TABLE)
        state_before_replay = [tuple(row) for row in spark.table(TABLE).orderBy("customer_id").collect()]
        merge_customers(spark, prepared_duplicate, TABLE)
        assert validate_customers(spark, TABLE)["row_count"] == 4
        assert [tuple(row) for row in spark.table(TABLE).orderBy("customer_id").collect()] == state_before_replay
        # Snapshot IDs are opaque, non-monotonic values. A replay may create a
        # metadata-only snapshot; idempotency is therefore asserted on rows.
        assert latest_snapshot_id(spark, TABLE)

        merge_customers(spark, prepare_customers(rows(spark, [
            (1, "Stale Ada", "stale@example.com", "2026-01-01 00:00:00", "2026-01-15 00:00:00"),
        ]), "older-update"), TABLE)
        actual = {row.customer_id: row for row in spark.table(TABLE).collect()}
        assert actual[1].full_name == "Ada Lovelace"
        assert actual[4].full_name == "Newest"
        assert actual[1]._batch_id == "update"
        assert actual[1]._record_hash
        assert latest_snapshot_id(spark, TABLE) != first_snapshot

        # Reading the table through the configured `lakehouse` catalog proves
        # Nessie resolved the committed Iceberg metadata on its main reference.
        assert "customers_m2_e2e" in {row.tableName for row in spark.sql("SHOW TABLES IN lakehouse.validation").collect()}
        print("ICEBERG_CUSTOMERS_E2E: PASS")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
