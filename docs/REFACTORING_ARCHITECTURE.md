# Refactoring Architecture

Session A removes the obsolete analytical Warehouse/dbt layer while preserving PostgreSQL source ingestion and Airflow metadata. The resulting dependency direction is:

`source/PostgreSQL → ingestion/Bronze → verified Silver Parquet → Gold Iceberg`.

No database, object-storage bucket, checkpoint, or Compose volume is deleted by this refactor. Gold routing remains guarded and defaults to reading verified Parquet with `DRY_RUN` writes.
