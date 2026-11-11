# Refactoring Architecture

Session A removes the obsolete analytical Warehouse/dbt layer while preserving PostgreSQL source ingestion and Airflow metadata. The resulting dependency direction is:

`source/PostgreSQL → ingestion/Bronze → Silver → lakehouse/Iceberg → Gold`.

No database, object-storage bucket, checkpoint, or Compose volume is deleted by this refactor. Gold routing remains guarded and defaults to Parquet snapshots/DRY_RUN.
