import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
# The runtime always supplies this value.  A harmless default keeps pure
# transformation modules importable for unit tests and DAG parsing.
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "ecommerce-data-lake")
