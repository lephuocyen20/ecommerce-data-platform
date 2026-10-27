"""Consistent Spark S3A and MinIO client setup for pipeline jobs."""

import os

from minio import Minio


def get_minio_settings():
    """Read the MinIO runtime settings required by Spark jobs."""
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    if not endpoint or not access_key or not secret_key:
        raise RuntimeError("MINIO_ENDPOINT, MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required")
    return {"endpoint": endpoint, "access_key": access_key, "secret_key": secret_key}


def create_minio_client():
    """Create the non-TLS MinIO client used by the local Docker platform."""
    settings = get_minio_settings()
    return Minio(settings["endpoint"], access_key=settings["access_key"], secret_key=settings["secret_key"], secure=False)


def configure_s3a(spark):
    """Configure Spark's Hadoop S3A connector for the configured MinIO server."""
    settings = get_minio_settings()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.s3a.endpoint", f"http://{settings['endpoint']}")
    conf.set("fs.s3a.access.key", settings["access_key"])
    conf.set("fs.s3a.secret.key", settings["secret_key"])
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
