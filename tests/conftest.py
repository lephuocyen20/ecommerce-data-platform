"""Automatic test-suite classification for selective local and CI runs."""

from pathlib import Path

import pytest


def pytest_collection_modifyitems(items):
    """Classify tests by filename without duplicating decorators in every file."""
    for item in items:
        test_path = Path(str(item.fspath))
        name = test_path.name
        if name.endswith("_e2e.py"):
            item.add_marker(pytest.mark.e2e)
        elif (
            "SparkSession" in test_path.read_text(encoding="utf-8")
            or any(
            token in name
            for token in (
                "_integration.py",
                "_verifier_spark.py",
                "_writer_parquet.py",
                "_publisher_minio.py",
                "_s3a_connectivity.py",
            )
            )
        ):
            item.add_marker(pytest.mark.integration)
        else:
            item.add_marker(pytest.mark.unit)
