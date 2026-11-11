"""End-to-end validation for the local Iceberg/Nessie/MinIO integration."""

import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.e2e
def test_iceberg_nessie_smoke_test():
    """Create, write, and read a disposable Iceberg table through Nessie."""
    smoke_test = Path("/opt/spark/work-dir/lakehouse/diagnostics/smoke_test.py")
    if not smoke_test.exists():
        pytest.skip("Lakehouse E2E test must run inside the configured Spark container")

    result = subprocess.run(
        ["/opt/spark/bin/spark-submit", str(smoke_test)],
        capture_output=True,
        text=True,
        timeout=int(os.getenv("LAKEHOUSE_SMOKE_TIMEOUT_SECONDS", "90")),
        check=False,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    assert "ICEBERG_NESSIE_SMOKE_TEST: PASS" in result.stdout


@pytest.mark.e2e
def test_iceberg_customers_incremental_merge():
    """Cover inserts, newer updates, duplicates, replay, and stale updates."""
    scenario = Path("/opt/spark/work-dir/lakehouse/customers_e2e.py")
    if not scenario.exists():
        pytest.skip("Lakehouse E2E test must run inside the configured Spark container")
    result = subprocess.run(
        ["/opt/spark/bin/spark-submit", str(scenario)],
        capture_output=True,
        text=True,
        timeout=int(os.getenv("LAKEHOUSE_CUSTOMERS_E2E_TIMEOUT_SECONDS", "180")),
        check=False,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    assert "ICEBERG_CUSTOMERS_E2E: PASS" in result.stdout


@pytest.mark.e2e
def test_iceberg_customers_manifest_migration():
    """Exercise fresh Parquet, manifest integrity, migration progress and replay."""
    scenario = Path("/opt/spark/work-dir/lakehouse/customers_manifest_e2e.py")
    if not scenario.exists():
        pytest.skip("Lakehouse E2E test must run inside the configured Spark container")
    result = subprocess.run(
        ["/opt/spark/bin/spark-submit", str(scenario)], capture_output=True, text=True,
        timeout=int(os.getenv("LAKEHOUSE_CUSTOMERS_E2E_TIMEOUT_SECONDS", "180")), check=False,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    assert "ICEBERG_CUSTOMERS_MANIFEST_E2E: PASS" in result.stdout
