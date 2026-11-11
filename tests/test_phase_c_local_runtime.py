import pytest
from pyspark.sql import SparkSession

from tests.run_phase_c_local_runtime import PhaseCLocalRuntimeError, run_phase_c_dry_run

KWARGS = {"bucket": "ecommerce-quality-it", "endpoint": "http://localhost:19130/api/v2", "ref": "main", "schema_before": "a" * 64, "schema_after": "a" * 64}


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[2]").appName("phase-c-local-runtime").getOrCreate()
    yield session
    session.stop()


def test_phase_c_local_runner_reports_late_correction_as_dry_run():
    result = run_phase_c_dry_run(**KWARGS)
    assert result["status"] == "DRY_RUN"
    assert result["late_correction"] == "OVERLAP_REPROCESS_PLANNED"
    assert result["can_write"] is False
    assert result["tombstone_policy"] == "TOMBSTONE_ONLY"


def test_phase_c_local_runner_rejects_schema_drift_and_physical_delete():
    with pytest.raises(PhaseCLocalRuntimeError, match="Schema drift"):
        run_phase_c_dry_run(**{**KWARGS, "schema_after": "b" * 64})
    with pytest.raises(PhaseCLocalRuntimeError, match="Tombstone-only"):
        run_phase_c_dry_run(**{**KWARGS, "physical_delete": True})


def test_phase_c_local_runner_accepts_compose_internal_nessie_service():
    result = run_phase_c_dry_run(**{**KWARGS, "endpoint": "http://nessie:19120/api/v2"})
    assert result["status"] == "DRY_RUN"


def test_phase_c_spark_runner_detects_late_correction_with_new_lineage(spark):
    from tests.run_phase_c_local_runtime import run_phase_c_spark_dry_run

    result = run_phase_c_spark_dry_run(
        spark, bucket="ecommerce-quality-it", endpoint="http://localhost:19130/api/v2", ref="main"
    )
    assert result["status"] == "DRY_RUN"
    assert result["late_correction"] == "DETECTED"
    assert result["before_lineage"] != result["after_lineage"]
    assert result["can_write"] is False


@pytest.mark.parametrize("field,value", [("bucket", "ecommerce-data-lake"), ("endpoint", "https://production-nessie:19120/api/v2"), ("ref", "production")])
def test_phase_c_local_runner_rejects_non_local_scope(field, value):
    with pytest.raises(PhaseCLocalRuntimeError):
        run_phase_c_dry_run(**{**KWARGS, field: value})
