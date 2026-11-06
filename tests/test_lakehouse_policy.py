import pytest

from common.lakehouse_policy import customers_silver_source, require_iceberg_cutover_enabled


@pytest.mark.unit
def test_customers_source_defaults_to_parquet():
    assert customers_silver_source({}) == "parquet"
    assert require_iceberg_cutover_enabled({}) is False


@pytest.mark.unit
def test_iceberg_cutover_requires_second_explicit_flag():
    with pytest.raises(RuntimeError, match="cutover blocked"):
        require_iceberg_cutover_enabled({"CUSTOMERS_SILVER_SOURCE": "iceberg"})
    assert require_iceberg_cutover_enabled({"CUSTOMERS_SILVER_SOURCE": "iceberg", "CUSTOMERS_ICEBERG_CUTOVER_ENABLED": "true"}) is True


@pytest.mark.unit
def test_customers_source_rejects_unknown_value():
    with pytest.raises(ValueError, match="CUSTOMERS_SILVER_SOURCE"):
        customers_silver_source({"CUSTOMERS_SILVER_SOURCE": "duckdb"})
