from gold.implementation.dimensional.contracts import WRITE_ORDER
from gold.implementation.gold_table_registry import (
    GOLD_TABLES,
    RUN06_EXPECTED_ROW_COUNTS,
    RUN06_SCHEMA,
    RUN06_SNAPSHOT_IDS,
    RUN06_TABLES,
    require_exact_gold_tables,
)
from gold.implementation.local_endpoint import validate_local_http_endpoint


def test_registry_is_exact_and_compatible_with_dimensional_write_order():
    assert GOLD_TABLES == (
        "dim_date", "dim_customer", "dim_product", "fact_orders",
        "fact_order_items", "fact_payments", "fact_shipments",
    )
    assert RUN06_TABLES == GOLD_TABLES
    assert WRITE_ORDER == GOLD_TABLES
    assert set(RUN06_SNAPSHOT_IDS) == set(GOLD_TABLES)
    assert set(RUN06_EXPECTED_ROW_COUNTS) == set(GOLD_TABLES)
    assert RUN06_SCHEMA == "gold_run_20261112_gold_06"


def test_registry_rejects_missing_extra_or_duplicate_tables():
    assert require_exact_gold_tables(reversed(GOLD_TABLES)) == GOLD_TABLES
    for invalid in (GOLD_TABLES[:-1], GOLD_TABLES + ("unknown",), GOLD_TABLES[:-1] + ("fact_orders",)):
        try:
            require_exact_gold_tables(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid Gold table set was accepted")


def test_shared_local_endpoint_validator_keeps_private_boundary():
    assert validate_local_http_endpoint(
        "http://127.0.0.1:8182", name="rest", hosts={"127.0.0.1"}, ports={8182}
    ) == "http://127.0.0.1:8182"
    for value in ("https://127.0.0.1:8182", "http://example.com:8182", "http://127.0.0.1:8182/api"):
        try:
            validate_local_http_endpoint(value, name="rest", hosts={"127.0.0.1"}, ports={8182})
        except RuntimeError:
            pass
        else:
            raise AssertionError("unsafe endpoint was accepted")
