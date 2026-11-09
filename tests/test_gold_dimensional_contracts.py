from gold.implementation.dimensional.contracts import DIMENSIONS, FACTS, UNKNOWN_KEY

def test_dimensional_contracts_declare_grain_and_sources():
    assert UNKNOWN_KEY == -1
    assert DIMENSIONS["dim_customer"]["type"] == "type_1"
    assert FACTS["fact_orders"]["grain"] == ("order_id",)
    assert set(FACTS["fact_orders"]["sources"]) == {"orders", "order_items", "customers"}
    assert FACTS["fact_orders"]["foreign_keys"] == {"customer_key": "dim_customer"}
