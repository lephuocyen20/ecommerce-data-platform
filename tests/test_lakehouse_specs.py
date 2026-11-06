import pytest

from lakehouse.models.spec import SPECS, get_spec
from lakehouse.warehouse.reader import BUSINESS_COLUMNS


@pytest.mark.unit
def test_specs_match_warehouse_contracts():
    assert set(SPECS) == set(BUSINESS_COLUMNS)
    for name, spec in SPECS.items():
        assert spec.columns == BUSINESS_COLUMNS[name]
        assert spec.key in spec.columns


@pytest.mark.unit
def test_financial_and_fk_rules_are_declarative():
    assert SPECS["order_items"].immutable
    assert SPECS["payments"].aggregate == "amount"
    assert SPECS["shipments"].aggregate == "shipping_fee"
    assert SPECS["payments"].foreign_keys == {"order_id": "orders"}
    assert get_spec("orders").foreign_keys == {"customer_id": "customers"}
