
import unittest
from unittest.mock import patch

from gold.implementation.contracts.datasets import (
    GOLD_DATASETS,
    get_gold_contract,
)
from gold.implementation.contracts.validator import (
    validate_column_type,
    validate_gold_contract,
    validate_all_gold_contracts,
)


class TestGoldContracts(unittest.TestCase):

    def test_registered_contracts(self):
        self.assertEqual(len(GOLD_DATASETS), 4)

    def test_all_contracts_valid(self):
        result = validate_all_gold_contracts()

        self.assertEqual(result["validated_count"], 4)
        self.assertEqual(result["status"], "PASS")

    def test_unsupported_dataset(self):
        with self.assertRaises(ValueError):
            get_gold_contract("unknown_dataset")

    def test_valid_column_types(self):
        for column_type in (
            "date",
            "long",
            "string",
            "decimal(20,2)",
            "decimal(38,10)",
        ):
            with self.subTest(column_type=column_type):
                self.assertTrue(
                    validate_column_type(column_type)
                )

    def test_invalid_column_types(self):
        for column_type in (
            "float",
            "decimal(39,2)",
            "decimal(10,11)",
            "decimal(0,0)",
            "decimal(10,-1)",
            None,
        ):
            with self.subTest(column_type=column_type):
                with self.assertRaises(ValueError):
                    validate_column_type(column_type)

    def test_missing_primary_key_column(self):
        invalid = {
            "grain": ["missing_id"],
            "sources": ["orders"],
            "primary_key": ["missing_id"],
            "columns": {
                "order_count": "long",
            },
        }

        with patch(
            "gold.implementation.contracts.validator.get_gold_contract",
            return_value=invalid,
        ):
            with self.assertRaises(ValueError):
                validate_gold_contract("test_dataset")

    def test_duplicate_sources(self):
        invalid = {
            "grain": ["order_date"],
            "sources": ["orders", "orders"],
            "primary_key": ["order_date"],
            "columns": {
                "order_date": "date",
            },
        }

        with patch(
            "gold.implementation.contracts.validator.get_gold_contract",
            return_value=invalid,
        ):
            with self.assertRaises(ValueError):
                validate_gold_contract("test_dataset")

    def test_grain_primary_key_mismatch(self):
        invalid = {
            "grain": ["order_date"],
            "sources": ["orders"],
            "primary_key": ["order_count"],
            "columns": {
                "order_date": "date",
                "order_count": "long",
            },
        }

        with patch(
            "gold.implementation.contracts.validator.get_gold_contract",
            return_value=invalid,
        ):
            with self.assertRaises(ValueError):
                validate_gold_contract("test_dataset")

    def test_unsupported_source(self):
        invalid = {
            "grain": ["order_date"],
            "sources": ["unknown_table"],
            "primary_key": ["order_date"],
            "columns": {
                "order_date": "date",
            },
        }

        with patch(
            "gold.implementation.contracts.validator.get_gold_contract",
            return_value=invalid,
        ):
            with self.assertRaises(ValueError):
                validate_gold_contract("test_dataset")


if __name__ == "__main__":
    unittest.main(verbosity=2)
