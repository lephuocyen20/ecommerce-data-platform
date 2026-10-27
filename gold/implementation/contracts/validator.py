
import re

from gold.implementation.contracts.datasets import (
    GOLD_DATASETS,
    get_gold_contract,
)
from ingestion.incremental.config import TABLE_CONFIG


SUPPORTED_TYPES = {
    "date",
    "long",
    "string",
}

DECIMAL_PATTERN = re.compile(
    r"decimal\((\d+),(\d+)\)"
)


def validate_column_type(column_type):
    if not isinstance(column_type, str):
        raise ValueError("Invalid column type")

    if column_type in SUPPORTED_TYPES:
        return True

    match = DECIMAL_PATTERN.fullmatch(column_type)

    if match is None:
        raise ValueError(
            f"Unsupported column type: {column_type}"
        )

    precision = int(match.group(1))
    scale = int(match.group(2))

    if not (1 <= precision <= 38):
        raise ValueError("Invalid decimal precision")

    if not (0 <= scale <= precision):
        raise ValueError("Invalid decimal scale")

    return True


def validate_gold_contract(dataset_name):
    """
    Validate Gold dataset contract structure.

    This function does not access databases,
    Spark or object storage.
    """

    contract = get_gold_contract(dataset_name)

    required_fields = {
        "grain",
        "sources",
        "primary_key",
        "columns",
    }

    if set(contract) != required_fields:
        raise ValueError(
            f"{dataset_name}: invalid contract fields"
        )

    columns = contract["columns"]
    grain = contract["grain"]
    primary_key = contract["primary_key"]
    sources = contract["sources"]

    if not isinstance(columns, dict) or not columns:
        raise ValueError(
            f"{dataset_name}: invalid columns"
        )

    if not isinstance(grain, list) or not grain:
        raise ValueError(
            f"{dataset_name}: invalid grain"
        )

    if not isinstance(primary_key, list) or not primary_key:
        raise ValueError(
            f"{dataset_name}: invalid primary key"
        )

    if not isinstance(sources, list) or not sources:
        raise ValueError(
            f"{dataset_name}: invalid sources"
        )

    for field_name, values in (
        ("grain", grain),
        ("primary_key", primary_key),
        ("sources", sources),
    ):
        if any(
            not isinstance(value, str) or not value.strip()
            for value in values
        ):
            raise ValueError(
                f"{dataset_name}: invalid {field_name} value"
            )

        if len(values) != len(set(values)):
            raise ValueError(
                f"{dataset_name}: duplicate {field_name}"
            )

    for column_name, column_type in columns.items():
        if (
            not isinstance(column_name, str)
            or not column_name.strip()
        ):
            raise ValueError(
                f"{dataset_name}: invalid column name"
            )

        validate_column_type(column_type)

    for key in primary_key:
        if key not in columns:
            raise ValueError(
                f"{dataset_name}: missing primary key column {key}"
            )

    for key in grain:
        if key not in columns:
            raise ValueError(
                f"{dataset_name}: missing grain column {key}"
            )

    if set(grain) != set(primary_key):
        raise ValueError(
            f"{dataset_name}: grain and primary key mismatch"
        )

    for source in sources:
        if source not in TABLE_CONFIG:
            raise ValueError(
                f"{dataset_name}: unsupported source {source}"
            )

    return True


def validate_all_gold_contracts():
    """
    Validate all registered Gold contracts.
    """

    for dataset_name in GOLD_DATASETS:
        validate_gold_contract(dataset_name)

    return {
        "validated_count": len(GOLD_DATASETS),
        "datasets": list(GOLD_DATASETS),
        "status": "PASS",
    }
