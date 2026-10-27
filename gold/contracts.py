"""Public Gold contract API during the package migration."""

from gold.implementation.contracts.datasets import GOLD_DATASETS, get_gold_contract
from gold.implementation.contracts.datasets import GOLD_DATASET_SOURCES

__all__ = ["GOLD_DATASETS", "GOLD_DATASET_SOURCES", "get_gold_contract"]
