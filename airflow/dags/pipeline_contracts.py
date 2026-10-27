"""Data-only orchestration contracts used while parsing Airflow DAGs."""

import sys

AIRFLOW_PROJECT_PATH = "/opt/airflow/project"
if AIRFLOW_PROJECT_PATH not in sys.path:
    sys.path.insert(0, AIRFLOW_PROJECT_PATH)

from common.table_registry import SUPPORTED_TABLES
from gold.lineage import GOLD_DATASET_SOURCES

TABLES = SUPPORTED_TABLES
