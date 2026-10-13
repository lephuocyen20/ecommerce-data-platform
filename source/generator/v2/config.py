import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent.parent / ".env.test"


def load_config():
    """Load database configuration for the test environment only."""
    if not ENV_PATH.is_file():
        raise FileNotFoundError(f"Missing config: {ENV_PATH}")

    load_dotenv(ENV_PATH, override=True)
    if os.getenv("DB_NAME") != "ecommerce_test":
        raise RuntimeError("Safety check failed: Generator V2 only supports ecommerce_test")


def get_db_connection():
    """Create a PostgreSQL connection using the test configuration."""
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=5,
    )

