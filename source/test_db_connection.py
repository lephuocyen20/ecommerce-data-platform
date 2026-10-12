import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent / ".env.test"
load_dotenv(env_path, override=True)

# Safety check
if os.getenv("DB_NAME") != "ecommerce_test":
    raise RuntimeError("Safety check failed: DB_NAME must be ecommerce_test")

with psycopg2.connect(
    host=os.environ["DB_HOST"],
    port=os.environ["DB_PORT"],
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    connect_timeout=5,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database(), current_user")
        print("Connection SUCCESS:", cursor.fetchone())