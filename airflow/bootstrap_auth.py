"""Seed Airflow 3 SimpleAuth credentials from environment variables.

Airflow stores SimpleAuth passwords as plaintext JSON.  This script writes the
file to a persistent Docker volume before the API server starts, so a recreated
container keeps the credential configured in ``.env``.
"""

import json
import os
from pathlib import Path


username = os.environ["AIRFLOW_ADMIN_USERNAME"]
password = os.environ["AIRFLOW_ADMIN_PASSWORD"]
password_file = Path(os.environ["AIRFLOW_SIMPLE_AUTH_PASSWORDS_FILE"])

if not username.strip() or not password:
    raise RuntimeError("AIRFLOW_ADMIN_USERNAME and AIRFLOW_ADMIN_PASSWORD are required")

password_file.parent.mkdir(parents=True, exist_ok=True)
temporary_file = password_file.with_suffix(".tmp")
temporary_file.write_text(json.dumps({username: password}) + "\n", encoding="utf-8")
temporary_file.replace(password_file)
# Airflow services run as uid 50000.  SimpleAuth opens this file in ``a+``
# mode during initialization, so it must be writable as well as readable.
os.chown(password_file, 50000, 0)
os.chmod(password_file, 0o664)
