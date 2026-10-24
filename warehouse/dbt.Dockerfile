FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core==1.11.6 \
    dbt-postgres==1.11.0

WORKDIR /app

ENTRYPOINT ["dbt"]