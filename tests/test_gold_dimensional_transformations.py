"""Spark regression tests for the first dimensional Gold slice."""

import pytest


pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from gold.implementation.dimensional.reconciliation import reconcile_fact_orders
from gold.implementation.dimensional.transformations import (
    build_dim_customer,
    build_fact_orders,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gold-dimensional-transform-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_dim_customer_uses_latest_version_and_typed_unknown_member(spark):
    customers = spark.createDataFrame(
        [
            (1, "Old name", "old@example.com", "2026-01-01", "2026-01-02"),
            (1, "New name", "new@example.com", "2026-01-01", "2026-01-03"),
        ],
        "customer_id long, full_name string, email string, created_at string, updated_at string",
    )

    result = build_dim_customer(customers).orderBy("customer_key").collect()

    assert len(result) == 2
    assert result[0].customer_key == -1
    assert result[0].customer_id is None
    assert result[1].customer_key == 1
    assert result[1].full_name == "New name"


def test_fact_orders_aggregates_items_without_fanout_and_maps_unknown_customer(spark):
    orders = spark.createDataFrame(
        [(10, 1, "2026-02-01", "paid"), (11, 999, "2026-02-01", "new")],
        "order_id long, customer_id long, order_date string, status string",
    )
    items = spark.createDataFrame(
        [
            (100, 10, 2, 10.00, 1.50),
            (101, 10, 1, 5.00, 0.00),
        ],
        "order_item_id long, order_id long, quantity long, unit_price decimal(10,2), discount_amount decimal(10,2)",
    )
    dimensions = build_dim_customer(
        spark.createDataFrame(
            [(1, "Customer", "customer@example.com", "2026-01-01", "2026-01-01")],
            "customer_id long, full_name string, email string, created_at string, updated_at string",
        )
    )

    rows = {row.order_id: row for row in build_fact_orders(orders, items, dimensions).collect()}

    assert len(rows) == 2
    assert rows[10].customer_key == 1
    assert rows[10].line_count == 2
    assert rows[10].item_count == 3
    assert str(rows[10].gross_amount) == "25.00"
    assert str(rows[10].discount_amount) == "1.50"
    assert str(rows[10].net_amount) == "23.50"
    assert rows[11].customer_key == -1
    assert rows[11].line_count == 0


def test_fact_orders_parity_maps_customer_natural_key():
    legacy = [{
        "order_id": 10, "customer_id": 1, "order_date_key": 20260201,
        "order_status": "paid", "line_count": 2, "item_count": 3,
        "gross_amount": "25.00", "discount_amount": "1.50", "net_amount": "23.50",
    }]
    iceberg = [{
        "order_id": 10, "customer_natural_key": 1, "customer_key": 1,
        "order_date_key": 20260201, "order_status": "paid", "line_count": 2,
        "item_count": 3, "gross_amount": "25.0", "discount_amount": "1.500",
        "net_amount": "23.500",
    }]

    report = reconcile_fact_orders(legacy, iceberg)

    assert report["status"] == "PASS"
