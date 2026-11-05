import unittest
from datetime import datetime, date
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType,
    DecimalType,
)

from gold.implementation.transformations.customer_metrics import (
    build_customer_metrics,
)
from gold.implementation.quality.customer_metrics import (
    validate_customer_metrics_sources,
    validate_customer_metrics_output,
)
from gold.implementation.quality.daily_sales import (
    GoldDataQualityError,
)


CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", LongType(), False),
    StructField("full_name", StringType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", LongType(), False),
    StructField("customer_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("order_date", TimestampType(), True),
])

ITEM_SCHEMA = StructType([
    StructField("order_item_id", LongType(), False),
    StructField("order_id", LongType(), True),
    StructField("quantity", LongType(), True),
    StructField("unit_price", DecimalType(20, 2), True),
    StructField("discount_amount", DecimalType(20, 2), True),
])


def money(value):
    return Decimal(str(value))


class TestGoldCustomerMetrics(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("gold-customer-metrics-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def make_sources(self):
        customers = self.spark.createDataFrame(
            [
                (1, "Nguyen Van A"),
                (2, "Tran Thi B"),
                (3, "Le Van C"),
            ],
            CUSTOMER_SCHEMA,
        )

        orders = self.spark.createDataFrame(
            [
                (
                    101, 1, "DELIVERED",
                    datetime(2026, 1, 10, 8, 0),
                ),
                (
                    102, 1, "CANCELLED",
                    datetime(2026, 1, 15, 9, 0),
                ),
                (
                    103, 2, "PENDING",
                    datetime(2026, 2, 1, 10, 0),
                ),
            ],
            ORDER_SCHEMA,
        )

        items = self.spark.createDataFrame(
            [
                (1, 101, 2, money(100000), money(10000)),
                (2, 101, 1, money(50000), money(0)),
                (3, 102, 1, money(80000), money(5000)),
            ],
            ITEM_SCHEMA,
        )

        return customers, orders, items

    def test_valid_sources(self):
        customers, orders, items = self.make_sources()

        result = validate_customer_metrics_sources(
            customers, orders, items
        )

        self.assertEqual(result["status"], "PASS")

    def test_transformation_metrics(self):
        customers, orders, items = self.make_sources()

        result = build_customer_metrics(
            customers, orders, items
        )

        rows = {
            row["customer_id"]: row.asDict()
            for row in result.collect()
        }

        self.assertEqual(len(rows), 3)

        # Customer 1: delivered + cancelled
        customer_1 = rows[1]

        self.assertEqual(customer_1["order_count"], 2)
        self.assertEqual(customer_1["delivered_order_count"], 1)
        self.assertEqual(customer_1["cancelled_order_count"], 1)
        self.assertEqual(
            customer_1["total_order_value"],
            money("315000.00"),
        )
        self.assertEqual(
            customer_1["delivered_order_value"],
            money("240000.00"),
        )
        self.assertEqual(
            customer_1["first_order_date"],
            date(2026, 1, 10),
        )
        self.assertEqual(
            customer_1["last_order_date"],
            date(2026, 1, 15),
        )

        # Customer 2: order without items
        customer_2 = rows[2]

        self.assertEqual(customer_2["order_count"], 1)
        self.assertEqual(
            customer_2["total_order_value"],
            money("0.00"),
        )
        self.assertEqual(
            customer_2["first_order_date"],
            date(2026, 2, 1),
        )

        # Customer 3: no orders
        customer_3 = rows[3]

        self.assertEqual(customer_3["order_count"], 0)
        self.assertEqual(
            customer_3["total_order_value"],
            money("0.00"),
        )
        self.assertIsNone(customer_3["first_order_date"])
        self.assertIsNone(customer_3["last_order_date"])

    def test_output_quality(self):
        customers, orders, items = self.make_sources()

        result = build_customer_metrics(
            customers, orders, items
        )

        validation = validate_customer_metrics_output(result)

        self.assertEqual(validation["status"], "PASS")

    def test_orphan_customer_rejected(self):
        customers, orders, items = self.make_sources()

        invalid_orders = self.spark.createDataFrame(
            [
                (
                    999, 999, "DELIVERED",
                    datetime(2026, 1, 10, 8, 0),
                ),
            ],
            ORDER_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_customer_metrics_sources(
                customers, invalid_orders, items
            )

    def test_invalid_discount_rejected(self):
        customers, orders, items = self.make_sources()

        invalid_items = self.spark.createDataFrame(
            [
                (
                    999, 101, 1,
                    money(10000),
                    money(20000),
                ),
            ],
            ITEM_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_customer_metrics_sources(
                customers, orders, invalid_items
            )

    def test_duplicate_customer_rejected(self):
        customers, orders, items = self.make_sources()

        duplicate_customers = self.spark.createDataFrame(
            [
                (1, "Nguyen Van A"),
                (1, "Nguyen Van B"),
            ],
            CUSTOMER_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_customer_metrics_sources(
                duplicate_customers, orders, items
            )

    def test_invalid_order_status_rejected(self):
        customers, orders, items = self.make_sources()

        invalid_orders = self.spark.createDataFrame(
            [
                (
                    999, 1, "UNKNOWN",
                    datetime(2026, 1, 10, 8, 0),
                ),
            ],
            ORDER_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_customer_metrics_sources(
                customers, invalid_orders, items
            )

    def test_null_order_timestamp_rejected(self):
        customers, orders, items = self.make_sources()

        invalid_orders = self.spark.createDataFrame(
            [
                (999, 1, "DELIVERED", None),
            ],
            ORDER_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_customer_metrics_sources(
                customers, invalid_orders, items
            )

    def test_orphan_order_item_rejected(self):
        customers, orders, items = self.make_sources()

        invalid_items = self.spark.createDataFrame(
            [
                (
                    999, 999, 1,
                    money(10000),
                    money(0),
                ),
            ],
            ITEM_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_customer_metrics_sources(
                customers, orders, invalid_items
            )

    def test_empty_order_items(self):
        customers, orders, _ = self.make_sources()

        empty_items = self.spark.createDataFrame(
            [],
            ITEM_SCHEMA,
        )

        validate_customer_metrics_sources(
            customers, orders, empty_items
        )

        result = build_customer_metrics(
            customers, orders, empty_items
        )

        rows = {
            row["customer_id"]: row.asDict()
            for row in result.collect()
        }

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[1]["order_count"], 2)
        self.assertEqual(
            rows[1]["total_order_value"],
            money("0.00"),
        )
        self.assertEqual(rows[2]["order_count"], 1)
        self.assertEqual(rows[3]["order_count"], 0)

        validation = validate_customer_metrics_output(result)
        self.assertEqual(validation["status"], "PASS")

    def test_empty_orders(self):
        customers, _, _ = self.make_sources()

        empty_orders = self.spark.createDataFrame(
            [],
            ORDER_SCHEMA,
        )

        empty_items = self.spark.createDataFrame(
            [],
            ITEM_SCHEMA,
        )

        validate_customer_metrics_sources(
            customers, empty_orders, empty_items
        )

        result = build_customer_metrics(
            customers, empty_orders, empty_items
        )

        rows = result.collect()

        self.assertEqual(len(rows), 3)

        for row in rows:
            self.assertEqual(row["order_count"], 0)
            self.assertEqual(
                row["total_order_value"],
                money("0.00"),
            )
            self.assertIsNone(row["first_order_date"])
            self.assertIsNone(row["last_order_date"])

        validation = validate_customer_metrics_output(result)
        self.assertEqual(validation["status"], "PASS")


if __name__ == "__main__":
    unittest.main()