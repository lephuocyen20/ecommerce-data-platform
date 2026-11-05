import unittest
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DecimalType,
)

from gold.implementation.transformations.product_performance import (
    build_product_performance,
)

from gold.implementation.quality.product_performance import (
    validate_product_performance_sources,
    validate_product_performance_output,
)

from gold.implementation.quality.daily_sales import (
    GoldDataQualityError,
)


PRODUCT_SCHEMA = StructType([
    StructField("product_id", LongType(), False),
    StructField("product_name", StringType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", LongType(), False),
    StructField("status", StringType(), True),
])

ITEM_SCHEMA = StructType([
    StructField("order_item_id", LongType(), False),
    StructField("order_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("quantity", LongType(), True),
    StructField("unit_price", DecimalType(20, 2), True),
    StructField("discount_amount", DecimalType(20, 2), True),
])


def money(value):
    return Decimal(str(value))


class TestGoldProductPerformance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("gold-product-performance-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def make_sources(self):
        products = self.spark.createDataFrame(
            [
                (1, "Keyboard"),
                (2, "Mouse"),
                (3, "Monitor"),
            ],
            PRODUCT_SCHEMA,
        )

        orders = self.spark.createDataFrame(
            [
                (101, "DELIVERED"),
                (102, "CANCELLED"),
            ],
            ORDER_SCHEMA,
        )

        items = self.spark.createDataFrame(
            [
                (1, 101, 1, 2, money(100000), money(10000)),
                (2, 101, 1, 1, money(100000), money(0)),
                (3, 102, 1, 3, money(100000), money(20000)),
                (4, 101, 2, 1, money(50000), money(0)),
            ],
            ITEM_SCHEMA,
        )

        return products, orders, items

    def test_valid_sources(self):
        products, orders, items = self.make_sources()

        result = validate_product_performance_sources(
            products, orders, items
        )

        self.assertEqual(result["status"], "PASS")

    def test_transformation_metrics(self):
        products, orders, items = self.make_sources()

        result = build_product_performance(
            products, orders, items
        )

        rows = {
            row["product_id"]: row.asDict()
            for row in result.collect()
        }

        self.assertEqual(len(rows), 3)

        keyboard = rows[1]

        self.assertEqual(keyboard["order_count"], 2)
        self.assertEqual(keyboard["units_ordered"], 6)
        self.assertEqual(
            keyboard["gross_merchandise_value"],
            money("600000.00"),
        )
        self.assertEqual(
            keyboard["discount_amount"],
            money("30000.00"),
        )
        self.assertEqual(
            keyboard["net_order_value"],
            money("570000.00"),
        )
        self.assertEqual(keyboard["delivered_units"], 3)
        self.assertEqual(
            keyboard["delivered_order_value"],
            money("290000.00"),
        )

        monitor = rows[3]

        self.assertEqual(monitor["order_count"], 0)
        self.assertEqual(monitor["units_ordered"], 0)
        self.assertEqual(
            monitor["net_order_value"],
            money("0.00"),
        )

    def test_output_quality(self):
        products, orders, items = self.make_sources()

        result = build_product_performance(
            products, orders, items
        )

        validation = validate_product_performance_output(result)

        self.assertEqual(validation["status"], "PASS")

    def test_orphan_product_rejected(self):
        products, orders, _ = self.make_sources()

        orphan_items = self.spark.createDataFrame(
            [
                (
                    99,
                    101,
                    999,
                    1,
                    money(100000),
                    money(0),
                ),
            ],
            ITEM_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_product_performance_sources(
                products,
                orders,
                orphan_items,
            )

    def test_invalid_discount_rejected(self):
        products, orders, _ = self.make_sources()

        invalid_items = self.spark.createDataFrame(
            [
                (
                    99,
                    101,
                    1,
                    1,
                    money(100000),
                    money(150000),
                ),
            ],
            ITEM_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_product_performance_sources(
                products,
                orders,
                invalid_items,
            )

    def test_duplicate_product_rejected(self):
        _, orders, items = self.make_sources()

        products = self.spark.createDataFrame(
            [
                (1, "Keyboard"),
                (1, "Duplicate Keyboard"),
            ],
            PRODUCT_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_product_performance_sources(
                products, orders, items
            )

    def test_orphan_order_rejected(self):
        products, orders, _ = self.make_sources()

        items = self.spark.createDataFrame(
            [
                (99, 999, 1, 1, money(100000), money(0)),
            ],
            ITEM_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_product_performance_sources(
                products, orders, items
            )

    def test_invalid_quantity_rejected(self):
        products, orders, _ = self.make_sources()

        items = self.spark.createDataFrame(
            [
                (99, 101, 1, 0, money(100000), money(0)),
            ],
            ITEM_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_product_performance_sources(
                products, orders, items
            )

    def test_invalid_order_status_rejected(self):
        products, _, items = self.make_sources()

        orders = self.spark.createDataFrame(
            [
                (101, "UNKNOWN"),
                (102, "CANCELLED"),
            ],
            ORDER_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_product_performance_sources(
                products, orders, items
            )

    def test_null_product_name_rejected(self):
        _, orders, items = self.make_sources()

        products = self.spark.createDataFrame(
            [
                (1, None),
                (2, "Mouse"),
            ],
            PRODUCT_SCHEMA,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_product_performance_sources(
                products, orders, items
            )

    def test_empty_order_items(self):
        products, orders, _ = self.make_sources()

        items = self.spark.createDataFrame(
            [],
            ITEM_SCHEMA,
        )

        validation = validate_product_performance_sources(
            products, orders, items
        )

        self.assertEqual(validation["status"], "PASS")

        result = build_product_performance(
            products, orders, items
        )

        self.assertEqual(result.count(), 3)

        self.assertEqual(
            validate_product_performance_output(result)["status"],
            "PASS",
        )

        for row in result.collect():
            self.assertEqual(row["order_count"], 0)
            self.assertEqual(row["units_ordered"], 0)
            self.assertEqual(row["net_order_value"], money("0.00"))
            self.assertEqual(row["delivered_order_value"], money("0.00"))


if __name__ == "__main__":
    unittest.main()