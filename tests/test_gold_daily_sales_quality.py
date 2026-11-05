
import unittest
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType,
)

from gold.implementation.quality.daily_sales import (
    GoldDataQualityError,
    validate_daily_sales_sources,
    validate_daily_sales_output,
)
from gold.implementation.transformations.daily_sales import (
    build_daily_sales,
)



class TestGoldDailySalesQuality(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("GoldDailySalesQualityTests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        cls.orders_schema = StructType([
            StructField("order_id", LongType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("status", StringType(), True),
        ])

        cls.items_schema = StructType([
            StructField("order_item_id", LongType(), True),
            StructField("order_id", LongType(), True),
            StructField("quantity", IntegerType(), True),
            StructField(
                "unit_price", DecimalType(12, 2), True
            ),
            StructField(
                "discount_amount", DecimalType(12, 2), True
            ),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def valid_data(self):
        orders = [
            (1, datetime(2026, 10, 1), "DELIVERED"),
            (2, datetime(2026, 10, 1), "CANCELLED"),
            (3, datetime(2026, 10, 2), "PENDING"),
        ]

        items = [
            (101, 1, 2, Decimal("100.00"), Decimal("10.00")),
            (102, 1, 1, Decimal("50.00"), Decimal("0.00")),
            (103, 2, 1, Decimal("80.00"), Decimal("5.00")),
        ]

        return orders, items

    def make_frames(self, orders=None, items=None):
        valid_orders, valid_items = self.valid_data()

        return (
            self.spark.createDataFrame(
                valid_orders if orders is None else orders,
                self.orders_schema,
            ),
            self.spark.createDataFrame(
                valid_items if items is None else items,
                self.items_schema,
            ),
        )

    def assert_source_rejected(
        self, orders=None, items=None
    ):
        orders_df, items_df = self.make_frames(
            orders=orders,
            items=items,
        )

        with self.assertRaises(GoldDataQualityError):
            validate_daily_sales_sources(
                orders_df,
                items_df,
            )

    def test_valid_sources(self):
        orders_df, items_df = self.make_frames()

        result = validate_daily_sales_sources(
            orders_df,
            items_df,
        )

        self.assertEqual(result["status"], "PASS")
        self.assertEqual(len(result["checks"]), 6)

    def test_duplicate_order_id(self):
        orders, _ = self.valid_data()
        self.assert_source_rejected(
            orders=orders + [orders[0]]
        )

    def test_null_order_id(self):
        orders, _ = self.valid_data()
        orders[0] = (
            None,
            datetime(2026, 10, 1),
            "DELIVERED",
        )
        self.assert_source_rejected(orders=orders)

    def test_duplicate_order_item_id(self):
        _, items = self.valid_data()
        items[1] = (101, 1, 1, Decimal("50"), Decimal("0"))
        self.assert_source_rejected(items=items)

    def test_orphan_order_item(self):
        _, items = self.valid_data()
        items[0] = (
            101, 999, 2,
            Decimal("100"), Decimal("10")
        )
        self.assert_source_rejected(items=items)

    def test_null_item_order_id(self):
        _, items = self.valid_data()
        items[0] = (
            101, None, 2,
            Decimal("100"), Decimal("10")
        )
        self.assert_source_rejected(items=items)

    def test_zero_quantity(self):
        _, items = self.valid_data()
        items[0] = (
            101, 1, 0,
            Decimal("100"), Decimal("10")
        )
        self.assert_source_rejected(items=items)

    def test_negative_quantity(self):
        _, items = self.valid_data()
        items[0] = (
            101, 1, -1,
            Decimal("100"), Decimal("10")
        )
        self.assert_source_rejected(items=items)

    def test_negative_price(self):
        _, items = self.valid_data()
        items[0] = (
            101, 1, 2,
            Decimal("-100"), Decimal("10")
        )
        self.assert_source_rejected(items=items)

    def test_negative_discount(self):
        _, items = self.valid_data()
        items[0] = (
            101, 1, 2,
            Decimal("100"), Decimal("-10")
        )
        self.assert_source_rejected(items=items)

    def test_discount_exceeds_gross(self):
        _, items = self.valid_data()
        items[0] = (
            101, 1, 2,
            Decimal("100"), Decimal("250")
        )
        self.assert_source_rejected(items=items)

    def test_null_order_date(self):
        orders, _ = self.valid_data()
        orders[0] = (1, None, "DELIVERED")
        self.assert_source_rejected(orders=orders)

    def test_invalid_order_status(self):
        orders, _ = self.valid_data()
        orders[0] = (
            1,
            datetime(2026, 10, 1),
            "UNKNOWN",
        )
        self.assert_source_rejected(orders=orders)

    def build_valid_gold(self):
        orders_df, items_df = self.make_frames()
        return build_daily_sales(
            orders_df,
            items_df,
        )

    def test_valid_gold_output(self):
        result = validate_daily_sales_output(
            self.build_valid_gold()
        )

        self.assertEqual(result["status"], "PASS")
        self.assertEqual(len(result["checks"]), 4)

    def test_duplicate_gold_grain(self):
        gold = self.build_valid_gold()

        duplicate = gold.unionByName(
            gold.limit(1)
        )

        with self.assertRaises(GoldDataQualityError):
            validate_daily_sales_output(duplicate)

    def test_null_gold_metric(self):
        gold = self.build_valid_gold().withColumn(
            "net_order_value",
            F.lit(None).cast("decimal(20,2)"),
        )

        with self.assertRaises(GoldDataQualityError):
            validate_daily_sales_output(gold)

    def test_invalid_gold_net_value(self):
        gold = self.build_valid_gold().withColumn(
            "net_order_value",
            F.lit(-1).cast("decimal(20,2)"),
        )

        with self.assertRaises(GoldDataQualityError):
            validate_daily_sales_output(gold)

    def test_invalid_gold_schema(self):
        gold = self.build_valid_gold().withColumn(
            "order_count",
            F.col("order_count").cast("integer"),
        )

        with self.assertRaises(GoldDataQualityError):
            validate_daily_sales_output(gold)


if __name__ == "__main__":
    unittest.main(verbosity=2)
