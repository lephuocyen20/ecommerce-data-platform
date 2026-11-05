
import unittest
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType,
    IntegerType,
    DecimalType,
)

from gold.implementation.transformations.daily_sales import (
    build_daily_sales,
)
from gold.implementation.contracts.datasets import (
    get_gold_contract,
)


class TestGoldDailySales(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("GoldDailySalesUnitTests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        cls.orders_schema = StructType([
            StructField("order_id", LongType(), False),
            StructField("order_date", TimestampType(), False),
            StructField("status", StringType(), False),
        ])

        cls.items_schema = StructType([
            StructField("order_id", LongType(), False),
            StructField("quantity", IntegerType(), False),
            StructField(
                "unit_price", DecimalType(12, 2), False
            ),
            StructField(
                "discount_amount", DecimalType(12, 2), False
            ),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def build_result(self):
        orders = [
            (1, datetime(2026, 10, 1), "DELIVERED"),
            (2, datetime(2026, 10, 1), "CANCELLED"),
            (3, datetime(2026, 10, 2), "PENDING"),
            (4, datetime(2026, 10, 2), "DELIVERED"),
        ]

        items = [
            (1, 2, Decimal("100.00"), Decimal("10.00")),
            (1, 1, Decimal("50.00"), Decimal("0.00")),
            (2, 1, Decimal("80.00"), Decimal("5.00")),
            (3, 3, Decimal("20.00"), Decimal("0.00")),
        ]

        orders_df = self.spark.createDataFrame(
            orders,
            schema=self.orders_schema,
        )

        items_df = self.spark.createDataFrame(
            items,
            schema=self.items_schema,
        )

        return build_daily_sales(
            orders_df,
            items_df,
        )

    def test_order_count_no_double_counting(self):
        rows = {
            str(row["order_date"]): row
            for row in self.build_result().collect()
        }

        self.assertEqual(
            rows["2026-10-01"]["order_count"], 2
        )
        self.assertEqual(
            rows["2026-10-02"]["order_count"], 2
        )

    def test_delivered_and_cancelled_counts(self):
        rows = {
            str(row["order_date"]): row
            for row in self.build_result().collect()
        }

        self.assertEqual(
            rows["2026-10-01"]["delivered_order_count"], 1
        )
        self.assertEqual(
            rows["2026-10-01"]["cancelled_order_count"], 1
        )
        self.assertEqual(
            rows["2026-10-02"]["delivered_order_count"], 1
        )

    def test_gross_discount_and_net(self):
        rows = {
            str(row["order_date"]): row
            for row in self.build_result().collect()
        }

        day1 = rows["2026-10-01"]

        self.assertEqual(
            day1["gross_merchandise_value"],
            Decimal("330.00"),
        )
        self.assertEqual(
            day1["discount_amount"],
            Decimal("15.00"),
        )
        self.assertEqual(
            day1["net_order_value"],
            Decimal("315.00"),
        )

    def test_cancelled_included_in_net_value(self):
        rows = {
            str(row["order_date"]): row
            for row in self.build_result().collect()
        }

        self.assertEqual(
            rows["2026-10-01"]["net_order_value"],
            Decimal("315.00"),
        )

    def test_delivered_order_value(self):
        rows = {
            str(row["order_date"]): row
            for row in self.build_result().collect()
        }

        self.assertEqual(
            rows["2026-10-01"]["delivered_order_value"],
            Decimal("240.00"),
        )
        self.assertEqual(
            rows["2026-10-02"]["delivered_order_value"],
            Decimal("0.00"),
        )

    def test_order_without_items(self):
        rows = {
            str(row["order_date"]): row
            for row in self.build_result().collect()
        }

        self.assertEqual(
            rows["2026-10-02"]["order_count"], 2
        )
        self.assertEqual(
            rows["2026-10-02"]["net_order_value"],
            Decimal("60.00"),
        )


    def test_gold_contract_schema(self):
        result = self.build_result()
        contract = get_gold_contract("daily_sales")

        self.assertEqual(
            result.columns,
            list(contract["columns"]),
        )

        spark_type_aliases = {
            "bigint": "long",
        }

        actual_types = {
            field.name: spark_type_aliases.get(
                field.dataType.simpleString(),
                field.dataType.simpleString(),
            )
            for field in result.schema.fields
        }

        self.assertEqual(
            actual_types,
            contract["columns"],
        )


    def test_unique_grain(self):
        result = self.build_result()

        total_rows = result.count()

        distinct_dates = (
            result.select("order_date")
            .distinct()
            .count()
        )

        self.assertEqual(total_rows, distinct_dates)


if __name__ == "__main__":
    unittest.main(verbosity=2)
