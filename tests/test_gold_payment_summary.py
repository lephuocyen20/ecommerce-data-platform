
"""Spark unit tests for Gold payment_summary."""

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

from gold.implementation.transformations.payment_summary import (
    build_payment_summary,
)
from gold.implementation.quality.payment_summary import (
    validate_payment_summary_sources,
    validate_payment_summary_output,
)
from gold.implementation.quality.daily_sales import GoldDataQualityError


PAYMENT_SCHEMA = StructType([
    StructField("payment_id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount", DecimalType(20, 2), True),
])


def payment_row(
    payment_id,
    order_id,
    method,
    status,
    amount,
):
    return (
        payment_id,
        order_id,
        method,
        status,
        Decimal(amount) if amount is not None else None,
    )


class TestGoldPaymentSummary(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("GoldPaymentSummaryUnitTests")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def make_payments(self, rows):
        return self.spark.createDataFrame(
            rows,
            PAYMENT_SCHEMA,
        )

    def valid_payments(self):
        return self.make_payments([
            payment_row(1, 100, "COD", "SUCCESS", "100.00"),
            payment_row(2, 100, "COD", "SUCCESS", "50.00"),
            payment_row(3, 101, "COD", "SUCCESS", "200.00"),
            payment_row(4, 102, "COD", "FAILED", "300.00"),
            payment_row(5, 103, "E_WALLET", "PENDING", "75.00"),
        ])

    def test_valid_source_quality(self):
        result = validate_payment_summary_sources(
            self.valid_payments()
        )
        self.assertEqual(result["status"], "PASS")

    def test_valid_output_quality(self):
        gold = build_payment_summary(self.valid_payments())
        result = validate_payment_summary_output(gold)
        self.assertEqual(result["status"], "PASS")

    def test_grouping_and_distinct_orders(self):
        gold = build_payment_summary(self.valid_payments())

        rows = {
            (row["payment_method"], row["payment_status"]): row
            for row in gold.collect()
        }

        cod_success = rows[("COD", "SUCCESS")]

        self.assertEqual(cod_success["payment_count"], 3)
        self.assertEqual(cod_success["distinct_order_count"], 2)
        self.assertEqual(
            cod_success["total_payment_amount"],
            Decimal("350.00"),
        )

    def test_all_statuses_preserved(self):
        gold = build_payment_summary(self.valid_payments())

        statuses = {
            row["payment_status"]
            for row in gold.collect()
        }

        self.assertEqual(
            statuses,
            {"SUCCESS", "FAILED", "PENDING"},
        )

    def test_financial_reconciliation(self):
        payments = self.valid_payments()
        gold = build_payment_summary(payments)

        source_total = sum(
            row["amount"]
            for row in payments.collect()
        )

        gold_total = sum(
            row["total_payment_amount"]
            for row in gold.collect()
        )

        self.assertEqual(source_total, gold_total)

    def test_duplicate_payment_id_rejected(self):
        payments = self.make_payments([
            payment_row(1, 100, "COD", "SUCCESS", "100.00"),
            payment_row(1, 101, "COD", "SUCCESS", "200.00"),
        ])

        with self.assertRaises(GoldDataQualityError):
            validate_payment_summary_sources(payments)

    def test_null_order_id_rejected(self):
        payments = self.make_payments([
            payment_row(1, None, "COD", "SUCCESS", "100.00"),
        ])

        with self.assertRaises(GoldDataQualityError):
            validate_payment_summary_sources(payments)

    def test_invalid_payment_method_rejected(self):
        payments = self.make_payments([
            payment_row(1, 100, "UNKNOWN", "SUCCESS", "100.00"),
        ])

        with self.assertRaises(GoldDataQualityError):
            validate_payment_summary_sources(payments)

    def test_invalid_payment_status_rejected(self):
        payments = self.make_payments([
            payment_row(1, 100, "COD", "UNKNOWN", "100.00"),
        ])

        with self.assertRaises(GoldDataQualityError):
            validate_payment_summary_sources(payments)

    def test_negative_amount_rejected(self):
        payments = self.make_payments([
            payment_row(1, 100, "COD", "SUCCESS", "-1.00"),
        ])

        with self.assertRaises(GoldDataQualityError):
            validate_payment_summary_sources(payments)

    def test_null_amount_rejected(self):
        payments = self.make_payments([
            payment_row(1, 100, "COD", "SUCCESS", None),
        ])

        with self.assertRaises(GoldDataQualityError):
            validate_payment_summary_sources(payments)

    def test_missing_source_column_rejected(self):
        payments = self.valid_payments().drop("amount")

        with self.assertRaises(GoldDataQualityError):
            validate_payment_summary_sources(payments)

    def test_invalid_gold_schema_rejected(self):
        gold = build_payment_summary(
            self.valid_payments()
        ).withColumn(
            "payment_count",
            __import__("pyspark").sql.functions
            .col("payment_count").cast("string"),
        )

        with self.assertRaises(GoldDataQualityError):
            validate_payment_summary_output(gold)

    def test_duplicate_gold_grain_rejected(self):
        gold = build_payment_summary(self.valid_payments())
        duplicate = gold.unionByName(gold.limit(1))

        with self.assertRaises(GoldDataQualityError):
            validate_payment_summary_output(duplicate)

    def test_invalid_gold_metrics_rejected(self):
        from pyspark.sql import functions as F

        gold = build_payment_summary(
            self.valid_payments()
        ).withColumn(
            "payment_count",
            F.lit(0).cast("long"),
        )

        with self.assertRaises(GoldDataQualityError):
            validate_payment_summary_output(gold)


if __name__ == "__main__":
    unittest.main(verbosity=2)
