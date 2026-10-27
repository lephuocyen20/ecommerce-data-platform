
"""Source and output quality checks for Gold payment_summary."""

from pyspark.sql import functions as F

from gold.implementation.contracts.datasets import get_gold_contract
from gold.implementation.quality.daily_sales import (
    require,
    has_rows,
    check_primary_key,
)


VALID_PAYMENT_METHODS = {
    "COD",
    "BANK_TRANSFER",
    "CREDIT_CARD",
    "E_WALLET",
}

VALID_PAYMENT_STATUSES = {
    "PENDING",
    "SUCCESS",
    "FAILED",
    "REFUNDED",
}


def validate_payment_summary_sources(payments_df):
    """Validate Silver payments before aggregation."""

    required_columns = {
        "payment_id",
        "order_id",
        "payment_method",
        "status",
        "amount",
    }

    missing = required_columns - set(payments_df.columns)

    require(
        not missing,
        f"payments: missing columns {sorted(missing)}",
    )

    check_primary_key(
        payments_df,
        ["payment_id"],
        "payments",
    )

    require(
        not has_rows(
            payments_df.filter(F.col("order_id").isNull())
        ),
        "payments: null order_id",
    )

    invalid_methods = payments_df.filter(
        F.col("payment_method").isNull()
        | ~F.col("payment_method").isin(*VALID_PAYMENT_METHODS)
    )

    require(
        not has_rows(invalid_methods),
        "payments: invalid payment_method",
    )

    invalid_statuses = payments_df.filter(
        F.col("status").isNull()
        | ~F.col("status").isin(*VALID_PAYMENT_STATUSES)
    )

    require(
        not has_rows(invalid_statuses),
        "payments: invalid status",
    )

    amount = F.col("amount").cast("decimal(20,2)")

    invalid_amounts = payments_df.filter(
        F.col("amount").isNull()
        | amount.isNull()
        | (amount < F.lit(0))
    )

    require(
        not has_rows(invalid_amounts),
        "payments: invalid amount",
    )

    return {
        "status": "PASS",
        "dataset": "payment_summary",
        "source": "payments",
    }


def validate_payment_summary_output(gold_df):
    """Validate payment_summary against Gold Contract v1.0."""

    contract = get_gold_contract("payment_summary")
    expected_columns = contract["columns"]

    require(
        gold_df.columns == list(expected_columns.keys()),
        (
            "payment_summary: column mismatch; "
            f"actual={gold_df.columns}, "
            f"expected={list(expected_columns.keys())}"
        ),
    )

    actual_types = {
        field.name: (
            "long"
            if field.dataType.simpleString() == "bigint"
            else field.dataType.simpleString()
        )
        for field in gold_df.schema.fields
    }

    require(
        actual_types == expected_columns,
        (
            "payment_summary: schema mismatch; "
            f"actual={actual_types}, expected={expected_columns}"
        ),
    )

    check_primary_key(
        gold_df,
        contract["primary_key"],
        "payment_summary",
    )

    invalid_methods = gold_df.filter(
        F.col("payment_method").isNull()
        | ~F.col("payment_method").isin(*VALID_PAYMENT_METHODS)
    )

    require(
        not has_rows(invalid_methods),
        "payment_summary: invalid payment_method",
    )

    invalid_statuses = gold_df.filter(
        F.col("payment_status").isNull()
        | ~F.col("payment_status").isin(*VALID_PAYMENT_STATUSES)
    )

    require(
        not has_rows(invalid_statuses),
        "payment_summary: invalid payment_status",
    )

    invalid_metrics = gold_df.filter(
        F.col("payment_count").isNull()
        | (F.col("payment_count") <= 0)
        | F.col("distinct_order_count").isNull()
        | (F.col("distinct_order_count") <= 0)
        | (
            F.col("distinct_order_count")
            > F.col("payment_count")
        )
        | F.col("total_payment_amount").isNull()
        | (F.col("total_payment_amount") < 0)
    )

    require(
        not has_rows(invalid_metrics),
        "payment_summary: invalid metrics",
    )

    return {
        "status": "PASS",
        "dataset": "payment_summary",
    }
