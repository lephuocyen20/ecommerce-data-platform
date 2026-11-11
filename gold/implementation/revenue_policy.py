"""Shared signed-revenue policy; base monetary measures remain non-negative."""

from pyspark.sql import functions as F


def signed_amount(amount_column, status_column):
    """Return one signed revenue measure for cancellations/refunds only."""
    return F.when(F.upper(F.col(status_column)).isin("CANCELLED", "DELETED", "REFUNDED"), -amount_column).otherwise(amount_column)
