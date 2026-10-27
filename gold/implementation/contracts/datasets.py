
"""
Gold Lakehouse Data Contracts v1.

Business model:
- Order Value Analytics
- CANCELLED orders are reported separately
- REFUNDED payments are grouped by status
- No automatic accounting revenue recognition
"""

from gold.lineage import GOLD_DATASET_SOURCES

GOLD_CONTRACT_VERSION = "1.0"


GOLD_DATASETS = {
    "daily_sales": {
        "grain": ["order_date"],
        "sources": list(GOLD_DATASET_SOURCES["daily_sales"]),
        "primary_key": ["order_date"],
        "columns": {
            "order_date": "date",
            "order_count": "long",
            "delivered_order_count": "long",
            "cancelled_order_count": "long",
            "gross_merchandise_value": "decimal(20,2)",
            "discount_amount": "decimal(20,2)",
            "net_order_value": "decimal(20,2)",
            "delivered_order_value": "decimal(20,2)",
        },
    },

    "product_performance": {
        "grain": ["product_id"],
        "sources": list(GOLD_DATASET_SOURCES["product_performance"]),
        "primary_key": ["product_id"],
        "columns": {
            "product_id": "long",
            "product_name": "string",
            "order_count": "long",
            "units_ordered": "long",
            "gross_merchandise_value": "decimal(20,2)",
            "discount_amount": "decimal(20,2)",
            "net_order_value": "decimal(20,2)",
            "delivered_units": "long",
            "delivered_order_value": "decimal(20,2)",
        },
    },

    "customer_metrics": {
        "grain": ["customer_id"],
        "sources": list(GOLD_DATASET_SOURCES["customer_metrics"]),
        "primary_key": ["customer_id"],
        "columns": {
            "customer_id": "long",
            "full_name": "string",
            "order_count": "long",
            "delivered_order_count": "long",
            "cancelled_order_count": "long",
            "total_order_value": "decimal(20,2)",
            "delivered_order_value": "decimal(20,2)",
            "first_order_date": "date",
            "last_order_date": "date",
        },
    },

    "payment_summary": {
        "grain": [
            "payment_method",
            "payment_status",
        ],
        "sources": list(GOLD_DATASET_SOURCES["payment_summary"]),
        "primary_key": [
            "payment_method",
            "payment_status",
        ],
        "columns": {
            "payment_method": "string",
            "payment_status": "string",
            "payment_count": "long",
            "distinct_order_count": "long",
            "total_payment_amount": "decimal(20,2)",
        },
    },
}


def get_gold_contract(dataset_name):
    """
    Return the Gold dataset contract.

    Reject unsupported dataset names.
    """
    try:
        return GOLD_DATASETS[dataset_name]
    except KeyError:
        raise ValueError(
            f"Unsupported Gold dataset: {dataset_name}"
        ) from None
