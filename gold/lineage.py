"""Canonical Silver-source lineage for every Gold dataset."""

GOLD_DATASET_SOURCES = {
    "daily_sales": ("orders", "order_items"),
    "product_performance": ("products", "orders", "order_items"),
    "customer_metrics": ("customers", "orders", "order_items"),
    "payment_summary": ("payments",),
}


def get_gold_dataset_sources(dataset_name):
    """Return the ordered Silver source tables required by one Gold dataset."""
    try:
        return GOLD_DATASET_SOURCES[dataset_name]
    except KeyError:
        raise ValueError(f"Unsupported Gold dataset: {dataset_name}") from None
