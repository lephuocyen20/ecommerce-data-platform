from pyspark.sql import functions as F

from gold.implementation.contracts.datasets import (
    get_gold_contract,
)
from gold.implementation.quality.daily_sales import (
    GoldDataQualityError,
    require,
    has_rows,
    check_primary_key,
)


VALID_ORDER_STATUSES = {
    "PENDING",
    "CONFIRMED",
    "PROCESSING",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
}


def validate_customer_metrics_sources(
    customers_df,
    orders_df,
    order_items_df,
):
    """
    Validate Silver sources before building customer_metrics.
    """

    required_columns = {
        "customers": {
            "customer_id",
            "full_name",
        },
        "orders": {
            "order_id",
            "customer_id",
            "status",
            "order_date",
        },
        "order_items": {
            "order_item_id",
            "order_id",
            "quantity",
            "unit_price",
            "discount_amount",
        },
    }

    sources = {
        "customers": customers_df,
        "orders": orders_df,
        "order_items": order_items_df,
    }

    # 1. Required source columns
    for name, df in sources.items():
        missing = required_columns[name] - set(df.columns)

        require(
            not missing,
            f"{name}: missing columns {sorted(missing)}",
        )

    # 2. Customer primary key
    check_primary_key(
        customers_df,
        ["customer_id"],
        "customers",
    )

    # 3. Order primary key
    check_primary_key(
        orders_df,
        ["order_id"],
        "orders",
    )

    # 4. Order item primary key
    check_primary_key(
        order_items_df,
        ["order_item_id"],
        "order_items",
    )

    # 5. Customer name
    invalid_names = customers_df.filter(
        F.col("full_name").isNull()
        | (F.length(F.trim(F.col("full_name"))) == 0)
    )

    require(
        not has_rows(invalid_names),
        "customers: invalid full_name",
    )

    # 6. Order status
    invalid_statuses = orders_df.filter(
        F.col("status").isNull()
        | ~F.col("status").isin(*VALID_ORDER_STATUSES)
    )

    require(
        not has_rows(invalid_statuses),
        "orders: invalid status",
    )

    # 7. Order timestamp
    invalid_dates = orders_df.filter(
        F.col("order_date").isNull()
        | F.to_date("order_date").isNull()
    )

    require(
        not has_rows(invalid_dates),
        "orders: invalid order_date",
    )

    # 8. Orphan customer references
    orphan_customers = (
        orders_df
        .select("customer_id")
        .join(
            customers_df.select("customer_id"),
            on="customer_id",
            how="left_anti",
        )
    )

    null_customer_refs = orders_df.filter(
        F.col("customer_id").isNull()
    )

    require(
        not has_rows(null_customer_refs),
        "orders: null customer_id",
    )

    require(
        not has_rows(orphan_customers),
        "orders: orphan customer_id",
    )

    # 9. Orphan order references
    orphan_orders = (
        order_items_df
        .select("order_id")
        .join(
            orders_df.select("order_id"),
            on="order_id",
            how="left_anti",
        )
    )

    null_order_refs = order_items_df.filter(
        F.col("order_id").isNull()
    )

    require(
        not has_rows(null_order_refs),
        "order_items: null order_id",
    )

    require(
        not has_rows(orphan_orders),
        "order_items: orphan order_id",
    )

    # 10. Invalid monetary values
    invalid_items = order_items_df.filter(
        F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("unit_price").isNull()
        | (F.col("unit_price") < 0)
        | F.col("discount_amount").isNull()
        | (F.col("discount_amount") < 0)
        | (
            F.col("discount_amount")
            > F.col("quantity") * F.col("unit_price")
        )
    )

    require(
        not has_rows(invalid_items),
        "order_items: invalid quantity or monetary values",
    )

    return {
        "status": "PASS",
        "dataset": "customer_metrics",
        "checks": [
            "required_columns",
            "customer_primary_key",
            "order_primary_key",
            "order_item_primary_key",
            "customer_name",
            "order_status",
            "order_timestamp",
            "customer_references",
            "order_references",
            "monetary_values",
        ],
    }


def validate_customer_metrics_output(gold_df):
    """
    Validate customer_metrics against its Gold Contract.
    """

    contract = get_gold_contract("customer_metrics")

    expected_columns = contract["columns"]

    # 1. Exact column order and names
    require(
        gold_df.columns == list(expected_columns),
        "customer_metrics: column mismatch",
    )

    # 2. Exact Spark data types
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
        "customer_metrics: schema mismatch\n"
        f"Expected: {expected_columns}\n"
        f"Actual:   {actual_types}"
    )

    # 3. Customer grain
    check_primary_key(
        gold_df,
        ["customer_id"],
        "customer_metrics",
    )

    # 4. Customer name
    invalid_names = gold_df.filter(
        F.col("full_name").isNull()
        | (F.length(F.trim(F.col("full_name"))) == 0)
    )

    require(
        not has_rows(invalid_names),
        "customer_metrics: invalid full_name",
    )

    # 5. Null or negative metrics
    numeric_columns = [
        "order_count",
        "delivered_order_count",
        "cancelled_order_count",
        "total_order_value",
        "delivered_order_value",
    ]

    invalid_numeric = gold_df.filter(
        F.lit(False)
    )

    for column in numeric_columns:
        invalid_numeric = invalid_numeric.unionByName(
            gold_df.filter(
                F.col(column).isNull()
                | (F.col(column) < 0)
            )
        )

    require(
        not has_rows(invalid_numeric),
        "customer_metrics: invalid numeric metrics",
    )

    # 6. Business consistency
    invalid_consistency = gold_df.filter(
        (
            F.col("delivered_order_count")
            + F.col("cancelled_order_count")
            > F.col("order_count")
        )
        | (
            F.col("delivered_order_value")
            > F.col("total_order_value")
        )
        | (
            (F.col("order_count") == 0)
            & (
                (F.col("delivered_order_count") != 0)
                | (F.col("cancelled_order_count") != 0)
                | (F.col("total_order_value") != 0)
                | (F.col("delivered_order_value") != 0)
                | F.col("first_order_date").isNotNull()
                | F.col("last_order_date").isNotNull()
            )
        )
        | (
            (F.col("order_count") > 0)
            & (
                F.col("first_order_date").isNull()
                | F.col("last_order_date").isNull()
            )
        )
        | (
            F.col("first_order_date")
            > F.col("last_order_date")
        )
    )

    require(
        not has_rows(invalid_consistency),
        "customer_metrics: inconsistent metrics",
    )

    return {
        "status": "PASS",
        "dataset": "customer_metrics",
        "checks": [
            "contract_columns",
            "contract_types",
            "customer_primary_key",
            "customer_name",
            "numeric_metrics",
            "business_consistency",
        ],
    }
