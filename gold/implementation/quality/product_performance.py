from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
)

from gold.implementation.contracts.datasets import (
    get_gold_contract,
)

from gold.implementation.quality.daily_sales import (
    require,
    has_rows,
    check_primary_key,
)


MONEY_TYPE = "decimal(20,2)"

VALID_ORDER_STATUSES = (
    "PENDING",
    "CONFIRMED",
    "PROCESSING",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
)


def validate_product_performance_sources(
    products_df,
    orders_df,
    order_items_df,
):
    """
    Validate Silver sources before product performance
    transformation.
    """

    required_products = {
        "product_id",
        "product_name",
    }

    required_orders = {
        "order_id",
        "status",
    }

    required_items = {
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
    }

    for name, df, required in (
        ("products", products_df, required_products),
        ("orders", orders_df, required_orders),
        ("order_items", order_items_df, required_items),
    ):
        require(
            required.issubset(set(df.columns)),
            f"{name}: missing required columns",
        )

    check_primary_key(
        products_df,
        ["product_id"],
        "products",
    )

    check_primary_key(
        orders_df,
        ["order_id"],
        "orders",
    )

    check_primary_key(
        order_items_df,
        ["order_item_id"],
        "order_items",
    )

    require(
        not has_rows(
            products_df.filter(
                F.col("product_name").isNull()
                | (F.length(F.trim("product_name")) == 0)
            )
        ),
        "products: invalid product_name",
    )

    require(
        not has_rows(
            orders_df.filter(
                F.col("status").isNull()
                | ~F.col("status").isin(*VALID_ORDER_STATUSES)
            )
        ),
        "orders: invalid status",
    )

    orphan_orders = (
        order_items_df
        .select("order_id")
        .join(
            orders_df.select("order_id"),
            on="order_id",
            how="left_anti",
        )
    )

    require(
        not has_rows(orphan_orders),
        "order_items: orphan order_id",
    )

    orphan_products = (
        order_items_df
        .select("product_id")
        .join(
            products_df.select("product_id"),
            on="product_id",
            how="left_anti",
        )
    )

    require(
        not has_rows(orphan_products),
        "order_items: orphan product_id",
    )

    invalid_money = order_items_df.filter(
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
        not has_rows(invalid_money),
        "order_items: invalid monetary values",
    )

    return {
        "status": "PASS",
        "checks": [
            "products_primary_key",
            "orders_primary_key",
            "items_primary_key",
            "product_names",
            "order_status",
            "items_order_foreign_key",
            "items_product_foreign_key",
            "monetary_values",
        ],
    }


def validate_product_performance_output(gold_df):
    """
    Validate product_performance Gold output.

    Grain: one row per product_id.
    """

    contract = get_gold_contract("product_performance")

    require(
        gold_df.columns == list(contract["columns"]),
        "product_performance: column mismatch",
    )

    expected_types = {
        "long": LongType(),
        "string": StringType(),
        "decimal(20,2)": DecimalType(20, 2),
    }

    for field in gold_df.schema.fields:
        expected_type = expected_types[
            contract["columns"][field.name]
        ]

        require(
            field.dataType == expected_type,
            f"product_performance: invalid type for {field.name}",
        )

    check_primary_key(
        gold_df,
        contract["primary_key"],
        "product_performance",
    )

    require(
        not has_rows(
            gold_df.filter(
                F.col("product_name").isNull()
                | (F.length(F.trim("product_name")) == 0)
            )
        ),
        "product_performance: invalid product_name",
    )

    metrics = [
        "order_count",
        "units_ordered",
        "gross_merchandise_value",
        "discount_amount",
        "net_order_value",
        "delivered_units",
        "delivered_order_value",
    ]

    null_condition = None

    for column in metrics:
        condition = F.col(column).isNull()
        null_condition = (
            condition
            if null_condition is None
            else null_condition | condition
        )

    require(
        not has_rows(gold_df.filter(null_condition)),
        "product_performance: NULL metrics",
    )

    invalid_values = gold_df.filter(
        (F.col("order_count") < 0)
        | (F.col("units_ordered") < 0)
        | (F.col("delivered_units") < 0)
        | (F.col("delivered_units") > F.col("units_ordered"))
        | (F.col("gross_merchandise_value") < 0)
        | (F.col("discount_amount") < 0)
        | (F.col("net_order_value") < 0)
        | (F.col("delivered_order_value") < 0)
        | (
            F.col("delivered_order_value")
            > F.col("net_order_value")
        )
        | (
            F.col("net_order_value")
            != F.col("gross_merchandise_value")
            - F.col("discount_amount")
        )
        | (
            (F.col("order_count") == 0)
            & (
                (F.col("units_ordered") != 0)
                | (F.col("gross_merchandise_value") != 0)
                | (F.col("discount_amount") != 0)
                | (F.col("net_order_value") != 0)
                | (F.col("delivered_units") != 0)
                | (F.col("delivered_order_value") != 0)
            )
        )
    )

    require(
        not has_rows(invalid_values),
        "product_performance: invalid metric values",
    )

    return {
        "status": "PASS",
        "checks": [
            "contract_schema",
            "unique_product_grain",
            "product_names",
            "non_null_metrics",
            "metric_consistency",
        ],
    }
