WITH order_items AS (

    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount,
        _silver_batch_id,
        _loaded_at

    FROM {{ ref('stg_order_items') }}

),

orders AS (

    SELECT
        order_id,
        customer_id,
        order_date

    FROM {{ ref('stg_orders') }}

),

final AS (

    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.product_id,

        o.customer_id,

        TO_CHAR(
            o.order_date,
            'YYYYMMDD'
        )::INTEGER AS order_date_key,

        oi.quantity,
        oi.unit_price,

        (
            oi.quantity * oi.unit_price
        ) AS gross_amount,

        oi.discount_amount,

        (
            oi.quantity * oi.unit_price
            - oi.discount_amount
        ) AS net_amount,

        oi._silver_batch_id,
        oi._loaded_at

    FROM order_items oi

    INNER JOIN orders o
        ON oi.order_id = o.order_id

)

SELECT *
FROM final