WITH orders AS (

    SELECT
        order_id,
        customer_id,
        status AS order_status,
        order_date,
        _silver_batch_id,
        _loaded_at

    FROM {{ ref('stg_orders') }}

),

order_items_aggregated AS (

    SELECT
        order_id,

        COUNT(*) AS line_count,

        SUM(quantity)::BIGINT AS item_count,

        SUM(quantity * unit_price) AS gross_amount,

        SUM(discount_amount) AS discount_amount

    FROM {{ ref('stg_order_items') }}

    GROUP BY order_id

),

final AS (

    SELECT
        o.order_id,
        o.customer_id,

        TO_CHAR(
            o.order_date,
            'YYYYMMDD'
        )::INTEGER AS order_date_key,

        o.order_date,
        o.order_status,

        COALESCE(oi.line_count, 0)::BIGINT AS line_count,

        COALESCE(oi.item_count, 0)::BIGINT AS item_count,

        COALESCE(oi.gross_amount, 0) AS gross_amount,

        COALESCE(oi.discount_amount, 0) AS discount_amount,

        COALESCE(oi.gross_amount, 0)
            - COALESCE(oi.discount_amount, 0) AS net_amount,

        (
            COALESCE(oi.line_count, 0) > 0
        ) AS has_items,

        o._silver_batch_id,
        o._loaded_at

    FROM orders o

    LEFT JOIN order_items_aggregated oi
        ON o.order_id = oi.order_id

)

SELECT *
FROM final