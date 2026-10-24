WITH shipments AS (

    SELECT
        shipment_id,
        order_id,
        carrier,
        tracking_number,
        shipping_address,
        shipping_fee,
        status,
        shipped_at,
        delivered_at,
        estimated_delivery_at,
        created_at,
        updated_at,
        _silver_batch_id,
        _loaded_at

    FROM {{ ref('stg_shipments') }}

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
        s.shipment_id,
        s.order_id,
        o.customer_id,

        TO_CHAR(
            o.order_date, 'YYYYMMDD'
        )::INTEGER AS order_date_key,

        TO_CHAR(
            s.created_at, 'YYYYMMDD'
        )::INTEGER AS shipment_created_date_key,

        TO_CHAR(
            s.shipped_at, 'YYYYMMDD'
        )::INTEGER AS shipped_date_key,

        TO_CHAR(
            s.delivered_at, 'YYYYMMDD'
        )::INTEGER AS delivered_date_key,

        TO_CHAR(
            s.estimated_delivery_at, 'YYYYMMDD'
        )::INTEGER AS estimated_delivery_date_key,

        TO_CHAR(
            s.updated_at, 'YYYYMMDD'
        )::INTEGER AS updated_date_key,

        s.carrier,
        s.tracking_number,
        s.shipping_address,
        s.shipping_fee,

        s.status AS shipment_status,

        s.created_at AS shipment_created_at,
        s.shipped_at,
        s.delivered_at,
        s.estimated_delivery_at,
        s.updated_at,

        (s.status = 'DELIVERED') AS is_delivered,

        CASE
            WHEN s.status = 'DELIVERED'
             AND s.shipped_at IS NOT NULL
             AND s.delivered_at IS NOT NULL
            THEN ROUND(
                (
                    EXTRACT(
                        EPOCH FROM (
                            s.delivered_at - s.shipped_at
                        )
                    ) / 3600
                )::NUMERIC,
                2
            )
            ELSE NULL
        END AS delivery_duration_hours,

        CASE
            WHEN s.status = 'DELIVERED'
             AND s.delivered_at IS NOT NULL
             AND s.estimated_delivery_at IS NOT NULL
            THEN (
                s.delivered_at > s.estimated_delivery_at
            )
            ELSE NULL
        END AS is_late_delivery,

        s._silver_batch_id,
        s._loaded_at

    FROM shipments s

    INNER JOIN orders o
        ON s.order_id = o.order_id

)

SELECT *
FROM final