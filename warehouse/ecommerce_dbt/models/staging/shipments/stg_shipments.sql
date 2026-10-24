WITH source_data AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'shipments') }}

),

renamed AS (

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

    FROM source_data

)

SELECT *
FROM renamed