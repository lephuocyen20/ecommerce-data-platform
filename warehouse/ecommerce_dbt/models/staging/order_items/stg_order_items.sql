WITH source_data AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'order_items') }}

),

renamed AS (

    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount,

        _silver_batch_id,
        _loaded_at

    FROM source_data

)

SELECT *
FROM renamed